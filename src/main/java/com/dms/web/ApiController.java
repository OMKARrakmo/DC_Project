package com.dms.web;

import com.dms.core.*;
import com.dms.rmi.RemoteDonationService;
import com.dms.rmi.RemoteDonationServiceImpl;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.*;

@RestController
@RequestMapping("/api")
public class ApiController {

    private final NodeConfig nodeConfig;
    private final NodeRegistry nodeRegistry;
    private final LamportClock lamportClock;
    private final EventLogger eventLogger;
    private final RemoteDonationServiceImpl serviceImpl;
    private final DonationRepository donationRepository;
    private final com.dms.lb.LoadBalancingService loadBalancingService;

    public ApiController(NodeConfig nodeConfig,
                         NodeRegistry nodeRegistry,
                         LamportClock lamportClock,
                         EventLogger eventLogger,
                         RemoteDonationServiceImpl serviceImpl,
                         DonationRepository donationRepository,
                         com.dms.lb.LoadBalancingService loadBalancingService) {
        this.nodeConfig = nodeConfig;
        this.nodeRegistry = nodeRegistry;
        this.lamportClock = lamportClock;
        this.eventLogger = eventLogger;
        this.serviceImpl = serviceImpl;
        this.donationRepository = donationRepository;
        this.loadBalancingService = loadBalancingService;
    }

    @PostMapping("/donate")
    public ResponseEntity<Receipt> donate(@RequestBody DonationRequest request) {
        long currentClock = lamportClock.tick();
        request.setClientLamportTs(currentClock);

        Integer targetId = request.getTargetNodeId();

        // If target is not specified, use load balancer to select node
        if (targetId == null || targetId <= 0) {
            targetId = loadBalancingService.chooseNode();
        }

        try (com.dms.lb.LoadBalancingService.InFlightHandle inFlight = loadBalancingService.trackInFlight(targetId)) {
            if (targetId != nodeConfig.getId()) {
                // Forward via RMI to target peer
                RemoteDonationService peerStub = nodeRegistry.getPeerStub(targetId);
                if (peerStub == null) {
                    eventLogger.log("EXP1-RMI", "Failed to route to target node=" + targetId + ": Node unavailable");
                    Receipt failure = new Receipt(
                            targetId,
                            "unknown",
                            System.getProperty("os.name"),
                            Thread.currentThread().getName(),
                            lamportClock.tick(),
                            nodeRegistry.getPrimaryId(),
                            "FAILED",
                            "Target node " + targetId + " is not reachable via RMI"
                    );
                    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(failure);
                }

                try {
                    eventLogger.log("EXP1-RMI", String.format(
                            "Gateway node=%d forwarding donation for %s (%s) to node=%d via Java RMI",
                            nodeConfig.getId(), request.getDonor(), request.getAmount(), targetId));

                    Receipt receipt = peerStub.process(request);

                    // Update Lamport clock on return
                    lamportClock.update(receipt.getLamportTs());

                    eventLogger.logWithLamport("EXP1-RMI", receipt.getLamportTs(), String.format(
                            "Gateway node=%d received RMI receipt from node=%d (status=%s, thread=%s, host=%s)",
                            nodeConfig.getId(), receipt.getProcessingNodeId(), receipt.getStatus(),
                            receipt.getThreadName(), receipt.getHost()));

                    return ResponseEntity.ok(receipt);
                } catch (Exception e) {
                    eventLogger.log("EXP1-RMI", "RMI call to node=" + targetId + " failed: " + e.getMessage());
                    Receipt failure = new Receipt(
                            targetId,
                            "error",
                            System.getProperty("os.name"),
                            Thread.currentThread().getName(),
                            lamportClock.tick(),
                            nodeRegistry.getPrimaryId(),
                            "FAILED",
                            "RMI error calling node " + targetId + ": " + e.getMessage()
                    );
                    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(failure);
                }
            } else {
                // Process locally
                try {
                    Receipt receipt = serviceImpl.process(request);
                    return ResponseEntity.ok(receipt);
                } catch (Exception e) {
                    Receipt failure = new Receipt(
                            nodeConfig.getId(),
                            nodeConfig.getHost(),
                            System.getProperty("os.name"),
                            Thread.currentThread().getName(),
                            lamportClock.tick(),
                            nodeRegistry.getPrimaryId(),
                            "FAILED",
                            "Local error: " + e.getMessage()
                    );
                    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(failure);
                }
            }
        }
    }

    @GetMapping("/lb/config")
    public ResponseEntity<Map<String, Object>> getLbConfig() {
        Map<String, Object> resp = new HashMap<>();
        resp.put("strategy", loadBalancingService.getStrategy());
        resp.put("artificialDelayMs", loadBalancingService.getArtificialDelayMs());
        resp.put("activeConnections", loadBalancingService.getActiveConnectionsSnapshot());
        return ResponseEntity.ok(resp);
    }

    @PostMapping("/lb/strategy")
    public ResponseEntity<Map<String, Object>> setLbStrategy(@RequestBody Map<String, String> body) {
        String newStrategy = body.getOrDefault("strategy", com.dms.lb.LoadBalancingService.STRATEGY_LEAST_CONNECTIONS);
        loadBalancingService.setStrategy(newStrategy);
        Map<String, Object> resp = new HashMap<>();
        resp.put("strategy", loadBalancingService.getStrategy());
        return ResponseEntity.ok(resp);
    }

    @PostMapping("/lb/delay")
    public ResponseEntity<Map<String, Object>> setLbDelay(@RequestBody Map<String, Object> body) {
        Object val = body.get("delayMs");
        long delay = 0;
        if (val instanceof Number) {
            delay = ((Number) val).longValue();
        } else if (val instanceof String) {
            try {
                delay = Long.parseLong((String) val);
            } catch (NumberFormatException ignored) {}
        }
        loadBalancingService.setArtificialDelayMs(delay);
        Map<String, Object> resp = new HashMap<>();
        resp.put("artificialDelayMs", loadBalancingService.getArtificialDelayMs());
        return ResponseEntity.ok(resp);
    }

    @PostMapping("/lb/burst")
    public ResponseEntity<Map<String, Object>> burstRequests(@RequestBody Map<String, Object> body) {
        int count = 12;
        if (body.containsKey("count")) {
            count = Math.min(100, Math.max(1, ((Number) body.get("count")).intValue()));
        }

        if (body.containsKey("strategy")) {
            loadBalancingService.setStrategy((String) body.get("strategy"));
        }
        if (body.containsKey("delayMs")) {
            long delay = ((Number) body.get("delayMs")).longValue();
            loadBalancingService.setArtificialDelayMs(delay);
        }

        eventLogger.log("EXP6-LB", String.format(
                "Starting burst of %d requests (strategy=%s, delay=%dms)",
                count, loadBalancingService.getStrategy(), loadBalancingService.getArtificialDelayMs()));

        ExecutorService executor = Executors.newFixedThreadPool(Math.min(count, 32));
        List<Future<Map<String, Object>>> futures = new ArrayList<>();

        for (int i = 1; i <= count; i++) {
            final int index = i;
            futures.add(executor.submit(() -> {
                long start = System.currentTimeMillis();
                DonationRequest req = new DonationRequest(
                        "BurstDonor-" + index,
                        new BigDecimal("100.00"),
                        (index % 3 == 0) ? "Mumbai" : ((index % 3 == 1) ? "Pune" : "Chennai"),
                        true, // isolated = true per SPEC Section 3
                        0L,
                        null
                );
                req.setDelayMs((int) loadBalancingService.getArtificialDelayMs());

                ResponseEntity<Receipt> response = donate(req);
                long elapsed = System.currentTimeMillis() - start;

                Map<String, Object> item = new HashMap<>();
                item.put("index", index);
                item.put("receipt", response.getBody());
                item.put("latencyMs", elapsed);
                return item;
            }));
        }

        executor.shutdown();

        List<Map<String, Object>> results = new ArrayList<>();
        Map<Integer, Integer> distribution = new TreeMap<>();

        for (Future<Map<String, Object>> f : futures) {
            try {
                Map<String, Object> res = f.get();
                results.add(res);
                Receipt r = (Receipt) res.get("receipt");
                if (r != null) {
                    distribution.put(r.getProcessingNodeId(),
                            distribution.getOrDefault(r.getProcessingNodeId(), 0) + 1);
                }
            } catch (Exception e) {
                eventLogger.log("EXP6-LB", "Burst request execution error: " + e.getMessage());
            }
        }

        // Sort results by index
        results.sort(Comparator.comparingInt(m -> (int) m.get("index")));

        eventLogger.log("EXP6-LB", String.format(
                "Burst completed: %d requests handled across nodes %s", count, distribution));

        Map<String, Object> response = new HashMap<>();
        response.put("strategy", loadBalancingService.getStrategy());
        response.put("count", count);
        response.put("distribution", distribution);
        response.put("results", results);

        return ResponseEntity.ok(response);
    }

    @GetMapping("/cluster")
    public ResponseEntity<Map<String, Object>> getCluster() {
        Map<String, Object> response = new HashMap<>();
        response.put("localNode", nodeRegistry.getLocalNodeInfo());
        response.put("nodes", nodeRegistry.getAllNodes());
        response.put("coordinatorId", nodeRegistry.getPrimaryId());
        return ResponseEntity.ok(response);
    }

    @GetMapping("/ledger")
    public ResponseEntity<List<Donation>> getLedger() {
        return ResponseEntity.ok(donationRepository.findByIsolatedFalseOrderByIdAsc());
    }

    @GetMapping("/branch-total")
    public ResponseEntity<Map<String, Object>> getBranchTotal(@RequestParam String branch,
                                                              @RequestParam(required = false) Integer targetNodeId) {
        Map<String, Object> resp = new HashMap<>();
        resp.put("branch", branch);

        if (targetNodeId != null && targetNodeId != nodeConfig.getId()) {
            RemoteDonationService peerStub = nodeRegistry.getPeerStub(targetNodeId);
            if (peerStub == null) {
                resp.put("error", "Target node " + targetNodeId + " is unavailable");
                return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(resp);
            }
            try {
                eventLogger.log("EXP1-RMI", String.format(
                        "Gateway node=%d querying branchTotal(%s) on node=%d via RMI",
                        nodeConfig.getId(), branch, targetNodeId));
                BigDecimal total = peerStub.branchTotal(branch);
                resp.put("total", total);
                resp.put("targetNodeId", targetNodeId);
                resp.put("queryingNodeId", nodeConfig.getId());
                resp.put("callPath", String.format("Node %d -> Node %d [Java RMI]", nodeConfig.getId(), targetNodeId));
                return ResponseEntity.ok(resp);
            } catch (Exception e) {
                resp.put("error", "RMI query failed: " + e.getMessage());
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(resp);
            }
        } else {
            try {
                BigDecimal total = serviceImpl.branchTotal(branch);
                resp.put("total", total);
                resp.put("targetNodeId", nodeConfig.getId());
                resp.put("queryingNodeId", nodeConfig.getId());
                resp.put("callPath", "Local Node " + nodeConfig.getId());
                return ResponseEntity.ok(resp);
            } catch (Exception e) {
                resp.put("error", e.getMessage());
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(resp);
            }
        }
    }
}
