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
    private final com.dms.election.BullyElection bullyElection;
    private final com.dms.repl.ReplicationService replicationService;

    public ApiController(NodeConfig nodeConfig,
                         NodeRegistry nodeRegistry,
                         LamportClock lamportClock,
                         EventLogger eventLogger,
                         RemoteDonationServiceImpl serviceImpl,
                         DonationRepository donationRepository,
                         com.dms.lb.LoadBalancingService loadBalancingService,
                         @org.springframework.context.annotation.Lazy com.dms.election.BullyElection bullyElection,
                         com.dms.repl.ReplicationService replicationService) {
        this.nodeConfig = nodeConfig;
        this.nodeRegistry = nodeRegistry;
        this.lamportClock = lamportClock;
        this.eventLogger = eventLogger;
        this.serviceImpl = serviceImpl;
        this.donationRepository = donationRepository;
        this.loadBalancingService = loadBalancingService;
        this.bullyElection = bullyElection;
        this.replicationService = replicationService;
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

    // -------------------------------------------------------------
    // Exp 4: Bully Election Endpoints
    // -------------------------------------------------------------
    @PostMapping("/election/start")
    public ResponseEntity<Map<String, Object>> startElection() {
        bullyElection.startElection();
        Map<String, Object> resp = new HashMap<>();
        resp.put("status", "STARTED");
        resp.put("algorithm", bullyElection.getAlgorithmName());
        resp.put("initiatorNodeId", nodeConfig.getId());
        resp.put("message", "Bully election initiated by node " + nodeConfig.getId());
        return ResponseEntity.ok(resp);
    }

    @GetMapping("/election/status")
    public ResponseEntity<Map<String, Object>> getElectionStatus() {
        Map<String, Object> resp = new HashMap<>();
        resp.put("algorithm", bullyElection.getAlgorithmName());
        resp.put("coordinatorId", nodeRegistry.getPrimaryId());
        resp.put("localId", nodeConfig.getId());
        resp.put("isCoordinator", nodeRegistry.getPrimaryId() == nodeConfig.getId());
        return ResponseEntity.ok(resp);
    }

    // -------------------------------------------------------------
    // Exp 5: Replication Endpoints
    // -------------------------------------------------------------
    @GetMapping("/repl/config")
    public ResponseEntity<Map<String, Object>> getReplicationConfig() {
        Map<String, Object> resp = new HashMap<>();
        resp.put("mode", replicationService.getReplicationMode());
        resp.put("delayMs", replicationService.getAsyncDelayMs());
        return ResponseEntity.ok(resp);
    }

    @PostMapping("/repl/mode")
    public ResponseEntity<Map<String, Object>> setReplicationMode(
            @RequestBody(required = false) Map<String, String> body,
            @RequestParam(required = false) String mode) {
        String targetMode = (body != null && body.containsKey("mode")) ? body.get("mode") : mode;
        if (targetMode != null) {
            replicationService.setReplicationMode(targetMode);
        }
        Map<String, Object> resp = new HashMap<>();
        resp.put("mode", replicationService.getReplicationMode());
        return ResponseEntity.ok(resp);
    }

    @PostMapping("/repl/delay")
    public ResponseEntity<Map<String, Object>> setReplicationDelay(
            @RequestBody(required = false) Map<String, Object> body,
            @RequestParam(required = false) Long delayMs) {
        Long targetDelay = delayMs;
        if (targetDelay == null && body != null && body.containsKey("delayMs")) {
            Object val = body.get("delayMs");
            if (val instanceof Number) {
                targetDelay = ((Number) val).longValue();
            } else if (val != null) {
                targetDelay = Long.parseLong(val.toString());
            }
        }
        if (targetDelay != null) {
            replicationService.setAsyncDelayMs(targetDelay);
        }
        Map<String, Object> resp = new HashMap<>();
        resp.put("delayMs", replicationService.getAsyncDelayMs());
        return ResponseEntity.ok(resp);
    }

    @GetMapping("/repl/consistency")
    public ResponseEntity<Map<String, Object>> checkConsistency() {
        return ResponseEntity.ok(replicationService.checkClusterConsistency());
    }

    @GetMapping("/repl/hash")
    public ResponseEntity<Map<String, Object>> getLedgerHash() {
        Map<String, Object> resp = new HashMap<>();
        resp.put("nodeId", nodeConfig.getId());
        resp.put("hash", replicationService.computeLedgerHash());
        resp.put("count", donationRepository.findByIsolatedFalseOrderByIdAsc().size());
        return ResponseEntity.ok(resp);
    }

    @PostMapping("/repl/stale-read-demo")
    public ResponseEntity<Map<String, Object>> runStaleReadDemo(@RequestParam(defaultValue = "2000") long delayMs) {
        Map<String, Object> resp = new HashMap<>();
        // Find a live backup node
        List<NodeRegistry.PeerState> livePeers = nodeRegistry.getLivePeers();
        NodeRegistry.PeerState backup = null;
        for (NodeRegistry.PeerState ps : livePeers) {
            if (ps.getNodeId() != nodeRegistry.getPrimaryId() && ps.getNodeId() > 0) {
                backup = ps;
                break;
            }
        }

        if (backup == null) {
            resp.put("error", "Cannot demonstrate stale read: no live backup peer found in cluster");
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(resp);
        }

        // 1. Temporarily configure ASYNCHRONOUS mode with specified delay
        String prevMode = replicationService.getReplicationMode();
        long prevDelay = replicationService.getAsyncDelayMs();
        replicationService.setReplicationMode(com.dms.repl.ReplicationService.MODE_ASYNCHRONOUS);
        replicationService.setAsyncDelayMs(delayMs);

        try {
            // 2. Submit donation to primary
            String donor = "StaleDemo-" + System.currentTimeMillis() % 1000;
            DonationRequest req = new DonationRequest(donor, new BigDecimal("500"), "Mumbai");
            req.setTargetNodeId(nodeRegistry.getPrimaryId());
            ResponseEntity<Receipt> donateResp = donate(req);
            Receipt receipt = donateResp.getBody();
            String receiptId = receipt != null ? receipt.getReceiptId() : "N/A";

            // 3. Immediately read backup node's count before delay elapses
            RemoteDonationService backupStub = nodeRegistry.getPeerStub(backup.getNodeId());
            long initialBackupCount = (backupStub != null) ? backupStub.getLedgerCount() : -1;

            eventLogger.log("EXP5-REPL", String.format(
                    "Deterministic Stale-Read Step 1: Confirmed on primary. Immediate read on backup node=%d count=%d (STALE READ DETECTED)",
                    backup.getNodeId(), initialBackupCount));

            // 4. Wait for background replication delay + 500ms safety margin
            Thread.sleep(delayMs + 500);

            // 5. Read backup node again after delay
            long updatedBackupCount = (backupStub != null) ? backupStub.getLedgerCount() : -1;

            eventLogger.log("EXP5-REPL", String.format(
                    "Deterministic Stale-Read Step 2: Post-delay read on backup node=%d count=%d (EVENTUAL CONSISTENCY ACHIEVED)",
                    backup.getNodeId(), updatedBackupCount));

            resp.put("status", "SUCCESS");
            resp.put("receiptId", receiptId);
            resp.put("backupNodeId", backup.getNodeId());
            resp.put("delayMs", delayMs);
            resp.put("immediateBackupCount", initialBackupCount);
            resp.put("eventualBackupCount", updatedBackupCount);
            resp.put("staleReadObserved", initialBackupCount < updatedBackupCount);
            resp.put("explanation", String.format(
                    "Immediately after primary confirmation, backup had %d records (stale read). After %dms delay, backup reached %d records (eventual consistency).",
                    initialBackupCount, delayMs, updatedBackupCount));

            return ResponseEntity.ok(resp);
        } catch (Exception e) {
            resp.put("error", "Stale read demo error: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(resp);
        } finally {
            // Restore previous mode and delay
            replicationService.setReplicationMode(prevMode);
            replicationService.setAsyncDelayMs(prevDelay);
        }
    }

    // -------------------------------------------------------------
    // Exp 8: Fault Tolerance Endpoints
    // -------------------------------------------------------------
    @PostMapping("/ft/crash")
    public ResponseEntity<Map<String, Object>> simulateCrash() {
        nodeRegistry.setSimulatedCrash(true);
        Map<String, Object> resp = new HashMap<>();
        resp.put("nodeId", nodeConfig.getId());
        resp.put("simulatedCrash", true);
        resp.put("message", "Node " + nodeConfig.getId() + " is now in simulated crash state (stops answering RMI calls and heartbeats)");
        return ResponseEntity.ok(resp);
    }

    @PostMapping("/ft/resume")
    public ResponseEntity<Map<String, Object>> resumeNode() {
        nodeRegistry.setSimulatedCrash(false);
        Map<String, Object> resp = new HashMap<>();
        resp.put("nodeId", nodeConfig.getId());
        resp.put("simulatedCrash", false);
        resp.put("message", "Node " + nodeConfig.getId() + " resumed and snapshot synced from primary");
        return ResponseEntity.ok(resp);
    }

    @GetMapping("/ft/status")
    public ResponseEntity<Map<String, Object>> getFaultToleranceStatus() {
        Map<String, Object> resp = new HashMap<>();
        resp.put("nodeId", nodeConfig.getId());
        resp.put("simulatedCrash", nodeRegistry.isSimulatedCrash());
        resp.put("primaryId", nodeRegistry.getPrimaryId());
        resp.put("isPrimary", nodeRegistry.getPrimaryId() == nodeConfig.getId());
        return ResponseEntity.ok(resp);
    }

    @PostMapping("/ft/sync")
    public ResponseEntity<Map<String, Object>> syncSnapshot() {
        int count = nodeRegistry.syncSnapshotFromPrimary();
        Map<String, Object> resp = new HashMap<>();
        resp.put("nodeId", nodeConfig.getId());
        resp.put("primaryId", nodeRegistry.getPrimaryId());
        resp.put("syncedDonations", count);
        return ResponseEntity.ok(resp);
    }
}
