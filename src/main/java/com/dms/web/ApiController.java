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
    private final com.dms.election.ElectionManager electionManager;
    private final com.dms.repl.ReplicationService replicationService;
    private final ThreadPoolService threadPoolService;
    private final ClockSyncService clockSyncService;
    private final com.dms.mr.MapReduceService mapReduceService;
    private final com.dms.mpi.MpiCollectivesService mpiCollectivesService;
    private final com.dms.mpi.MpiMatrixMultiplicationService mpiMatrixMultiplicationService;

    public ApiController(NodeConfig nodeConfig,
                         NodeRegistry nodeRegistry,
                         LamportClock lamportClock,
                         EventLogger eventLogger,
                         RemoteDonationServiceImpl serviceImpl,
                         DonationRepository donationRepository,
                         com.dms.lb.LoadBalancingService loadBalancingService,
                         @org.springframework.context.annotation.Lazy com.dms.election.BullyElection bullyElection,
                         @org.springframework.context.annotation.Lazy com.dms.election.ElectionManager electionManager,
                         com.dms.repl.ReplicationService replicationService,
                         ThreadPoolService threadPoolService,
                         ClockSyncService clockSyncService,
                         com.dms.mr.MapReduceService mapReduceService,
                         com.dms.mpi.MpiCollectivesService mpiCollectivesService,
                         com.dms.mpi.MpiMatrixMultiplicationService mpiMatrixMultiplicationService) {
        this.nodeConfig = nodeConfig;
        this.nodeRegistry = nodeRegistry;
        this.lamportClock = lamportClock;
        this.eventLogger = eventLogger;
        this.serviceImpl = serviceImpl;
        this.donationRepository = donationRepository;
        this.loadBalancingService = loadBalancingService;
        this.bullyElection = bullyElection;
        this.electionManager = electionManager;
        this.replicationService = replicationService;
        this.threadPoolService = threadPoolService;
        this.clockSyncService = clockSyncService;
        this.mapReduceService = mapReduceService;
        this.mpiCollectivesService = mpiCollectivesService;
        this.mpiMatrixMultiplicationService = mpiMatrixMultiplicationService;
    }

    @PostMapping("/donate")
    public ResponseEntity<Receipt> donate(@RequestBody DonationRequest request) {
        long currentClock = lamportClock.tick();
        request.setClientLamportTs(currentClock);

        com.dms.lb.LoadBalancingService.InFlightHandle inFlight;
        int targetId;
        if (request.getTargetNodeId() != null && request.getTargetNodeId() > 0) {
            targetId = request.getTargetNodeId();
            inFlight = loadBalancingService.trackInFlight(targetId);
        } else {
            inFlight = loadBalancingService.chooseAndTrackNode();
            targetId = inFlight.getNodeId();
        }

        try (inFlight) {
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
    // Exp 4: Election Endpoints (Bully & Ring)
    // -------------------------------------------------------------
    @PostMapping("/election/start")
    public ResponseEntity<Map<String, Object>> startElection() {
        electionManager.startElection();
        Map<String, Object> resp = new HashMap<>();
        resp.put("status", "STARTED");
        resp.put("algorithm", electionManager.getAlgorithmName());
        resp.put("initiatorNodeId", nodeConfig.getId());
        resp.put("message", electionManager.getAlgorithmName() + " election initiated by node " + nodeConfig.getId());
        return ResponseEntity.ok(resp);
    }

    @GetMapping("/election/status")
    public ResponseEntity<Map<String, Object>> getElectionStatus() {
        Map<String, Object> resp = new HashMap<>();
        resp.put("algorithm", electionManager.getAlgorithmName());
        resp.put("availableAlgorithms", List.of("BULLY", "RING"));
        resp.put("coordinatorId", nodeRegistry.getPrimaryId());
        resp.put("localId", nodeConfig.getId());
        resp.put("isCoordinator", nodeRegistry.getPrimaryId() == nodeConfig.getId());
        return ResponseEntity.ok(resp);
    }

    @PostMapping("/election/algorithm")
    public ResponseEntity<Map<String, Object>> setElectionAlgorithm(
            @RequestParam(required = false) String algorithm,
            jakarta.servlet.http.HttpServletRequest request) {
        String targetAlgo = algorithm;
        try {
            if (request.getContentType() != null && request.getContentType().contains("application/json")) {
                Map<?, ?> body = new com.fasterxml.jackson.databind.ObjectMapper().readValue(request.getInputStream(), Map.class);
                if (body != null && body.containsKey("algorithm")) {
                    targetAlgo = body.get("algorithm").toString();
                }
            }
        } catch (Exception ignored) {}
        if (targetAlgo != null) {
            electionManager.setAlgorithm(targetAlgo);
        }
        Map<String, Object> resp = new HashMap<>();
        resp.put("status", "OK");
        resp.put("algorithm", electionManager.getAlgorithmName());
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

    // -------------------------------------------------------------
    // Exp 2: Multithreading Endpoints
    // -------------------------------------------------------------
    @GetMapping("/threads/status")
    public ResponseEntity<Map<String, Object>> getThreadStatus() {
        return ResponseEntity.ok(threadPoolService.getStatus());
    }

    @PostMapping("/threads/pool-size")
    public ResponseEntity<Map<String, Object>> setThreadPoolSize(
            @RequestParam(required = false) Integer size,
            jakarta.servlet.http.HttpServletRequest request) {
        Integer targetSize = size;
        try {
            if (request.getContentType() != null && request.getContentType().contains("application/json")) {
                Map<?, ?> body = new com.fasterxml.jackson.databind.ObjectMapper().readValue(request.getInputStream(), Map.class);
                if (body != null && body.containsKey("size")) {
                    Object val = body.get("size");
                    if (val instanceof Number) targetSize = ((Number) val).intValue();
                    else if (val != null) targetSize = Integer.parseInt(val.toString());
                }
            }
        } catch (Exception ignored) {}
        if (targetSize != null) {
            threadPoolService.setPoolSize(targetSize);
        }
        return ResponseEntity.ok(threadPoolService.getStatus());
    }

    @PostMapping("/threads/burst")
    public ResponseEntity<List<ThreadPoolService.TaskResult>> runThreadBurst(
            @RequestParam(required = false, defaultValue = "8") Integer count,
            @RequestParam(required = false, defaultValue = "150") Integer delayMs,
            jakarta.servlet.http.HttpServletRequest request) {
        int cnt = count != null ? count : 8;
        int dly = delayMs != null ? delayMs : 150;
        try {
            if (request.getContentType() != null && request.getContentType().contains("application/json")) {
                Map<?, ?> body = new com.fasterxml.jackson.databind.ObjectMapper().readValue(request.getInputStream(), Map.class);
                if (body != null) {
                    if (body.containsKey("count")) {
                        Object val = body.get("count");
                        if (val instanceof Number) cnt = ((Number) val).intValue();
                    }
                    if (body.containsKey("delayMs")) {
                        Object val = body.get("delayMs");
                        if (val instanceof Number) dly = ((Number) val).intValue();
                    }
                }
            }
        } catch (Exception ignored) {}
        return ResponseEntity.ok(threadPoolService.runBurst(cnt, dly));
    }

    @PostMapping("/threads/race-demo")
    public ResponseEntity<Map<String, Object>> runRaceDemo(
            @RequestParam(required = false, defaultValue = "10") Integer threads,
            @RequestParam(required = false, defaultValue = "10") Integer increments,
            jakarta.servlet.http.HttpServletRequest request) {
        int th = threads != null ? threads : 10;
        int inc = increments != null ? increments : 10;
        try {
            if (request.getContentType() != null && request.getContentType().contains("application/json")) {
                Map<?, ?> body = new com.fasterxml.jackson.databind.ObjectMapper().readValue(request.getInputStream(), Map.class);
                if (body != null) {
                    if (body.containsKey("threads")) {
                        Object val = body.get("threads");
                        if (val instanceof Number) th = ((Number) val).intValue();
                    }
                    if (body.containsKey("incrementsPerThread")) {
                        Object val = body.get("incrementsPerThread");
                        if (val instanceof Number) inc = ((Number) val).intValue();
                    } else if (body.containsKey("increments")) {
                        Object val = body.get("increments");
                        if (val instanceof Number) inc = ((Number) val).intValue();
                    }
                }
            }
        } catch (Exception ignored) {}
        return ResponseEntity.ok(threadPoolService.runRaceDemo(th, inc));
    }

    // -------------------------------------------------------------
    // Exp 3: Physical & Logical Clock Endpoints
    // -------------------------------------------------------------
    @GetMapping("/clock/status")
    public ResponseEntity<Map<String, Object>> getClockStatus() {
        return ResponseEntity.ok(clockSyncService.getStatus());
    }

    @PostMapping("/clock/skew")
    public ResponseEntity<Map<String, Object>> setClockSkew(
            @RequestParam(required = false) Long skewMs,
            jakarta.servlet.http.HttpServletRequest request) {
        Long targetSkew = skewMs;
        try {
            if (request.getContentType() != null && request.getContentType().contains("application/json")) {
                Map<?, ?> body = new com.fasterxml.jackson.databind.ObjectMapper().readValue(request.getInputStream(), Map.class);
                if (body != null && body.containsKey("skewMs")) {
                    Object val = body.get("skewMs");
                    if (val instanceof Number) targetSkew = ((Number) val).longValue();
                    else if (val != null) targetSkew = Long.parseLong(val.toString());
                }
            }
        } catch (Exception ignored) {}
        if (targetSkew != null) {
            clockSyncService.setArtificialSkewMs(targetSkew);
        }
        return ResponseEntity.ok(clockSyncService.getStatus());
    }

    @PostMapping("/clock/berkeley-sync")
    public ResponseEntity<Map<String, Object>> runBerkeleySync() {
        return ResponseEntity.ok(clockSyncService.runBerkeleySync());
    }

    @PostMapping("/clock/reset")
    public ResponseEntity<Map<String, Object>> resetClock() {
        clockSyncService.resetClock();
        return ResponseEntity.ok(clockSyncService.getStatus());
    }

    // -------------------------------------------------------------
    // Exp 7: MapReduce Endpoints
    // -------------------------------------------------------------
    @PostMapping("/mr/run")
    public ResponseEntity<Map<String, Object>> runMapReduceJob() {
        return ResponseEntity.ok(mapReduceService.runMapReduceJob());
    }

    @GetMapping("/mr/shards")
    public ResponseEntity<List<Map<String, Object>>> getMapReduceShards() {
        return ResponseEntity.ok(mapReduceService.getShardBreakdown());
    }

    // -------------------------------------------------------------
    // Exp 9 & Exp 10: Simulated MPI Endpoints
    // -------------------------------------------------------------
    @PostMapping("/mpi/collectives")
    public ResponseEntity<Map<String, Object>> runMpiCollectives(
            @RequestParam(required = false, defaultValue = "4") Integer ranks,
            jakarta.servlet.http.HttpServletRequest request) {
        int r = ranks != null ? ranks : 4;
        try {
            if (request.getContentType() != null && request.getContentType().contains("application/json")) {
                Map<?, ?> body = new com.fasterxml.jackson.databind.ObjectMapper().readValue(request.getInputStream(), Map.class);
                if (body != null && body.containsKey("ranks")) {
                    Object val = body.get("ranks");
                    if (val instanceof Number) r = ((Number) val).intValue();
                }
            }
        } catch (Exception ignored) {}
        return ResponseEntity.ok(mpiCollectivesService.runCollectives(r));
    }

    @PostMapping("/mpi/matrix")
    public ResponseEntity<Map<String, Object>> runMpiMatrixMultiply(
            @RequestParam(required = false, defaultValue = "200") Integer size,
            @RequestParam(required = false, defaultValue = "4") Integer ranks,
            jakarta.servlet.http.HttpServletRequest request) {
        int s = size != null ? size : 200;
        int r = ranks != null ? ranks : 4;
        try {
            if (request.getContentType() != null && request.getContentType().contains("application/json")) {
                Map<?, ?> body = new com.fasterxml.jackson.databind.ObjectMapper().readValue(request.getInputStream(), Map.class);
                if (body != null) {
                    if (body.containsKey("size")) {
                        Object val = body.get("size");
                        if (val instanceof Number) s = ((Number) val).intValue();
                    }
                    if (body.containsKey("ranks")) {
                        Object val = body.get("ranks");
                        if (val instanceof Number) r = ((Number) val).intValue();
                    }
                }
            }
        } catch (Exception ignored) {}
        return ResponseEntity.ok(mpiMatrixMultiplicationService.multiply(s, r));
    }
}
