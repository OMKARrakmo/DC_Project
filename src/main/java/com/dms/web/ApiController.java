package com.dms.web;

import com.dms.core.*;
import com.dms.rmi.RemoteDonationService;
import com.dms.rmi.RemoteDonationServiceImpl;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class ApiController {

    private final NodeConfig nodeConfig;
    private final NodeRegistry nodeRegistry;
    private final LamportClock lamportClock;
    private final EventLogger eventLogger;
    private final RemoteDonationServiceImpl serviceImpl;
    private final DonationRepository donationRepository;

    public ApiController(NodeConfig nodeConfig,
                         NodeRegistry nodeRegistry,
                         LamportClock lamportClock,
                         EventLogger eventLogger,
                         RemoteDonationServiceImpl serviceImpl,
                         DonationRepository donationRepository) {
        this.nodeConfig = nodeConfig;
        this.nodeRegistry = nodeRegistry;
        this.lamportClock = lamportClock;
        this.eventLogger = eventLogger;
        this.serviceImpl = serviceImpl;
        this.donationRepository = donationRepository;
    }

    @PostMapping("/donate")
    public ResponseEntity<Receipt> donate(@RequestBody DonationRequest request) {
        long currentClock = lamportClock.tick();
        request.setClientLamportTs(currentClock);

        Integer targetId = request.getTargetNodeId();

        // If target is not specified, check if any peer is available; default to local
        if (targetId == null) {
            targetId = nodeConfig.getId();
        }

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
