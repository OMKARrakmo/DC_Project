package com.dms.repl;

import com.dms.core.Donation;
import com.dms.core.DonationRepository;
import com.dms.core.EventLogger;
import com.dms.core.LamportClock;
import com.dms.core.NodeConfig;
import com.dms.core.NodeRegistry;
import com.dms.rmi.RemoteDonationService;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class ReplicationService {

    public static final String MODE_SYNCHRONOUS = "SYNCHRONOUS";
    public static final String MODE_ASYNCHRONOUS = "ASYNCHRONOUS";

    private final NodeConfig nodeConfig;
    private final NodeRegistry nodeRegistry;
    private final DonationRepository donationRepository;
    private final EventLogger eventLogger;
    private final LamportClock lamportClock;

    private final AtomicReference<String> replicationMode = new AtomicReference<>(MODE_SYNCHRONOUS);
    private final AtomicLong asyncDelayMs = new AtomicLong(2000);
    private final ExecutorService asyncExecutor = Executors.newCachedThreadPool();

    public ReplicationService(NodeConfig nodeConfig,
                              @Lazy NodeRegistry nodeRegistry,
                              DonationRepository donationRepository,
                              EventLogger eventLogger,
                              LamportClock lamportClock) {
        this.nodeConfig = nodeConfig;
        this.nodeRegistry = nodeRegistry;
        this.donationRepository = donationRepository;
        this.eventLogger = eventLogger;
        this.lamportClock = lamportClock;
    }

    public String getReplicationMode() {
        return replicationMode.get();
    }

    public void setReplicationMode(String mode) {
        if (MODE_ASYNCHRONOUS.equalsIgnoreCase(mode)) {
            replicationMode.set(MODE_ASYNCHRONOUS);
        } else {
            replicationMode.set(MODE_SYNCHRONOUS);
        }
        lamportClock.tick();
        eventLogger.log("EXP5-REPL", "Replication mode updated to: " + replicationMode.get());
    }

    public long getAsyncDelayMs() {
        return asyncDelayMs.get();
    }

    public void setAsyncDelayMs(long ms) {
        asyncDelayMs.set(Math.max(0, ms));
        lamportClock.tick();
        eventLogger.log("EXP5-REPL", "Async replication delay set to: " + asyncDelayMs.get() + "ms");
    }

    /**
     * Primary replicates committed donation to all live backup nodes.
     * @return true if replicated / confirmed; false if required sync backup timed out.
     */
    public boolean replicate(Donation donation) {
        String mode = replicationMode.get();
        List<NodeRegistry.PeerState> livePeers = nodeRegistry.getLivePeers();

        // In SYNCHRONOUS mode: confirm ONLY after ALL live backups acknowledge
        if (MODE_SYNCHRONOUS.equals(mode)) {
            if (livePeers.isEmpty()) {
                eventLogger.log("EXP5-REPL", "No live backups present. Synchronous commit confirmed locally.");
                return true;
            }

            for (NodeRegistry.PeerState ps : livePeers) {
                if (ps.getNodeId() == nodeConfig.getId()) continue;
                try {
                    RemoteDonationService stub = nodeRegistry.getPeerStub(ps.getNodeId());
                    if (stub == null) {
                        eventLogger.log("EXP5-REPL", String.format(
                                "Synchronous replication FAILED: peer node=%d stub unavailable", ps.getNodeId()));
                        return false;
                    }
                    stub.replicate(donation);
                    eventLogger.log("EXP5-REPL", String.format(
                            "Synchronous replication ACK from backup node=%d for receipt=%s",
                            ps.getNodeId(), donation.getReceiptId()));
                } catch (Exception e) {
                    eventLogger.log("EXP5-REPL", String.format(
                            "Synchronous replication FAILED on backup node=%d (%s). Donation not confirmed.",
                            ps.getNodeId(), e.getMessage()));
                    return false;
                }
            }
            return true;
        } else {
            // ASYNCHRONOUS mode: confirm immediately, replicate in background after delay
            long delay = asyncDelayMs.get();
            eventLogger.log("EXP5-REPL", String.format(
                    "Asynchronous mode: write confirmed immediately. Background replication scheduled in %dms.", delay));

            asyncExecutor.submit(() -> {
                try {
                    if (delay > 0) {
                        Thread.sleep(delay);
                    }
                    for (NodeRegistry.PeerState ps : nodeRegistry.getLivePeers()) {
                        if (ps.getNodeId() == nodeConfig.getId()) continue;
                        try {
                            RemoteDonationService stub = nodeRegistry.getPeerStub(ps.getNodeId());
                            if (stub != null) {
                                stub.replicate(donation);
                                eventLogger.log("EXP5-REPL", String.format(
                                        "Async replicated receipt=%s to backup node=%d",
                                        donation.getReceiptId(), ps.getNodeId()));
                            }
                        } catch (Exception ex) {
                            eventLogger.log("EXP5-REPL", String.format(
                                    "Async replication failed to node=%d (%s)", ps.getNodeId(), ex.getMessage()));
                        }
                    }
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            });
            return true;
        }
    }

    /**
     * Compute SHA-256 hash over non-isolated ledger entries in deterministic order.
     */
    public String computeLedgerHash() {
        List<Donation> ledger = donationRepository.findByIsolatedFalseOrderBySeqAsc();
        if (ledger.isEmpty()) {
            return "EMPTY";
        }

        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            for (Donation d : ledger) {
                String line = String.format("%d:%s:%.2f:%s:%s\n",
                        d.getSeq() != null ? d.getSeq() : 0,
                        d.getDonor(),
                        d.getAmount(),
                        d.getBranch(),
                        d.getReceiptId() != null ? d.getReceiptId() : "");
                md.update(line.getBytes(StandardCharsets.UTF_8));
            }
            return HexFormat.of().formatHex(md.digest());
        } catch (Exception e) {
            return "HASH_ERROR";
        }
    }

    /**
     * Check ledger consistency across all live cluster nodes.
     */
    public Map<String, Object> checkClusterConsistency() {
        Map<String, Object> result = new HashMap<>();
        int primaryId = nodeRegistry.getPrimaryId();
        result.put("primaryId", primaryId);
        result.put("mode", replicationMode.get());

        List<Map<String, Object>> nodeStatuses = new ArrayList<>();
        String localHash = computeLedgerHash();
        long localCount = donationRepository.findByIsolatedFalseOrderByIdAsc().size();

        Map<String, Object> localItem = new HashMap<>();
        localItem.put("nodeId", nodeConfig.getId());
        localItem.put("host", nodeConfig.getHost());
        localItem.put("hash", localHash);
        localItem.put("count", localCount);
        localItem.put("role", nodeConfig.getId() == primaryId ? "PRIMARY" : "BACKUP");
        localItem.put("alive", true);
        nodeStatuses.add(localItem);

        String referenceHash = (nodeConfig.getId() == primaryId) ? localHash : null;

        for (NodeRegistry.PeerState ps : nodeRegistry.getLivePeers()) {
            if (ps.getNodeId() == nodeConfig.getId()) continue;
            Map<String, Object> item = new HashMap<>();
            item.put("nodeId", ps.getNodeId());
            item.put("host", ps.getHost());
            item.put("role", ps.getNodeId() == primaryId ? "PRIMARY" : "BACKUP");
            item.put("alive", ps.isAlive());

            try {
                RemoteDonationService stub = nodeRegistry.getPeerStub(ps.getNodeId());
                if (stub != null) {
                    String peerHash = stub.getLedgerHash();
                    long peerCount = stub.getLedgerCount();
                    item.put("hash", peerHash);
                    item.put("count", peerCount);
                    if (ps.getNodeId() == primaryId) {
                        referenceHash = peerHash;
                    }
                } else {
                    item.put("hash", "UNREACHABLE");
                    item.put("count", -1);
                }
            } catch (Exception e) {
                item.put("hash", "ERROR: " + e.getMessage());
                item.put("count", -1);
            }
            nodeStatuses.add(item);
        }

        if (referenceHash == null) {
            referenceHash = localHash;
        }

        boolean allMatch = true;
        for (Map<String, Object> item : nodeStatuses) {
            String h = (String) item.get("hash");
            boolean match = referenceHash.equals(h);
            item.put("match", match);
            if (!match) {
                allMatch = false;
            }
        }

        result.put("consistent", allMatch);
        result.put("referenceHash", referenceHash);
        result.put("nodes", nodeStatuses);

        eventLogger.log("EXP5-REPL", String.format(
                "Consistency check completed: consistent=%s, referenceHash=%s", allMatch, referenceHash));

        return result;
    }
}
