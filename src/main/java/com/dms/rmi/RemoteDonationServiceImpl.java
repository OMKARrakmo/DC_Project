package com.dms.rmi;

import com.dms.core.*;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.rmi.RemoteException;
import java.util.*;

@Service
public class RemoteDonationServiceImpl implements RemoteDonationService {

    private final NodeConfig nodeConfig;
    private final NodeRegistry nodeRegistry;
    private final LamportClock lamportClock;
    private final EventLogger eventLogger;
    private final DonationRepository donationRepository;
    private final com.dms.lb.LoadBalancingService loadBalancingService;

    private static final Set<String> VALID_BRANCHES = Set.of("MUMBAI", "PUNE", "CHENNAI");

    public RemoteDonationServiceImpl(NodeConfig nodeConfig,
                                    NodeRegistry nodeRegistry,
                                    LamportClock lamportClock,
                                    EventLogger eventLogger,
                                    DonationRepository donationRepository,
                                    com.dms.lb.LoadBalancingService loadBalancingService) {
        this.nodeConfig = nodeConfig;
        this.nodeRegistry = nodeRegistry;
        this.lamportClock = lamportClock;
        this.eventLogger = eventLogger;
        this.donationRepository = donationRepository;
        this.loadBalancingService = loadBalancingService;
        this.nodeRegistry.setEventLogger(eventLogger);
    }

    @Override
    public NodeInfo ping() throws RemoteException {
        return nodeRegistry.getLocalNodeInfo();
    }

    @Override
    public Receipt process(DonationRequest r) throws RemoteException {
        nodeRegistry.incrementConnections();
        try {
            // Apply artificial processing delay if configured
            long delay = (r.getDelayMs() != null && r.getDelayMs() > 0)
                    ? r.getDelayMs()
                    : loadBalancingService.getArtificialDelayMs();

            if (delay > 0) {
                eventLogger.log("EXP6-LB", String.format(
                        "Applying artificial processing delay of %dms on node=%d (thread=%s)",
                        delay, nodeConfig.getId(), Thread.currentThread().getName()));
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            // Update Lamport clock on receiving request
            if (r.getClientLamportTs() > 0) {
                lamportClock.update(r.getClientLamportTs());
            }

            // Validation: amount >= 100 and known branch
            String branch = r.getBranch() != null ? r.getBranch().trim() : "";
            BigDecimal amount = r.getAmount();
            String donor = r.getDonor() != null ? r.getDonor().trim() : "";

            boolean validAmount = amount != null && amount.compareTo(new BigDecimal("100")) >= 0;
            boolean validBranch = !branch.isEmpty() && VALID_BRANCHES.contains(branch.toUpperCase());
            boolean validDonor = !donor.isEmpty();

            if (!validAmount || !validBranch || !validDonor) {
                long ts = lamportClock.tick();
                String reason = !validDonor ? "Donor name cannot be empty" :
                        (!validAmount ? "Amount must be >= 100" : "Invalid branch (must be Mumbai, Pune, or Chennai)");
                eventLogger.logWithLamport("EXP1-RMI", ts, "Validation REJECTED: " + reason);
                return new Receipt(
                        nodeConfig.getId(),
                        nodeConfig.getHost(),
                        System.getProperty("os.name"),
                        Thread.currentThread().getName(),
                        ts,
                        nodeRegistry.getPrimaryId(),
                        "REJECTED",
                        "Validation failed: " + reason
                );
            }

            String normalizedBranch = normalizeBranch(branch);
            long ts = lamportClock.tick();
            String receiptId = "RCP-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
            String threadName = Thread.currentThread().getName();
            String os = System.getProperty("os.name");

            Donation donation = new Donation(
                    null,
                    donor,
                    amount,
                    normalizedBranch,
                    ts,
                    receiptId,
                    "node-" + nodeConfig.getId(),
                    "CONFIRMED",
                    1L,
                    r.isIsolated()
            );

            // In P1: commit just saves locally
            commit(donation);

            eventLogger.logWithLamport("EXP1-RMI", ts, String.format(
                    "Processed donation receipt=%s donor=%s amount=%s branch=%s thread=%s",
                    receiptId, donor, amount, normalizedBranch, threadName));

            return new Receipt(
                    nodeConfig.getId(),
                    nodeConfig.getHost(),
                    os,
                    threadName,
                    ts,
                    nodeRegistry.getPrimaryId(),
                    "CONFIRMED",
                    "Processed on node " + nodeConfig.getId() + " (" + nodeConfig.getHost() + ")"
            );
        } finally {
            nodeRegistry.decrementConnections();
        }
    }

    @Override
    public Receipt commit(Donation d) throws RemoteException {
        // Primary / processing node: append to ledger
        donationRepository.save(d);
        eventLogger.log("EXP1-RMI", "Committed donation receipt=" + d.getReceiptId() + " to local H2 ledger");
        return new Receipt(
                nodeConfig.getId(),
                nodeConfig.getHost(),
                System.getProperty("os.name"),
                Thread.currentThread().getName(),
                d.getLamportTs(),
                nodeRegistry.getPrimaryId(),
                "COMMITTED",
                "Committed on node " + nodeConfig.getId()
        );
    }

    @Override
    public void replicate(Donation d) throws RemoteException {
        donationRepository.save(d);
        eventLogger.log("EXP5-REPL", "Replicated donation receipt=" + d.getReceiptId() + " to backup ledger");
    }

    @Override
    public BigDecimal branchTotal(String branch) throws RemoteException {
        long ts = lamportClock.tick();
        String normalized = normalizeBranch(branch);
        BigDecimal total = donationRepository.sumByBranch(normalized);
        if (total == null) {
            total = BigDecimal.ZERO;
        }
        eventLogger.logWithLamport("EXP1-RMI", ts, String.format("NGO query: branchTotal(%s) = %s", normalized, total));
        return total;
    }

    @Override
    public void electionMsg(int fromId) throws RemoteException {
        lamportClock.tick();
        eventLogger.log("EXP4-ELECT", "Received electionMsg from node=" + fromId);
    }

    @Override
    public void coordinatorMsg(int newCoordinatorId) throws RemoteException {
        lamportClock.tick();
        nodeRegistry.setPrimaryId(newCoordinatorId);
        eventLogger.log("EXP4-ELECT", "Received coordinatorMsg: new coordinator is node=" + newCoordinatorId);
    }

    @Override
    public long clockMillis() throws RemoteException {
        return System.currentTimeMillis();
    }

    @Override
    public void applyOffset(long millis) throws RemoteException {
        lamportClock.tick();
        eventLogger.log("EXP3-CLOCK", "Applied clock offset=" + millis + "ms");
    }

    @Override
    public Map<String, BigDecimal> mapPhase(List<Integer> liveSortedIds) throws RemoteException {
        long ts = lamportClock.tick();
        eventLogger.logWithLamport("EXP7-MR", ts, "MapPhase invoked with live nodes=" + liveSortedIds);
        Map<String, BigDecimal> result = new HashMap<>();
        for (String b : List.of("Mumbai", "Pune", "Chennai")) {
            BigDecimal sum = donationRepository.sumByBranch(b);
            result.put(b, sum != null ? sum : BigDecimal.ZERO);
        }
        return result;
    }

    @Override
    public List<Donation> snapshot() throws RemoteException {
        return donationRepository.findByIsolatedFalseOrderByIdAsc();
    }

    private String normalizeBranch(String branch) {
        if (branch == null) return "Mumbai";
        String lower = branch.trim().toLowerCase();
        if (lower.startsWith("mum")) return "Mumbai";
        if (lower.startsWith("pun")) return "Pune";
        if (lower.startsWith("chen")) return "Chennai";
        return branch.substring(0, 1).toUpperCase() + branch.substring(1).toLowerCase();
    }
}
