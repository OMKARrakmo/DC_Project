package com.dms.rmi;

import com.dms.core.*;
import com.dms.election.BullyElection;
import com.dms.lb.LoadBalancingService;
import com.dms.repl.ReplicationService;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class RemoteDonationServiceImpl implements RemoteDonationService {

    private final NodeConfig nodeConfig;
    private final NodeRegistry nodeRegistry;
    private final LamportClock lamportClock;
    private final EventLogger eventLogger;
    private final DonationRepository donationRepository;
    private final LoadBalancingService loadBalancingService;
    private final ReplicationService replicationService;
    private final BullyElection bullyElection;
    private final ClockSyncService clockSyncService;
    private final com.dms.mr.MapReduceService mapReduceService;

    private final AtomicLong nextSeq = new AtomicLong(0);

    private static final Set<String> VALID_BRANCHES = Set.of("MUMBAI", "PUNE", "CHENNAI");

    public RemoteDonationServiceImpl(NodeConfig nodeConfig,
                                    NodeRegistry nodeRegistry,
                                    LamportClock lamportClock,
                                    EventLogger eventLogger,
                                    DonationRepository donationRepository,
                                    LoadBalancingService loadBalancingService,
                                    ReplicationService replicationService,
                                    @Lazy BullyElection bullyElection,
                                    ClockSyncService clockSyncService,
                                    @Lazy com.dms.mr.MapReduceService mapReduceService) {
        this.nodeConfig = nodeConfig;
        this.nodeRegistry = nodeRegistry;
        this.lamportClock = lamportClock;
        this.eventLogger = eventLogger;
        this.donationRepository = donationRepository;
        this.loadBalancingService = loadBalancingService;
        this.replicationService = replicationService;
        this.bullyElection = bullyElection;
        this.clockSyncService = clockSyncService;
        this.mapReduceService = mapReduceService;

        this.nodeRegistry.setEventLogger(eventLogger);
        this.nodeRegistry.setBullyElection(bullyElection);
        this.nodeRegistry.setDonationRepository(donationRepository);
    }

    private void checkCrash() throws RemoteException {
        if (nodeRegistry.isSimulatedCrash()) {
            throw new RemoteException("Node " + nodeConfig.getId() + " is stopped (simulated crash)");
        }
    }

    @Override
    public NodeInfo ping() throws RemoteException {
        checkCrash();
        return nodeRegistry.getLocalNodeInfo();
    }

    @Override
    public Receipt process(DonationRequest r) throws RemoteException {
        checkCrash();
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
                    "PENDING",
                    0L,
                    r.isIsolated()
            );

            // WRITE PATH: Call commit() on the current primary
            int primaryId = nodeRegistry.getPrimaryId();
            Receipt commitReceipt;

            if (primaryId == nodeConfig.getId()) {
                // This processing node IS the primary
                commitReceipt = commit(donation);
            } else {
                // Forward commit to the primary node via Java RMI
                RemoteDonationService primaryStub = nodeRegistry.getPeerStub(primaryId);
                if (primaryStub == null) {
                    eventLogger.log("EXP8-FT", String.format(
                            "Primary node=%d unreachable during commit call! Triggering Bully election.", primaryId));
                    bullyElection.startElection();
                    return new Receipt(
                            nodeConfig.getId(),
                            nodeConfig.getHost(),
                            os,
                            threadName,
                            ts,
                            primaryId,
                            "FAILED",
                            "Primary node " + primaryId + " is unavailable. Election initiated."
                    );
                }

                eventLogger.log("EXP5-REPL", String.format(
                        "Processing node=%d calling commit() on primary node=%d via Java RMI for receipt=%s",
                        nodeConfig.getId(), primaryId, receiptId));
                commitReceipt = primaryStub.commit(donation);
                lamportClock.update(commitReceipt.getLamportTs());
            }

            eventLogger.logWithLamport("EXP1-RMI", ts, String.format(
                    "Processed donation receipt=%s donor=%s amount=%s branch=%s thread=%s (primary=%d status=%s)",
                    receiptId, donor, amount, normalizedBranch, threadName, primaryId, commitReceipt.getStatus()));

            return new Receipt(
                    nodeConfig.getId(),
                    nodeConfig.getHost(),
                    os,
                    threadName,
                    ts,
                    primaryId,
                    commitReceipt.getStatus(),
                    String.format("Processed on node %d, committed by primary node %d [%s]",
                            nodeConfig.getId(), primaryId, commitReceipt.getStatus()),
                    receiptId
            );
        } finally {
            nodeRegistry.decrementConnections();
        }
    }

    @Override
    public Receipt commit(Donation d) throws RemoteException {
        checkCrash();
        long ts = lamportClock.tick();

        // Assign sequence number
        d.setSeq(nextSeq.incrementAndGet());
        d.setStatus("PENDING");
        donationRepository.save(d);

        eventLogger.logWithLamport("EXP5-REPL", ts, String.format(
                "Primary node=%d appended donation receipt=%s seq=%d to ledger. Initiating replication...",
                nodeConfig.getId(), d.getReceiptId(), d.getSeq()));

        // Replicate to backups per selected replication mode (Sync or Async)
        boolean replicated = replicationService.replicate(d);

        if (!replicated) {
            d.setStatus("REJECTED");
            donationRepository.delete(d);
            eventLogger.log("EXP5-REPL", String.format(
                    "Primary node=%d rejected donation receipt=%s: synchronous backup acknowledgement failed",
                    nodeConfig.getId(), d.getReceiptId()));
            return new Receipt(
                    nodeConfig.getId(),
                    nodeConfig.getHost(),
                    System.getProperty("os.name"),
                    Thread.currentThread().getName(),
                    ts,
                    nodeConfig.getId(),
                    "REJECTED",
                    "Synchronous replication failed: required backup node did not acknowledge",
                    d.getReceiptId()
            );
        }

        d.setStatus("COMMITTED");
        donationRepository.save(d);
        eventLogger.log("EXP5-REPL", String.format(
                "Primary node=%d confirmed donation receipt=%s seq=%d",
                nodeConfig.getId(), d.getReceiptId(), d.getSeq()));

        return new Receipt(
                nodeConfig.getId(),
                nodeConfig.getHost(),
                System.getProperty("os.name"),
                Thread.currentThread().getName(),
                ts,
                nodeConfig.getId(),
                "CONFIRMED",
                "Committed on primary node " + nodeConfig.getId(),
                d.getReceiptId()
        );
    }

    @Override
    public void replicate(Donation d) throws RemoteException {
        checkCrash();
        d.setStatus("COMMITTED");
        java.util.Optional<Donation> existing = donationRepository.findByReceiptId(d.getReceiptId());
        if (existing.isPresent()) {
            Donation target = existing.get();
            target.setSeq(d.getSeq());
            target.setStatus("COMMITTED");
            target.setLamportTs(d.getLamportTs());
            target.setAmount(d.getAmount());
            target.setBranch(d.getBranch());
            target.setDonor(d.getDonor());
            target.setHandledBy(d.getHandledBy());
            target.setIsolated(d.isIsolated());
            donationRepository.save(target);
        } else {
            Donation toSave = new Donation(
                    null,
                    d.getDonor(),
                    d.getAmount(),
                    d.getBranch(),
                    d.getLamportTs(),
                    d.getReceiptId(),
                    d.getHandledBy(),
                    "COMMITTED",
                    d.getSeq(),
                    d.isIsolated()
            );
            donationRepository.save(toSave);
        }
        eventLogger.log("EXP5-REPL", String.format(
                "Backup node=%d stored replicated donation receipt=%s seq=%d in local ledger",
                nodeConfig.getId(), d.getReceiptId(), d.getSeq()));
    }

    @Override
    public BigDecimal branchTotal(String branch) throws RemoteException {
        checkCrash();
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
        checkCrash();
        bullyElection.handleElectionMessage(fromId);
    }

    @Override
    public void coordinatorMsg(int newCoordinatorId) throws RemoteException {
        checkCrash();
        bullyElection.handleCoordinatorMessage(newCoordinatorId);
    }

    @Override
    public long clockMillis() throws RemoteException {
        checkCrash();
        return clockSyncService.getVirtualClockMillis();
    }

    @Override
    public void applyOffset(long millis) throws RemoteException {
        checkCrash();
        clockSyncService.applyOffset(millis);
    }

    @Override
    public Map<String, BigDecimal> mapPhase(List<Integer> liveSortedIds) throws RemoteException {
        checkCrash();
        return mapReduceService.mapShard(liveSortedIds);
    }

    @Override
    public List<Donation> snapshot() throws RemoteException {
        checkCrash();
        return donationRepository.findByIsolatedFalseOrderByIdAsc();
    }

    @Override
    public String getLedgerHash() throws RemoteException {
        checkCrash();
        return replicationService.computeLedgerHash();
    }

    @Override
    public long getLedgerCount() throws RemoteException {
        checkCrash();
        return donationRepository.findByIsolatedFalseOrderByIdAsc().size();
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
