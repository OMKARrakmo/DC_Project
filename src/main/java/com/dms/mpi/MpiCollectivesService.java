package com.dms.mpi;

import com.dms.core.*;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.*;

@Service
public class MpiCollectivesService {

    public static final String SIMULATED_BADGE = "SIMULATED (not MPI)";

    private final NodeConfig nodeConfig;
    private final DonationRepository donationRepository;
    private final EventLogger eventLogger;
    private final LamportClock lamportClock;

    public MpiCollectivesService(NodeConfig nodeConfig,
                                 DonationRepository donationRepository,
                                 EventLogger eventLogger,
                                 LamportClock lamportClock) {
        this.nodeConfig = nodeConfig;
        this.donationRepository = donationRepository;
        this.eventLogger = eventLogger;
        this.lamportClock = lamportClock;
    }

    public record BroadcastConfig(List<String> branches, BigDecimal minAmount) {}

    /**
     * Executes the Exp 9 MPI Collectives workflow using simulated in-JVM threads as ranks.
     * Collective steps:
     * 1. Broadcast: Rank 0 broadcasts configuration (target branches, minimum donation amount).
     * 2. Scatter: Rank 0 scatters qualifying non-isolated donation records across ranks.
     * 3. Compute: Each simulated rank calculates partial branch totals for its assigned chunk.
     * 4. Gather: Ranks return partial totals to Rank 0, which aggregates final totals.
     * 5. Verification: Compares gathered branch totals against direct SQL SUM on Primary.
     */
    public Map<String, Object> runCollectives(int requestedRanks) {
        return runCollectives(requestedRanks, BigDecimal.ZERO);
    }

    public Map<String, Object> runCollectives(int requestedRanks, BigDecimal minAmountFilter) {
        int ranks = Math.max(1, Math.min(requestedRanks, 8));
        long startTs = lamportClock.tick();
        int nodeId = nodeConfig.getId();

        eventLogger.logWithLamport("EXP9-MPI", startTs, String.format(
                "Starting simulated MPI Collectives job across %d in-JVM ranks (Badge: %s)",
                ranks, SIMULATED_BADGE));

        List<String> rankLogs = new ArrayList<>();

        // Step 1: Broadcast Phase
        // Rank 0 prepares configuration and broadcasts to all ranks
        List<String> branches = List.of("Mumbai", "Pune", "Chennai");
        BigDecimal minAmount = minAmountFilter != null ? minAmountFilter : BigDecimal.ZERO;
        BroadcastConfig config = new BroadcastConfig(branches, minAmount);

        String bcastMsg = String.format("[Rank 0 - BROADCAST] Sent collective config to all %d ranks: branches=%s, minAmount=₹%s",
                ranks, branches, minAmount);
        rankLogs.add(bcastMsg);
        eventLogger.log("EXP9-MPI", bcastMsg);

        // Step 2: Scatter Phase
        // Rank 0 fetches non-isolated ledger records and scatters them across ranks
        List<Donation> allDonations = donationRepository.findByIsolatedFalseOrderByIdAsc();
        List<Donation> qualifying = new ArrayList<>();
        for (Donation d : allDonations) {
            if (d.getAmount() != null && d.getAmount().compareTo(minAmount) >= 0) {
                qualifying.add(d);
            }
        }

        Map<Integer, List<Donation>> scatteredChunks = new TreeMap<>();
        for (int r = 0; r < ranks; r++) {
            scatteredChunks.put(r, new ArrayList<>());
        }

        // Scatter records in round-robin / chunk fashion
        for (int i = 0; i < qualifying.size(); i++) {
            int targetRank = i % ranks;
            scatteredChunks.get(targetRank).add(qualifying.get(i));
        }

        Map<Integer, Integer> scatterCounts = new TreeMap<>();
        for (int r = 0; r < ranks; r++) {
            int cnt = scatteredChunks.get(r).size();
            scatterCounts.put(r, cnt);
            String scatMsg = String.format("[Rank 0 - SCATTER] Distributed chunk to Rank %d: %d donation records", r, cnt);
            rankLogs.add(scatMsg);
        }
        eventLogger.log("EXP9-MPI", String.format(
                "[Rank 0 - SCATTER] Distributed %d qualifying donation records across %d ranks: %s",
                qualifying.size(), ranks, scatterCounts));

        // Step 3: Compute Phase on Ranks
        ExecutorService rankPool = Executors.newFixedThreadPool(ranks, r -> {
            Thread t = new Thread(r, "mpi-rank-" + System.nanoTime());
            t.setDaemon(true);
            return t;
        });

        Map<Integer, Future<Map<String, BigDecimal>>> rankFutures = new TreeMap<>();
        for (int r = 0; r < ranks; r++) {
            final int rankId = r;
            final List<Donation> myRecords = scatteredChunks.get(r);
            rankFutures.put(r, rankPool.submit(() -> {
                Map<String, BigDecimal> partial = new TreeMap<>();
                for (String b : branches) {
                    partial.put(b, BigDecimal.ZERO);
                }
                for (Donation d : myRecords) {
                    String b = normalizeBranch(d.getBranch());
                    partial.put(b, partial.getOrDefault(b, BigDecimal.ZERO).add(d.getAmount()));
                }
                return partial;
            }));
        }

        // Step 4: Gather Phase
        // Rank 0 gathers partial branch totals from all ranks
        Map<Integer, Map<String, BigDecimal>> rankPartials = new TreeMap<>();
        Map<String, BigDecimal> gatheredTotals = new TreeMap<>();
        for (String b : branches) {
            gatheredTotals.put(b, BigDecimal.ZERO);
        }

        for (int r = 0; r < ranks; r++) {
            try {
                Map<String, BigDecimal> partial = rankFutures.get(r).get(5, TimeUnit.SECONDS);
                rankPartials.put(r, partial);

                String compMsg = String.format("[Rank %d - COMPUTE] Calculated partial totals: %s", r, partial);
                rankLogs.add(compMsg);
                eventLogger.log("EXP9-MPI", compMsg);

                for (Map.Entry<String, BigDecimal> entry : partial.entrySet()) {
                    gatheredTotals.put(entry.getKey(), gatheredTotals.get(entry.getKey()).add(entry.getValue()));
                }
            } catch (Exception e) {
                rankLogs.add(String.format("[Rank %d - ERROR] Execution failed: %s", r, e.getMessage()));
                eventLogger.log("EXP9-MPI", String.format("[Rank %d - ERROR] %s", r, e.getMessage()));
            }
        }
        rankPool.shutdown();

        String gatherMsg = String.format("[Rank 0 - GATHER] Collected partial totals from all %d ranks. Aggregated branch totals: %s",
                ranks, gatheredTotals);
        rankLogs.add(gatherMsg);
        eventLogger.log("EXP9-MPI", gatherMsg);

        // Step 5: Verification against Exp 7 / Primary SQL SUM
        Map<String, BigDecimal> sqlTotals = new TreeMap<>();
        Map<String, Boolean> branchMatches = new TreeMap<>();
        boolean verified = true;
        List<String> discrepancies = new ArrayList<>();

        for (String b : branches) {
            BigDecimal sqlSum = BigDecimal.ZERO;
            if (minAmount.compareTo(BigDecimal.ZERO) > 0) {
                for (Donation d : allDonations) {
                    if (normalizeBranch(d.getBranch()).equalsIgnoreCase(b)
                            && d.getAmount() != null
                            && d.getAmount().compareTo(minAmount) >= 0) {
                        sqlSum = sqlSum.add(d.getAmount());
                    }
                }
            } else {
                sqlSum = donationRepository.sumByBranch(b);
                if (sqlSum == null) sqlSum = BigDecimal.ZERO;
            }
            sqlTotals.put(b, sqlSum);

            BigDecimal mpiSum = gatheredTotals.getOrDefault(b, BigDecimal.ZERO);
            boolean match = (mpiSum.compareTo(sqlSum) == 0);
            branchMatches.put(b, match);
            if (!match) {
                verified = false;
                discrepancies.add(String.format("%s (Gathered: %s vs SQL: %s)", b, mpiSum, sqlSum));
            }
        }

        long endTs = lamportClock.tick();
        eventLogger.logWithLamport("EXP9-MPI", endTs, String.format(
                "MPI Collectives Job completed: verified=%s (Gathered=%s, SQL=%s)",
                verified, gatheredTotals, sqlTotals));

        Map<String, Object> response = new HashMap<>();
        response.put("badge", SIMULATED_BADGE);
        response.put("nodeId", nodeId);
        response.put("ranks", ranks);
        response.put("broadcastConfig", Map.of("branches", branches, "minAmount", minAmount));
        response.put("scatterCounts", scatterCounts);
        response.put("totalRecordsScattered", qualifying.size());
        response.put("rankPartials", rankPartials);
        response.put("gatheredTotals", gatheredTotals);
        response.put("sqlTotals", sqlTotals);
        response.put("branchMatches", branchMatches);
        response.put("verified", verified);
        response.put("discrepancies", discrepancies);
        response.put("rankLogs", rankLogs);

        return response;
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
