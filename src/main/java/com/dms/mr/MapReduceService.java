package com.dms.mr;

import com.dms.core.*;
import com.dms.rmi.RemoteDonationService;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.*;

@Service
public class MapReduceService {

    public record JobResult(
            int coordinatorId,
            int liveCount,
            boolean verified,
            Map<Integer, Map<String, BigDecimal>> mapOutputs,
            Map<String, List<BigDecimal>> shuffleOutputs,
            Map<String, BigDecimal> reduceOutputs,
            Map<String, BigDecimal> sqlSums,
            List<String> discrepancies,
            Map<String, Object> rawMap
    ) {}

    private final NodeConfig nodeConfig;
    private final NodeRegistry nodeRegistry;
    private final DonationRepository donationRepository;
    private final EventLogger eventLogger;
    private final LamportClock lamportClock;

    public MapReduceService(NodeConfig nodeConfig,
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

    /**
     * Executes the Map Phase on this local node for a specific shard index and live count.
     * Sharding rule: donation belongs to node if (donationId % liveCount == shardIndex).
     */
    public Map<String, BigDecimal> mapShard(int shardIndex, int liveCount) {
        long ts = lamportClock.tick();
        int myId = nodeConfig.getId();

        List<Donation> all = donationRepository.findByIsolatedFalseOrderByIdAsc();
        List<Donation> myShard = new ArrayList<>();

        for (Donation d : all) {
            long dId = d.getId() != null ? d.getId() : 0L;
            if (Math.floorMod(dId, liveCount) == shardIndex) {
                myShard.add(d);
            }
        }

        Map<String, BigDecimal> branchSums = new HashMap<>();
        Set<String> branches = getKnownBranches();
        for (String b : branches) {
            branchSums.put(b, BigDecimal.ZERO);
        }

        for (Donation d : myShard) {
            String b = normalizeBranch(d.getBranch());
            branchSums.put(b, branchSums.getOrDefault(b, BigDecimal.ZERO).add(d.getAmount()));
        }

        eventLogger.logWithLamport("EXP7-MR", ts, String.format(
                "Map Phase on Node %d (shard index %d/%d): sharded %d of %d total donations -> %s",
                myId, shardIndex, liveCount, myShard.size(), all.size(), branchSums));

        return branchSums;
    }

    /**
     * Executes the Map Phase on this local node for its assigned shard.
     * Sharding rule: donation belongs to node if (donationId % liveCount == indexOf(node in sorted live ids)).
     */
    public Map<String, BigDecimal> mapShard(List<Integer> liveSortedIds) {
        int myId = nodeConfig.getId();
        int liveCount = liveSortedIds.size();
        int myIndex = liveSortedIds.indexOf(myId);

        if (myIndex < 0 || liveCount <= 0) {
            Map<String, BigDecimal> empty = new HashMap<>();
            for (String b : getKnownBranches()) {
                empty.put(b, BigDecimal.ZERO);
            }
            return empty;
        }

        return mapShard(myIndex, liveCount);
    }

    /**
     * Coordinator runs the complete MapReduce job:
     * 1. Map phase dispatched to every live node over its shard.
     * 2. Shuffle phase groups intermediate totals by branch.
     * 3. Reduce phase sums values per branch.
     * 4. SQL Verification: compares MapReduce totals with direct SQL SUM on Primary.
     */
    public Map<String, Object> runMapReduceJob() {
        long startTs = lamportClock.tick();
        int myId = nodeConfig.getId();
        int primaryId = nodeRegistry.getPrimaryId();

        // 1. Discover live nodes and sort ascending
        List<Integer> liveSortedIds = new ArrayList<>();
        liveSortedIds.add(myId);

        for (NodeRegistry.PeerState ps : nodeRegistry.getLivePeers()) {
            if (ps.getNodeId() > 0 && !liveSortedIds.contains(ps.getNodeId())) {
                liveSortedIds.add(ps.getNodeId());
            }
        }
        Collections.sort(liveSortedIds);

        eventLogger.logWithLamport("EXP7-MR", startTs, String.format(
                "Initiating MapReduce 'total donation per branch' across live cluster nodes: %s",
                liveSortedIds));

        // 2. Map Phase across all live nodes
        Map<Integer, Map<String, BigDecimal>> mapResultsByNode = new TreeMap<>();

        for (int nId : liveSortedIds) {
            try {
                if (nId == myId) {
                    Map<String, BigDecimal> localMap = mapShard(liveSortedIds);
                    mapResultsByNode.put(nId, localMap);
                } else {
                    RemoteDonationService stub = nodeRegistry.getPeerStub(nId);
                    if (stub != null) {
                        Map<String, BigDecimal> peerMap = stub.mapPhase(liveSortedIds);
                        mapResultsByNode.put(nId, peerMap);
                    } else {
                        eventLogger.log("EXP7-MR", String.format("Node %d stub unavailable for Map phase", nId));
                    }
                }
            } catch (Exception e) {
                eventLogger.log("EXP7-MR", String.format("Map phase error on node %d: %s", nId, e.getMessage()));
            }
        }

        // 3. Shuffle Phase: group values by branch
        Set<String> branches = getKnownBranches();
        for (Map<String, BigDecimal> nodeMap : mapResultsByNode.values()) {
            branches.addAll(nodeMap.keySet());
        }

        Map<String, List<BigDecimal>> shuffleGrouped = new TreeMap<>();
        for (String branch : branches) {
            shuffleGrouped.put(branch, new ArrayList<>());
        }

        for (Map<String, BigDecimal> nodeMap : mapResultsByNode.values()) {
            for (String branch : branches) {
                BigDecimal val = nodeMap.getOrDefault(branch, BigDecimal.ZERO);
                shuffleGrouped.get(branch).add(val);
            }
        }

        eventLogger.log("EXP7-MR", String.format("Shuffle Phase grouped intermediate values: %s", shuffleGrouped));

        // 4. Reduce Phase: aggregate grouped branch totals
        Map<String, BigDecimal> reduceTotals = new TreeMap<>();
        for (String branch : branches) {
            BigDecimal sum = BigDecimal.ZERO;
            for (BigDecimal v : shuffleGrouped.get(branch)) {
                sum = sum.add(v);
            }
            reduceTotals.put(branch, sum);
        }

        long reduceTs = lamportClock.tick();
        eventLogger.logWithLamport("EXP7-MR", reduceTs, String.format(
                "Reduce Phase completed on Coordinator Node %d: %s", myId, reduceTotals));

        // 5. SQL Verification: direct SQL SUM on Primary
        Map<String, BigDecimal> sqlTotals = new TreeMap<>();
        Map<String, Boolean> branchMatches = new TreeMap<>();
        List<String> discrepancies = new ArrayList<>();
        boolean allMatch = true;

        for (String branch : branches) {
            BigDecimal sqlSum = BigDecimal.ZERO;
            try {
                if (primaryId == myId) {
                    BigDecimal localSql = donationRepository.sumByBranch(branch);
                    sqlSum = localSql != null ? localSql : BigDecimal.ZERO;
                } else {
                    RemoteDonationService primaryStub = nodeRegistry.getPeerStub(primaryId);
                    if (primaryStub != null) {
                        sqlSum = primaryStub.branchTotal(branch);
                    } else {
                        sqlSum = donationRepository.sumByBranch(branch);
                    }
                }
            } catch (Exception e) {
                sqlSum = donationRepository.sumByBranch(branch);
            }

            sqlTotals.put(branch, sqlSum != null ? sqlSum : BigDecimal.ZERO);
            BigDecimal mrSum = reduceTotals.getOrDefault(branch, BigDecimal.ZERO);
            boolean match = (mrSum.compareTo(sqlTotals.get(branch)) == 0);
            branchMatches.put(branch, match);
            if (!match) {
                allMatch = false;
                discrepancies.add(String.format("%s (MapReduce: %s vs Primary SQL: %s)", branch, mrSum, sqlTotals.get(branch)));
            }
        }

        long endTs = lamportClock.tick();
        eventLogger.logWithLamport("EXP7-MR", endTs, String.format(
                "MapReduce vs SQL Verification: allMatch=%s (MR=%s, SQL=%s)",
                allMatch, reduceTotals, sqlTotals));

        Map<String, Object> response = new HashMap<>();
        response.put("coordinatorId", myId);
        response.put("primaryId", primaryId);
        response.put("liveNodes", liveSortedIds);
        response.put("liveCount", liveSortedIds.size());
        response.put("mapPhase", mapResultsByNode);
        response.put("mapOutputs", mapResultsByNode);
        response.put("shufflePhase", shuffleGrouped);
        response.put("shuffleOutputs", shuffleGrouped);
        response.put("reducePhase", reduceTotals);
        response.put("reduceOutputs", reduceTotals);
        response.put("sqlTotals", sqlTotals);
        response.put("sqlSums", sqlTotals);
        response.put("branchMatches", branchMatches);
        response.put("verifiedAllMatch", allMatch);
        response.put("verified", allMatch);
        response.put("discrepancies", discrepancies);

        return response;
    }

    public JobResult runJob() {
        Map<String, Object> map = runMapReduceJob();
        @SuppressWarnings("unchecked")
        Map<Integer, Map<String, BigDecimal>> mapOutputs = (Map<Integer, Map<String, BigDecimal>>) map.get("mapPhase");
        @SuppressWarnings("unchecked")
        Map<String, List<BigDecimal>> shuffleOutputs = (Map<String, List<BigDecimal>>) map.get("shufflePhase");
        @SuppressWarnings("unchecked")
        Map<String, BigDecimal> reduceOutputs = (Map<String, BigDecimal>) map.get("reducePhase");
        @SuppressWarnings("unchecked")
        Map<String, BigDecimal> sqlSums = (Map<String, BigDecimal>) map.get("sqlTotals");
        @SuppressWarnings("unchecked")
        List<String> discrepancies = (List<String>) map.getOrDefault("discrepancies", Collections.emptyList());
        boolean verified = Boolean.TRUE.equals(map.get("verified"));
        int coordId = (int) map.get("coordinatorId");
        int liveCount = (int) map.get("liveCount");

        return new JobResult(coordId, liveCount, verified, mapOutputs, shuffleOutputs, reduceOutputs, sqlSums, discrepancies, map);
    }

    /**
     * Inspect shard distribution across cluster nodes for the UI.
     */
    public List<Map<String, Object>> getShardBreakdown() {
        List<Donation> all = donationRepository.findByIsolatedFalseOrderByIdAsc();
        List<Integer> liveSortedIds = new ArrayList<>();
        liveSortedIds.add(nodeConfig.getId());
        for (NodeRegistry.PeerState ps : nodeRegistry.getLivePeers()) {
            if (ps.getNodeId() > 0 && !liveSortedIds.contains(ps.getNodeId())) {
                liveSortedIds.add(ps.getNodeId());
            }
        }
        Collections.sort(liveSortedIds);

        int liveCount = liveSortedIds.size();
        List<Map<String, Object>> list = new ArrayList<>();

        for (int i = 0; i < liveSortedIds.size(); i++) {
            int nId = liveSortedIds.get(i);
            List<Long> donationIds = new ArrayList<>();
            BigDecimal sum = BigDecimal.ZERO;

            for (Donation d : all) {
                long dId = d.getId() != null ? d.getId() : 0L;
                if (Math.floorMod(dId, liveCount) == i) {
                    donationIds.add(dId);
                    sum = sum.add(d.getAmount());
                }
            }

            Map<String, Object> nodeInfo = new HashMap<>();
            nodeInfo.put("nodeId", nId);
            nodeInfo.put("shardIndex", i);
            nodeInfo.put("shardFormula", String.format("id %% %d == %d", liveCount, i));
            nodeInfo.put("shardingFormula", String.format("donationId %% %d == %d", liveCount, i));
            nodeInfo.put("donationIds", donationIds);
            nodeInfo.put("donationCount", donationIds.size());
            nodeInfo.put("assignedCount", donationIds.size());
            nodeInfo.put("shardTotal", sum);
            list.add(nodeInfo);
        }
        return list;
    }

    private Set<String> getKnownBranches() {
        Set<String> branches = new TreeSet<>();
        branches.add("Mumbai");
        branches.add("Pune");
        branches.add("Chennai");
        for (Donation d : donationRepository.findByIsolatedFalseOrderByIdAsc()) {
            if (d.getBranch() != null && !d.getBranch().isBlank()) {
                branches.add(normalizeBranch(d.getBranch()));
            }
        }
        return branches;
    }

    private String normalizeBranch(String branch) {
        if (branch == null) return "Mumbai";
        String lower = branch.trim().toLowerCase();
        if (lower.startsWith("mum")) return "Mumbai";
        if (lower.startsWith("pun")) return "Pune";
        if (lower.startsWith("chen")) return "Chennai";
        if (lower.startsWith("del")) return "Delhi";
        return branch.substring(0, 1).toUpperCase() + branch.substring(1).toLowerCase();
    }
}
