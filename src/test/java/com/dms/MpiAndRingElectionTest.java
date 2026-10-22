package com.dms;

import com.dms.core.Donation;
import com.dms.core.DonationRepository;
import com.dms.core.NodeConfig;
import com.dms.core.NodeRegistry;
import com.dms.election.BullyElection;
import com.dms.election.ElectionManager;
import com.dms.election.RingElection;
import com.dms.mpi.MpiCollectivesService;
import com.dms.mpi.MpiMatrixMultiplicationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class MpiAndRingElectionTest {

    @Autowired
    private NodeConfig nodeConfig;

    @Autowired
    private NodeRegistry nodeRegistry;

    @Autowired
    private DonationRepository donationRepository;

    @Autowired
    private BullyElection bullyElection;

    @Autowired
    private RingElection ringElection;

    @Autowired
    private ElectionManager electionManager;

    @Autowired
    private MpiCollectivesService collectivesService;

    @Autowired
    private MpiMatrixMultiplicationService matrixService;

    @BeforeEach
    public void setup() {
        donationRepository.deleteAll();
        nodeRegistry.setSimulatedCrash(false);
        nodeRegistry.setPrimaryId(nodeConfig.getId());
        electionManager.setAlgorithm("BULLY");
    }

    @Test
    public void testElectionManagerAlgorithmSwitching() {
        assertEquals("BULLY", electionManager.getAlgorithmName());
        assertEquals("BULLY", nodeRegistry.getElectionAlgorithm().getAlgorithmName());

        electionManager.setAlgorithm("RING");
        assertEquals("RING", electionManager.getAlgorithmName());
        assertEquals("RING", nodeRegistry.getElectionAlgorithm().getAlgorithmName());

        electionManager.setAlgorithm("BULLY");
        assertEquals("BULLY", electionManager.getAlgorithmName());
        assertEquals("BULLY", nodeRegistry.getElectionAlgorithm().getAlgorithmName());
    }

    @Test
    public void testRingElectionBasicContract() {
        assertEquals("RING", ringElection.getAlgorithmName());

        // Coordinator message updates primary ID
        ringElection.handleCoordinatorMessage(42);
        assertEquals(42, nodeRegistry.getPrimaryId());

        // Reset
        nodeRegistry.setPrimaryId(nodeConfig.getId());
    }

    @Test
    public void testRingElectionCircuitCompletionElectsMaxId() {
        electionManager.setAlgorithm("RING");

        // Simulate incoming token that already contains this node's id (full circuit completed)
        int myId = nodeConfig.getId();
        List<Integer> token = new ArrayList<>(Arrays.asList(myId, 5, 2, 8));

        ringElection.handleRingElectionMessage(token);

        // Highest ID in {myId, 5, 2, 8} is 8 (assuming myId < 8)
        int expectedMax = Math.max(myId, 8);
        assertEquals(expectedMax, nodeRegistry.getPrimaryId());

        // Reset
        nodeRegistry.setPrimaryId(nodeConfig.getId());
    }

    @Test
    public void testMpiCollectivesEmptyRepository() {
        Map<String, Object> result = collectivesService.runCollectives(2, new BigDecimal("50.00"));

        assertNotNull(result);
        assertEquals("SIMULATED (not MPI)", result.get("badge"));
        assertEquals(Boolean.TRUE, result.get("verified"));

        @SuppressWarnings("unchecked")
        Map<Integer, Map<String, BigDecimal>> rankPartials =
                (Map<Integer, Map<String, BigDecimal>>) result.get("rankPartials");
        assertNotNull(rankPartials);
        assertEquals(2, rankPartials.size());

        @SuppressWarnings("unchecked")
        Map<String, BigDecimal> gatheredTotals = (Map<String, BigDecimal>) result.get("gatheredTotals");
        assertNotNull(gatheredTotals);
        assertEquals(0, BigDecimal.ZERO.compareTo(gatheredTotals.getOrDefault("Mumbai", BigDecimal.ZERO)));
    }

    @Test
    public void testMpiCollectivesWithDataAndVerification() {
        // Seed donations across branches and amounts
        donationRepository.save(new Donation(null, "Alice", new BigDecimal("500.00"), "Mumbai", 1L, "RCP-1", "node-1", "COMMITTED", 1L, false));
        donationRepository.save(new Donation(null, "Bob", new BigDecimal("750.50"), "Mumbai", 2L, "RCP-2", "node-1", "COMMITTED", 2L, false));
        donationRepository.save(new Donation(null, "Charlie", new BigDecimal("300.00"), "Pune", 3L, "RCP-3", "node-1", "COMMITTED", 3L, false));
        donationRepository.save(new Donation(null, "Dave", new BigDecimal("1200.00"), "Delhi", 4L, "RCP-4", "node-1", "COMMITTED", 4L, false));
        donationRepository.save(new Donation(null, "Eve", new BigDecimal("25.00"), "Mumbai", 5L, "RCP-5", "node-1", "COMMITTED", 5L, false)); // Below 50

        // Run with 4 ranks, minAmount = 50.00
        Map<String, Object> result = collectivesService.runCollectives(4, new BigDecimal("50.00"));

        assertNotNull(result);
        assertEquals("SIMULATED (not MPI)", result.get("badge"));
        assertEquals(Boolean.TRUE, result.get("verified"), "Gathered branch totals must equal direct SQL verification");

        @SuppressWarnings("unchecked")
        Map<String, BigDecimal> gatheredTotals = (Map<String, BigDecimal>) result.get("gatheredTotals");
        assertNotNull(gatheredTotals);

        // Mumbai total: 500.00 + 750.50 = 1250.50 (25.00 excluded)
        BigDecimal expectedMumbai = new BigDecimal("1250.50");
        BigDecimal gatheredMumbai = gatheredTotals.get("Mumbai");
        assertNotNull(gatheredMumbai);
        assertEquals(0, expectedMumbai.compareTo(gatheredMumbai));

        // Pune total: 300.00
        BigDecimal expectedPune = new BigDecimal("300.00");
        BigDecimal gatheredPune = gatheredTotals.get("Pune");
        assertNotNull(gatheredPune);
        assertEquals(0, expectedPune.compareTo(gatheredPune));

        // Assert 4 ranks participated
        @SuppressWarnings("unchecked")
        Map<Integer, Map<String, BigDecimal>> rankPartials =
                (Map<Integer, Map<String, BigDecimal>>) result.get("rankPartials");
        assertEquals(4, rankPartials.size());

        // Logs verification
        @SuppressWarnings("unchecked")
        List<String> logs = (List<String>) result.get("rankLogs");
        assertNotNull(logs);
        assertFalse(logs.isEmpty());
        assertTrue(logs.stream().anyMatch(l -> l.contains("BROADCAST")));
        assertTrue(logs.stream().anyMatch(l -> l.contains("SCATTER")));
        assertTrue(logs.stream().anyMatch(l -> l.contains("GATHER")));
    }

    @Test
    public void testMpiMatrixMultiplicationCorrectnessAndVerification() {
        int size = 100;
        int ranks = 4;

        Map<String, Object> result = matrixService.multiply(size, ranks);

        assertNotNull(result);
        assertEquals("SIMULATED (not MPI)", result.get("badge"));
        assertEquals(Boolean.TRUE, result.get("verified"), "Parallel result must exactly equal sequential result");
        assertEquals(0.0, ((Number) result.get("maxDifference")).doubleValue(), 1e-9);

        // Timing and metrics
        assertTrue(((Number) result.get("sequentialTimeMs")).doubleValue() >= 0);
        assertTrue(((Number) result.get("parallelTimeMs")).doubleValue() >= 0);
        assertTrue(((Number) result.get("speedup")).doubleValue() >= 0);
        assertTrue(((Number) result.get("efficiency")).doubleValue() >= 0);

        // Sample C dimensions: 4x4
        double[][] sampleC = (double[][]) result.get("sampleC");
        assertNotNull(sampleC);
        assertEquals(4, sampleC.length);
        assertEquals(4, sampleC[0].length);

        // Rank assignments
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rankDetails = (List<Map<String, Object>>) result.get("rankDetails");
        assertEquals(ranks, rankDetails.size());
    }

    @Test
    public void testMpiMatrixMultiplicationWithSingleRank() {
        int size = 50;
        int ranks = 1;

        Map<String, Object> result = matrixService.multiply(size, ranks);

        assertNotNull(result);
        assertEquals("SIMULATED (not MPI)", result.get("badge"));
        assertEquals(Boolean.TRUE, result.get("verified"));
        assertEquals(0.0, ((Number) result.get("maxDifference")).doubleValue(), 1e-9);
    }
}
