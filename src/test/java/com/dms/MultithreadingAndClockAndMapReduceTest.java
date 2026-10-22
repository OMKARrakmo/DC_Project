package com.dms;

import com.dms.core.*;
import com.dms.mr.MapReduceService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class MultithreadingAndClockAndMapReduceTest {

    @Autowired
    private ThreadPoolService threadPoolService;

    @Autowired
    private ClockSyncService clockSyncService;

    @Autowired
    private MapReduceService mapReduceService;

    @Autowired
    private DonationRepository donationRepository;

    @Autowired
    private NodeRegistry nodeRegistry;

    @Autowired
    private NodeConfig nodeConfig;

    @BeforeEach
    public void setup() {
        donationRepository.deleteAll();
        clockSyncService.resetClock();
        nodeRegistry.setSimulatedCrash(false);
        nodeRegistry.setPrimaryId(nodeConfig.getId());
    }

    // =========================================================================
    // EXP 2: MULTITHREADING TESTS
    // =========================================================================

    @Test
    public void testThreadPoolNamingAndBurstExecution() {
        threadPoolService.setCorePoolSize(4);
        assertEquals(4, threadPoolService.getCorePoolSize());

        Map<String, Object> status = threadPoolService.getPoolStatus();
        assertEquals(4, status.get("corePoolSize"));
        assertNotNull(status.get("queueSize"));
        assertNotNull(status.get("activeCount"));

        List<ThreadPoolService.TaskResult> results = threadPoolService.runBurst(6, 30);
        assertEquals(6, results.size());

        for (ThreadPoolService.TaskResult r : results) {
            assertTrue(r.threadName().startsWith("dms-worker-"),
                    "Thread name must start with 'dms-worker-', got: " + r.threadName());
            assertTrue(r.latencyMs() >= 25, "Latency should reflect the sleep delay");
        }
    }

    @Test
    public void testRaceConditionDemoLostUpdates() {
        // Run with 10 threads doing 10 increments each (expected = 100)
        Map<String, Object> demo = threadPoolService.runRaceDemo(10, 10);

        int expected = (int) demo.get("expected");
        int unsafeActual = (int) demo.get("unsafeActual");
        int unsafeLost = (int) demo.get("unsafeLostUpdates");
        int safeActual = (int) demo.get("safeActual");
        int safeLost = (int) demo.get("safeLostUpdates");

        assertEquals(100, expected);
        // CyclicBarrier + 2ms sleep forces context switches; unsafe count must lose updates
        assertTrue(unsafeActual < expected, "Unsafe counter should lose updates due to race condition");
        assertTrue(unsafeLost > 0, "Unsafe lost updates must be greater than zero");

        // AtomicInteger must be 100% exact
        assertEquals(expected, safeActual, "AtomicInteger safe counter must equal expected count");
        assertEquals(0, safeLost, "AtomicInteger must have zero lost updates");
    }

    // =========================================================================
    // EXP 3: CLOCK SYNCHRONIZATION TESTS
    // =========================================================================

    @Test
    public void testVirtualClockSkewAndOffsetCalculations() {
        assertEquals(0L, clockSyncService.getArtificialSkew());
        assertEquals(0L, clockSyncService.getPhysicalClockOffset());

        long before = clockSyncService.getVirtualClockMillis();
        clockSyncService.setArtificialSkew(4000L);
        assertEquals(4000L, clockSyncService.getArtificialSkew());

        long withSkew = clockSyncService.getVirtualClockMillis();
        assertTrue(withSkew - before >= 3900L, "Virtual clock must include artificial skew");

        clockSyncService.applyOffset(-1500L);
        assertEquals(-1500L, clockSyncService.getPhysicalClockOffset());

        long withOffset = clockSyncService.getVirtualClockMillis();
        assertTrue(withOffset - before >= 2400L, "Virtual clock must reflect skew + offset");

        clockSyncService.resetClock();
        assertEquals(0L, clockSyncService.getArtificialSkew());
        assertEquals(0L, clockSyncService.getPhysicalClockOffset());
    }

    @Test
    public void testBerkeleySyncSingleNodeCoordinator() {
        // Ensure coordinator is local
        nodeRegistry.setPrimaryId(nodeConfig.getId());
        clockSyncService.setArtificialSkew(2000L);

        Map<String, Object> syncResult = clockSyncService.runBerkeleySync();
        assertNotNull(syncResult);
        assertEquals(nodeConfig.getId(), syncResult.get("coordinatorId"));
        assertEquals(1, syncResult.get("liveNodeCount"));

        // With only 1 node, average is equal to its own clock, so relative offset is 0
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> nodeClocks = (List<Map<String, Object>>) syncResult.get("nodeClocks");
        assertEquals(1, nodeClocks.size());
        assertEquals(0L, nodeClocks.get(0).get("relativeOffsetMillis"));
    }

    // =========================================================================
    // EXP 7: DISTRIBUTED MAPREDUCE TESTS
    // =========================================================================

    @Test
    public void testMapReduceMatchesSqlSum() {
        // Insert test donations
        Donation d1 = new Donation(null, "Donor 1", new BigDecimal("100.50"), "Mumbai", 1L, "RCP-MR-1", "node-1", "COMMITTED", 1L, false);
        Donation d2 = new Donation(null, "Donor 2", new BigDecimal("250.00"), "Pune", 2L, "RCP-MR-2", "node-1", "COMMITTED", 2L, false);
        Donation d3 = new Donation(null, "Donor 3", new BigDecimal("400.25"), "Mumbai", 3L, "RCP-MR-3", "node-1", "COMMITTED", 3L, false);
        Donation d4 = new Donation(null, "Donor 4", new BigDecimal("150.00"), "Delhi", 4L, "RCP-MR-4", "node-1", "COMMITTED", 4L, false);
        Donation d5 = new Donation(null, "Donor 5", new BigDecimal("99.99"), "Pune", 5L, "RCP-MR-5", "node-1", "COMMITTED", 5L, false);

        donationRepository.saveAll(List.of(d1, d2, d3, d4, d5));

        // Test Map phase locally
        Map<String, BigDecimal> localMap = mapReduceService.mapShard(0, 1);
        assertEquals(new BigDecimal("500.75"), localMap.get("Mumbai"));
        assertEquals(new BigDecimal("349.99"), localMap.get("Pune"));
        assertEquals(new BigDecimal("150.00"), localMap.get("Delhi"));

        // Run full MapReduce job and verify against SQL SUM
        MapReduceService.JobResult result = mapReduceService.runJob();
        assertTrue(result.verified(), "MapReduce totals must match SQL SUM");
        assertTrue(result.discrepancies().isEmpty(), "No discrepancies should be found");

        Map<String, BigDecimal> reduce = result.reduceOutputs();
        Map<String, BigDecimal> sql = result.sqlSums();

        assertEquals(new BigDecimal("500.75"), reduce.get("Mumbai"));
        assertEquals(new BigDecimal("500.75"), sql.get("Mumbai"));

        assertEquals(new BigDecimal("349.99"), reduce.get("Pune"));
        assertEquals(new BigDecimal("349.99"), sql.get("Pune"));

        assertEquals(new BigDecimal("150.00"), reduce.get("Delhi"));
        assertEquals(new BigDecimal("150.00"), sql.get("Delhi"));
    }
}
