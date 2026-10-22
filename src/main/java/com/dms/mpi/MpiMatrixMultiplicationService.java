package com.dms.mpi;

import com.dms.core.EventLogger;
import com.dms.core.LamportClock;
import com.dms.core.NodeConfig;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.*;

@Service
public class MpiMatrixMultiplicationService {

    public static final String SIMULATED_BADGE = "SIMULATED (not MPI)";

    private final NodeConfig nodeConfig;
    private final EventLogger eventLogger;
    private final LamportClock lamportClock;

    public MpiMatrixMultiplicationService(NodeConfig nodeConfig,
                                         EventLogger eventLogger,
                                         LamportClock lamportClock) {
        this.nodeConfig = nodeConfig;
        this.eventLogger = eventLogger;
        this.lamportClock = lamportClock;
    }

    /**
     * Executes the Exp 10 Parallel Matrix Multiplication using simulated MPI ranks.
     *
     * Domain Meaning:
     * Matrix A: Donors x Campaigns
     * Matrix B: Campaigns x Impact Weights
     * Matrix C: Donor Impact Report (A x B)
     */
    public Map<String, Object> multiply(int requestedSize, int requestedRanks) {
        int n = Math.max(10, Math.min(requestedSize, 800));
        int ranks = Math.max(1, Math.min(requestedRanks, 8));
        long startTs = lamportClock.tick();

        eventLogger.logWithLamport("EXP10-MPI", startTs, String.format(
                "Starting parallel matrix multiplication: size=%dx%d, ranks=%d (Badge: %s)",
                n, n, ranks, SIMULATED_BADGE));

        // Generate deterministic matrices A and B
        double[][] a = new double[n][n];
        double[][] b = new double[n][n];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                a[i][j] = (double) ((i + j) % 10 + 1);
                b[i][j] = (double) ((i * j) % 7 + 1);
            }
        }

        // 1. Sequential Benchmark Computation
        long seqStart = System.nanoTime();
        double[][] cSeq = new double[n][n];
        for (int i = 0; i < n; i++) {
            for (int k = 0; k < n; k++) {
                double aik = a[i][k];
                for (int j = 0; j < n; j++) {
                    cSeq[i][j] += aik * b[k][j];
                }
            }
        }
        long seqElapsedNanos = System.nanoTime() - seqStart;
        double seqTimeMs = seqElapsedNanos / 1_000_000.0;

        // 2. Simulated MPI Parallel Computation
        long parStart = System.nanoTime();

        // Broadcast: Rank 0 broadcasts B to all ranks
        eventLogger.log("EXP10-MPI", String.format(
                "[Rank 0 - BROADCAST] Broadcasting matrix B (%dx%d) to all %d ranks", n, n, ranks));

        // Scatter: Rank 0 scatters row-blocks of A
        eventLogger.log("EXP10-MPI", String.format(
                "[Rank 0 - SCATTER] Scattering row blocks of A across %d ranks", ranks));

        ExecutorService rankPool = Executors.newFixedThreadPool(ranks, r -> {
            Thread t = new Thread(r, "mpi-matrix-rank-" + System.nanoTime());
            t.setDaemon(true);
            return t;
        });

        int baseRows = n / ranks;
        int remRows = n % ranks;

        List<Future<RankResult>> futures = new ArrayList<>();
        int currentRow = 0;

        for (int r = 0; r < ranks; r++) {
            final int rankId = r;
            final int startRow = currentRow;
            final int rowCount = baseRows + (r < remRows ? 1 : 0);
            currentRow += rowCount;

            // Extract row block of A for this rank
            double[][] aBlock = new double[rowCount][n];
            for (int i = 0; i < rowCount; i++) {
                System.arraycopy(a[startRow + i], 0, aBlock[i], 0, n);
            }

            futures.add(rankPool.submit(() -> {
                long rankStart = System.nanoTime();
                double[][] cBlock = new double[rowCount][n];

                // Local block multiplication: C_block = A_block * B
                for (int i = 0; i < rowCount; i++) {
                    for (int k = 0; k < n; k++) {
                        double aik = aBlock[i][k];
                        for (int j = 0; j < n; j++) {
                            cBlock[i][j] += aik * b[k][j];
                        }
                    }
                }
                long rankTimeNanos = System.nanoTime() - rankStart;
                return new RankResult(rankId, startRow, rowCount, cBlock, rankTimeNanos / 1_000_000.0);
            }));
        }

        // Gather: Rank 0 gathers row blocks from all ranks to assemble C
        double[][] cPar = new double[n][n];
        List<Map<String, Object>> rankDetails = new ArrayList<>();

        for (Future<RankResult> f : futures) {
            try {
                RankResult rr = f.get(10, TimeUnit.SECONDS);
                for (int i = 0; i < rr.rowCount; i++) {
                    System.arraycopy(rr.cBlock[i], 0, cPar[rr.startRow + i], 0, n);
                }

                Map<String, Object> det = new HashMap<>();
                det.put("rank", rr.rank);
                det.put("startRow", rr.startRow);
                det.put("rowCount", rr.rowCount);
                det.put("executionTimeMs", Math.round(rr.elapsedMs * 100.0) / 100.0);
                rankDetails.add(det);
            } catch (Exception e) {
                eventLogger.log("EXP10-MPI", "Gather error from rank: " + e.getMessage());
            }
        }
        rankPool.shutdown();

        long parElapsedNanos = System.nanoTime() - parStart;
        double parTimeMs = parElapsedNanos / 1_000_000.0;

        eventLogger.log("EXP10-MPI", String.format(
                "[Rank 0 - GATHER] Assembled full matrix C (%dx%d) from %d ranks in %.2fms",
                n, n, ranks, parTimeMs));

        // 3. Verification: Element-wise comparison between Sequential and Parallel
        double maxDiff = 0.0;
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                double diff = Math.abs(cPar[i][j] - cSeq[i][j]);
                if (diff > maxDiff) {
                    maxDiff = diff;
                }
            }
        }
        boolean verified = (maxDiff == 0.0);

        // 4. Performance metrics: Speedup & Efficiency
        double speedup = seqTimeMs > 0 && parTimeMs > 0 ? (seqTimeMs / parTimeMs) : 1.0;
        double efficiency = speedup / ranks;

        long endTs = lamportClock.tick();
        eventLogger.logWithLamport("EXP10-MPI", endTs, String.format(
                "Matrix mult completed: verified=%s (Seq=%.2fms, Par=%.2fms, Speedup=%.2fx, Eff=%.2f)",
                verified, seqTimeMs, parTimeMs, speedup, efficiency));

        // Prepare a small 4x4 sample of matrix C for display in the UI
        int sampleSize = Math.min(4, n);
        double[][] sampleC = new double[sampleSize][sampleSize];
        for (int i = 0; i < sampleSize; i++) {
            for (int j = 0; j < sampleSize; j++) {
                sampleC[i][j] = cPar[i][j];
            }
        }

        Map<String, Object> result = new HashMap<>();
        result.put("badge", SIMULATED_BADGE);
        result.put("size", n);
        result.put("ranks", ranks);
        result.put("sequentialTimeMs", Math.round(seqTimeMs * 100.0) / 100.0);
        result.put("parallelTimeMs", Math.round(parTimeMs * 100.0) / 100.0);
        result.put("speedup", Math.round(speedup * 100.0) / 100.0);
        result.put("efficiency", Math.round(efficiency * 100.0) / 100.0);
        result.put("verified", verified);
        result.put("maxDifference", maxDiff);
        result.put("dmsMeaning", "Matrix A (Donors x Campaigns) x Matrix B (Campaigns x Impact Weights) = Matrix C (Donor Impact Report)");
        result.put("rankDetails", rankDetails);
        result.put("sampleC", sampleC);

        return result;
    }

    private record RankResult(int rank, int startRow, int rowCount, double[][] cBlock, double elapsedMs) {}
}
