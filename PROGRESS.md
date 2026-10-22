# PROGRESS.md: Distributed Donation Management System (DMS)

## Phases Done
- **P1: Core + Exp 1 (Java RMI)** [COMPLETED]
  - Bootstrapped Spring Boot 3.3.5 Maven project from scratch with Java 21 (JDK 17+).
  - Generated Maven wrapper (`mvnw` / `mvnw.cmd`) using Apache Maven 3.9.9.
  - Configured file-based H2 database per node (`./data/node-<id>/dms_db`).
  - Implemented `TimeoutClientSocketFactory` per SPEC Section 5 (2s connect timeout, 4s read timeout).
  - Implemented `RemoteDonationService` contract with `ping()`, `process()`, `commit()`, `replicate()`, and `branchTotal()`.
  - Implemented `LamportClock` with thread-safe atomic clock operations.
  - Implemented `EventLogger` with standard tag format (`[EXPn-TAG] node=<id> lamport=<n> message`) and SSE broadcasting.
  - Implemented `NodeRegistry` with 2s peer heartbeats and 6s failure timeout.
  - Implemented REST endpoints: `POST /api/donate`, `GET /api/cluster`, `GET /api/events` (SSE), `GET /api/ledger`, and `GET /api/branch-total`.
  - Built web dashboard with Cluster Topology tab, Exp 1 RMI tab (Donor Client + NGO Client + Local Ledger), and persistent live event stream.
  - Created operational scripts: `run-node.bat`, `run-node.sh`, `demo-local.bat`, `demo-local.sh`, `stop-demo-local.bat`, `stop-demo-local.sh`.
  - Created smoke testing scripts with `--local` and two-machine modes: `smoke-two-machine.bat`, `smoke-two-machine.ps1`, `smoke-two-machine.sh`.

- **P2: Exp 6 (Load Balancing)** [COMPLETED]
  - Implemented `LoadBalancer` interface and strategy implementations:
    - `LeastConnectionsLoadBalancer` (default): selects candidate node with minimum active connections; uses round-robin tie breaking for even initial distribution.
    - `RoundRobinLoadBalancer`: cycles evenly through candidate nodes using an atomic index.
  - Implemented `LoadBalancingService`:
    - Dynamic strategy selector (`LEAST_CONNECTIONS` / `ROUND_ROBIN`) via `POST /api/lb/strategy`.
    - Configurable artificial processing delay via `POST /api/lb/delay` and `DonationRequest.delayMs`.
    - Real-time in-flight connection tracking at gateway (`InFlightHandle`) combined with node active connections.
    - Standardized logging under `[EXP6-LB]` tag with node ID and Lamport timestamp.
  - Integrated load balancing into `POST /api/donate`: auto-routes incoming requests through the load balancer when `targetNodeId` is omitted.
  - Implemented burst execution endpoint `POST /api/lb/burst`: runs concurrent overlapping requests across cluster with per-request latency tracking and distribution statistics.
  - Added "Exp 6: Load Balancing" tab to the web dashboard:
    - Interactive strategy selector (Least Connections vs Round Robin).
    - Delay configuration input.
    - "Send N requests" burst generator.
    - Live active-connection gauge cards per cluster node.
    - Detailed handled-by table displaying Node ID, Host IP, OS, Thread Name, Lamport TS, and Latency.
  - Created automated test scripts: `test-p2-load-balancer.bat`, `test-p2-load-balancer.ps1`, `test-p2-load-balancer.sh`.

- **P3: Bully Election (Exp 4), Replication (Exp 5), Fault Tolerance (Exp 8)** [COMPLETED]
  - Implemented `ElectionAlgorithm` interface and `BullyElection` component:
    - Initiates election to higher node IDs via RMI `electionMsg()`.
    - If higher answers OK, waits for coordinator announcement; if none answers, becomes Coordinator/Primary and broadcasts `coordinatorMsg()`.
    - Real-time `[EXP4-ELECT]` event emission and live message trace.
  - Implemented Replication and Consistency (`ReplicationService`):
    - Full write path: Gateway -> Processing Node (validates, stamps Lamport, generates receipt) -> Primary `commit()` via RMI -> Primary assigns `seq` and replicates to backups -> returns receipt with processing node AND primary node ID.
    - `SYNCHRONOUS` mode: Primary confirms only after all live backups acknowledge; rejection if required backup fails.
    - `ASYNCHRONOUS` mode: Confirms immediately, replicates in background after configurable delay.
    - Deterministic SHA-256 ledger hashing over non-isolated records sorted by canonical sequence number.
    - Cluster consistency checker comparing hashes and counts across all active nodes.
    - Deterministic stale-read demonstration (`POST /api/repl/stale-read-demo`): immediate read shows backup lag, post-delay read shows eventual consistency.
  - Implemented Fault Tolerance (`NodeRegistry`):
    - Heartbeat failure detection (2s interval, 6s timeout).
    - Automatic Bully election triggered when heartbeat detects primary failure.
    - Simulated crash toggle (`POST /api/ft/crash`, `POST /api/ft/resume`): node stops answering RMI and heartbeats while JVM remains running.
    - Snapshot rejoin (`POST /api/ft/sync`): restarted or resumed nodes retrieve full non-isolated snapshot from primary and rebuild local database.
  - Added Web Dashboard UI Tabs:
    - Exp 4: Bully Election tab (trigger button, status cards, algorithm guide, message trace box).
    - Exp 5: Replication tab (mode toggle, delay input, stale-read demo, consistency check table, and mandatory failure-assumption help text).
    - Exp 8: Fault Tolerance tab (local node state, simulate crash & resume buttons, manual snapshot sync, heartbeat telemetry table).
  - Created End-to-End Test Suite: `test-p3-e2e.ps1`, `test-p3-e2e.bat`, and `test-p3-e2e.sh`.
    - Check (1): Cross-node RMI routing verified (Gateway 1 -> Processing Node 2 -> Primary 3).
    - Check (2): Synchronous replication consistency verified (identical SHA-256 ledger hash across all 3 nodes).
    - Check (3): Primary process kill -> heartbeat timeout (>6s) -> Bully election failover to Node 2 -> post-failover write confirmed -> pre-kill confirmed donation intact in ledger.
    - Gate 2 MET.

- **P4: Multithreading (Exp 2), Clock Synchronization (Exp 3), Distributed MapReduce (Exp 7)** [COMPLETED]
  - Implemented Multithreading (`ThreadPoolService`):
    - Configurable `ThreadPoolExecutor` with named worker threads (`dms-worker-%d`).
    - Dynamic pool resizing (`setCorePoolSize`) and metrics reporting (core size, active threads, queue length, completed tasks).
    - Task burst runner (`runBurst`) returning per-task worker thread assignments and latency.
    - Race condition demonstration (`runRaceDemo`) using `CyclicBarrier` and brief context switch delays on an unprotected counter, demonstrating lost updates under concurrency vs 100% exact increments using `AtomicInteger`.
  - Implemented Clock Synchronization (`ClockSyncService`):
    - Lamport logical clock integration with virtual process physical clock (`System.currentTimeMillis() + artificialSkew + physicalClockOffset`).
    - Adhered strictly to the Operating System Protection Rule: the host OS clock is never altered.
    - Berkeley physical clock synchronization algorithm (`runBerkeleySync`): coordinator collects clock readings from all active nodes via RMI (`clockMillis()`), computes the cluster average, and dispatches relative $\Delta_i$ offsets via RMI (`applyOffset()`).
    - Artificial skew injection (`setArtificialSkew`) and clock reset endpoints.
  - Implemented Distributed MapReduce (`MapReduceService`):
    - Pure Java distributed MapReduce calculation of "total donation per branch" over the non-isolated ledger.
    - Deterministic sharding formula: donation belongs to a node if `donationId % liveCount == indexOf(node in sorted live ids)` (using `Math.floorMod`).
    - Map Phase: runs on every live node over its assigned shard via RMI `mapPhase()`.
    - Shuffle Phase: groups intermediate shard totals by branch.
    - Reduce Phase: coordinator aggregates grouped branch totals.
    - Primary SQL Verification: coordinator queries direct SQL SUM (`donationRepository.sumByBranch()`) on Primary and verifies 100% equality against MapReduce totals.
  - Web UI Dashboard Updates:
    - Enabled Exp 2 (Multithreading), Exp 3 (Clocks), and Exp 7 (MapReduce) tabs.
    - Added ThreadPool telemetry gauges, resize controls, burst task generator, and race demo comparison display.
    - Added Lamport & virtual physical clock gauges, skew injector, and Berkeley synchronization results table.
    - Added MapReduce shard breakdown table, phase outputs (Map & Shuffle), and MapReduce vs Primary SQL verification comparison table.
    - Added `EXP2-THREAD` and `EXP7-MR` filters to live log stream.
  - Automated Testing:
    - Created `MultithreadingAndClockAndMapReduceTest`: 5 unit tests validating thread naming, burst execution, race demo lost updates, virtual clock skew/offset math, single-node Berkeley sync, and MapReduce SQL SUM verification.
    - Created `test-p4-e2e.ps1`, `test-p4-e2e.bat`, and `test-p4-e2e.sh`: executes Checks (1)-(4) end-to-end on a live 3-node cluster.
    - Gate 3 MET.

## Tests Passing
- `com.dms.DmsApplicationTests`: Spring Boot context initialization passed.
- `com.dms.LoadBalancerTest`:
  - `testRoundRobinCyclesEvenly`: Passed (12 requests cycle exactly 4, 4, 4 across 3 nodes).
  - `testLeastConnectionsSelectsMinimum`: Passed (identifies node with fewest active connections).
  - `testLeastConnectionsSpreadsOverlappingRequests`: Passed (12 overlapping requests spread 4, 4, 4).
  - `testLoadBalancingServiceStrategyAndDelayConfiguration`: Passed.
- `com.dms.RmiDonationServiceTest`:
  - `testRmiLookupAndPing`: Passed.
  - `testProcessValidDonation`: Passed.
  - `testValidationRejection`: Passed.
- `com.dms.ElectionAndReplicationTest`:
  - `testBullyElectionBasicContract`: Passed.
  - `testReplicationModeAndDelayConfiguration`: Passed.
  - `testLedgerHashConsistency`: Passed (SHA-256 deterministic 64-char hex hash).
  - `testSimulatedCrashRejectsRmiCalls`: Passed.
  - `testReplicatePreservesSequenceAndReceipt`: Passed.
- `com.dms.MultithreadingAndClockAndMapReduceTest`:
  - `testThreadPoolNamingAndBurstExecution`: Passed (`dms-worker-*` named threads, latency tracking).
  - `testRaceConditionDemoLostUpdates`: Passed (lost updates observed on unsafe counter; AtomicInteger exact).
  - `testVirtualClockSkewAndOffsetCalculations`: Passed (virtual time = system + skew + offset).
  - `testBerkeleySyncSingleNodeCoordinator`: Passed (computes average and relative offsets).
  - `testMapReduceMatchesSqlSum`: Passed (pure Java MapReduce matches SQL SUM exactly across all branches).
- `test-p4-e2e.bat` / `test-p4-e2e.ps1` (3 local nodes on ports 8080, 8081, 8082):
  - Check 1 (Cross-node RMI routing) Passed.
  - Check 2 (Synchronous replication consistency) Passed.
  - Exp 2 & Exp 3 (ThreadPool burst, race demo, Berkeley clock sync) Passed.
  - Check 3 (Primary process kill, Bully election failover to Node 2, post-failover write confirmed, pre-kill donation intact) Passed.
  - Check 4 (Distributed MapReduce == Primary SQL SUM 100% verified across all live shards) Passed.
- `test-p2-load-balancer.bat`:
  - Regression re-run passed (Round Robin 4, 4, 4; Least Connections 4, 4, 4).
- `smoke-two-machine.bat --local`:
  - Regression re-run passed (cross-node RMI, receipt fields, NGO query).

## Decisions Made
1. **In-Flight Connection Tracking**: Built an `InFlightHandle` in `LoadBalancingService` so the gateway tracks concurrent requests in-flight to each candidate node. This ensures `Least Connections` makes instantaneous, optimal distribution decisions for concurrent requests before heartbeat intervals.
2. **Artificial Delay**: Supported both global node delay (`POST /api/lb/delay`) and per-request delay (`DonationRequest.delayMs`), applied during `RemoteDonationServiceImpl.process()` while `activeConnections` is incremented.
3. **Demo Isolation**: Burst requests generated by Exp 6 use `isolated = true` per SPEC Section 3, ensuring demo bursts do not contaminate regular ledger consistency checks.
4. **H2 AUTO_SERVER**: Added `AUTO_SERVER=TRUE` to JDBC URL to ensure multi-connection resilience and prevent connection lock issues during multi-node concurrent executions.
5. **Canonical Distributed Ledger Hashing**: Computed SHA-256 over records ordered by canonical primary sequence number (`seq`), hashing `seq:donor:amount:branch:receiptId`. This eliminates sensitivity to local auto-increment primary key discrepancies across nodes.
6. **Replication & Snapshot Rejoin Persistence**: Sanitized transient entity primary keys on backup replication and snapshot rejoin (`saveAll`), allowing JPA/Hibernate to insert clean records with local database keys without detached entity merge conflicts.
7. **Receipt Tracking**: Added `receiptId` getter and populated constructor to `Receipt` to allow client-side tracking and assertion of individual donation receipts without altering mandatory fixed contract fields.
8. **Deterministic Stale-Read Demo**: Implemented dedicated endpoint `POST /api/repl/stale-read-demo` and UI action demonstrating step 1 (immediate read on backup - stale data observed) and step 2 (post-delay read - eventual consistency achieved).
9. **Primary Process Kill**: Used TCP port binding process lookup in PowerShell to cleanly terminate the JVM process running the primary node, accurately testing heartbeat timeout (>6s) and Bully election failover.
10. **OS Clock Protection Rule**: Process clock virtualization (`virtualMillis = systemMillis + artificialSkew + physicalClockOffset`) ensures Java process never mutates or requires OS-level physical clock changes.
11. **FloorMod for Sharding**: Used `Math.floorMod(dId, liveCount)` instead of `%` to prevent negative indices if negative donation IDs or hashes were ever encountered.
12. **Reproducible Race Conditions**: Synchronized worker threads at a `CyclicBarrier` followed by a 2ms sleep to reliably force thread context switches and reproduce lost updates under concurrent read-modify-write patterns.
13. **Flexible HTTP Parameter Parsing**: In `ApiController`, parsed request payload via `HttpServletRequest` and Jackson fallback so endpoints transparently accept either JSON request bodies or URL query parameters without throwing HTTP 415.

## Known Bugs
None.

## Exact Next Step
Phase P5: Exps 9, 10 (MPI-style collectives and parallel matrix multiplication).
