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

## Tests Passing
- `com.dms.DmsApplicationTests`: Spring Boot context initialization passed.
- `com.dms.RmiDonationServiceTest`:
  - `testRmiLookupAndPing`: Passed.
  - `testProcessValidDonation`: Passed (RMI process, receipt fields, Lamport tick, branch sum).
  - `testValidationRejection`: Passed (rejected amount < 100 and unknown branch).
- `smoke-two-machine.bat --local`:
  - Cluster membership check passed (Node 1 and Node 2 alive).
  - Cross-node RMI donation submitted to Node 1 and processed on Node 2 passed.
  - All receipt assertions passed (processingNodeId == 2, status == CONFIRMED, threadName valid, OS valid, lamportTs > 0).
  - NGO branch total query over Java RMI passed.

## Decisions Made
1. **Maven Wrapper**: Downloaded Apache Maven 3.9.9 binary into `.tools/` (ignored in `.gitignore`) and ran `mvn wrapper:wrapper` to create `mvnw` and `mvnw.cmd`.
2. **Database Isolation**: Used file-mode H2 configured with URL `jdbc:h2:file:./data/node-${node.id:1}/dms_db` to guarantee complete process isolation when running multiple nodes.
3. **RMI Timeouts**: Implemented `TimeoutClientSocketFactory` with `equals()` and `hashCode()` to ensure robust connection caching and socket timeouts below the 6s failure threshold.
4. **P1 Primary Determination**: Used highest active node ID among reachable cluster nodes (to be superseded by Bully algorithm in P3).
5. **Cross-Node Routing**: `POST /api/donate` accepts optional `targetNodeId`. When target is a remote peer, Gateway looks up peer RMI stub and invokes `remoteService.process()`, returning the remote Receipt to the client with full call path trace.

## Known Bugs
None.

## Exact Next Step
Phase P2: Exp 6 Load Balancing (Least Connections & Round Robin strategies, active connection tracking, artificial delay, UI tab with strategy selector, burst generator, and handled-by table).
