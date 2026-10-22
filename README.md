# Distributed Donation Management System (DMS)

> A distributed systems laboratory project demonstrating 10 core distributed algorithms in a single integrated Spring Boot application. Engineered for seamless execution on a single laptop or distributed across two machines (Windows 11 + Ubuntu 22.04 on the same mobile hotspot / LAN).

---

## 1. Overview & Architecture

The **Distributed Donation Management System (DMS)** allows donors to contribute funds across regional NGO branches (Mumbai, Pune, Chennai). Incoming requests can enter through any cluster node acting as an HTTP Gateway, which utilizes dynamic load balancing to dispatch processing across cluster nodes via Java Remote Method Invocation (RMI).

A designated Primary Coordinator node serializes all non-isolated donations into a canonical ledger and replicates records to live backup nodes using either synchronous or asynchronous replication. If the Primary Coordinator fails, live nodes detect the failure via peer heartbeats and elect a new Coordinator using either the **Bully** or **Ring** election algorithm. The system also features pure Java distributed MapReduce and in-JVM simulated MPI collective communications and parallel matrix multiplication.

### Architecture Diagram

```
                              [ HTTP / Browser Client ]
                                          |
                              (REST: POST /api/donate)
                                          v
                         +----------------------------------+
                         |       Gateway Node (Node 1)      |
                         |  - Load Balancer (LC / RR)       |
                         |  - Lamport Clock Tick            |
                         +----------------------------------+
                                          |
                   [Java RMI: TimeoutClientSocketFactory (2s / 4s)]
                                          |
                                          v
                         +----------------------------------+
                         |     Processing Node (Node 2)     |
                         |  - Input Validation              |
                         |  - Generate Receipt & Issue TS   |
                         +----------------------------------+
                                          |
                             [Java RMI: commit(donation)]
                                          |
                                          v
                         +----------------------------------+
                         | Primary Coordinator Node (Node 3)|
                         |  - Assign Canonical Seq #        |
                         |  - Append to Local Ledger (H2)   |
                         +----------------------------------+
                                     /          \
            [Java RMI: replicate(d)]/            \[Java RMI: replicate(d)]
                                   v              v
                        +-------------+        +-------------+
                        | Backup Node |        | Backup Node |
                        |   (Node 1)  |        |   (Node 2)  |
                        +-------------+        +-------------+

   ========================================================================
   [Peer Heartbeats: every 2s]  <---------->  [Failure Timeout: 6s]
   [Elections: Bully / Ring]    <---------->  [Snapshot Rejoin on Recovery]
   ========================================================================
```

---

## 2. Distributed Write Path

1. **Client Submission (Gateway)**:
   - A donor submits a donation request to any cluster node via `POST /api/donate`.
   - The Gateway ticks its local Lamport clock and stamps `clientLamportTs`.
2. **Node Selection (Load Balancer)**:
   - If no specific `targetNodeId` is specified, the Gateway's `LoadBalancingService` chooses a candidate node using either **Least Connections** (factoring in-flight connections + active processing threads) or **Round Robin**.
3. **Dispatch to Processing Node (Java RMI)**:
   - If the chosen node is remote, the Gateway connects via Java RMI using `TimeoutClientSocketFactory` (2s connect timeout, 4s socket read timeout).
   - The processing node executes `RemoteDonationService.process(DonationRequest)`.
4. **Validation & Receipt Generation**:
   - The processing node verifies amount ($\ge 100$), non-empty donor name, and valid branch (Mumbai, Pune, Chennai).
   - Generates a unique receipt ID (`RCP-XXXXXXXX`), captures current operating system and thread name, and stamps a new Lamport logical timestamp.
5. **Commit to Primary Coordinator (Java RMI)**:
   - The processing node contacts the current cluster Primary Coordinator via `commit(donation)`.
   - The Primary Coordinator assigns the next monotonic canonical sequence number (`seq`) and persists the donation to its local H2 database.
6. **Replication to Backups**:
   - **SYNCHRONOUS Mode**: The Primary replicates to all live backup nodes via `RemoteDonationService.replicate(d)`. The Primary confirms only after **all** live backups acknowledge. If an acknowledgment times out, the donation is marked rejected.
   - **ASYNCHRONOUS Mode**: The Primary confirms immediately to the client and asynchronously replicates the record to backups in the background after a configurable delay (`delayMs`).
7. **Client Receipt**:
   - The receipt returns through the Gateway to the client, reporting both the `processingNodeId` and the `primaryId`.

---

## 3. Experiment-to-Feature Matrix

| Exp # | Experiment Title | Primary Code Locations | Web UI Tab | How to Verify |
|---|---|---|---|---|
| **Exp 1** | **Java RMI Client-Server** | `com.dms.rmi.RemoteDonationService`<br>`com.dms.rmi.RemoteDonationServiceImpl`<br>`com.dms.rmi.TimeoutClientSocketFactory` | **Exp 1: Java RMI** | Submit a donation from Node A with target Node B. Verify receipt displays `processingNodeId = B`, host IP, OS, thread name, and call path `Node A -> Node B [Java RMI]`. Query NGO branch total via RMI. |
| **Exp 2** | **Multithreading & Race Condition** | `com.dms.core.ThreadPoolService`<br>`com.dms.web.ApiController` | **Exp 2: Multithreading** | Resize thread pool (1-32). Click "Burst Tasks" to observe named worker threads (`dms-worker-%d`). Click "Run Race Demo" to observe lost updates on unsafe counter vs 100% exact increments on `AtomicInteger`. |
| **Exp 3** | **Clock Synchronization** | `com.dms.core.LamportClock`<br>`com.dms.core.ClockSyncService` | **Exp 3: Clocks** | Observe Lamport timestamp incrementing on every action. Inject artificial skew (+3000ms) on Node A, click "Run Berkeley Sync" on Coordinator to observe average time calculation and $\Delta_i$ offset distribution bringing clocks in sync. |
| **Exp 4** | **Bully & Ring Election** | `com.dms.election.BullyElection`<br>`com.dms.election.RingElection`<br>`com.dms.election.ElectionManager` | **Exp 4: Election (Bully/Ring)** | Switch algorithm radio button between Bully and Ring. Click "Trigger Election". In Bully, higher IDs answer OK and highest wins. In Ring, token circulates logical ring collecting IDs until circuit completes and max ID is elected. |
| **Exp 5** | **Replication & Consistency Models** | `com.dms.repl.ReplicationService`<br>`com.dms.core.DonationRepository` | **Exp 5: Replication** | In SYNCHRONOUS mode, click "Check Consistency" to verify identical SHA-256 ledger hashes across all nodes. In ASYNCHRONOUS mode, click "Run Stale-Read Demo" to observe immediate backup read returning stale data followed by eventual consistency. |
| **Exp 6** | **Load Balancing** | `com.dms.lb.LoadBalancingService`<br>`com.dms.lb.LeastConnectionsLoadBalancer`<br>`com.dms.lb.RoundRobinLoadBalancer` | **Exp 6: Load Balancing** | Select Least Connections or Round Robin. Configure artificial delay (e.g. 500ms). Click "Send 12 Requests". Verify handled-by table shows requests distributed evenly across active nodes according to selected strategy. |
| **Exp 7** | **Distributed MapReduce** | `com.dms.mr.MapReduceService`<br>`com.dms.rmi.RemoteDonationService` | **Exp 7: MapReduce** | Click "Execute MapReduce Job". View Map phase running across local node shards (`donationId % liveCount == index`), Shuffle phase grouping by branch, Reduce aggregating totals, and side-by-side verification matching direct SQL SUM on Primary. |
| **Exp 8** | **Fault Tolerance & Primary-Backup** | `com.dms.core.NodeRegistry`<br>`com.dms.rmi.RemoteDonationServiceImpl` | **Exp 8: Fault Tolerance** | Click "Simulate Crash" or kill Primary process. Heartbeat timeout (>6s) triggers election failover. Promote backup to Primary and accept new donations. Click "Resume Node" or restart node to observe full snapshot synchronization from Primary. |
| **Exp 9** | **MPI Collectives (Broadcast, Scatter, Gather)** | `com.dms.mpi.MpiCollectivesService` | **Exp 9: MPI Collectives** | Select simulated ranks (1, 2, 4, 8) and click "Execute MPI Collectives". Inspect Broadcast config, Scatter record counts, rank partial sums, Gather aggregation, and Primary SQL SUM verification. Displays "SIMULATED (not MPI)" badge. |
| **Exp 10** | **Parallel Matrix Multiplication with MPI** | `com.dms.mpi.MpiMatrixMultiplicationService` | **Exp 10: Matrix Mult** | Select size ($100 \times 100$ to $400 \times 400$) and ranks (1, 2, 4). Click "Run Parallel Matrix Mult". Verify parallel result matches sequential result ($C_{parallel} == C_{sequential}$, max difference 0.0), view $4 \times 4$ sample output, timing, speedup, and efficiency. Displays "SIMULATED (not MPI)" badge. |

---

## 4. Environment Setup (Windows & Ubuntu)

### Prerequisites
- **JDK 17 or higher** (Oracle OpenJDK, Eclipse Temurin, or Microsoft OpenJDK).
- Verify installation:
  ```bash
  java -version
  ```
  *(Output must report version 17.x or higher, e.g., OpenJDK 21)*.
- Target machines do **not** require Maven, Node.js, npm, Docker, or external databases. The self-contained Maven Wrapper (`mvnw` / `mvnw.cmd`) and embedded H2 database handle everything.

### Windows Setup
1. Clone or copy the repository to your Windows machine:
   ```powershell
   cd "C:\Users\<username>\Desktop\DC_Project"
   ```
2. Build the project executable jar:
   ```cmd
   .\mvnw.cmd package -DskipTests
   ```
3. Verify that `target\dms.jar` is generated.

### Ubuntu Setup
1. Install OpenJDK 17 if not already installed:
   ```bash
   sudo apt update
   sudo apt install -y openjdk-17-jdk
   ```
2. Clone or copy the repository to your Ubuntu machine:
   ```bash
   cd ~/DC_Project
   chmod +x mvnw *.sh
   ```
3. Build the project executable jar:
   ```bash
   ./mvnw package -DskipTests
   ```
4. Verify that `target/dms.jar` is generated.

---

## 5. Two-Machine Demo Script (Windows + Ubuntu)

Follow this step-by-step walkthrough to demonstrate the complete distributed system across two laptops connected to the same mobile hotspot or Wi-Fi network.

### Step 1: Network & IP Configuration
Connect both laptops to the same Wi-Fi / mobile hotspot.

- **On Machine A (Windows Laptop)**:
  ```powershell
  ipconfig
  ```
  *Note IPv4 Address (e.g. `192.168.43.10`)*.
- **On Machine B (Ubuntu Laptop)**:
  ```bash
  ip route get 1.1.1.1 | awk '{print $7}'
  # or: hostname -I
  ```
  *Note IPv4 Address (e.g. `192.168.43.20`)*.

### Step 2: Open Firewall Ports
Allow inbound traffic on HTTP port `8080`, RMI registry port `1099`, and dynamic RMI communication ports.

- **On Windows (Machine A - Administrator PowerShell)**:
  ```powershell
  New-NetFirewallRule -DisplayName "DMS HTTP" -Direction Inbound -LocalPort 8080 -Protocol TCP -Action Allow
  New-NetFirewallRule -DisplayName "DMS RMI" -Direction Inbound -LocalPort 1099,1100 -Protocol TCP -Action Allow
  ```
- **On Ubuntu (Machine B)**:
  ```bash
  sudo ufw allow 8080/tcp
  sudo ufw allow 1099/tcp
  sudo ufw allow 1100/tcp
  sudo ufw reload
  ```

### Step 3: Launch Cluster Nodes
Assign **Node 1** to Machine A and **Node 2** to Machine B. (Optionally run Node 3 on either machine).

- **On Machine A (Windows - Node 1)**:
  ```powershell
  java -jar target\dms.jar --node.id=1 --node.host=192.168.43.10 --server.port=8080 --rmi.port=1099 --peers=192.168.43.20:8080:1099
  ```
- **On Machine B (Ubuntu - Node 2)**:
  ```bash
  java -jar target/dms.jar --node.id=2 --node.host=192.168.43.20 --server.port=8080 --rmi.port=1099 --peers=192.168.43.10:8080:1099
  ```

### Step 4: Verify Cluster Peering
1. Open the browser on Machine A: `http://192.168.43.10:8080`
2. Open the browser on Machine B: `http://192.168.43.20:8080`
3. Click the **Cluster Topology** tab on either machine:
   - Verify both Node 1 (`192.168.43.10`) and Node 2 (`192.168.43.20`) are listed.
   - Status shows `ALIVE` with active Lamport clocks.
   - Coordinator shows Node 2 (higher ID wins initially).

### Step 5: Run Automated Cross-Machine Smoke Test
On either laptop, execute the cross-machine smoke script specifying both IP addresses:
```cmd
smoke-two-machine.bat 192.168.43.10 192.168.43.20
```
or on Ubuntu:
```bash
./smoke-two-machine.sh 192.168.43.10 192.168.43.20
```
- **Expected Assertion**: The script submits a request to Node 1, asserts it was processed by Node 2 on Ubuntu (`RMI TCP Connection`, Linux OS reported), status is `CONFIRMED`, and NGO query returns updated branch total.

---

### Step 6: Walkthrough of All 10 Experiments

#### Exp 1: Cross-Machine RMI
- On Machine A's dashboard (`http://192.168.43.10:8080`):
  1. Navigate to **Exp 1: Java RMI**.
  2. Fill form: Donor = "Ravi Kumar", Amount = 1500, Branch = Mumbai, Target Node = "Node 2 (Remote)".
  3. Click **Submit Donation via Gateway**.
  4. **Expected Result**: Receipt card appears showing `Processing Node: 2`, `Host: 192.168.43.20`, `OS: Linux`, and thread name. Call trace displays: `Node 1 -> Node 2 [Java RMI]`.
  5. In NGO Client, select "Mumbai" and click **Query Branch Total**. The RMI branch total is displayed.

#### Exp 2: Multithreading & Race Condition
- Navigate to **Exp 2: Multithreading**:
  1. Set Core Pool Size = 4, click **Update Pool Size**.
  2. Click **Burst Tasks** (8 tasks, 150ms delay).
  3. Observe worker thread log displaying tasks distributed across `dms-worker-1` through `dms-worker-4`.
  4. Under **Race Condition Demonstration**, set Threads = 10, Increments = 10 (Expected = 100). Click **Run Race Demo**.
  5. **Expected Result**: Unsafe counter shows lost updates (e.g. Actual = 10 to 45), while Safe Counter (`AtomicInteger`) shows exactly 100.

#### Exp 3: Logical & Physical Clocks
- Navigate to **Exp 3: Clocks**:
  1. Observe local Lamport logical clock counter ticking on every action.
  2. In **Inject Artificial Clock Skew**, enter `3000` ms and click **Set Skew**. Virtual time advances by +3000ms.
  3. On the Coordinator node (Node 2), click **Run Berkeley Sync**.
  4. **Expected Result**: Coordinator polls all nodes via RMI, computes cluster average, and dispatches relative offsets ($\Delta_1 \approx -1500\text{ms}$, $\Delta_2 \approx +1500\text{ms}$). Both nodes' virtual clocks synchronize. The host OS clock is completely untouched.

#### Exp 4: Bully & Ring Election
- Navigate to **Exp 4: Election**:
  1. Select **Bully Algorithm**. Click **Trigger Bully Election**.
  2. Trace shows: Node 1 sends `ELECTION` to Node 2; Node 2 answers `OK` and broadcasts `COORDINATOR (Node 2)`.
  3. Select **Ring Algorithm**. Click **Trigger Ring Election**.
  4. Trace shows: Election token circulates from Node 1 -> Node 2 -> Node 1 (circuit complete). Highest active ID (Node 2) is elected Coordinator.

#### Exp 5: Replication & Consistency
- Navigate to **Exp 5: Replication**:
  1. Ensure **SYNCHRONOUS** mode is selected. Click **Check Consistency**.
  2. Table displays identical SHA-256 ledger hashes across both nodes (`CONSISTENT`).
  3. Select **ASYNCHRONOUS** mode with 2000ms delay.
  4. Click **Run Stale-Read Demo**.
  5. **Expected Result**: Immediate read on backup node reports missing donation (stale read observed!). After 2000ms background replication delay, backup re-read reports identical data (eventual consistency confirmed).

#### Exp 6: Load Balancing
- Navigate to **Exp 6: Load Balancing**:
  1. Select **Round Robin**. Set delay = 300ms. Enter Burst Count = 12. Click **Send 12 Requests**.
  2. Handled-by table shows requests cycling evenly: 6 to Node 1, 6 to Node 2.
  3. Switch strategy to **Least Connections**. Click **Send 12 Requests**.
  4. In-flight connection tracking balances concurrent requests dynamically across both nodes.

#### Exp 7: Distributed MapReduce
- Navigate to **Exp 7: MapReduce**:
  1. Click **Execute MapReduce Job**.
  2. View **Cluster Shard Breakdown**: Shard 0 allocated to Node 1 (`donationId % 2 == 0`), Shard 1 allocated to Node 2 (`donationId % 2 == 1`).
  3. Map phase computes partial sums on each machine via RMI; Shuffle phase groups by branch; Reduce phase aggregates final totals.
  4. **Expected Result**: Side-by-side comparison table confirms MapReduce totals **exactly match** direct Primary SQL SUM with 100% verification banner.

#### Exp 8: Fault Tolerance & Process Kill Mid-Demo
- Navigate to **Exp 8: Fault Tolerance**:
  1. On Machine B (Primary Node 2), terminate the process in terminal: press `Ctrl+C` or kill process.
  2. On Machine A's dashboard, watch the live event log and cluster monitor:
     - Within 6s, Node 1's heartbeat detects Node 2 dead (`heartbeat timeout > 6s`).
     - Node 1 automatically initiates election and promotes itself to **Primary Coordinator**.
  3. Submit a new donation via Machine A: donation succeeds with `Primary: 1` (`CONFIRMED`).
  4. Pre-kill donations remain completely intact in the ledger.
  5. Restart Node 2 on Machine B. Within 2s, Node 2 detects Node 1 as Primary and issues an RMI `snapshot()` request, downloading the full ledger snapshot and restoring cluster consistency.

#### Exp 9: Simulated MPI Collectives
- Navigate to **Exp 9: MPI Collectives**:
  1. Observe explicit badge: **SIMULATED (not MPI)**.
  2. Select Ranks = 4. Click **Execute MPI Collectives**.
  3. Inspect Phase 1 Broadcast (branches & min amount), Phase 2 Scatter (record chunks across ranks), Phase 3 Compute (local rank partial sums), and Phase 4 Gather (aggregated totals).
  4. Gathered totals verify 100% against Primary SQL SUM.

#### Exp 10: Simulated MPI Parallel Matrix Multiplication
- Navigate to **Exp 10: Matrix Mult**:
  1. Observe explicit badge: **SIMULATED (not MPI)**.
  2. Select Size = $200 \times 200$, Ranks = 4. Click **Run Parallel Matrix Mult**.
  3. Rank 0 broadcasts matrix B, scatters row blocks of matrix A, ranks compute assigned blocks, and Gather reconstructs matrix C (Donor Impact Report).
  4. **Expected Result**: Verification banner asserts $C_{parallel} == C_{sequential}$ with `Max Difference = 0.0`. Speedup factor and parallel efficiency are calculated and displayed.

---

## 6. Troubleshooting & Common Pitfalls

1. **Hotspot Client Isolation**:
   - Some Android/iOS mobile hotspots enable "AP Isolation" or "Client Isolation", which prevents connected devices from communicating directly with each other.
   - **Fix**: In your phone's hotspot configuration, disable "AP Isolation" or "Device Isolation". If unavailable, connect both laptops to a local Wi-Fi router or create an ad-hoc Wi-Fi network.
   - **Verification**: Ensure `ping <other_ip>` returns replies between laptops.

2. **Firewall Blocking RMI or HTTP**:
   - Windows Defender or Ubuntu `ufw` may silently drop inbound connections on ports 8080, 1099, or RMI ephemeral ports.
   - **Fix**: Run the firewall commands listed in Section 5, Step 2. On Windows, you can temporarily set the Wi-Fi connection to "Private Network".

3. **RMI Hostname Issues (`java.rmi.server.hostname`)**:
   - If a node binds its RMI stub to `127.0.0.1` or `localhost`, remote machines will receive stubs pointing back to their own loopback address and fail with `Connection refused`.
   - **DMS Protection**: The application automatically sets `System.setProperty("java.rmi.server.hostname", node.host)` at boot prior to exporting any RMI stub. Always provide `--node.host=<LAN_IP>` when running cross-machine.

4. **Port Conflicts**:
   - If a previous test or instance is still running on port 8080 or 1099:
     - On Windows: Run `stop-demo-local.bat` or `powershell -Command "Get-Process -Id (Get-NetTCPConnection -LocalPort 8080).OwningProcess | Stop-Process -Force"`
     - On Ubuntu: Run `./stop-demo-local.sh` or `fuser -k 8080/tcp 1099/tcp`

---

## 7. Architectural Decisions & Rationale

1. **In-Flight Connection Tracking**:
   The gateway tracks active in-flight requests per node using `InFlightHandle`. This allows `Least Connections` to make instantaneous, optimal distribution decisions for concurrent requests even during high-frequency bursts before 2s heartbeat intervals sync.
2. **Deterministic Canonical Ledger Hashing**:
   Ledger SHA-256 hashes are computed over non-isolated records sorted strictly by primary sequence number (`seq`), hashing `seq:donor:amount:branch:receiptId`. This eliminates dependence on local auto-increment database primary keys across different machines.
3. **OS Clock Protection Rule**:
   All Berkeley clock offsets and skew adjustments are strictly virtualized in software (`virtualMillis = systemMillis + skew + offset`). The host operating system physical clock is never altered.
4. **Pure Java Distributed MapReduce**:
   MapReduce uses deterministic sharding (`Math.floorMod(donationId, liveCount) == nodeIndex`) executed via Java RMI, eliminating any external Hadoop/Spark dependency while demonstrating complete Map, Shuffle, and Reduce phases with SQL verification.
5. **MPI Honesty Rule**:
   All in-JVM thread-based ranks for collective operations and matrix multiplication are explicitly identified with the badge `"SIMULATED (not MPI)"` across the backend, REST endpoints, and UI dashboard.

---

## 8. Known Limitations

1. **Simulated Clock Skew**:
   Physical clock skew is injected in software via process offset variables. It demonstrates the Berkeley algorithm without requiring root privileges or mutating OS clocks.
2. **Heartbeat Timeout vs Slow Nodes**:
   Heartbeat failure detection uses a 6s threshold. Severe network latency spikes or protracted GC pauses could falsely trigger election failover (split-brain risk if partitions occur).
3. **Asynchronous Replication Durability**:
   Under asynchronous replication mode, writes confirmed on Primary before background replication occurs can be lost if the Primary crashes before flushing.
4. **Synchronous Guarantee Assumptions**:
   The synchronous no-data-loss guarantee strictly holds under the stated assumptions: crash-stop failures only, at most one failure at a time, no network partitions, and failure detection by heartbeat timeout.
5. **File-Based H2 Databases**:
   Each node maintains its own local H2 database file. This is intended for educational isolation and lab demonstrations, not production shared-storage replication.
6. **Simulated MPI**:
   Exp 9 and Exp 10 run in-JVM threads modeling MPI collective semantics (Broadcast, Scatter, Gather). They simulate MPI behavior on standard JDKs without native Open MPI C bindings.

---

## 9. Test Suite Execution

### Unit Tests
Execute the complete JUnit 5 test suite (25 unit tests across RMI, Load Balancing, Bully & Ring Election, Replication, ThreadPool, Berkeley Clocks, MapReduce, and Simulated MPI):
```cmd
.\mvnw.cmd test
```
*(On Ubuntu: `./mvnw test`)*

### End-to-End Automated Tests
Run the comprehensive multi-node integration test suite:
- **Phase 5 Comprehensive Suite (Checks 1 to 5)**:
  ```cmd
  .\test-p5-e2e.bat
  ```
  *(Tests RMI cross-routing, sync replication hash match, primary kill failover, MapReduce SQL verification, Ring election token circulation, MPI collectives, and matrix multiplication)*.
- **Load Balancer Concurrency Suite**:
  ```cmd
  .\test-p2-load-balancer.bat
  ```
- **Local Two-Node Smoke Suite**:
  ```cmd
  .\smoke-two-machine.bat --local
  ```
