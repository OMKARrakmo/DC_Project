\# SPEC.md: Distributed Donation Management System (DMS)

\#\# 0\. Working rules (every phase)  
1\. Do ONLY the phase you are given. Never start a later phase. After finishing, print ONE status line and STOP.  
2\. At the start of a phase read SPEC.md and PROGRESS.md (if it exists). At the end update PROGRESS.md: phases done, tests passing, decisions made, known bugs, exact next step.  
3\. Ask no questions. Decide yourself and record the decision in PROGRESS.md under "Decisions".  
4\. Build and run the phase tests. Fix failures before finishing. Never claim a test passed unless you ran it and saw it pass. Never fake results. Use commands valid for the OS you are running on.  
5\. Do not refactor finished phases unless a test fails.  
6\. GIT (the user's own laptop): at the end of each phase, after build and tests pass, run \`git init\` (first time only), \`git add \-A\`, \`git commit \-m "P\<n\>: \<short summary\>"\`. NEVER run git push, git config, git remote, reset, rebase, or force operations. Never commit while tests fail.  
7\. Educational scope: no authentication, TLS, Docker, Kubernetes, Node or npm tooling.

\#\# 1\. Goal  
ONE Spring Boot "Donation Management System (DMS)" that, as one integrated system, demonstrates these 10 lab experiments in a web dashboard:  
1 Java RMI client-server | 2 Multithreading | 3 Clock synchronization (logical \+ physical) | 4 Bully and Ring election | 5 Replication and consistency models | 6 Load balancing | 7 MapReduce | 8 Fault tolerance with primary-backup | 9 MPI Broadcast/Scatter/Gather | 10 Parallel matrix multiplication with MPI.

MOST IMPORTANT: it must run on TWO machines (Windows \+ Ubuntu, same mobile hotspot). A request made in the UI on machine A must be processed and answered by machine B. The response must show the ACTUAL processing node id, host IP, OS, thread name and Lamport timestamp, and machine B's UI/log must show the same event.

\#\# 2\. Stack, project bootstrap, layout  
\- Java 17+, Spring Boot 3.3.x or newer 3.x, Maven, ONE runnable jar named dms.jar. Target machines have only a JDK 17+.  
\- No project exists yet. In P1 create it from scratch: check \`java \-version\` is 17+ (if not, report and stop). Create pom.xml with spring-boot-starter-web, spring-boot-starter-data-jpa, h2, spring-boot-starter-test, and finalName \`dms\`. If neither \`mvn\` nor \`mvnw\` exists, download Apache Maven 3.9.9 (https\://archive.apache.org/dist/maven/maven-3/3.9.9/binaries/apache-maven-3.9.9-bin.zip) into \`.tools/\` (add \`.tools/\` to .gitignore), then run \`mvn wrapper:wrapper\` so \`mvnw\` / \`mvnw.cmd\` exist. Use the wrapper from then on.  
\- Database: H2 in file mode, one DB file per node, Spring Data JPA. No other database.  
\- Frontend: plain HTML \+ vanilla JS in src/main/resources/static (index.html, app.js, style.css). No Node, npm, Vite, React or bundler. One tab per experiment plus a "Cluster" tab. A live event log (Server-Sent Events) is visible on every tab. Plain and functional, not stylish. Create the frontend in P1 and add each experiment's tab in the phase that implements it.  
\- Packages under com.dms: core (config, node registry, Lamport clock, event log), rmi, lb, election, repl, mr, mpi, web.  
\- Config per node (CLI args / env / application.yml): node.id (integer, higher wins elections), node.host (LAN IP, also used as java.rmi.server.hostname), server.port (default 8080), rmi.port (default 1099), peers (comma list host:serverPort:rmiPort).  
\- Scripts: run-node.sh, run-node.bat, demo-local.sh (3 nodes on one machine, different ports).  
\- A node's UI talks only to its own node. Cluster-wide calls go through that node's /api/cluster/\*\* endpoints (no CORS).

\#\# 3\. Domain  
Donation(id, donor, amount, branch, lamportTs, receiptId, handledBy, status, seq, isolated). Branches: Mumbai, Pune, Chennai. Validation: amount \>= 100 and known branch. Data created by demos (race counter, thread bursts) is flagged isolated=true and excluded from ledger hashes, consistency checks and MapReduce-vs-SQL checks.

\#\# 4\. Fixed contracts (do not rename)  
Receipt { processingNodeId, host, os, threadName, lamportTs, primaryId, status, message }

RemoteDonationService extends java.rmi.Remote (every method throws RemoteException; extend if needed but keep these):  
\- NodeInfo ping()  
\- Receipt process(DonationRequest r)  // processing node: validate, issue receipt, stamp Lamport, send to the primary  
\- Receipt commit(Donation d)          // primary: append to ledger, replicate to backups  
\- void replicate(Donation d)          // backup  
\- BigDecimal branchTotal(String branch)  
\- void electionMsg(int fromId); void coordinatorMsg(int newCoordinatorId)  
\- long clockMillis(); void applyOffset(long millis)  
\- Map\<String,BigDecimal\> mapPhase(List\<Integer\> liveSortedIds)  
\- List\<Donation\> snapshot()

REST: POST /api/donate (gateway entry, returns Receipt JSON), GET /api/cluster (nodes, alive, coordinator, active connections, OS, IP), GET /api/events (SSE), GET /api/ledger. Add more as needed.

Every log line: \`\[EXPn-TAG\] node=\<id\> lamport=\<n\> message\` with tags EXP1-RMI, EXP2-THREAD, EXP3-CLOCK, EXP4-ELECT, EXP5-REPL, EXP6-LB, EXP7-MR, EXP8-FT, EXP9-MPI, EXP10-MPI.

\#\# 5\. RMI timeouts (mandatory: a hung call would freeze failover)  
Java RMI has no simple outbound connect timeout. Export objects with a custom serializable client socket factory and use it for registry lookups too:

public class TimeoutClientSocketFactory implements RMIClientSocketFactory, Serializable {  
    public Socket createSocket(String host, int port) throws IOException {  
        Socket s \= new Socket();  
        s.connect(new InetSocketAddress(host, port), 2000);  // connect timeout  
        s.setSoTimeout(4000);                                 // read timeout  
        return s;  
    }  
}  
// export:   UnicastRemoteObject.exportObject(impl, 0, new TimeoutClientSocketFactory(), null);  
// registry: LocateRegistry.createRegistry(rmiPort); lookups via LocateRegistry.getRegistry(host, rmiPort, new TimeoutClientSocketFactory())

Total remote-call time must stay below the 6s heartbeat failure timeout. Call System.setProperty("java.rmi.server.hostname", node.host) BEFORE exporting anything.

\#\# 6\. Experiment specifications  
Exp 1 RMI. All node-to-node processing uses RMI, not REST. UI: "Donor client" form and "NGO client" branch-total query, with the call path A \-\> B shown in the trace.

Exp 2 Multithreading. A ThreadPoolExecutor (configurable size) processes requests. UI "Burst N donations" shows thread names, queue size and totals. Race demo on an isolated counter: a CyclicBarrier makes threads read, sleep, then write. Show the unsafe result (lost updates, expected vs actual), then the correct result with AtomicInteger/synchronized.

Exp 3 Clocks. Lamport clock on every event and message (max(local, received)+1). Physical: Berkeley-style. The coordinator calls clockMillis() on all nodes, averages, and sends each node an offset via applyOffset. Each node has a configurable artificial skew so before/after is visible. The OS clock is never changed (document it).

Exp 4 Election. One ElectionAlgorithm interface. Bully first: contact all higher ids; if none answers, become coordinator and broadcast coordinatorMsg. Ring is added in P5: a token circulates the ring of live nodes collecting ids; the highest wins. Once both exist the UI selects between them. The coordinator is the primary for Exps 5 and 8\. UI shows ELECTION/OK/COORDINATOR messages.

Exp 5 Replication. Write path (document in README): any node can be the gateway; the load balancer (Exp 6\) picks a processing node; that node validates and stamps Lamport, then calls commit() on the current primary; the primary appends to its ledger and replicates to backups. The response reports the processing node AND the primary. UI toggle:  
\- SYNCHRONOUS: the primary confirms only after ALL live backups acknowledge; if a required backup times out, the donation is NOT confirmed (reject and say so in the trace).  
\- ASYNCHRONOUS: confirm immediately, replicate after a configurable delay; demonstrate a stale read deterministically.  
Show replication lag, a per-node ledger hash, and a "consistency check" across nodes. State the failure assumptions in the UI help text and README: crash-stop failures only, at most one failure at a time, no network partitions, failure detection by heartbeat timeout. Under these assumptions no confirmed synchronous donation is lost on failover. This is NOT a general guarantee (partitions, simultaneous failures and split-brain break it).

Exp 6 Load balancing. Any node can be the gateway. Least Connections (default) and Round Robin, selectable. Show live active-connection counters per node and which node handled each request. A configurable artificial processing delay makes connections overlap. This is the main cross-machine demo path.

Exp 7 MapReduce. Job "total donation per branch", pure Java. The Map phase runs on EVERY live node over its own shard of the non-isolated ledger: a donation belongs to a node if donationId % liveCount \== index of that node in the sorted live ids. Shuffle groups by branch; Reduce runs on the coordinator. Verify against a direct SQL SUM on the primary (never sum all nodes' full copies). UI shows each phase per node.

Exp 8 Fault tolerance. Heartbeats every 2s, failure timeout 6s. UI "Simulate crash" (node stops answering, JVM stays up) and real process kill both work. On primary failure: detect, elect (Exp 4), promote, continue accepting donations. A restarted node rejoins as a backup and copies a FULL snapshot from the primary (no sequence-number catch-up). In async mode show that recent unreplicated writes can be lost.

Exp 9 MPI collectives. Broadcast config (branches, min amount), Scatter donation records across ranks, Gather per-rank partial branch totals to rank 0\. Show per-rank logs. Final totals must equal the Exp 7 totals.

Exp 10 Matrix multiplication. Rank 0 broadcasts B, scatters row-blocks of A, ranks multiply their blocks, Gather assembles C. DMS meaning: A \= donors x campaigns, B \= campaigns x impact weights, C \= donor impact report. UI: size 100-800, ranks 1/2/4, verification against the sequential result, timing/speedup table.

MPI honesty rule. The DEFAULT for Exps 9 and 10 is an in-JVM version where threads act as ranks with the same collective semantics. The UI badge MUST read "SIMULATED (not MPI)". Never write any UI text, log line or README sentence claiming it is real MPI. Real MPI (Open MPI Java bindings via mpiexec, Ubuntu only) is the OPTIONAL phase P7; if it exists the badge reads "REAL MPI". The default build and tests never require MPI.

\#\# 7\. Testing and gates  
\- smoke-two-machine.sh/.bat take two node IPs, submit a donation via A, and assert the processing node is B, B's host IP is not loopback and differs from A's; print OS, thread name and Lamport timestamp. A \`--local\` mode (two nodes on different ports of this machine, loopback allowed) is the only mode the agent can run itself.  
\- Core end-to-end script (3 local JVM nodes, different ports): (1) cross-node RMI routing, (2) synchronous replication consistency, (3) kill primary \-\> election \-\> failover \-\> a donation confirmed before the kill is still present, (4) MapReduce \== SQL SUM.  
\- GATE 1: after P1 run \`--local\`, then STOP and print exactly: "P1 READY: run smoke-two-machine on both laptops and reply 'go P2'".  
\- GATE 2: do not start P4 until the P3 failover test passes.

\#\# 8\. Phases  
P1 core \+ Exp 1: project bootstrap (section 2); config; node registry; Lamport clock; event logger with SSE; Donation entity and repository; RMI server/client with the section 5 factory implementing ping, process, commit, branchTotal (commit just saves locally for now); heartbeat between peers (2s); POST /api/donate and GET /api/cluster; the static dashboard with a Cluster tab, an Exp 1 tab and the event log; a donation submitted on A may be routed to any live peer via RMI so cross-node processing already works; run-node scripts; smoke-two-machine scripts with \--local. Run \`--local\`, fix failures, commit, STOP per Gate 1\.

P2 Exp 6: strategies, active-connection tracking, artificial delay, UI tab with strategy selector, delay input, "Send N requests" button, live counters and a handled-by table (node, host IP, OS, thread, Lamport). Test: 3 local nodes, 12 overlapping requests; Least Connections spreads them, Round Robin cycles evenly. Re-run smoke \--local.

P3 Bully (Exp 4), Exp 5, Exp 8 (hardest phase): election interface \+ Bully; the write path; sync/async modes with delay; ledger hash and consistency check; stale-read demo; heartbeat failure detection; Simulate crash and real kill; failover; snapshot rejoin. UI tabs: Election, Replication (with failure-assumption help text), Fault Tolerance. Create the end-to-end script with checks (1)-(3), run it, fix failures. Do not finish until the failover test passes.

P4 Exps 2, 3, 7: thread pool, burst, barrier race demo; Clocks tab with Lamport and Berkeley-style skew correction; MapReduce with sharding and SQL verification; add check (4) to the end-to-end script and re-run everything.

P5 Ring election and simulated MPI: add Ring and the UI selector; Exps 9 and 10 with simulated ranks, badge "SIMULATED (not MPI)"; add automated checks for matrix correctness and collective totals; re-run all tests.

P6 finish: dashboard cleanup (every experiment has action, trace, result, verification); README.md with overview, architecture diagram, write path, experiment-to-feature table (Exp, code location, UI tab, how to verify), Windows and Ubuntu setup (JDK 17), a TWO-MACHINE DEMO SCRIPT (find IPs, open firewall ports 8080 and the RMI port with Windows firewall commands and ufw, start nodes, check the Cluster tab, run the smoke script, walk through all 10 experiments with expected results, then kill a node mid-demo), troubleshooting (hotspot client isolation, firewall, RMI hostname), Decisions, Known limitations (Berkeley skew is simulated; heartbeat timeouts can falsely suspect slow nodes and cause split-brain; async replication can lose writes; the synchronous guarantee holds only under the stated assumptions; H2-per-node is not a production replication setup; simulated MPI is not MPI). Re-run all tests, make the final commit (no push). Finish with exactly five lines: implementation status, build status, tests, two-machine readiness, known limitations.

P7 (OPTIONAL, real MPI on Ubuntu): detect mpiexec and Open MPI Java bindings at startup. If present, run Exps 9 and 10 via real \`mpiexec \-n \<ranks\> java ...\` as a subprocess, stream output to the UI, badge "REAL MPI"; route MPI requests from non-MPI nodes to an MPI-capable peer ("executed on \<node\>"). If absent, change nothing and add Ubuntu install steps to the README. The default build and tests must still pass without MPI.  
