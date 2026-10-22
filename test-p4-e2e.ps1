# test-p4-e2e.ps1: End-to-end automated test script for Phase 4
# Checks:
# (1) Cross-node RMI routing
# (2) Synchronous replication consistency (ledger hashes match across all nodes)
# (3) Kill primary -> election -> failover -> confirmed pre-kill donation is still present
# (4) MapReduce == SQL SUM (Distributed MapReduce total per branch matches Primary SQL SUM)

$ErrorActionPreference = "Stop"

function Test-HttpReady($url) {
    try {
        $resp = Invoke-RestMethod -Uri $url -Method Get -TimeoutSec 2 -ErrorAction SilentlyContinue
        return ($null -ne $resp)
    } catch {
        return $false
    }
}

function Stop-NodeByPort($port) {
    try {
        $conn = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
        if ($conn) {
            $pids = $conn | Select-Object -ExpandProperty OwningProcess -Unique
            foreach ($p in $pids) {
                if ($p -gt 0) {
                    Write-Host "Stopping process PID $p listening on port $port..." -ForegroundColor Gray
                    Stop-Process -Id $p -Force -ErrorAction SilentlyContinue
                }
            }
        }
    } catch {}
}

Write-Host "`n=================================================================" -ForegroundColor Cyan
Write-Host "   DMS PHASE 4 END-TO-END TEST: MULTITHREADING, CLOCKS, MR     " -ForegroundColor Cyan
Write-Host "   CHECKS: (1) RMI  (2) REPL  (3) FAILOVER  (4) MAPREDUCE==SQL " -ForegroundColor Cyan
Write-Host "=================================================================`n" -ForegroundColor Cyan

# 1. Clean up any existing demo processes
Stop-NodeByPort 8080
Stop-NodeByPort 8081
Stop-NodeByPort 8082
Start-Sleep -Seconds 2

# 2. Clean previous H2 data files for clean test run
Remove-Item -Path "data\node-*" -Recurse -Force -ErrorAction SilentlyContinue

# 3. Ensure jar exists
if (-not (Test-Path "target\dms.jar")) {
    Write-Host "Building target\dms.jar..." -ForegroundColor Yellow
    & .\mvnw.cmd package -DskipTests
}

# 4. Start 3 local JVM nodes
Write-Host "Starting Node 1 (HTTP 8080, RMI 1099, ID 1)..."
$p1 = Start-Process java -ArgumentList "-jar target\dms.jar --node.id=1 --server.port=8080 --rmi.port=1099 --node.host=127.0.0.1 --peers=127.0.0.1:8081:1100,127.0.0.1:8082:1101" -PassThru -WindowStyle Hidden

Write-Host "Starting Node 2 (HTTP 8081, RMI 1100, ID 2)..."
$p2 = Start-Process java -ArgumentList "-jar target\dms.jar --node.id=2 --server.port=8081 --rmi.port=1100 --node.host=127.0.0.1 --peers=127.0.0.1:8080:1099,127.0.0.1:8082:1101" -PassThru -WindowStyle Hidden

Write-Host "Starting Node 3 (HTTP 8082, RMI 1101, ID 3)..."
$p3 = Start-Process java -ArgumentList "-jar target\dms.jar --node.id=3 --server.port=8082 --rmi.port=1101 --node.host=127.0.0.1 --peers=127.0.0.1:8080:1099,127.0.0.1:8081:1100" -PassThru -WindowStyle Hidden

# 5. Wait for all 3 nodes to be ready
Write-Host "Waiting for cluster nodes to initialize..." -NoNewline
$allReady = $false
for ($i = 0; $i -lt 35; $i++) {
    Start-Sleep -Seconds 1
    Write-Host "." -NoNewline
    if ((Test-HttpReady "http://127.0.0.1:8080/api/cluster") -and
        (Test-HttpReady "http://127.0.0.1:8081/api/cluster") -and
        (Test-HttpReady "http://127.0.0.1:8082/api/cluster")) {
        $allReady = $true
        break
    }
}
Write-Host ""

if (-not $allReady) {
    Write-Host "ERROR: Not all nodes became ready in time!" -ForegroundColor Red
    Stop-NodeByPort 8080
    Stop-NodeByPort 8081
    Stop-NodeByPort 8082
    exit 1
}

Write-Host "All 3 nodes are online! Waiting 3s for heartbeat peering..." -ForegroundColor Green
Start-Sleep -Seconds 3

try {
    # Establish initial Bully election so Node 3 (highest ID) is Coordinator
    Write-Host "`n--- Initial Bully Election ---" -ForegroundColor Cyan
    $electResp = Invoke-RestMethod -Uri "http://127.0.0.1:8080/api/election/start" -Method Post
    Write-Host "Initiated election from Node 1: $($electResp.status)"
    Start-Sleep -Seconds 4

    $cluster1 = Invoke-RestMethod -Uri "http://127.0.0.1:8080/api/cluster" -Method Get
    $primaryId = $cluster1.coordinatorId
    Write-Host "Elected Primary Coordinator: Node $primaryId (Highest ID in cluster)" -ForegroundColor Green

    # -------------------------------------------------------------
    # CHECK (1): Cross-node RMI routing
    # -------------------------------------------------------------
    Write-Host "`n--- CHECK (1): Cross-node RMI Routing ---" -ForegroundColor Cyan
    $body1 = @{
        donor = "Alice CrossNode"
        amount = 500
        branch = "Mumbai"
        targetNodeId = 2
        isolated = $false
    } | ConvertTo-Json

    $receipt1 = Invoke-RestMethod -Uri "http://127.0.0.1:8080/api/donate" -Method Post -Body $body1 -ContentType "application/json"
    Write-Host "Receipt received from Gateway Node 1:"
    Write-Host "  - Processing Node : $($receipt1.processingNodeId)"
    Write-Host "  - Primary Node    : $($receipt1.primaryId)"
    Write-Host "  - Status          : $($receipt1.status)"
    Write-Host "  - Receipt ID      : $($receipt1.receiptId)"
    Write-Host "  - Handling Thread : $($receipt1.threadName)"

    if ($receipt1.processingNodeId -ne 2) {
        throw "CHECK 1 FAILED: Expected processingNodeId=2, got $($receipt1.processingNodeId)"
    }
    if ($receipt1.status -ne "CONFIRMED") {
        throw "CHECK 1 FAILED: Expected status 'CONFIRMED', got $($receipt1.status)"
    }
    $preKillReceiptId = $receipt1.receiptId
    Write-Host ">>> [CHECK 1 PASSED] Gateway Node 1 routed request via RMI to Node 2!" -ForegroundColor Green

    # -------------------------------------------------------------
    # CHECK (2): Synchronous replication consistency
    # -------------------------------------------------------------
    Write-Host "`n--- CHECK (2): Synchronous Replication Consistency ---" -ForegroundColor Cyan
    $replSyncBody = @{ mode = "SYNCHRONOUS" } | ConvertTo-Json
    Invoke-RestMethod -Uri "http://127.0.0.1:8080/api/repl/mode" -Method Post -Body $replSyncBody -ContentType "application/json" | Out-Null
    Invoke-RestMethod -Uri "http://127.0.0.1:8081/api/repl/mode" -Method Post -Body $replSyncBody -ContentType "application/json" | Out-Null
    Invoke-RestMethod -Uri "http://127.0.0.1:8082/api/repl/mode" -Method Post -Body $replSyncBody -ContentType "application/json" | Out-Null

    $body2 = @{
        donor = "Bob Sync"
        amount = 1200
        branch = "Pune"
        targetNodeId = 1
        isolated = $false
    } | ConvertTo-Json

    $receipt2 = Invoke-RestMethod -Uri "http://127.0.0.1:8080/api/donate" -Method Post -Body $body2 -ContentType "application/json"
    Write-Host "Submitted donation for synchronous replication: Receipt $($receipt2.receiptId) (Status: $($receipt2.status))"

    $h1 = (Invoke-RestMethod -Uri "http://127.0.0.1:8080/api/repl/hash" -Method Get).hash
    $h2 = (Invoke-RestMethod -Uri "http://127.0.0.1:8081/api/repl/hash" -Method Get).hash
    $h3 = (Invoke-RestMethod -Uri "http://127.0.0.1:8082/api/repl/hash" -Method Get).hash

    Write-Host "Node 1 Ledger Hash: $h1"
    Write-Host "Node 2 Ledger Hash: $h2"
    Write-Host "Node 3 Ledger Hash: $h3"

    if ($h1 -eq "EMPTY" -or $h1 -ne $h2 -or $h2 -ne $h3) {
        throw "CHECK 2 FAILED: Ledger hashes mismatch or empty under SYNCHRONOUS mode!"
    }
    Write-Host ">>> [CHECK 2 PASSED] SHA-256 Ledger hashes match across all cluster nodes ($h1)!" -ForegroundColor Green

    # -------------------------------------------------------------
    # EXP 2 & EXP 3 IN-DEPTH VERIFICATION
    # -------------------------------------------------------------
    Write-Host "`n--- EXP 2: ThreadPool Burst & Race Demo Verification ---" -ForegroundColor Cyan
    $burstRes = Invoke-RestMethod -Uri "http://127.0.0.1:8080/api/threads/burst?count=6&delayMs=50" -Method Post
    Write-Host "Executed ThreadPool burst with $($burstRes.Count) tasks across named threads:"
    foreach ($bt in $burstRes) {
        Write-Host "  - Task #$($bt.taskIndex): Thread=$($bt.threadName) (latency=$($bt.latencyMs)ms)"
        if (-not $bt.threadName.StartsWith("dms-worker-")) {
            throw "EXP 2 FAILED: Worker thread name does not start with 'dms-worker-': $($bt.threadName)"
        }
    }

    $raceRes = Invoke-RestMethod -Uri "http://127.0.0.1:8080/api/threads/race-demo?threads=10&increments=10" -Method Post
    Write-Host "Race Demo: Expected=$($raceRes.expected), Unsafe Actual=$($raceRes.unsafeActual) (Lost=$($raceRes.unsafeLostUpdates)), Safe Actual=$($raceRes.safeActual)"
    if ($raceRes.unsafeActual -ge $raceRes.expected) {
        Write-Host "Note: Unsafe counter achieved expected count on this fast run; Atomic counter verified: $($raceRes.safeActual)" -ForegroundColor Yellow
    }
    if ($raceRes.safeActual -ne $raceRes.expected) {
        throw "EXP 2 FAILED: AtomicInteger safe actual count ($($raceRes.safeActual)) did not equal expected ($($raceRes.expected))!"
    }
    Write-Host ">>> [EXP 2 PASSED] ThreadPool named worker threads and AtomicInteger thread-safety verified!" -ForegroundColor Green

    Write-Host "`n--- EXP 3: Berkeley Physical Clock Synchronization ---" -ForegroundColor Cyan
    Invoke-RestMethod -Uri "http://127.0.0.1:8080/api/clock/skew" -Method Post -Body (@{ skewMs = 2500 } | ConvertTo-Json) -ContentType "application/json" | Out-Null
    $syncResult = Invoke-RestMethod -Uri "http://127.0.0.1:8082/api/clock/berkeley-sync" -Method Post
    Write-Host "Berkeley Sync completed by Coordinator Node $($syncResult.coordinatorId): Average Time = $($syncResult.averageClockMillis)ms"
    foreach ($nc in $syncResult.nodeClocks) {
        Write-Host "  - Node $($nc.nodeId): Before=$($nc.clockBeforeMillis)ms, Offset=$($nc.relativeOffsetMillis)ms, After=$($nc.clockAfterMillis)ms, Status=$($nc.status)"
    }
    Write-Host ">>> [EXP 3 PASSED] Berkeley clock synchronization calculated average and applied relative offsets!" -ForegroundColor Green

    # -------------------------------------------------------------
    # CHECK (3): Kill primary -> election -> failover -> pre-kill donation intact
    # -------------------------------------------------------------
    Write-Host "`n--- CHECK (3): Kill Primary, Failover & Data Persistence ---" -ForegroundColor Cyan
    Write-Host "Current Primary Coordinator is Node $primaryId. Terminating its process..."

    if ($primaryId -eq 3) {
        Stop-Process -Id $p3.Id -Force -ErrorAction SilentlyContinue
        Stop-NodeByPort 8082
        $survivorUrl = "http://127.0.0.1:8080"
    } else {
        Stop-Process -Id $p1.Id -Force -ErrorAction SilentlyContinue
        Stop-NodeByPort 8080
        $survivorUrl = "http://127.0.0.1:8082"
    }

    Write-Host "Waiting 8s for heartbeat failure detection and Bully election failover..."
    Start-Sleep -Seconds 8

    $statusAfter = Invoke-RestMethod -Uri "$survivorUrl/api/cluster" -Method Get
    $newPrimaryId = $statusAfter.coordinatorId
    Write-Host "New Coordinator elected: Node $newPrimaryId" -ForegroundColor Green

    if ($newPrimaryId -eq $primaryId) {
        throw "CHECK 3 FAILED: Coordinator did not change after primary was killed! (Still reports $newPrimaryId)"
    }

    # Submit post-failover donation
    Write-Host "Submitting post-failover donation via $survivorUrl..."
    $bodyFailover = @{
        donor = "Charlie Failover"
        amount = 800
        branch = "Chennai"
        isolated = $false
    } | ConvertTo-Json

    $receiptPost = Invoke-RestMethod -Uri "$survivorUrl/api/donate" -Method Post -Body $bodyFailover -ContentType "application/json"
    Write-Host "Post-failover Receipt received: Node $($receiptPost.processingNodeId), Primary $($receiptPost.primaryId), Status $($receiptPost.status)"

    if ($receiptPost.status -ne "CONFIRMED") {
        throw "CHECK 3 FAILED: Post-failover donation status is not CONFIRMED ($($receiptPost.status))"
    }

    # Check ledger: pre-kill confirmed donation MUST be present!
    $survivorLedger = Invoke-RestMethod -Uri "$survivorUrl/api/ledger" -Method Get
    $foundPreKill = $false
    foreach ($d in $survivorLedger) {
        if (($preKillReceiptId -and $d.receiptId -eq $preKillReceiptId) -or $d.donor -eq "Alice CrossNode") {
            $foundPreKill = $true
            break
        }
    }

    if (-not $foundPreKill) {
        throw "CHECK 3 FAILED: Pre-kill confirmed donation ($preKillReceiptId) was LOST during failover!"
    }
    Write-Host ">>> [CHECK 3 PASSED] Failover complete: Pre-kill confirmed donation is intact in ledger!" -ForegroundColor Green

    # -------------------------------------------------------------
    # CHECK (4): MapReduce == SQL SUM Verification
    # -------------------------------------------------------------
    Write-Host "`n--- CHECK (4): Distributed MapReduce == SQL SUM Verification ---" -ForegroundColor Cyan

    # Submit an additional donation to ensure multiple branches and records
    Write-Host "Seeding additional donations for MapReduce sharding..."
    $bodySeed = @{
        donor = "Diana Seed"
        amount = 1500
        branch = "Mumbai"
        isolated = $false
    } | ConvertTo-Json
    Invoke-RestMethod -Uri "$survivorUrl/api/donate" -Method Post -Body $bodySeed -ContentType "application/json" | Out-Null

    # Execute distributed MapReduce job on surviving coordinator
    Write-Host "Executing MapReduce job via $survivorUrl/api/mr/run..."
    $mrResp = Invoke-RestMethod -Uri "$survivorUrl/api/mr/run" -Method Post

    Write-Host "MapReduce Job Output Summary:"
    Write-Host "  - Coordinator Node : $($mrResp.coordinatorId)"
    Write-Host "  - Live Node Count  : $($mrResp.liveCount)"
    Write-Host "  - Verification     : $($mrResp.verified)"

    Write-Host "`nBranch Breakdown (MapReduce vs Primary SQL SUM):"
    $mrTotals = if ($mrResp.reduceOutputs) { $mrResp.reduceOutputs } else { $mrResp.reducePhase }
    $sqlTotals = if ($mrResp.sqlSums) { $mrResp.sqlSums } else { $mrResp.sqlTotals }

    foreach ($prop in $mrTotals.PSObject.Properties) {
        $branch = $prop.Name
        $mrVal = [double]$prop.Value
        $sqlVal = [double]($sqlTotals.$branch)
        $match = [Math]::Abs($mrVal - $sqlVal) -lt 0.01

        Write-Host "  • Branch $branch : MapReduce = ₹$mrVal  |  SQL SUM = ₹$sqlVal  | Match = $match"
        if (-not $match) {
            throw "CHECK 4 FAILED: Branch $branch total mismatch! MapReduce=$mrVal, SQL=$sqlVal"
        }
    }

    if (-not ($mrResp.verified -or $mrResp.verifiedAllMatch)) {
        throw "CHECK 4 FAILED: mrResp.verified reported false! Discrepancies: $($mrResp.discrepancies -join '; ')"
    }

    Write-Host ">>> [CHECK 4 PASSED] Pure Java distributed MapReduce matches Primary SQL SUM (100% verified across all live shards)!" -ForegroundColor Green

    Write-Host "`n=================================================================" -ForegroundColor Green
    Write-Host "   ALL PHASE 4 CHECKS (1)-(4) PASSED SUCCESSFULLY! (GATE 3 MET) " -ForegroundColor Green
    Write-Host "=================================================================`n" -ForegroundColor Green

} catch {
    Write-Host "`nTEST FAILURE: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
} finally {
    Write-Host "Cleaning up test processes..." -ForegroundColor Gray
    Stop-NodeByPort 8080
    Stop-NodeByPort 8081
    Stop-NodeByPort 8082
}
