# test-p3-e2e.ps1: End-to-end test script for Phase 3
# Checks:
# (1) Cross-node RMI routing
# (2) Synchronous replication consistency (ledger hashes match across all nodes)
# (3) Kill primary -> election -> failover -> confirmed pre-kill donation is still present

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
Write-Host "   DMS PHASE 3 END-TO-END TEST: BULLY, REPLICATION, FAILOVER   " -ForegroundColor Cyan
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
    Write-Host "`n--- CHECK 1: Cross-node RMI Routing ---" -ForegroundColor Cyan
    # Submit via Gateway Node 1 (8080) explicitly targeting Node 2
    $bodyRmi = @{
        donor = "Alice CrossNode"
        amount = 500
        branch = "Mumbai"
        targetNodeId = 2
        isolated = $false
    } | ConvertTo-Json

    $receipt1 = Invoke-RestMethod -Uri "http://127.0.0.1:8080/api/donate" -Method Post -Body $bodyRmi -ContentType "application/json"
    Write-Host "Receipt received from Gateway Node 1:"
    Write-Host "  - Processing Node ID : $($receipt1.processingNodeId)"
    Write-Host "  - Primary Node ID    : $($receipt1.primaryId)"
    Write-Host "  - Receipt ID         : $($receipt1.receiptId)"
    Write-Host "  - Status             : $($receipt1.status)"
    Write-Host "  - Thread             : $($receipt1.threadName)"
    Write-Host "  - Host               : $($receipt1.host)"
    Write-Host "  - Lamport TS         : $($receipt1.lamportTs)"

    if ($receipt1.processingNodeId -ne 2) {
        throw "CHECK 1 FAILED: Expected processingNodeId=2, but got $($receipt1.processingNodeId)"
    }
    if ($receipt1.status -ne "CONFIRMED") {
        throw "CHECK 1 FAILED: Expected status CONFIRMED, but got $($receipt1.status)"
    }
    if ($receipt1.lamportTs -le 0) {
        throw "CHECK 1 FAILED: Expected lamportTs > 0, but got $($receipt1.lamportTs)"
    }
    Write-Host ">>> [CHECK 1 PASSED] Cross-node RMI routing verified (Gateway 1 -> Processing Node 2 -> Primary $primaryId)" -ForegroundColor Green

    # -------------------------------------------------------------
    # CHECK (2): Synchronous replication consistency
    # -------------------------------------------------------------
    Write-Host "`n--- CHECK 2: Synchronous Replication Consistency ---" -ForegroundColor Cyan
    # Set mode to SYNCHRONOUS
    Invoke-RestMethod -Uri "http://127.0.0.1:8080/api/repl/mode" -Method Post -Body (@{mode="SYNCHRONOUS"} | ConvertTo-Json) -ContentType "application/json" | Out-Null
    Invoke-RestMethod -Uri "http://127.0.0.1:8081/api/repl/mode" -Method Post -Body (@{mode="SYNCHRONOUS"} | ConvertTo-Json) -ContentType "application/json" | Out-Null
    Invoke-RestMethod -Uri "http://127.0.0.1:8082/api/repl/mode" -Method Post -Body (@{mode="SYNCHRONOUS"} | ConvertTo-Json) -ContentType "application/json" | Out-Null

    # Submit second donation
    $bodySync = @{
        donor = "Bob Sync"
        amount = 1200
        branch = "Pune"
        targetNodeId = 1
        isolated = $false
    } | ConvertTo-Json

    $receipt2 = Invoke-RestMethod -Uri "http://127.0.0.1:8080/api/donate" -Method Post -Body $bodySync -ContentType "application/json"
    Write-Host "Synchronous Donation Confirmed: Receipt $($receipt2.receiptId) for $($receipt2.status)"

    # Verify hashes on all 3 nodes
    $h1 = Invoke-RestMethod -Uri "http://127.0.0.1:8080/api/repl/hash" -Method Get
    $h2 = Invoke-RestMethod -Uri "http://127.0.0.1:8081/api/repl/hash" -Method Get
    $h3 = Invoke-RestMethod -Uri "http://127.0.0.1:8082/api/repl/hash" -Method Get

    Write-Host "Node 1 Ledger Hash: $($h1.hash) (Count: $($h1.count))"
    Write-Host "Node 2 Ledger Hash: $($h2.hash) (Count: $($h2.count))"
    Write-Host "Node 3 Ledger Hash: $($h3.hash) (Count: $($h3.count))"

    if ($h1.hash -eq "EMPTY" -or $h1.hash -ne $h2.hash -or $h2.hash -ne $h3.hash) {
        throw "CHECK 2 FAILED: Ledger hashes do not match across cluster!"
    }
    if ($h1.count -lt 2) {
        throw "CHECK 2 FAILED: Expected at least 2 donations committed, but found $($h1.count)"
    }
    Write-Host ">>> [CHECK 2 PASSED] Synchronous replication consistency verified (All 3 nodes have matching SHA-256 hash)" -ForegroundColor Green

    # Record the confirmed pre-kill receipt ID
    $preKillReceiptId = $receipt1.receiptId
    Write-Host "`nTarget pre-kill confirmed receipt to track through failover: $preKillReceiptId" -ForegroundColor Yellow

    # -------------------------------------------------------------
    # CHECK (3): Kill primary -> election -> failover -> confirmed pre-kill donation still present
    # -------------------------------------------------------------
    Write-Host "`n--- CHECK 3: Primary Process Kill, Bully Failover & Data Persistence ---" -ForegroundColor Cyan
    Write-Host "Current Primary to kill: Node $primaryId"

    $primaryPort = switch ($primaryId) {
        1 { 8080 }
        2 { 8081 }
        3 { 8082 }
        Default { 8082 }
    }

    $survivorUrl = if ($primaryId -eq 3) { "http://127.0.0.1:8080" } else { "http://127.0.0.1:8082" }

    Write-Host "Killing Primary Node $primaryId on port $primaryPort..." -ForegroundColor Red
    Stop-NodeByPort $primaryPort

    Write-Host "Primary process terminated. Waiting 8s for heartbeat timeout (>6s) and Bully election failover..." -ForegroundColor Yellow
    Start-Sleep -Seconds 8

    # Query surviving node
    $statusAfter = Invoke-RestMethod -Uri "$survivorUrl/api/cluster" -Method Get
    $newPrimaryId = $statusAfter.coordinatorId
    Write-Host "New Coordinator elected after Primary failure: Node $newPrimaryId" -ForegroundColor Green

    if ($newPrimaryId -eq $primaryId) {
        throw "CHECK 3 FAILED: Coordinator did not change after primary was killed! (Still reports $newPrimaryId)"
    }

    # Verify dead node is marked dead
    $deadNode = $statusAfter.nodes | Where-Object { $_.nodeId -eq $primaryId }
    if ($deadNode -and $deadNode.alive) {
        Write-Host "Warning: Dead node not yet marked inactive in peers map, but coordinator switched to $newPrimaryId" -ForegroundColor Yellow
    }

    # Submit a NEW donation to the cluster via surviving node
    Write-Host "Submitting post-failover donation to surviving cluster via $survivorUrl..."
    $bodyFailover = @{
        donor = "Charlie Failover"
        amount = 800
        branch = "Chennai"
        isolated = $false
    } | ConvertTo-Json

    $receiptPost = Invoke-RestMethod -Uri "$survivorUrl/api/donate" -Method Post -Body $bodyFailover -ContentType "application/json"
    Write-Host "Post-failover Receipt received:"
    Write-Host "  - Processing Node : $($receiptPost.processingNodeId)"
    Write-Host "  - Primary Node    : $($receiptPost.primaryId)"
    Write-Host "  - Status          : $($receiptPost.status)"

    if ($receiptPost.status -ne "CONFIRMED") {
        throw "CHECK 3 FAILED: Post-failover donation could not be confirmed! Status: $($receiptPost.status)"
    }
    if ($receiptPost.primaryId -ne $newPrimaryId) {
        throw "CHECK 3 FAILED: Post-failover donation not committed by new primary! Expected $newPrimaryId, got $($receiptPost.primaryId)"
    }

    # Check ledger on surviving node: pre-kill confirmed donation MUST be present!
    $survivorLedger = Invoke-RestMethod -Uri "$survivorUrl/api/ledger" -Method Get
    $foundPreKill = $false
    foreach ($d in $survivorLedger) {
        if (($preKillReceiptId -and $d.receiptId -eq $preKillReceiptId) -or $d.donor -eq "Alice CrossNode") {
            $foundPreKill = $true
            $matchedReceipt = $d.receiptId
            break
        }
    }

    if (-not $foundPreKill) {
        throw "CHECK 3 FAILED: Pre-kill confirmed donation ($preKillReceiptId) was LOST during failover!"
    }

    Write-Host "Pre-kill confirmed donation ($matchedReceipt / Alice CrossNode) is verified present in surviving node's ledger!" -ForegroundColor Green
    Write-Host ">>> [CHECK 3 PASSED] Failover complete: Primary failure detected, Bully election elected Node $newPrimaryId, post-failover writes succeed, and pre-kill confirmed donation is intact!" -ForegroundColor Green

    Write-Host "`n=================================================================" -ForegroundColor Green
    Write-Host "   ALL PHASE 3 CHECKS (1)-(3) PASSED SUCCESSFULLY! (GATE 2 MET) " -ForegroundColor Green
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
