# P2 Load Balancing Test: 3 local nodes, 12 overlapping requests
# Tests Least Connections and Round Robin distribution

$ErrorActionPreference = "Stop"

function Test-HttpReady($url) {
    try {
        $resp = Invoke-RestMethod -Uri $url -Method Get -TimeoutSec 2 -ErrorAction SilentlyContinue
        return ($null -ne $resp)
    } catch {
        return $false
    }
}

Write-Host "`n========================================================" -ForegroundColor Cyan
Write-Host " RUNNING P2 LOAD BALANCING TEST (3 LOCAL NODES)" -ForegroundColor Cyan
Write-Host "========================================================" -ForegroundColor Cyan

$url1 = "http://127.0.0.1:8080"
$url2 = "http://127.0.0.1:8081"
$url3 = "http://127.0.0.1:8082"

# Check if all 3 nodes are running
$r1 = Test-HttpReady "$url1/api/cluster"
$r2 = Test-HttpReady "$url2/api/cluster"
$r3 = Test-HttpReady "$url3/api/cluster"

$started = @()
if (-not $r1 -or -not $r2 -or -not $r3) {
    Write-Host "Starting missing local nodes for 3-node cluster..." -ForegroundColor Yellow
    if (-not (Test-Path "target\dms.jar")) {
        Write-Host "Building target\dms.jar..." -ForegroundColor Yellow
        & .\mvnw.cmd package -DskipTests
    }

    if (-not $r1) {
        Write-Host "Starting Node 1 (8080, RMI 1099)..."
        $p1 = Start-Process java -ArgumentList "-jar target\dms.jar --node.id=1 --server.port=8080 --rmi.port=1099 --node.host=127.0.0.1 --peers=127.0.0.1:8081:1100,127.0.0.1:8082:1101" -PassThru -WindowStyle Hidden
        $started += $p1
    }
    if (-not $r2) {
        Write-Host "Starting Node 2 (8081, RMI 1100)..."
        $p2 = Start-Process java -ArgumentList "-jar target\dms.jar --node.id=2 --server.port=8081 --rmi.port=1100 --node.host=127.0.0.1 --peers=127.0.0.1:8080:1099,127.0.0.1:8082:1101" -PassThru -WindowStyle Hidden
        $started += $p2
    }
    if (-not $r3) {
        Write-Host "Starting Node 3 (8082, RMI 1101)..."
        $p3 = Start-Process java -ArgumentList "-jar target\dms.jar --node.id=3 --server.port=8082 --rmi.port=1101 --node.host=127.0.0.1 --peers=127.0.0.1:8080:1099,127.0.0.1:8081:1100" -PassThru -WindowStyle Hidden
        $started += $p3
    }

    Write-Host "Waiting for all 3 nodes to initialize..." -NoNewline
    for ($i = 0; $i -lt 30; $i++) {
        Start-Sleep -Seconds 1
        Write-Host "." -NoNewline
        if ((Test-HttpReady "$url1/api/cluster") -and (Test-HttpReady "$url2/api/cluster") -and (Test-HttpReady "$url3/api/cluster")) {
            Write-Host " All 3 nodes ready!" -ForegroundColor Green
            break
        }
    }
}

# Allow heartbeats to establish
Start-Sleep -Seconds 3

# Verify cluster membership
$cluster1 = Invoke-RestMethod -Uri "$url1/api/cluster" -Method Get
Write-Host "`nCluster Status from Gateway (Node 1):"
$cluster1.nodes | ForEach-Object {
    Write-Host "  Node $($_.nodeId): Host=$($_.host):$($_.serverPort), Alive=$($_.alive), Conns=$($_.activeConnections)"
}

$failed = $false

# TEST 1: Round Robin cycles evenly (12 requests across 3 nodes -> 4 each)
Write-Host "`n--- [TEST 1] Testing Round Robin Strategy (12 requests) ---" -ForegroundColor Yellow
$bodyRR = @{
    count = 12
    strategy = "ROUND_ROBIN"
    delayMs = 100
} | ConvertTo-Json

$respRR = Invoke-RestMethod -Uri "$url1/api/lb/burst" -Method Post -Body $bodyRR -ContentType "application/json"
Write-Host "Round Robin Distribution: $($respRR.distribution | ConvertTo-Json -Compress)"

$n1_rr = $respRR.distribution."1"
$n2_rr = $respRR.distribution."2"
$n3_rr = $respRR.distribution."3"

if ($n1_rr -eq 4 -and $n2_rr -eq 4 -and $n3_rr -eq 4) {
    Write-Host "ASSERTION PASSED: Round Robin cycled evenly: Node 1=$n1_rr, Node 2=$n2_rr, Node 3=$n3_rr" -ForegroundColor Green
} else {
    Write-Host "ASSERTION FAILED: Round Robin expected 4,4,4 but got Node 1=$n1_rr, Node 2=$n2_rr, Node 3=$n3_rr" -ForegroundColor Red
    $failed = $true
}

# TEST 2: Least Connections spreads overlapping requests (12 requests, 300ms delay)
Write-Host "`n--- [TEST 2] Testing Least Connections Strategy with Overlap (12 requests, 300ms delay) ---" -ForegroundColor Yellow
$bodyLC = @{
    count = 12
    strategy = "LEAST_CONNECTIONS"
    delayMs = 300
} | ConvertTo-Json

$respLC = Invoke-RestMethod -Uri "$url1/api/lb/burst" -Method Post -Body $bodyLC -ContentType "application/json"
Write-Host "Least Connections Distribution: $($respLC.distribution | ConvertTo-Json -Compress)"

$n1_lc = $respLC.distribution."1"
$n2_lc = $respLC.distribution."2"
$n3_lc = $respLC.distribution."3"

# In a balanced cluster with 12 overlapping requests and equal delay across 3 nodes, Least Connections spreads them evenly (4 each)
if ($n1_lc -eq 4 -and $n2_lc -eq 4 -and $n3_lc -eq 4) {
    Write-Host "ASSERTION PASSED: Least Connections spread overlapping requests evenly: Node 1=$n1_lc, Node 2=$n2_lc, Node 3=$n3_lc" -ForegroundColor Green
} elseif ($n1_lc -gt 0 -and $n2_lc -gt 0 -and $n3_lc -gt 0) {
    Write-Host "ASSERTION PASSED: Least Connections spread overlapping requests across all nodes: Node 1=$n1_lc, Node 2=$n2_lc, Node 3=$n3_lc" -ForegroundColor Green
} else {
    Write-Host "ASSERTION FAILED: Least Connections failed to spread requests: Node 1=$n1_lc, Node 2=$n2_lc, Node 3=$n3_lc" -ForegroundColor Red
    $failed = $true
}

# Display sample of handled-by table details
Write-Host "`nSample of Handled-By Table Records:"
$respLC.results | Select-Object -First 3 | ForEach-Object {
    $r = $_.receipt
    Write-Host "  Req #$($_.index): Node $($r.processingNodeId) ($($r.host)), OS: $($r.os), Thread: $($r.threadName), Lamport: $($r.lamportTs), Latency: $($_.latencyMs)ms"
}

if ($failed) {
    Write-Host "`n=== P2 LOAD BALANCER TESTS FAILED ===" -ForegroundColor Red
    exit 1
} else {
    Write-Host "`n=== ALL P2 LOAD BALANCER TESTS PASSED ===" -ForegroundColor Green
    exit 0
}
