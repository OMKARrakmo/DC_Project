param (
    [switch]$local,

    [Parameter(Position=0)]
    [string]$Arg1 = "",

    [Parameter(Position=1)]
    [string]$Arg2 = "",

    [Parameter(Position=2)]
    [string]$PortA = "8080",

    [Parameter(Position=3)]
    [string]$PortB = "8080"
)

$ErrorActionPreference = "Stop"

function Test-HttpReady($url) {
    try {
        $resp = Invoke-RestMethod -Uri $url -Method Get -TimeoutSec 2 -ErrorAction SilentlyContinue
        return ($null -ne $resp)
    } catch {
        return $false
    }
}

if ($local.IsPresent -or $Arg1 -eq "--local" -or $Arg1 -eq "-local" -or ([string]::IsNullOrWhiteSpace($Arg1) -and [string]::IsNullOrWhiteSpace($Arg2))) {
    Write-Host "`n========================================================" -ForegroundColor Cyan
    Write-Host " RUNNING SMOKE TEST IN --local MODE" -ForegroundColor Cyan
    Write-Host "========================================================" -ForegroundColor Cyan

    $urlA = "http://127.0.0.1:8080"
    $urlB = "http://127.0.0.1:8081"

    # Check if nodes are already running
    $readyA = Test-HttpReady "$urlA/api/cluster"
    $readyB = Test-HttpReady "$urlB/api/cluster"

    $startedProcs = @()
    if (-not $readyA -or -not $readyB) {
        Write-Host "Local nodes not fully running. Auto-starting Node 1 and Node 2 in background..." -ForegroundColor Yellow
        if (-not (Test-Path "target\dms.jar")) {
            Write-Host "Building target\dms.jar..." -ForegroundColor Yellow
            & .\mvnw.cmd package -DskipTests
        }

        if (-not $readyA) {
            Write-Host "Starting Node 1 (Port 8080, RMI 1099)..."
            $p1 = Start-Process java -ArgumentList "-jar target\dms.jar --node.id=1 --server.port=8080 --rmi.port=1099 --node.host=127.0.0.1 --peers=127.0.0.1:8081:1100" -PassThru -WindowStyle Hidden
            $startedProcs += $p1
        }
        if (-not $readyB) {
            Write-Host "Starting Node 2 (Port 8081, RMI 1100)..."
            $p2 = Start-Process java -ArgumentList "-jar target\dms.jar --node.id=2 --server.port=8081 --rmi.port=1100 --node.host=127.0.0.1 --peers=127.0.0.1:8080:1099" -PassThru -WindowStyle Hidden
            $startedProcs += $p2
        }

        Write-Host "Waiting for nodes to initialize..." -NoNewline
        for ($i = 0; $i -lt 30; $i++) {
            Start-Sleep -Seconds 1
            Write-Host "." -NoNewline
            if ((Test-HttpReady "$urlA/api/cluster") -and (Test-HttpReady "$urlB/api/cluster")) {
                Write-Host " Ready!" -ForegroundColor Green
                break
            }
        }
    }

    # Wait an extra second for heartbeats to exchange
    Start-Sleep -Seconds 2

    # Verify cluster state
    Write-Host "`n[Check 1] Verifying Cluster Membership via Gateway (Node 1)..."
    $cluster = Invoke-RestMethod -Uri "$urlA/api/cluster" -Method Get
    Write-Host "  Local Node: Node $($cluster.localNode.nodeId) ($($cluster.localNode.host):$($cluster.localNode.serverPort))"
    Write-Host "  Coordinator: Node $($cluster.coordinatorId)"
    Write-Host "  Nodes in Cluster: $(($cluster.nodes | ForEach-Object { "$($_.nodeId)(alive=$($_.alive))" }) -join ', ')"

    # Submit cross-node donation via RMI
    Write-Host "`n[Check 2] Submitting donation to Node 1 with targetNodeId=2 (Cross-Node RMI)..."
    $body = @{
        donor = "SmokeLocalUser"
        amount = 500
        branch = "Mumbai"
        targetNodeId = 2
        isolated = $false
    } | ConvertTo-Json

    $receipt = Invoke-RestMethod -Uri "$urlA/api/donate" -Method Post -Body $body -ContentType "application/json"

    Write-Host "`n----------------- RECEIPT DETAILS -----------------" -ForegroundColor Green
    Write-Host "  Processing Node ID : $($receipt.processingNodeId)"
    Write-Host "  Host IP            : $($receipt.host)"
    Write-Host "  Operating System   : $($receipt.os)"
    Write-Host "  Thread Name        : $($receipt.threadName)"
    Write-Host "  Lamport Timestamp  : $($receipt.lamportTs)"
    Write-Host "  Primary Node ID    : $($receipt.primaryId)"
    Write-Host "  Receipt Status     : $($receipt.status)"
    Write-Host "  Message            : $($receipt.message)"
    Write-Host "---------------------------------------------------" -ForegroundColor Green

    # Assertions
    $failed = $false
    if ($receipt.processingNodeId -ne 2) {
        Write-Host "ASSERTION FAILED: processingNodeId expected 2, got $($receipt.processingNodeId)" -ForegroundColor Red
        $failed = $true
    } else {
        Write-Host "ASSERTION PASSED: processingNodeId == 2" -ForegroundColor Green
    }

    if ($receipt.status -ne "CONFIRMED") {
        Write-Host "ASSERTION FAILED: status expected CONFIRMED, got $($receipt.status)" -ForegroundColor Red
        $failed = $true
    } else {
        Write-Host "ASSERTION PASSED: status == CONFIRMED" -ForegroundColor Green
    }

    if ([string]::IsNullOrWhiteSpace($receipt.threadName)) {
        Write-Host "ASSERTION FAILED: threadName is empty" -ForegroundColor Red
        $failed = $true
    } else {
        Write-Host "ASSERTION PASSED: threadName is '$($receipt.threadName)'" -ForegroundColor Green
    }

    if ([string]::IsNullOrWhiteSpace($receipt.os)) {
        Write-Host "ASSERTION FAILED: OS is empty" -ForegroundColor Red
        $failed = $true
    } else {
        Write-Host "ASSERTION PASSED: OS is '$($receipt.os)'" -ForegroundColor Green
    }

    if ($receipt.lamportTs -le 0) {
        Write-Host "ASSERTION FAILED: lamportTs <= 0 ($($receipt.lamportTs))" -ForegroundColor Red
        $failed = $true
    } else {
        Write-Host "ASSERTION PASSED: lamportTs > 0 ($($receipt.lamportTs))" -ForegroundColor Green
    }

    # NGO Branch query via RMI
    Write-Host "`n[Check 3] Querying NGO branch total from Node 1 targeting Node 2..."
    $ngo = Invoke-RestMethod -Uri "$urlA/api/branch-total?branch=Mumbai&targetNodeId=2" -Method Get
    Write-Host "  Call Path: $($ngo.callPath)"
    Write-Host "  Branch Total: $($ngo.total)"

    if ($failed) {
        Write-Host "`n=== SMOKE TEST FAILED ===" -ForegroundColor Red
        exit 1
    } else {
        Write-Host "`n=== ALL LOCAL SMOKE CHECKS PASSED ===" -ForegroundColor Green
        exit 0
    }
}
else {
    # Two-machine mode
    $ipA = $Arg1
    $ipB = $Arg2

    if ([string]::IsNullOrWhiteSpace($ipA) -or [string]::IsNullOrWhiteSpace($ipB)) {
        Write-Host "Usage:" -ForegroundColor Yellow
        Write-Host "  smoke-two-machine.bat --local"
        Write-Host "  smoke-two-machine.bat <NODE_A_IP> <NODE_B_IP> [NODE_A_PORT] [NODE_B_PORT]"
        exit 1
    }

    Write-Host "`n========================================================" -ForegroundColor Cyan
    Write-Host " RUNNING SMOKE TEST IN TWO-MACHINE MODE" -ForegroundColor Cyan
    Write-Host " Gateway Node A: ${ipA}:${PortA}" -ForegroundColor Cyan
    Write-Host " Expected Node B: ${ipB}:${PortB}" -ForegroundColor Cyan
    Write-Host "========================================================" -ForegroundColor Cyan

    $urlA = "http://${ipA}:${PortA}"

    Write-Host "`n[Step 1] Submitting donation via Gateway Node A ($urlA)..."
    $body = @{
        donor = "SmokeTwoMachineUser"
        amount = 750
        branch = "Mumbai"
        isolated = $false
    } | ConvertTo-Json

    $receipt = Invoke-RestMethod -Uri "$urlA/api/donate" -Method Post -Body $body -ContentType "application/json"

    Write-Host "`n----------------- RECEIPT DETAILS -----------------" -ForegroundColor Green
    Write-Host "  Processing Node ID : $($receipt.processingNodeId)"
    Write-Host "  Host IP            : $($receipt.host)"
    Write-Host "  Operating System   : $($receipt.os)"
    Write-Host "  Thread Name        : $($receipt.threadName)"
    Write-Host "  Lamport Timestamp  : $($receipt.lamportTs)"
    Write-Host "  Primary Node ID    : $($receipt.primaryId)"
    Write-Host "  Receipt Status     : $($receipt.status)"
    Write-Host "  Message            : $($receipt.message)"
    Write-Host "---------------------------------------------------" -ForegroundColor Green

    $failed = $false

    # Assert host is not loopback
    if ($receipt.host -eq "127.0.0.1" -or $receipt.host -eq "localhost" -or $receipt.host -eq "::1") {
        Write-Host "ASSERTION FAILED: Host IP is loopback ($($receipt.host))" -ForegroundColor Red
        $failed = $true
    } else {
        Write-Host "ASSERTION PASSED: Host IP is NOT loopback ($($receipt.host))" -ForegroundColor Green
    }

    # Assert host differs from A's IP
    if ($receipt.host -eq $ipA) {
        Write-Host "ASSERTION FAILED: Processing Host IP is identical to Gateway A IP ($ipA)" -ForegroundColor Red
        $failed = $true
    } else {
        Write-Host "ASSERTION PASSED: Processing Host IP differs from Gateway A ($($receipt.host) != $ipA)" -ForegroundColor Green
    }

    # Assert thread name and OS
    if ([string]::IsNullOrWhiteSpace($receipt.threadName)) {
        Write-Host "ASSERTION FAILED: threadName is empty" -ForegroundColor Red
        $failed = $true
    } else {
        Write-Host "ASSERTION PASSED: threadName is '$($receipt.threadName)'" -ForegroundColor Green
    }

    if ([string]::IsNullOrWhiteSpace($receipt.os)) {
        Write-Host "ASSERTION FAILED: OS is empty" -ForegroundColor Red
        $failed = $true
    } else {
        Write-Host "ASSERTION PASSED: OS is '$($receipt.os)'" -ForegroundColor Green
    }

    if ($receipt.lamportTs -le 0) {
        Write-Host "ASSERTION FAILED: lamportTs <= 0 ($($receipt.lamportTs))" -ForegroundColor Red
        $failed = $true
    } else {
        Write-Host "ASSERTION PASSED: lamportTs > 0 ($($receipt.lamportTs))" -ForegroundColor Green
    }

    if ($failed) {
        Write-Host "`n=== TWO-MACHINE SMOKE TEST FAILED ===" -ForegroundColor Red
        exit 1
    } else {
        Write-Host "`n=== TWO-MACHINE SMOKE TEST PASSED ===" -ForegroundColor Green
        exit 0
    }
}
