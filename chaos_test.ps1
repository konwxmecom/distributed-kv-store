# Chaos testing script for the distributed KV store.
# Starts a 3-node cluster, then repeatedly kills and restarts a random node
# to verify the cluster recovers automatically (re-election + log catch-up).

$ports = @(50051, 50052, 50053)
$exePath = "D:\distributed-kv-store\hello-grpc\build\Debug\server.exe"
$processes = @{}

function Start-Node($port) {
    $peers = $ports | Where-Object { $_ -ne $port }
    $peerArgs = $peers -join " "
    $proc = Start-Process -FilePath $exePath -ArgumentList "$port $peerArgs" -PassThru -WindowStyle Normal
    $processes[$port] = $proc
    Write-Host "Started node $port (PID: $($proc.Id))" -ForegroundColor Green
}

# Start all nodes.
Write-Host "=== Starting all 3 nodes ===" -ForegroundColor Cyan
foreach ($port in $ports) {
    Start-Node $port
    Start-Sleep -Milliseconds 500
}

Write-Host "`nWaiting 8 seconds for initial leader election..." -ForegroundColor Cyan
Start-Sleep -Seconds 8

# Chaos loop: kill and restart a random node, 5 times.
for ($i = 1; $i -le 5; $i++) {
    $victimPort = Get-Random -InputObject $ports
    $victimProc = $processes[$victimPort]

    Write-Host "`n=== Round ${i}: Killing node $victimPort (PID: $($victimProc.Id)) ===" -ForegroundColor Red
    Stop-Process -Id $victimProc.Id -Force

    Start-Sleep -Seconds 4   # give the remaining nodes time to detect the failure and re-elect

    Write-Host "=== Restarting node $victimPort ===" -ForegroundColor Yellow
    Start-Node $victimPort   # restarted node should catch up via log replication

    Start-Sleep -Seconds 6   # let the cluster stabilize before the next round
}

Write-Host "`n=== Chaos test complete. All nodes should still be running. ===" -ForegroundColor Cyan