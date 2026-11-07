<#
.SYNOPSIS
  Two-process replication proof harness for redis-csharp (Windows).

.DESCRIPTION
  Starts a primary and a replica from a build output directory, each in
  its own data directory with the AOF enabled, then drives
  replica-proof.py against them:

    phase A (always): REPLICAOF + read-only check, mixed dataset load,
                      offset convergence, full keyspace comparison, a
                      live write wave, re-convergence, re-comparison.
    phase B (-KillPrimary): the primary process is stopped and restarted
                      on the same data directory. It replays its AOF,
                      returns with a fresh replication id, the replica
                      must full-resync, and the keyspaces must compare
                      identical again (proof --mode verify).

  Both processes are stopped on exit. Server logs land in each data
  directory (server.out.log / server.err.log) for diagnosis.

.EXAMPLE
  .\replica-proof.ps1
  .\replica-proof.ps1 -KillPrimary -BuildDir D:\build\net8.0
#>
param(
    [string]$BuildDir = (Join-Path $PSScriptRoot '..\src\RedisServer\bin\Release\net8.0'),
    [int]$PrimaryPort = 7771,
    [int]$ReplicaPort = 7772,
    [string]$WorkDir = (Join-Path $env:TEMP 'redis-replica-proof'),
    [int]$Keys = 10000,
    [switch]$KillPrimary
)

$ErrorActionPreference = 'Stop'

function Resolve-Python {
    foreach ($name in 'python', 'python3', 'py') {
        $cmd = Get-Command $name -ErrorAction SilentlyContinue
        if ($cmd) {
            if ($name -eq 'py') { return @($cmd.Source, '-3') }
            return @($cmd.Source)
        }
    }
    throw 'No Python interpreter found (tried python, python3, py).'
}

function Wait-Port([int]$Port, [int]$TimeoutSec = 60) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        try {
            $client = [System.Net.Sockets.TcpClient]::new()
            $client.Connect('127.0.0.1', $Port)
            $client.Close()
            return $true
        } catch {
            Start-Sleep -Milliseconds 250
        }
    }
    return $false
}

function Start-Redis([string]$Dir, [int]$Port) {
    $serverArgs = @('-p', "$Port", '--dir', $Dir, '--appendonly', 'yes', '--appendfsync', 'always')
    $exe = Join-Path $BuildDir 'RedisServer.exe'
    $out = Join-Path $Dir 'server.out.log'
    $err = Join-Path $Dir 'server.err.log'
    if (Test-Path $exe) {
        return Start-Process -FilePath $exe -ArgumentList $serverArgs -PassThru -WindowStyle Hidden `
            -RedirectStandardOutput $out -RedirectStandardError $err
    }
    $dll = Join-Path $BuildDir 'RedisServer.dll'
    if (-not (Test-Path $dll)) {
        throw "Neither RedisServer.exe nor RedisServer.dll found in $BuildDir - build Release first."
    }
    return Start-Process -FilePath 'dotnet' -ArgumentList (@($dll) + $serverArgs) -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput $out -RedirectStandardError $err
}

function Stop-Redis($Process, [string]$Name) {
    if ($Process -and -not $Process.HasExited) {
        Write-Host "Stopping $Name (pid $($Process.Id))"
        Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue
        $Process.WaitForExit(10000) | Out-Null
    }
}

$python = @(Resolve-Python)
$pyPreArgs = @($python | Select-Object -Skip 1)
$proofScript = Join-Path $PSScriptRoot 'replica-proof.py'
$primaryDir = Join-Path $WorkDir 'primary'
$replicaDir = Join-Path $WorkDir 'replica'

if (Test-Path $WorkDir) { Remove-Item $WorkDir -Recurse -Force }
New-Item -ItemType Directory -Path $primaryDir, $replicaDir -Force | Out-Null

$primary = $null
$replica = $null
$exitCode = 1
try {
    Write-Host "Starting primary on port $PrimaryPort (data: $primaryDir)"
    $primary = Start-Redis $primaryDir $PrimaryPort
    Write-Host "Starting replica on port $ReplicaPort (data: $replicaDir)"
    $replica = Start-Redis $replicaDir $ReplicaPort
    if (-not (Wait-Port $PrimaryPort)) { throw 'Primary did not start listening.' }
    if (-not (Wait-Port $ReplicaPort)) { throw 'Replica did not start listening.' }

    Write-Host '--- Phase A: full proof ---'
    & $python[0] @pyPreArgs $proofScript --primary-port $PrimaryPort --replica-port $ReplicaPort --keys $Keys
    if ($LASTEXITCODE -ne 0) { throw "Phase A failed (exit $LASTEXITCODE)." }

    if ($KillPrimary) {
        Write-Host '--- Phase B: kill and restart the primary ---'
        Stop-Redis $primary 'primary'
        $primary = $null
        Start-Sleep -Seconds 2
        Write-Host "Restarting primary on port $PrimaryPort (same data directory)"
        $primary = Start-Redis $primaryDir $PrimaryPort
        if (-not (Wait-Port $PrimaryPort)) { throw 'Primary did not restart.' }

        & $python[0] @pyPreArgs $proofScript --primary-port $PrimaryPort --replica-port $ReplicaPort --mode verify
        if ($LASTEXITCODE -ne 0) { throw "Phase B failed (exit $LASTEXITCODE)." }
    }

    $exitCode = 0
    Write-Host 'REPLICA PROOF HARNESS: PASS'
} catch {
    Write-Host "REPLICA PROOF HARNESS: FAIL - $($_.Exception.Message)"
    $exitCode = 1
} finally {
    Stop-Redis $primary 'primary'
    Stop-Redis $replica 'replica'
}
exit $exitCode
