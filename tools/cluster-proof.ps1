<#
.SYNOPSIS
  Three-process cluster proof harness for redis-csharp (Windows).

.DESCRIPTION
  Starts three cluster-enabled instances from a build output
  directory, each with the SAME static slot map (--cluster-slots):

    node 1 (port 7411): slots 0-5460
    node 2 (port 7412): slots 5461-10922
    node 3 (port 7413): slots 10923-16383

  then drives cluster-proof.py against them, which checks topology
  agreement, MOVED routing against locally computed CRC16 slots, a
  followable MOVED from the wrong node, a cross-slot MULTI refused
  at EXEC, and the complete keyspace union across the three nodes.

  All three processes are stopped on exit. Server logs land in each
  data directory (server.out.log / server.err.log) for diagnosis.

.EXAMPLE
  .\cluster-proof.ps1
  .\cluster-proof.ps1 -BuildDir D:\build\net8.0 -Keys 1000
#>
param(
    [string]$BuildDir = (Join-Path $PSScriptRoot '..\src\RedisServer\bin\Release\net8.0'),
    [int[]]$Ports = @(7411, 7412, 7413),
    [string]$WorkDir = (Join-Path $env:TEMP 'redis-cluster-proof'),
    [int]$Keys = 300
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

$ranges = @('0-5460', '5461-10922', '10923-16383')
$slotMap = (0..2 | ForEach-Object { "$($ranges[$_])@127.0.0.1:$($Ports[$_])" }) -join ','

function Start-Node([string]$Dir, [int]$Port) {
    $serverArgs = @('-p', "$Port", '--dir', $Dir,
                    '--cluster-enabled', 'yes', '--cluster-slots', $slotMap)
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

function Stop-Node($Process, [string]$Name) {
    if ($Process -and -not $Process.HasExited) {
        Write-Host "Stopping $Name (pid $($Process.Id))"
        Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue
        $Process.WaitForExit(10000) | Out-Null
    }
}

if ($Ports.Count -ne 3) { throw 'Exactly three node ports are required.' }

$python = @(Resolve-Python)
$pyPreArgs = @($python | Select-Object -Skip 1)
$proofScript = Join-Path $PSScriptRoot 'cluster-proof.py'

if (Test-Path $WorkDir) { Remove-Item $WorkDir -Recurse -Force }
$nodeDirs = 0..2 | ForEach-Object {
    $dir = Join-Path $WorkDir "node$($_ + 1)"
    New-Item -ItemType Directory -Path $dir -Force | Out-Null
    $dir
}

$nodes = @($null, $null, $null)
$exitCode = 1
try {
    foreach ($i in 0..2) {
        Write-Host "Starting node $($i + 1) on port $($Ports[$i]) (slots $($ranges[$i]), data: $($nodeDirs[$i]))"
        $nodes[$i] = Start-Node $nodeDirs[$i] $Ports[$i]
        if (-not (Wait-Port $Ports[$i])) { throw "Node $($i + 1) did not start listening." }
    }

    Write-Host '--- Cluster proof ---'
    & $python[0] @pyPreArgs $proofScript --ports ($Ports -join ',') --keys $Keys
    if ($LASTEXITCODE -ne 0) { throw "Cluster proof failed (exit $LASTEXITCODE)." }

    $exitCode = 0
    Write-Host 'CLUSTER PROOF HARNESS: PASS'
} catch {
    Write-Host "CLUSTER PROOF HARNESS: FAIL - $($_.Exception.Message)"
    $exitCode = 1
} finally {
    foreach ($i in 0..2) { Stop-Node $nodes[$i] "node $($i + 1)" }
}
exit $exitCode
