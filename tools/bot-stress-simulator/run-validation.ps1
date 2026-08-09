[CmdletBinding()]
param(
    [ValidateRange(1, 1000)]
    [int]$Matches = 10,
    [ValidateRange(1, 1000)]
    [int]$UsersPerMatch = 20,
    [ValidateRange(1, 600)]
    [int]$DurationSeconds = 15,
    [ValidateRange(0, 60000)]
    [int]$RampMilliseconds = 25,
    [switch]$ForceBlocked
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if (($Matches * $UsersPerMatch) -gt 1000) {
    throw "Matches × UsersPerMatch must be at most 1000"
}

$toolRoot = Split-Path -Parent $PSCommandPath
$clientDirectory = Join-Path $toolRoot 'client'
$env:CITADEL_STRESS_DURATION_SECONDS = $DurationSeconds.ToString()
if ($ForceBlocked) {
    $env:CITADEL_STRESS_FORCE_BLOCKED = '1'
} else {
    Remove-Item Env:CITADEL_STRESS_FORCE_BLOCKED -ErrorAction SilentlyContinue
}
$answers = @(
    $Matches.ToString(),
    $UsersPerMatch.ToString(),
    '1',
    '2',
    '',
    $RampMilliseconds.ToString(),
    'n'
) -join [Environment]::NewLine

Push-Location $clientDirectory
try {
    $answers | & cargo run --release --bin citadel-bot-stress
    if ($LASTEXITCODE -ne 0) {
        throw "Stress client exited with code $LASTEXITCODE"
    }
} finally {
    Pop-Location
}
