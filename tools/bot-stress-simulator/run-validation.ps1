[CmdletBinding()]
param(
    [ValidateRange(1, 1000)]
    [int]$Bots = 200,
    [ValidateRange(1, 600)]
    [int]$DurationSeconds = 15,
    [ValidateRange(0, 60000)]
    [int]$RampMilliseconds = 25,
    [switch]$ForceBlocked
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$toolRoot = Split-Path -Parent $PSCommandPath
$clientDirectory = Join-Path $toolRoot 'client'
$env:CITADEL_STRESS_DURATION_SECONDS = $DurationSeconds.ToString()
if ($ForceBlocked) {
    $env:CITADEL_STRESS_FORCE_BLOCKED = '1'
} else {
    Remove-Item Env:CITADEL_STRESS_FORCE_BLOCKED -ErrorAction SilentlyContinue
}
$answers = @(
    $Bots.ToString(),
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
