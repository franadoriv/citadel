[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$toolRoot = Split-Path -Parent $PSScriptRoot
$repoRoot = Split-Path -Parent $toolRoot
$serverDir = Join-Path $PSScriptRoot "server"
$binary = Join-Path $repoRoot "target\release\citadel.exe"

Push-Location $repoRoot
try {
    cargo build --release
} finally {
    Pop-Location
}

if (-not (Test-Path -LiteralPath $binary)) {
    throw "No se encontró el servidor compilado en $binary"
}

New-Item -ItemType Directory -Path $serverDir -Force | Out-Null
Copy-Item -LiteralPath $binary -Destination (Join-Path $serverDir "citadel.exe") -Force
Write-Host "Servidor Windows actualizado: $serverDir\citadel.exe" -ForegroundColor Green
