[CmdletBinding(SupportsShouldProcess)]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$toolRoot = Split-Path -Parent $PSCommandPath
$directories = @(
    (Join-Path $toolRoot 'client\logs'),
    (Join-Path $toolRoot 'client\reports')
)

Add-Type -AssemblyName Microsoft.VisualBasic
$recycled = 0
foreach ($directory in $directories) {
    if (-not (Test-Path -LiteralPath $directory -PathType Container)) {
        continue
    }
    foreach ($file in @(Get-ChildItem -LiteralPath $directory -File -Force)) {
        if ($PSCmdlet.ShouldProcess($file.FullName, 'Send generated stress artifact to Recycle Bin')) {
            [Microsoft.VisualBasic.FileIO.FileSystem]::DeleteFile(
                $file.FullName,
                [Microsoft.VisualBasic.FileIO.UIOption]::OnlyErrorDialogs,
                [Microsoft.VisualBasic.FileIO.RecycleOption]::SendToRecycleBin
            )
            $recycled += 1
        }
    }
}

Write-Output "Recycled generated artifacts: $recycled"
