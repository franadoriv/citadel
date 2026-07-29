# Citadel Windows developer task runner.
#
# Usage:
#   .\make.ps1 help
#   .\make.ps1 setup
#   .\make.ps1 check
#   .\make.ps1 demo-web

[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet(
        "help",
        "setup",
        "build",
        "check",
        "fmt",
        "clippy",
        "test",
        "clean",
        "server",
        "web",
        "native",
        "demo-web",
        "demo-native",
        "demo-native2",
        "bin-benchmark",
        "benchmark-serve",
        "docs-install",
        "docs-build",
        "docs-serve",
        "unity-plugin",
        "package-windows",
        "package-windows-python",
        "package-client-unity",
        "package-client-unreal",
        "package-client-godot",
        "package-client-godot-web",
        "package-client-js",
        "package-clients-windows",
        "bin-server",
        "bin-server-python",
        "bin-client-unity",
        "bin-client-unreal",
        "bin-client-godot",
        "bin-client-godot-web",
        "bin-client-js",
        "bin-client-rust",
        "bin-clients",
        "bin-all",
        "db-up",
        "db-down",
        "db-migrate"
    )]
    [string] $Target = "help",

    [string] $Config = "examples/configs/demo.toml",
    [string] $WebDir = "examples/web-demo",
    [int] $WebPort = 8000,
    [string] $QuicAddr = "127.0.0.1:7351",
    [string] $BenchmarkDir = "bin/benchmark",
    [int] $BenchmarkWebPort = 8080,
    [int] $Wait = 3,
    [string] $DocsDir = "website",
    [string] $DocsBase = "origin/main",
    [string] $UnityPluginDir = "clients/unity/Plugins/x86_64",
    [string] $ClientsDir = "bin/clients",
    [string] $DistDir = "dist",
    [string] $PgImage = "postgres:16-alpine",
    [string] $PgContainer = "citadel-postgres",
    [int] $PgPort = 5432,
    [string] $PgUser = "citadel",
    [string] $PgPassword = "citadel",
    [string] $PgDb = "citadel",
    [string] $DatabaseUrl = "",
    [switch] $NoInstall
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSCommandPath
Set-Location $RepoRoot

function Test-Command {
    param([Parameter(Mandatory = $true)][string] $Name)

    return $null -ne (Get-Command $Name -ErrorAction SilentlyContinue)
}

function Format-NativeCommand {
    param(
        [Parameter(Mandatory = $true)][string] $FilePath,
        [string[]] $Arguments = @()
    )

    return (@($FilePath) + $Arguments) -join " "
}

function Invoke-Native {
    param(
        [Parameter(Mandatory = $true)][string] $FilePath,
        [string[]] $Arguments = @()
    )

    $display = Format-NativeCommand -FilePath $FilePath -Arguments $Arguments
    Write-Host ">> $display"
    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code ${LASTEXITCODE}: $display"
    }
}

function Start-NativeProcess {
    param(
        [Parameter(Mandatory = $true)][string] $FilePath,
        [string[]] $Arguments = @(),
        [string] $WorkingDirectory = $RepoRoot
    )

    $display = Format-NativeCommand -FilePath $FilePath -Arguments $Arguments
    Write-Host ">> start $display"
    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $FilePath
    $startInfo.WorkingDirectory = $WorkingDirectory
    $startInfo.UseShellExecute = $false
    foreach ($argument in $Arguments) {
        [void] $startInfo.ArgumentList.Add($argument)
    }

    $process = [System.Diagnostics.Process]::Start($startInfo)

    Start-Sleep -Milliseconds 250
    if ($process.HasExited) {
        throw "Process exited early with code $($process.ExitCode): $display"
    }

    return $process
}

function Stop-StartedProcesses {
    param([System.Diagnostics.Process[]] $Processes)

    foreach ($process in $Processes) {
        if ($null -ne $process -and -not $process.HasExited) {
            Write-Host ">> stop process $($process.Id)"
            Stop-Process -Id $process.Id -ErrorAction SilentlyContinue
        }
    }
}

function Add-CargoBinToPath {
    if ([string]::IsNullOrWhiteSpace($env:USERPROFILE)) {
        return
    }

    $cargoBin = Join-Path $env:USERPROFILE ".cargo\bin"
    if ((Test-Path -LiteralPath $cargoBin) -and (($env:Path -split ";") -notcontains $cargoBin)) {
        $env:Path = "$cargoBin;$env:Path"
    }
}

function Install-Rustup {
    if ($NoInstall) {
        throw "Rust is not installed. Run '.\make.ps1 setup' without -NoInstall to install it."
    }

    if (Test-Command "winget") {
        Invoke-Native -FilePath "winget" -Arguments @(
            "install",
            "--id",
            "Rustlang.Rustup",
            "--exact",
            "--source",
            "winget",
            "--accept-source-agreements",
            "--accept-package-agreements"
        )
    } else {
        $installer = Join-Path ([System.IO.Path]::GetTempPath()) "rustup-init.exe"
        Write-Host ">> winget not found; downloading rustup-init.exe"
        Invoke-WebRequest -Uri "https://win.rustup.rs/x86_64" -OutFile $installer
        Invoke-Native -FilePath $installer -Arguments @(
            "-y",
            "--default-toolchain",
            "stable",
            "--profile",
            "default"
        )
    }

    Add-CargoBinToPath
}

function Assert-RustTools {
    if (-not (Test-Command "cargo")) {
        throw "cargo was not found on PATH. Restart PowerShell or add %USERPROFILE%\.cargo\bin to PATH."
    }

    Invoke-Native -FilePath "rustc" -Arguments @("--version")
    Invoke-Native -FilePath "cargo" -Arguments @("--version")
    Invoke-Native -FilePath "cargo" -Arguments @("fmt", "--version")
    Invoke-Native -FilePath "cargo" -Arguments @("clippy", "--version")
}

function Invoke-Setup {
    Add-CargoBinToPath

    if (-not (Test-Command "cargo")) {
        Install-Rustup
    }

    Add-CargoBinToPath

    if (Test-Command "rustup") {
        if ($NoInstall) {
            Write-Host ">> -NoInstall set; verifying the existing Rust toolchain only."
        } else {
            Invoke-Native -FilePath "rustup" -Arguments @(
                "toolchain",
                "install",
                "stable",
                "--component",
                "rustfmt",
                "--component",
                "clippy"
            )
        }
    } elseif (-not $NoInstall) {
        Write-Warning "rustup was not found. Verifying existing cargo, rustfmt, and clippy commands."
    }

    Assert-RustTools
}

function Invoke-PythonModule {
    param(
        [Parameter(Mandatory = $true)][string] $Module,
        [string[]] $Arguments = @()
    )

    if (Test-Command "py") {
        Invoke-Native -FilePath "py" -Arguments (@("-3", "-m", $Module) + $Arguments)
        return
    }

    if (Test-Command "python") {
        Invoke-Native -FilePath "python" -Arguments (@("-m", $Module) + $Arguments)
        return
    }

    if (Test-Command "python3") {
        Invoke-Native -FilePath "python3" -Arguments (@("-m", $Module) + $Arguments)
        return
    }

    throw "Python was not found. Install Python 3 or use the py launcher."
}

function Invoke-Build {
    Invoke-Native -FilePath "cargo" -Arguments @("build", "--workspace")
}

function Invoke-GitLines {
    param([string[]] $Arguments = @())

    $oldErrorActionPreference = $ErrorActionPreference
    try {
        $script:ErrorActionPreference = "Continue"
        $output = & git @Arguments 2> $null
        $exitCode = $LASTEXITCODE
    } finally {
        $script:ErrorActionPreference = $oldErrorActionPreference
    }

    return [pscustomobject]@{
        ExitCode = $exitCode
        Lines = @($output)
    }
}

function Invoke-DocsCheck {
    if (-not (Test-Command "git")) {
        return
    }

    $insideWorkTree = Invoke-GitLines -Arguments @("rev-parse", "--is-inside-work-tree")
    if ($insideWorkTree.ExitCode -ne 0) {
        return
    }

    $changedFiles = @()

    $verifiedBase = Invoke-GitLines -Arguments @("rev-parse", "--verify", $DocsBase)
    if ($verifiedBase.ExitCode -eq 0) {
        $mergeBaseResult = Invoke-GitLines -Arguments @("merge-base", "HEAD", $DocsBase)
        $mergeBase = $mergeBaseResult.Lines | Select-Object -First 1
        if ($mergeBaseResult.ExitCode -eq 0 -and -not [string]::IsNullOrWhiteSpace($mergeBase)) {
            $changedResult = Invoke-GitLines -Arguments @("diff", "--name-only", "$mergeBase...HEAD")
            if ($changedResult.ExitCode -eq 0) {
                $changedFiles = @($changedResult.Lines)
            }
        }
    }

    if ($changedFiles.Count -eq 0) {
        $cachedChanges = Invoke-GitLines -Arguments @("diff", "--name-only", "--cached")
        $worktreeChanges = Invoke-GitLines -Arguments @("diff", "--name-only")
        $changedFiles = @($cachedChanges.Lines) + @($worktreeChanges.Lines)
    }

    if ($changedFiles.Count -eq 0) {
        return
    }

    $codeChanged = $false
    $docsChanged = $false

    foreach ($file in $changedFiles) {
        if (
            $file -like "src/*" -or
            $file -like "tests/*" -or
            $file -eq "Cargo.toml" -or
            $file -eq "Cargo.lock" -or
            $file -like "migrations/*" -or
            $file -like "proto/*" -or
            $file -like "crates/*"
        ) {
            $codeChanged = $true
        }

        if (
            $file -like "docs/*" -or
            $file -like "website/src/content/docs/*" -or
            $file -eq "website/README.md" -or
            $file -eq "README.md"
        ) {
            $docsChanged = $true
        }
    }

    if ($codeChanged -and -not $docsChanged) {
        throw "Code changed without documentation updates. Update docs/ or README.md, or document why docs are not required in the handoff."
    }
}

function Get-GitBash {
    $candidates = @(
        (Join-Path $env:ProgramFiles "Git\bin\bash.exe"),
        "C:\Program Files\Git\bin\bash.exe"
    ) | Select-Object -Unique

    foreach ($candidate in $candidates) {
        if (-not [string]::IsNullOrWhiteSpace($candidate) -and (Test-Path -LiteralPath $candidate)) {
            return $candidate
        }
    }

    throw "Git Bash is required for '.\\make.ps1 check'. Install Git for Windows, then rerun the command."
}

function Invoke-Check {
    # WSL's system `bash.exe` does not inherit the Windows Rust toolchain. Git
    # Bash does, so this runs the actual canonical check script rather than a
    # partial PowerShell approximation of it.
    Invoke-Native -FilePath (Get-GitBash) -Arguments @("scripts/check.sh")
}

function Get-DebugBinary {
    param([Parameter(Mandatory = $true)][string] $Name)

    $path = Join-Path $RepoRoot "target\debug\$Name.exe"
    if (-not (Test-Path -LiteralPath $path)) {
        Invoke-Build
    }

    return $path
}

function Invoke-Server {
    Invoke-Native -FilePath "cargo" -Arguments @("run", "--", "--config", $Config, "serve")
}

function Invoke-Web {
    Write-Host ">> Web demo at http://127.0.0.1:$WebPort/"
    Invoke-PythonModule -Module "http.server" -Arguments @(
        "$WebPort",
        "--directory",
        $WebDir
    )
}

function Invoke-NativeClient {
    Invoke-Native -FilePath "cargo" -Arguments @("run", "-p", "demo-client", "--", $QuicAddr)
}

function Invoke-DemoWeb {
    Invoke-Build
    $server = $null

    try {
        $serverExe = Get-DebugBinary -Name "citadel"
        $server = Start-NativeProcess -FilePath $serverExe -Arguments @("--config", $Config, "serve")
        Start-Sleep -Seconds $Wait
        Write-Host ">> Server up. Open http://127.0.0.1:$WebPort/ in your browser"
        Write-Host ">> Open two tabs to see the relay; WebSocket connects with no setup."
        Invoke-Web
    } finally {
        Stop-StartedProcesses -Processes @($server)
    }
}

function Invoke-DemoNative {
    Invoke-Build
    $server = $null

    try {
        $serverExe = Get-DebugBinary -Name "citadel"
        $server = Start-NativeProcess -FilePath $serverExe -Arguments @("--config", $Config, "serve")
        Start-Sleep -Seconds $Wait
        Write-Host ">> Server up. Launching native client ($QuicAddr)"
        $clientExe = Get-DebugBinary -Name "demo-client"
        Invoke-Native -FilePath $clientExe -Arguments @($QuicAddr)
    } finally {
        Stop-StartedProcesses -Processes @($server)
    }
}

function Invoke-DemoNative2 {
    Invoke-Build
    $server = $null
    $client = $null

    try {
        $serverExe = Get-DebugBinary -Name "citadel"
        $clientExe = Get-DebugBinary -Name "demo-client"
        $server = Start-NativeProcess -FilePath $serverExe -Arguments @("--config", $Config, "serve")
        Start-Sleep -Seconds $Wait
        $client = Start-NativeProcess -FilePath $clientExe -Arguments @($QuicAddr)
        Start-Sleep -Seconds 1
        Write-Host ">> Server + one client up. Launching the second client."
        Invoke-Native -FilePath $clientExe -Arguments @($QuicAddr)
    } finally {
        Stop-StartedProcesses -Processes @($client, $server)
    }
}

function Invoke-DocsInstall {
    Push-Location $DocsDir
    try {
        try {
            Invoke-Native -FilePath "npm" -Arguments @("ci")
        } catch {
            Write-Host ">> npm ci failed; falling back to npm install"
            Invoke-Native -FilePath "npm" -Arguments @("install")
        }
    } finally {
        Pop-Location
    }
}

function Invoke-DocsBuild {
    Invoke-Native -FilePath "cargo" -Arguments @("doc", "--no-deps", "--workspace")

    $rustdocDir = Join-Path $DocsDir "public\rustdoc"
    if (Test-Path -LiteralPath $rustdocDir) {
        Remove-Item -LiteralPath $rustdocDir -Recurse -Force
    }

    New-Item -ItemType Directory -Path $rustdocDir -Force | Out-Null
    Copy-Item -Path (Join-Path $RepoRoot "target\doc\*") -Destination $rustdocDir -Recurse -Force

    Push-Location $DocsDir
    try {
        Invoke-Native -FilePath "npm" -Arguments @("run", "build")
    } finally {
        Pop-Location
    }
}

function Invoke-DocsServe {
    Push-Location $DocsDir
    try {
        Invoke-Native -FilePath "npm" -Arguments @("run", "preview")
    } finally {
        Pop-Location
    }
}

function Invoke-UnityPlugin {
    # Build the C ABI cdylib and copy it into the Unity SDK's plugin folder
    # (git-ignored; built, not committed).
    Invoke-Native -FilePath "cargo" -Arguments @(
        "build",
        "--release",
        "-p",
        "citadel-client-ffi"
    )

    $destDir = Join-Path $RepoRoot $UnityPluginDir
    New-Item -ItemType Directory -Path $destDir -Force | Out-Null

    $dll = Join-Path $RepoRoot "target\release\citadel_client_ffi.dll"
    if (-not (Test-Path -LiteralPath $dll)) {
        throw "Expected native plugin not found: $dll. Did the release build succeed?"
    }

    Copy-Item -LiteralPath $dll -Destination $destDir -Force
    Write-Host ">> Installed citadel_client_ffi.dll -> $UnityPluginDir/"
}

function Get-CargoVersion {
    # Read the workspace/binary version from Cargo.toml. The first line-anchored
    # `version = "..."` is the [package] version (inline dependency versions are
    # not line-anchored). The developer bumps this per milestone.
    $cargoToml = Join-Path $RepoRoot "Cargo.toml"
    foreach ($line in Get-Content -LiteralPath $cargoToml) {
        if ($line -match '^\s*version\s*=\s*"([^"]+)"') {
            return $Matches[1]
        }
    }
    throw "Could not read the package version from Cargo.toml"
}

function Invoke-PackageWindows {
    # Build the server + Unity plugin DLL, stage the release layout, and zip it.
    # This is the shared definition the release CI reuses (windows-latest calls
    # this target) and the local verification path.
    $version = Get-CargoVersion
    Write-Host ">> Packaging Citadel v$version for windows-x86_64"

    Invoke-Native -FilePath "cargo" -Arguments @("build", "--release")
    Invoke-Native -FilePath "cargo" -Arguments @("build", "--release", "-p", "citadel-client-ffi")

    $pkgName = "citadel-windows-x86_64-v$version"
    $distDir = Join-Path $RepoRoot $DistDir
    $stage = Join-Path $distDir $pkgName
    $unityRoot = Join-Path $stage "clients\unity"

    if (Test-Path -LiteralPath $stage) {
        Remove-Item -LiteralPath $stage -Recurse -Force
    }
    New-Item -ItemType Directory -Path (Join-Path $unityRoot "Citadel") -Force | Out-Null
    New-Item -ItemType Directory -Path (Join-Path $unityRoot "Demo") -Force | Out-Null
    New-Item -ItemType Directory -Path (Join-Path $unityRoot "Plugins\x86_64") -Force | Out-Null

    # Server binary + editable config + quickstart README.
    $exe = Join-Path $RepoRoot "target\release\citadel.exe"
    if (-not (Test-Path -LiteralPath $exe)) {
        throw "Expected server binary not found: $exe. Did the release build succeed?"
    }
    Copy-Item -LiteralPath $exe -Destination (Join-Path $stage "citadel.exe") -Force
    Copy-Item -LiteralPath (Join-Path $RepoRoot "citadel.toml") -Destination (Join-Path $stage "citadel.toml") -Force
    Copy-Item -LiteralPath (Join-Path $RepoRoot "packaging\windows\README.md") -Destination (Join-Path $stage "README.md") -Force

    # Unity plugin: C# bindings + native DLL + import README.
    Copy-Item -Path (Join-Path $RepoRoot "clients\unity\Citadel\*.cs") -Destination (Join-Path $unityRoot "Citadel") -Force
    Copy-Item -Path (Join-Path $RepoRoot "clients\unity\Demo\*.cs") -Destination (Join-Path $unityRoot "Demo") -Force
    $dll = Join-Path $RepoRoot "target\release\citadel_client_ffi.dll"
    if (-not (Test-Path -LiteralPath $dll)) {
        throw "Expected native plugin not found: $dll. Did the release build succeed?"
    }
    Copy-Item -LiteralPath $dll -Destination (Join-Path $unityRoot "Plugins\x86_64") -Force
    Copy-Item -LiteralPath (Join-Path $RepoRoot "packaging\windows\unity-README.md") -Destination (Join-Path $unityRoot "README.md") -Force

    # Zip the staged package.
    $zipPath = Join-Path $distDir "$pkgName.zip"
    if (Test-Path -LiteralPath $zipPath) {
        Remove-Item -LiteralPath $zipPath -Force
    }
    Compress-Archive -Path $stage -DestinationPath $zipPath -Force
    Write-Host ">> Packaged $zipPath"
}

function Get-PythonHome {
    if (-not (Test-Command "python")) {
        throw "python was not found. Install CPython or activate the intended Python environment."
    }

    $pythonPrefix = & python -c "import sys; print(getattr(sys, 'base_prefix', '') or sys.prefix)"
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($pythonPrefix)) {
        throw "Could not resolve Python base_prefix from python."
    }
    return $pythonPrefix.Trim()
}

function Set-PythonRuntimeBuildEnv {
    $env:CARGO_BUILD_JOBS = "2"
    $env:RUST_TEST_THREADS = "2"
    if ([string]::IsNullOrWhiteSpace($env:PYO3_PYTHON)) {
        $env:PYO3_PYTHON = "python"
    }
    if ([string]::IsNullOrWhiteSpace($env:PYTHONHOME)) {
        $env:PYTHONHOME = Get-PythonHome
    }
}

function Copy-PythonBundle {
    param([Parameter(Mandatory = $true)][string] $Stage)

    $pythonHome = Get-PythonHome
    $lib = Join-Path $pythonHome "Lib"
    if (-not (Test-Path -LiteralPath $lib)) {
        throw "Python Lib directory not found: $lib"
    }

    $bundle = Join-Path $Stage "python"
    if (Test-Path -LiteralPath $bundle) {
        Remove-Item -LiteralPath $bundle -Recurse -Force
    }
    New-Item -ItemType Directory -Path $bundle -Force | Out-Null

    Write-Host ">> Copying CPython from $pythonHome"
    Copy-Item -LiteralPath $lib -Destination (Join-Path $bundle "Lib") -Recurse -Force
    $dllsDir = Join-Path $pythonHome "DLLs"
    if (Test-Path -LiteralPath $dllsDir) {
        Copy-Item -LiteralPath $dllsDir -Destination (Join-Path $bundle "DLLs") -Recurse -Force
    } else {
        New-Item -ItemType Directory -Path (Join-Path $bundle "DLLs") -Force | Out-Null
    }

    $sitePackages = Join-Path $bundle "Lib\site-packages"
    if (Test-Path -LiteralPath $sitePackages) {
        Remove-Item -LiteralPath $sitePackages -Recurse -Force
    }
    Get-ChildItem -LiteralPath $bundle -Directory -Recurse -Filter "__pycache__" |
        Remove-Item -Recurse -Force

    $pythonDlls = @(Get-ChildItem -LiteralPath $pythonHome -Filter "python3*.dll" -File)
    if ($pythonDlls.Count -eq 0) {
        throw "No python3*.dll found in $pythonHome"
    }
    foreach ($dll in @(Get-ChildItem -LiteralPath $pythonHome -Filter "*.dll" -File)) {
        Copy-Item -LiteralPath $dll.FullName -Destination $Stage -Force
    }
}

function Invoke-PythonBundleSmoke {
    param([Parameter(Mandatory = $true)][string] $Stage)

    $exe = Join-Path $Stage "citadel.exe"
    if (-not (Test-Path -LiteralPath $exe)) {
        throw "Staged citadel.exe not found: $exe"
    }
    if (-not (Test-Path -LiteralPath (Join-Path $Stage "scripts\main.py"))) {
        throw "Staged scripts\main.py not found under $Stage"
    }
    if (-not (Test-Path -LiteralPath (Join-Path $Stage "python\Lib\os.py"))) {
        throw "Staged python\Lib\os.py not found under $Stage"
    }

    Write-Host ">> Smoke: $exe check using bundled CPython"
    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $exe
    $startInfo.WorkingDirectory = $Stage
    $startInfo.UseShellExecute = $false
    $startInfo.Arguments = "check"
    [void] $startInfo.Environment.Remove("PYO3_PYTHON")
    [void] $startInfo.Environment.Remove("PYTHONPATH")
    $startInfo.Environment["PYTHONHOME"] = Join-Path $Stage "python"
    $startInfo.Environment["PYTHONNOUSERSITE"] = "1"
    $path = $startInfo.Environment["Path"]
    if (-not [string]::IsNullOrWhiteSpace($path)) {
        $filtered = ($path -split ";" | Where-Object {
            $lower = $_.ToLowerInvariant()
            -not ($lower.Contains("miniconda") -or $lower.Contains("anaconda") -or $lower.Contains("conda"))
        }) -join ";"
        $startInfo.Environment["Path"] = $filtered
    }

    $process = [System.Diagnostics.Process]::Start($startInfo)
    $process.WaitForExit()
    if ($process.ExitCode -ne 0) {
        throw "Python bundle smoke failed with exit code $($process.ExitCode)"
    }
}

function Invoke-BinServer {
    # Stage a ready-to-run server folder at bin\server: citadel.exe + citadel.toml
    # (scripts_dir pointed at ./scripts) + scripts\main.lua boilerplate + an empty
    # maps\ folder (matches the server's maps_dir default of ./maps). This is the
    # local equivalent of the unzipped "server" release package. bin\ is
    # git-ignored.
    Write-Host ">> Staging runnable server at bin\server"
    Invoke-Native -FilePath "cargo" -Arguments @("build", "--release")

    $binDir = Join-Path $RepoRoot "bin"
    $stage = Join-Path $binDir "server"
    $scriptsDir = Join-Path $stage "scripts"
    $mapsDir = Join-Path $stage "maps"

    if (Test-Path -LiteralPath $stage) {
        Remove-Item -LiteralPath $stage -Recurse -Force
    }
    New-Item -ItemType Directory -Path $scriptsDir -Force | Out-Null
    New-Item -ItemType Directory -Path $mapsDir -Force | Out-Null

    $exe = Join-Path $RepoRoot "target\release\citadel.exe"
    if (-not (Test-Path -LiteralPath $exe)) {
        throw "Expected server binary not found: $exe. Did the release build succeed?"
    }
    Copy-Item -LiteralPath $exe -Destination (Join-Path $stage "citadel.exe") -Force

    # Config with the game-logic folder pointed at the bundled ./scripts.
    $toml = Get-Content -LiteralPath (Join-Path $RepoRoot "citadel.toml") -Raw
    $toml = $toml.Replace('scripts_dir = "./game"', 'scripts_dir = "./scripts"')
    $toml = $toml.Replace('tick_hz = 0', 'tick_hz = 20')
    Set-Content -LiteralPath (Join-Path $stage "citadel.toml") -Value $toml -Encoding utf8 -NoNewline

    Copy-Item -LiteralPath (Join-Path $RepoRoot "packaging\server\scripts\main.lua") -Destination (Join-Path $scriptsDir "main.lua") -Force
    Copy-Item -LiteralPath (Join-Path $RepoRoot "packaging\server\README.txt") -Destination (Join-Path $stage "README.txt") -Force

    Write-Host ">> Ready: bin\server (run: cd bin\server; .\citadel.exe serve)"
}

function New-PythonServerStage {
    param([Parameter(Mandatory = $true)][string] $Stage)

    $scriptsDir = Join-Path $Stage "scripts"
    $mapsDir = Join-Path $Stage "maps"
    if (Test-Path -LiteralPath $Stage) {
        Remove-Item -LiteralPath $Stage -Recurse -Force
    }
    New-Item -ItemType Directory -Path $scriptsDir -Force | Out-Null
    New-Item -ItemType Directory -Path $mapsDir -Force | Out-Null

    $exe = Join-Path $RepoRoot "target\release\citadel.exe"
    if (-not (Test-Path -LiteralPath $exe)) {
        throw "Expected server binary not found: $exe. Did the release build succeed?"
    }
    Copy-Item -LiteralPath $exe -Destination (Join-Path $Stage "citadel.exe") -Force

    $toml = Get-Content -LiteralPath (Join-Path $RepoRoot "citadel.toml") -Raw
    $toml = $toml.Replace('# language = "lua"', 'language = "python"')
    $toml = $toml.Replace('scripts_dir = "./game"', 'scripts_dir = "./scripts"')
    Set-Content -LiteralPath (Join-Path $Stage "citadel.toml") -Value $toml -Encoding utf8 -NoNewline

    Copy-Item -LiteralPath (Join-Path $RepoRoot "packaging\server\scripts\main.py") -Destination (Join-Path $scriptsDir "main.py") -Force
    Copy-Item -LiteralPath (Join-Path $RepoRoot "packaging\server\README-python.txt") -Destination (Join-Path $Stage "README.txt") -Force
    Copy-PythonBundle -Stage $Stage
    Invoke-PythonBundleSmoke -Stage $Stage
}

function Invoke-BinServerPython {
    Write-Host ">> Staging Python-enabled runnable server at bin\server-python"
    Set-PythonRuntimeBuildEnv
    Invoke-Native -FilePath "cargo" -Arguments @("build", "--release", "--features", "runtime-python")

    $stage = Join-Path $RepoRoot "bin\server-python"
    New-PythonServerStage -Stage $stage

    Write-Host ">> Ready: bin\server-python (run: cd bin\server-python; .\citadel.exe serve)"
}

function Invoke-PackageWindowsPython {
    $version = Get-CargoVersion
    Write-Host ">> Packaging Python-enabled Citadel v$version for windows-x86_64"
    Set-PythonRuntimeBuildEnv
    Invoke-Native -FilePath "cargo" -Arguments @("build", "--release", "--features", "runtime-python")

    $pkgName = "citadel-windows-x86_64-python-v$version"
    $distDir = Join-Path $RepoRoot $DistDir
    $stage = Join-Path $distDir $pkgName
    New-PythonServerStage -Stage $stage

    $zipPath = Join-Path $distDir "$pkgName.zip"
    if (Test-Path -LiteralPath $zipPath) {
        Remove-Item -LiteralPath $zipPath -Force
    }
    Compress-Archive -Path $stage -DestinationPath $zipPath -Force
    Write-Host ">> Packaged $zipPath"
}

# --- Godot GDExtension native build ---------------------------------------
# The Godot SDK's CitadelClient GDScript delegates every transport/auth/codec
# call to a native GDExtension (CitadelClientNative) built from
# clients/godot/native/ over the same citadel-client-ffi C ABI that Unity and
# Unreal use. A ready-to-use Godot package therefore ships the compiled
# extension libraries, NOT just the .gd source. These helpers build them so the
# release packaging (and CI) can produce a drop-in addons/citadel/ folder.

function Resolve-Python {
    foreach ($candidate in @("python", "python3")) {
        if (Test-Command $candidate) {
            return $candidate
        }
    }
    throw "Python 3 is required to build the Godot GDExtension. Install Python 3 and ensure 'python' is on PATH."
}

function Install-Scons {
    param([Parameter(Mandatory = $true)][string] $PythonExe)

    # In PowerShell 7, a non-zero native command can become a terminating
    # NativeCommandError when the caller uses ErrorActionPreference=Stop. A
    # missing SCons is the normal bootstrap path, not a package failure.
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        & $PythonExe -c "import SCons" 2>$null
        $sconsInstalled = $LASTEXITCODE -eq 0
    } finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    if ($sconsInstalled) {
        return
    }
    Write-Host ">> Installing SCons (python -m pip install scons)"
    Invoke-Native -FilePath $PythonExe -Arguments @("-m", "pip", "install", "--upgrade", "scons")

    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        & $PythonExe -c "import SCons" 2>$null
        $sconsInstalled = $LASTEXITCODE -eq 0
    } finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    if (-not $sconsInstalled) {
        throw "SCons is not importable after install; cannot build the Godot GDExtension."
    }
}

function Install-GodotCpp {
    # Clone a pinned godot-cpp checkout into target\godot-cpp (git-ignored) if it
    # is not already present. Branch 4.3 matches the extension's
    # compatibility_minimum and is forward-compatible with newer Godot 4.x.
    param(
        [Parameter(Mandatory = $true)][string] $Path,
        [string] $Branch = "4.3"
    )

    if (Test-Path -LiteralPath (Join-Path $Path "SConstruct")) {
        Write-Host ">> godot-cpp present at $Path"
        return
    }
    Write-Host ">> Cloning godot-cpp ($Branch) into $Path"
    Invoke-Native -FilePath "git" -Arguments @(
        "clone", "--depth", "1", "--branch", $Branch,
        "https://github.com/godotengine/godot-cpp.git", $Path
    )
}

function Build-GodotNative {
    # Build the Godot GDExtension for windows-x86_64 (both the editor 'debug' and
    # exported 'release' templates, since the editor loads the debug library and
    # exported games load the release one). After both builds the native bin\
    # directory holds the two extension DLLs plus the companion
    # citadel_client_ffi.dll the SConstruct copies next to them. This function
    # does not return that path (native stdout would pollute a captured return
    # value); callers compute clients\godot\native\bin themselves.
    $python = Resolve-Python
    Install-Scons -PythonExe $python

    $godotCpp = Join-Path $RepoRoot "target\godot-cpp"
    Install-GodotCpp -Path $godotCpp

    Invoke-Native -FilePath "cargo" -Arguments @("build", "--release", "-p", "citadel-client-ffi")

    $nativeDir = Join-Path $RepoRoot "clients\godot\native"
    $binDir = Join-Path $nativeDir "bin"
    if (Test-Path -LiteralPath $binDir) {
        Remove-Item -LiteralPath $binDir -Recurse -Force
    }

    $env:GODOT_CPP_PATH = $godotCpp
    $env:CITADEL_FFI_LIB_DIR = Join-Path $RepoRoot "target\release"
    foreach ($target in @("template_debug", "template_release")) {
        Write-Host ">> scons $target (Godot GDExtension, windows-x86_64)"
        Push-Location $nativeDir
        try {
            Invoke-Native -FilePath $python -Arguments @(
                "-m", "SCons",
                "target=$target",
                "platform=windows",
                "arch=x86_64",
                "build_profile=build_profile.json",
                "use_static_cpp=no"
            )
        } finally {
            Pop-Location
        }
    }
}

function Build-GodotDropInStage {
    # Assemble a drop-in Godot package at $Stage: an addons\citadel\ folder with
    # the GDScript bindings, the .gdextension descriptor, and the compiled native
    # libraries under bin\, plus the sample and README at the package root. A
    # developer copies addons\ into their Godot project's res:// root.
    param([Parameter(Mandatory = $true)][string] $Stage)

    Build-GodotNative
    $binDir = Join-Path $RepoRoot "clients\godot\native\bin"

    Reset-Directory -Path $Stage
    $addon = Join-Path $Stage "addons\citadel"
    $addonBin = Join-Path $addon "bin"
    New-Item -ItemType Directory -Path $addonBin -Force | Out-Null

    Copy-Item -Path (Join-Path $RepoRoot "clients\godot\citadel\*.gd") -Destination $addon -Force
    Copy-Item -LiteralPath (Join-Path $RepoRoot "clients\godot\native\citadel.gdextension") -Destination $addon -Force

    # Ship only the loadable libraries; drop the .exp/.lib linker byproducts.
    Copy-Item -Path (Join-Path $binDir "*.dll") -Destination $addonBin -Force

    Copy-Item -LiteralPath (Join-Path $RepoRoot "clients\godot\sample") -Destination (Join-Path $Stage "sample") -Recurse -Force
    Copy-Item -LiteralPath (Join-Path $RepoRoot "clients\godot\README.md") -Destination (Join-Path $Stage "README.md") -Force

    Write-Host ">> Ready: $Stage (copy addons\citadel into your Godot project's res://addons\)"
}

function Invoke-PackageClient {
    # Stage a single engine's copy-into-project SDK into a versioned dist\ folder
    # and zip it. The staging itself is delegated to the matching Invoke-BinClient*
    # function (passed a dist stage dir) so the versioned zip and the local
    # bin\clients\<engine> staging always share one layout. Produces
    # dist\citadel-client-<engine>-windows-x86_64-v<version>.zip.
    param(
        [Parameter(Mandatory = $true)][string] $Engine,
        [Parameter(Mandatory = $true)][scriptblock] $StageAction
    )

    $version = Get-CargoVersion
    Write-Host ">> Packaging Citadel $Engine client v$version for windows-x86_64"

    $pkgName = "citadel-client-$Engine-windows-x86_64-v$version"
    $distDir = Join-Path $RepoRoot $DistDir
    $stage = Join-Path $distDir $pkgName

    & $StageAction $stage

    $zipPath = Join-Path $distDir "$pkgName.zip"
    if (Test-Path -LiteralPath $zipPath) {
        Remove-Item -LiteralPath $zipPath -Force
    }
    Compress-Archive -Path $stage -DestinationPath $zipPath -Force
    Write-Host ">> Packaged $zipPath"
}

function Invoke-PackageClientUnity {
    Invoke-PackageClient -Engine "unity" -StageAction { param($s) Invoke-BinClientUnity -Stage $s }
}

function Invoke-PackageClientUnreal {
    Invoke-PackageClient -Engine "unreal" -StageAction { param($s) Invoke-BinClientUnreal -Stage $s }
}

function Invoke-PackageClientGodot {
    # Unlike Unity/Unreal, the Godot package builds a native GDExtension (see
    # Build-GodotDropInStage) so the published zip is drop-in, not GDScript-only.
    Invoke-PackageClient -Engine "godot" -StageAction { param($s) Build-GodotDropInStage -Stage $s }
}

function Invoke-PackageClientGodotWeb {
    # The official browser package is a real Godot WebAssembly export plus the
    # reusable addon. It remains GDScript-only (no browser GDExtension), but
    # needs a Godot 4 executable with Web export templates. Set GODOT_BIN when
    # the executable is not on PATH.
    $godotBin = if ([string]::IsNullOrWhiteSpace($env:GODOT_BIN)) { "godot" } else { $env:GODOT_BIN }
    $script = "scripts\package_godot_web_artifact.py"
    if (Test-Command "py") {
        Invoke-Native -FilePath "py" -Arguments @("-3", $script, "--godot", $godotBin)
        return
    }
    if (Test-Command "python") {
        Invoke-Native -FilePath "python" -Arguments @($script, "--godot", $godotBin)
        return
    }
    if (Test-Command "python3") {
        Invoke-Native -FilePath "python3" -Arguments @($script, "--godot", $godotBin)
        return
    }
    throw "Python 3 was not found. Install Python 3 or use the py launcher."
}

function Invoke-PackageClientJs {
    # The browser runtime remains dependency-free. Node and the pinned local
    # esbuild dependency are build-time only; the staged bundle is a direct ESM
    # import plus source-map and content-encoding sidecars.
    $version = Get-CargoVersion
    $pkgName = "citadel-client-js-v$version"
    $stage = Join-Path $RepoRoot (Join-Path $DistDir $pkgName)
    $zipPath = Join-Path $RepoRoot (Join-Path $DistDir "$pkgName.zip")
    $sdkDir = Join-Path $RepoRoot "clients\js"

    Push-Location $sdkDir
    try {
        Invoke-Native -FilePath "npm" -Arguments @("ci")
        Invoke-Native -FilePath "npm" -Arguments @("run", "package")
    }
    finally {
        Pop-Location
    }

    $requiredStageFiles = @(
        "dist\citadel-client.min.mjs",
        "dist\citadel-client.min.mjs.map",
        "dist\citadel-client.min.mjs.gz",
        "dist\citadel-client.min.mjs.br",
        "index.d.ts",
        "README.md",
        "examples\threejs-starter\index.html",
        "SHA256SUMS.txt"
    )
    foreach ($required in $requiredStageFiles) {
        if (-not (Test-Path -LiteralPath (Join-Path $stage $required) -PathType Leaf)) {
            throw "JS SDK staging is missing required release file: $required"
        }
    }

    if (Test-Path -LiteralPath $zipPath) {
        Remove-Item -LiteralPath $zipPath -Force
    }
    Add-Type -AssemblyName System.IO.Compression
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $archive = [System.IO.Compression.ZipFile]::Open(
        $zipPath,
        [System.IO.Compression.ZipArchiveMode]::Create
    )
    try {
        $stageParent = Split-Path -Parent $stage
        $epoch = [DateTimeOffset]::new(1980, 1, 1, 0, 0, 0, [TimeSpan]::Zero)
        Get-ChildItem -LiteralPath $stage -File -Recurse | Sort-Object FullName | ForEach-Object {
            $entryName = $_.FullName.Substring($stageParent.Length + 1).Replace("\", "/")
            $entry = $archive.CreateEntry($entryName, [System.IO.Compression.CompressionLevel]::Optimal)
            $entry.LastWriteTime = $epoch
            $input = [System.IO.File]::OpenRead($_.FullName)
            $output = $entry.Open()
            try {
                $input.CopyTo($output)
            }
            finally {
                $output.Dispose()
                $input.Dispose()
            }
        }
    }
    finally {
        $archive.Dispose()
    }

    $archive = [System.IO.Compression.ZipFile]::OpenRead($zipPath)
    try {
        $entryNames = @($archive.Entries | ForEach-Object { $_.FullName.Replace("\", "/") })
        foreach ($required in $requiredStageFiles) {
            $entryName = "$pkgName/" + $required.Replace("\", "/")
            if ($entryName -notin $entryNames) {
                throw "JS SDK ZIP is missing required release entry: $entryName"
            }
        }
    }
    finally {
        $archive.Dispose()
    }
    Write-Host ">> Packaged and verified $zipPath"
}

function Invoke-PackageClientsWindows {
    # Build + stage + zip the ready-to-use native Windows engine SDKs. The
    # browser WebAssembly archive is exported by the separate Godot-capable
    # release job, because it is platform-independent rather than Windows-native.
    Invoke-PackageClientUnity
    Invoke-PackageClientUnreal
    Invoke-PackageClientGodot
    Write-Host ">> Packaged the Unity, Unreal, and Godot Windows client zips under $DistDir\"
}

function Invoke-BinBenchmark {
    Write-Host ">> Staging combat benchmark at $BenchmarkDir"
    Invoke-Native -FilePath "cargo" -Arguments @("build", "--release")

    $stage = Join-Path $RepoRoot $BenchmarkDir
    $scriptsDir = Join-Path $stage "scripts"
    $jsDir = Join-Path $stage "clients\js\src"

    New-Item -ItemType Directory -Path $scriptsDir -Force | Out-Null
    if (Test-Path -LiteralPath $jsDir) {
        Remove-Item -LiteralPath $jsDir -Recurse -Force
    }
    New-Item -ItemType Directory -Path $jsDir -Force | Out-Null

    $exe = Join-Path $RepoRoot "target\release\citadel.exe"
    if (-not (Test-Path -LiteralPath $exe)) {
        throw "Expected server binary not found: $exe. Did the release build succeed?"
    }
    Copy-Item -LiteralPath $exe -Destination (Join-Path $stage "server.exe") -Force

    $toml = Get-Content -LiteralPath (Join-Path $RepoRoot "citadel.toml") -Raw
    $toml = $toml.Replace('scripts_dir = "./game"', 'scripts_dir = "./scripts"')
    Set-Content -LiteralPath (Join-Path $stage "citadel.toml") -Value $toml -Encoding utf8 -NoNewline

    Copy-Item -LiteralPath (Join-Path $RepoRoot "crates\citadel-client\examples\combat_server.lua") -Destination (Join-Path $scriptsDir "main.lua") -Force

    $html = Get-Content -LiteralPath (Join-Path $RepoRoot "crates\citadel-client\examples\combat_viz.html") -Raw
    $html = $html.Replace("../../../clients/js/src/index.js", "./clients/js/src/index.js")
    Set-Content -LiteralPath (Join-Path $stage "client.html") -Value $html -Encoding utf8 -NoNewline

    Copy-Item -Path (Join-Path $RepoRoot "clients\js\src\*.js") -Destination $jsDir -Force

    $readme = @"
Citadel combat benchmark

1. Start the server from this folder:
   .\server.exe serve

2. From the repository root, serve this folder:
   py -3 -m http.server $BenchmarkWebPort --directory bin/benchmark

3. Open:
   http://127.0.0.1:$BenchmarkWebPort/client.html

The HTML defaults to 30 bots and connects to ws://127.0.0.1:7352/.
Run .\make.ps1 bin-benchmark again after source changes to refresh this folder.
"@
    Set-Content -LiteralPath (Join-Path $stage "README.txt") -Value $readme -Encoding utf8

    Write-Host ">> Ready: $BenchmarkDir"
    Write-Host ">> Server: cd $BenchmarkDir; .\server.exe serve"
    Write-Host ">> Client: py -3 -m http.server $BenchmarkWebPort --directory $BenchmarkDir"
    Write-Host ">> Open: http://127.0.0.1:$BenchmarkWebPort/client.html"
}

function Invoke-BenchmarkServe {
    Invoke-BinBenchmark

    $stage = Join-Path $RepoRoot $BenchmarkDir
    $serverExe = Join-Path $stage "server.exe"
    $url = "http://127.0.0.1:${BenchmarkWebPort}/client.html"
    $server = $null
    $web = $null

    try {
        $server = Start-NativeProcess -FilePath $serverExe -Arguments @("serve") -WorkingDirectory $stage

        if (Test-Command "py") {
            $web = Start-NativeProcess -FilePath "py" -Arguments @("-3", "-m", "http.server", "$BenchmarkWebPort", "--directory", $BenchmarkDir)
        } elseif (Test-Command "python") {
            $web = Start-NativeProcess -FilePath "python" -Arguments @("-m", "http.server", "$BenchmarkWebPort", "--directory", $BenchmarkDir)
        } elseif (Test-Command "python3") {
            $web = Start-NativeProcess -FilePath "python3" -Arguments @("-m", "http.server", "$BenchmarkWebPort", "--directory", $BenchmarkDir)
        } else {
            throw "Python was not found. Install Python 3 or use the py launcher."
        }

        Start-Sleep -Seconds $Wait
        Write-Host ">> Opening $url"
        Start-Process $url
        Write-Host ">> Benchmark running. Press Enter to stop server + HTTP."
        [void][System.Console]::ReadLine()
    } finally {
        Stop-StartedProcesses -Processes @($web, $server)
    }
}

# --- Client SDK staging (bin\clients\<engine>) -----------------------------
# Copy-into-project SDK source per docs/architecture/client-sdk-layout.md:
# ship the SDK SOURCE (the engine compiles/interprets it) plus the built
# native FFI cdylib only where that SDK actually loads one (Unity, Unreal).
# Godot (skeleton) and JS are pure source over the wire protocol -- no native
# lib is staged for them. Re-running a target wipes and re-stages its folder.

function Reset-Directory {
    param([Parameter(Mandatory = $true)][string] $Path)

    if (Test-Path -LiteralPath $Path) {
        Remove-Item -LiteralPath $Path -Recurse -Force
    }
    New-Item -ItemType Directory -Path $Path -Force | Out-Null
}

function Invoke-BinClientUnity {
    # Bindings + demo + the built native FFI dll, staged at $Stage
    # (default bin\clients\unity). The release packager reuses this with a
    # dist\ stage dir so local staging and the versioned zip share one layout.
    param([string] $Stage = (Join-Path $RepoRoot (Join-Path $ClientsDir "unity")))

    Invoke-Native -FilePath "cargo" -Arguments @("build", "--release", "-p", "citadel-client-ffi")

    Reset-Directory -Path $Stage
    $stage = $Stage
    New-Item -ItemType Directory -Path (Join-Path $stage "Citadel") -Force | Out-Null
    New-Item -ItemType Directory -Path (Join-Path $stage "Demo") -Force | Out-Null
    New-Item -ItemType Directory -Path (Join-Path $stage "Plugins\x86_64") -Force | Out-Null

    Copy-Item -Path (Join-Path $RepoRoot "clients\unity\Citadel\*.cs") -Destination (Join-Path $stage "Citadel") -Force
    Copy-Item -Path (Join-Path $RepoRoot "clients\unity\Demo\*.cs") -Destination (Join-Path $stage "Demo") -Force
    Copy-Item -LiteralPath (Join-Path $RepoRoot "clients\unity\README.md") -Destination (Join-Path $stage "README.md") -Force

    $dll = Join-Path $RepoRoot "target\release\citadel_client_ffi.dll"
    if (-not (Test-Path -LiteralPath $dll)) {
        throw "Expected native plugin not found: $dll. Did the release build succeed?"
    }
    Copy-Item -LiteralPath $dll -Destination (Join-Path $stage "Plugins\x86_64") -Force

    Write-Host ">> Ready: $stage (copy Citadel\ and Demo\ into your project's Assets\)"
}

function Invoke-BinClientUnreal {
    # Drop-in plugin source + the built native FFI lib/header staged into the
    # plugin's ThirdParty\ folder (mirrors clients/unreal/bundle-ffi.sh, but
    # targets the staged copy under $Stage instead of the in-repo plugin tree).
    # The release packager reuses this with a dist\ stage dir.
    param([string] $Stage = (Join-Path $RepoRoot (Join-Path $ClientsDir "unreal")))

    $pluginSrc = Join-Path $RepoRoot "clients\unreal\Plugin\Citadel"
    $pluginDest = Join-Path $stage "Plugins\Citadel"

    Reset-Directory -Path $stage
    New-Item -ItemType Directory -Path (Join-Path $stage "Plugins") -Force | Out-Null
    Copy-Item -LiteralPath $pluginSrc -Destination $pluginDest -Recurse -Force

    foreach ($excluded in @("Intermediate", "Binaries", ".uebuild")) {
        $excludedPath = Join-Path $pluginDest $excluded
        if (Test-Path -LiteralPath $excludedPath) {
            Remove-Item -LiteralPath $excludedPath -Recurse -Force
        }
    }
    $thirdParty = Join-Path $pluginDest "Source\CitadelClient\ThirdParty"
    if (Test-Path -LiteralPath $thirdParty) {
        Remove-Item -LiteralPath $thirdParty -Recurse -Force
    }

    Invoke-Native -FilePath "cargo" -Arguments @("build", "--release", "-p", "citadel-client-ffi")

    $lib = Join-Path $RepoRoot "target\release\citadel_client_ffi.lib"
    if (-not (Test-Path -LiteralPath $lib)) {
        throw "Expected native staticlib not found: $lib. Did the release build succeed?"
    }
    New-Item -ItemType Directory -Path (Join-Path $thirdParty "Win64") -Force | Out-Null
    New-Item -ItemType Directory -Path (Join-Path $thirdParty "include") -Force | Out-Null
    Copy-Item -LiteralPath $lib -Destination (Join-Path $thirdParty "Win64\citadel_client_ffi.lib") -Force
    Copy-Item -LiteralPath (Join-Path $RepoRoot "crates\citadel-client-ffi\include\citadel_client.h") -Destination (Join-Path $thirdParty "include\citadel_client.h") -Force

    Write-Host ">> Ready: $pluginDest (drop into <YourProject>\Plugins\Citadel)"
}

function Invoke-BinClientGodot {
    # Godot addon source (skeleton) staged at $Stage (default bin\clients\godot).
    # No native binding is built yet (see clients/godot/README.md). The release
    # packager reuses this with a dist\ stage dir.
    param([string] $Stage = (Join-Path $RepoRoot (Join-Path $ClientsDir "godot")))

    Reset-Directory -Path $Stage
    $stage = $Stage

    Copy-Item -LiteralPath (Join-Path $RepoRoot "clients\godot\citadel") -Destination (Join-Path $stage "citadel") -Recurse -Force
    Copy-Item -LiteralPath (Join-Path $RepoRoot "clients\godot\sample") -Destination (Join-Path $stage "sample") -Recurse -Force
    Copy-Item -LiteralPath (Join-Path $RepoRoot "clients\godot\README.md") -Destination (Join-Path $stage "README.md") -Force

    Write-Host ">> Ready: $stage (copy citadel\ into res://addons/citadel/; native binding not wired yet)"
}

function Invoke-BinClientGodotWeb {
    # Development staging for the reusable browser addon. The distributable
    # release artifact is built by Invoke-PackageClientGodotWeb and additionally
    # contains the exported HTML/JS/PCK/WebAssembly verification payload.
    # This staging helper intentionally contains only GDScript and never a
    # GDExtension descriptor.
    param([string] $Stage = (Join-Path $RepoRoot (Join-Path $ClientsDir "godot-web")))

    Reset-Directory -Path $Stage
    $addonRoot = Join-Path $Stage "addons\citadel"
    New-Item -ItemType Directory -Path (Split-Path -Parent $addonRoot) -Force | Out-Null
    Copy-Item -LiteralPath (Join-Path $RepoRoot "clients\godot\citadel") -Destination $addonRoot -Recurse -Force
    Copy-Item -LiteralPath (Join-Path $RepoRoot "clients\godot\README.md") -Destination (Join-Path $Stage "README.md") -Force
    Copy-Item -LiteralPath (Join-Path $RepoRoot "clients\godot\sdk.manifest.json") -Destination (Join-Path $Stage "sdk.manifest.json") -Force

    Write-Host ">> Ready: $Stage (copy addons\ into your Web project's res:// root; no GDExtension is included)"
}

function Invoke-BinClientJs {
    # Pure-source JS/Web SDK staged at bin\clients\js (Three.js starter, no test\ folder).
    $stage = Join-Path $RepoRoot (Join-Path $ClientsDir "js")
    Reset-Directory -Path $stage

    Copy-Item -LiteralPath (Join-Path $RepoRoot "clients\js\src") -Destination (Join-Path $stage "src") -Recurse -Force
    Copy-Item -LiteralPath (Join-Path $RepoRoot "clients\js\examples") -Destination (Join-Path $stage "examples") -Recurse -Force
    Copy-Item -LiteralPath (Join-Path $RepoRoot "clients\js\index.d.ts") -Destination (Join-Path $stage "index.d.ts") -Force
    Copy-Item -LiteralPath (Join-Path $RepoRoot "clients\js\package.json") -Destination (Join-Path $stage "package.json") -Force
    Copy-Item -LiteralPath (Join-Path $RepoRoot "clients\js\README.md") -Destination (Join-Path $stage "README.md") -Force

    Write-Host ">> Ready: $ClientsDir\js (includes examples\threejs-starter; npm install @citadel/client once published)"
}

function Invoke-BinClientRust {
    # crates/citadel-client source staged at bin\clients\rust\citadel-client, for
    # consumption as a path or git Cargo dependency (it is not a standalone,
    # independently buildable crate -- see the generated README.txt).
    $stage = Join-Path $RepoRoot (Join-Path $ClientsDir "rust\citadel-client")
    Reset-Directory -Path (Join-Path $RepoRoot (Join-Path $ClientsDir "rust"))
    New-Item -ItemType Directory -Path $stage -Force | Out-Null

    Copy-Item -LiteralPath (Join-Path $RepoRoot "crates\citadel-client\Cargo.toml") -Destination (Join-Path $stage "Cargo.toml") -Force
    Copy-Item -LiteralPath (Join-Path $RepoRoot "crates\citadel-client\src") -Destination (Join-Path $stage "src") -Recurse -Force
    Copy-Item -LiteralPath (Join-Path $RepoRoot "crates\citadel-client\examples") -Destination (Join-Path $stage "examples") -Recurse -Force

    $readme = @"
Citadel Rust client SDK (crates/citadel-client)

This is staged SOURCE, consumed as a path or git Cargo dependency --
not a standalone crate published to crates.io. Its Cargo.toml still
references sibling workspace crates (e.g. citadel-wire) by relative
path, so build it from within a checkout of the Citadel repo, or vendor
those sibling crates too. Point your own Cargo.toml at this folder
(path dependency) or at the citadel repo (git dependency).
"@
    Set-Content -LiteralPath (Join-Path $stage "README.txt") -Value $readme -Encoding utf8

    Write-Host ">> Ready: $ClientsDir\rust\citadel-client"
}

function Invoke-BinClients {
    Invoke-BinClientUnity
    Invoke-BinClientUnreal
    Invoke-BinClientGodot
    Invoke-BinClientGodotWeb
    Invoke-BinClientJs
    Invoke-BinClientRust
    Write-Host ">> All client SDKs staged under $ClientsDir\"
}

function Invoke-BinAll {
    Invoke-BinServer
    Invoke-BinBenchmark
    Invoke-BinClients
    Write-Host ">> Everything staged under bin\"
}

function Resolve-DatabaseUrl {
    if (-not [string]::IsNullOrWhiteSpace($DatabaseUrl)) {
        return $DatabaseUrl
    }
    return "postgres://${PgUser}:${PgPassword}@localhost:${PgPort}/${PgDb}"
}

function Invoke-DbUp {
    # Start a throwaway Postgres container and apply migrations. Not for
    # production; see docs/features/persistence.md.
    & docker rm -f $PgContainer 2>$null | Out-Null

    Invoke-Native -FilePath "docker" -Arguments @(
        "run", "-d", "--name", $PgContainer,
        "-e", "POSTGRES_USER=$PgUser",
        "-e", "POSTGRES_PASSWORD=$PgPassword",
        "-e", "POSTGRES_DB=$PgDb",
        "-p", "${PgPort}:5432", $PgImage
    )

    Write-Host ">> Waiting for Postgres to accept connections..."
    $ready = $false
    for ($i = 0; $i -lt 40; $i++) {
        & docker exec $PgContainer pg_isready -U $PgUser -d $PgDb 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) {
            $ready = $true
            break
        }
        Start-Sleep -Seconds 1
    }
    if (-not $ready) {
        throw "Postgres did not become ready in time."
    }
    Write-Host ">> Postgres ready on localhost:$PgPort"

    Invoke-DbMigrate

    $url = Resolve-DatabaseUrl
    Write-Host ">> DATABASE_URL=$url"
}

function Invoke-DbDown {
    Invoke-Native -FilePath "docker" -Arguments @("rm", "-f", $PgContainer)
}

function Invoke-DbMigrate {
    $url = Resolve-DatabaseUrl
    $previous = $env:DATABASE_URL
    $env:DATABASE_URL = $url
    try {
        Invoke-Native -FilePath "cargo" -Arguments @("run", "--example", "db_migrate")
    } finally {
        $env:DATABASE_URL = $previous
    }
}

function Show-Help {
    $targets = @(
        @("help", "Show this help"),
        @("setup", "Install or verify Rust, rustfmt, and clippy"),
        @("build", "Build the whole workspace"),
        @("check", "Run fmt check, clippy, tests, and docs policy check"),
        @("fmt", "Format the workspace"),
        @("clippy", "Lint with warnings denied"),
        @("test", "Run the workspace test suite"),
        @("clean", "Remove build artifacts"),
        @("server", "Run only the server"),
        @("web", "Serve only the web demo static files"),
        @("native", "Run one native QUIC client"),
        @("demo-web", "Server + web demo static server"),
        @("demo-native", "Server + one native QUIC client"),
        @("demo-native2", "Server + two native clients"),
        @("bin-benchmark", "Stage the combat benchmark at bin/benchmark"),
        @("benchmark-serve", "Stage, run server, serve HTML, and open benchmark client"),
        @("docs-install", "Install the docs site's Node dependencies"),
        @("docs-build", "Build rustdoc and the docs site"),
        @("docs-serve", "Preview the built docs site locally"),
        @("unity-plugin", "Build the C ABI cdylib and install it into the Unity SDK"),
        @("package-windows", "Stage + zip the Windows release (dist/citadel-windows-x86_64-v{version}.zip)"),
        @("package-windows-python", "Stage + zip the Python-enabled Windows server with bundled CPython"),
        @("package-client-unity", "Stage + zip the Unity client SDK (dist/citadel-client-unity-windows-x86_64-v{version}.zip)"),
        @("package-client-unreal", "Stage + zip the Unreal client plugin (dist/citadel-client-unreal-windows-x86_64-v{version}.zip)"),
        @("package-client-godot", "Stage + zip the Godot client SDK (dist/citadel-client-godot-windows-x86_64-v{version}.zip)"),
        @("package-client-godot-web", "Stage + zip the Godot Web/WebAssembly SDK (requires GODOT_BIN; dist/citadel-client-godot-web-v{version}.zip)"),
        @("package-client-js", "Build, stage, and zip the browser ESM SDK (dist/citadel-client-js-v{version}.zip)"),
        @("package-clients-windows", "Stage + zip the Unity, Unreal, and native Godot Windows SDKs as versioned zips"),
        @("bin-server", "Stage a ready-to-run server at bin/server (exe + config + scripts/main.lua + empty maps/)"),
        @("bin-server-python", "Stage a Python-enabled server at bin/server-python (bundled CPython + scripts/main.py)"),
        @("bin-client-unity", "Stage the Unity SDK (bindings + demo + built FFI dll) at bin/clients/unity"),
        @("bin-client-unreal", "Stage the Unreal plugin (drop-in source + built FFI) at bin/clients/unreal"),
        @("bin-client-godot", "Stage the Godot addon source (skeleton) at bin/clients/godot"),
        @("bin-client-godot-web", "Stage the reusable GDScript Godot Web addon at bin/clients/godot-web"),
        @("bin-client-js", "Stage the JS/Web SDK (Three.js starter, no tests) at bin/clients/js"),
        @("bin-client-rust", "Stage the Rust client crate source at bin/clients/rust/citadel-client"),
        @("bin-clients", "Stage all client SDKs under bin/clients/"),
        @("bin-all", "Stage everything (server, benchmark, all client SDKs) under bin/"),
        @("db-up", "Start a throwaway Postgres in Docker and migrate"),
        @("db-down", "Stop and remove the throwaway Postgres container"),
        @("db-migrate", "Apply migrations to DATABASE_URL (default: local container)")
    )

    Write-Host "Citadel Windows targets:"
    Write-Host ""
    foreach ($target in $targets) {
        Write-Host ("  {0,-16} {1}" -f $target[0], $target[1])
    }
    Write-Host ""
    Write-Host "Usage: .\make.ps1 <target> [options]"
    Write-Host "Setup check only: .\make.ps1 setup -NoInstall"
    Write-Host "Demo config: $Config  (QUIC :7351  WebSocket :7352  WebTransport :7353)"
}

switch ($Target) {
    "help" { Show-Help }
    "setup" { Invoke-Setup }
    "build" { Invoke-Build }
    "check" { Invoke-Check }
    "fmt" {
        Invoke-Native -FilePath "cargo" -Arguments @(
            "fmt",
            "--",
            "--config",
            "newline_style=Auto"
        )
    }
    "clippy" {
        Invoke-Native -FilePath "cargo" -Arguments @(
            "clippy",
            "--all-targets",
            "--all-features",
            "--workspace",
            "--",
            "-D",
            "warnings"
        )
    }
    "test" {
        Invoke-Native -FilePath "cargo" -Arguments @(
            "test",
            "--workspace",
            "--all-targets",
            "--all-features"
        )
    }
    "clean" { Invoke-Native -FilePath "cargo" -Arguments @("clean") }
    "server" { Invoke-Server }
    "web" { Invoke-Web }
    "native" { Invoke-NativeClient }
    "demo-web" { Invoke-DemoWeb }
    "demo-native" { Invoke-DemoNative }
    "demo-native2" { Invoke-DemoNative2 }
    "bin-benchmark" { Invoke-BinBenchmark }
    "benchmark-serve" { Invoke-BenchmarkServe }
    "docs-install" { Invoke-DocsInstall }
    "docs-build" { Invoke-DocsBuild }
    "docs-serve" { Invoke-DocsServe }
    "unity-plugin" { Invoke-UnityPlugin }
    "package-windows" { Invoke-PackageWindows }
    "package-windows-python" { Invoke-PackageWindowsPython }
    "package-client-unity" { Invoke-PackageClientUnity }
    "package-client-unreal" { Invoke-PackageClientUnreal }
    "package-client-godot" { Invoke-PackageClientGodot }
    "package-client-godot-web" { Invoke-PackageClientGodotWeb }
    "package-client-js" { Invoke-PackageClientJs }
    "package-clients-windows" { Invoke-PackageClientsWindows }
    "bin-server" { Invoke-BinServer }
    "bin-server-python" { Invoke-BinServerPython }
    "bin-client-unity" { Invoke-BinClientUnity }
    "bin-client-unreal" { Invoke-BinClientUnreal }
    "bin-client-godot" { Invoke-BinClientGodot }
    "bin-client-godot-web" { Invoke-BinClientGodotWeb }
    "bin-client-js" { Invoke-BinClientJs }
    "bin-client-rust" { Invoke-BinClientRust }
    "bin-clients" { Invoke-BinClients }
    "bin-all" { Invoke-BinAll }
    "db-up" { Invoke-DbUp }
    "db-down" { Invoke-DbDown }
    "db-migrate" { Invoke-DbMigrate }
}
