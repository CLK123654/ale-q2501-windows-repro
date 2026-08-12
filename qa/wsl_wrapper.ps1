param(
  [Parameter(Mandatory=$true)][ValidateSet('reference','verify')][string]$Mode
)
$ErrorActionPreference = 'Stop'
$repoWindows = (Resolve-Path '.').Path
$drive = $repoWindows.Substring(0, 1).ToLowerInvariant()
$rest = $repoWindows.Substring(2).Replace('\', '/')
$repoWsl = "/mnt/$drive$rest"
if (-not $repoWsl) { throw 'WSL path conversion failed' }
$script = if ($Mode -eq 'reference') { './qa/generate_reference.py' } else { './qa/windows_verify.py' }
wsl.exe -d Ubuntu-24.04 -- bash -lc "set -e; cd '$repoWsl'; source .venv/bin/activate; export AIRFLOW__CORE__LOAD_EXAMPLES=False; python '$script'"
if ($LASTEXITCODE -ne 0) { throw "WSL task failed with exit code $LASTEXITCODE" }
