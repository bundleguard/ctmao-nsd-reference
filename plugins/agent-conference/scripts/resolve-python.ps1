[CmdletBinding()]
param()

$candidates = [System.Collections.Generic.List[string]]::new()
if ($env:VIRTUAL_ENV) {
    $candidates.Add((Join-Path $env:VIRTUAL_ENV 'Scripts\python.exe'))
}

$bundled = Join-Path $env:USERPROFILE '.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe'
$candidates.Add($bundled)

$command = Get-Command python -ErrorAction SilentlyContinue
if ($command -and $command.Source -notmatch '\\WindowsApps\\python(?:\.exe)?$') {
    $candidates.Add($command.Source)
}

foreach ($candidate in $candidates) {
    if ($candidate -and (Test-Path -LiteralPath $candidate -PathType Leaf)) {
        & $candidate -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)" *> $null
        if ($LASTEXITCODE -eq 0) {
            $candidate
            exit 0
        }
    }
}

throw 'Python 3.11 or newer is required. Create a project virtual environment or install Python.'
