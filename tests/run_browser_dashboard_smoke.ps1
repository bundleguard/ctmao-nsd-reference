[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$taskRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$taskResolver = Join-Path $taskRoot "plugins\agent-conference\scripts\resolve-python.ps1"
$taskPython = & $taskResolver
$taskPreviousPythonPath = $env:PYTHONPATH
$taskSourcePath = Join-Path $taskRoot "src"

try {
    $env:PYTHONPATH = if ([string]::IsNullOrWhiteSpace($taskPreviousPythonPath)) {
        $taskSourcePath
    } else {
        "$taskSourcePath$([IO.Path]::PathSeparator)$taskPreviousPythonPath"
    }
    & $taskPython (Join-Path $taskRoot "tests\browser_dashboard_smoke.py")
    exit $LASTEXITCODE
} finally {
    $env:PYTHONPATH = $taskPreviousPythonPath
}
