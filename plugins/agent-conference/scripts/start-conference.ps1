[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateLength(1, 4000)]
    [string]$Objective,

    [string]$RepositoryRoot,

    [ValidateRange(0, 3600)]
    [double]$TerminalHoldSeconds = 120
)

$bridgeProtocolVersion = '1.1'
$pluginRoot = Split-Path -Parent $PSScriptRoot
$repoRoot = if ($RepositoryRoot) {
    (Resolve-Path -LiteralPath $RepositoryRoot).Path
} else {
    (Get-Location).Path
}
$bridge = Join-Path $repoRoot 'examples\codex_conference_bridge.py'
if (-not (Test-Path -LiteralPath $bridge -PathType Leaf)) {
    $bundledRoot = (Resolve-Path -LiteralPath (Join-Path $pluginRoot '..\..')).Path
    $bundledBridge = Join-Path $bundledRoot 'examples\codex_conference_bridge.py'
    if (-not (Test-Path -LiteralPath $bundledBridge -PathType Leaf)) {
        throw 'Run this command from the CTMAO-NSD repository root.'
    }
    $repoRoot = $bundledRoot
    $bridge = $bundledBridge
}
$python = & (Join-Path $PSScriptRoot 'resolve-python.ps1')
$sourcePath = Join-Path $repoRoot 'src'
$previousPythonPath = $env:PYTHONPATH
$bridgeExitCode = 1
try {
    $env:PYTHONPATH = if ($previousPythonPath) {
        "$sourcePath$([IO.Path]::PathSeparator)$previousPythonPath"
    } else {
        $sourcePath
    }
    & $python $bridge serve --objective $Objective --terminal-hold $TerminalHoldSeconds --plugin-protocol-version $bridgeProtocolVersion
    $bridgeExitCode = $LASTEXITCODE
} finally {
    $env:PYTHONPATH = $previousPythonPath
}
exit $bridgeExitCode
