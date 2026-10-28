[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet('participant', 'message', 'finish')]
    [string]$Action,

    [Parameter(Mandatory = $true)]
    [string]$SessionFile,

    [string]$RepositoryRoot,

    [ValidateSet('Researcher', 'Builder', 'Reviewer', 'Moderator')]
    [string]$Participant,

    [ValidateSet('waiting', 'working', 'contributed', 'failed', 'finished')]
    [string]$State,

    [ValidateSet('analysis', 'proposal', 'review', 'decision', 'statement')]
    [string]$Kind,

    [Parameter(Mandatory = $true)]
    [string]$SourceRef,

    [string]$Text,
    [string]$Detail,
    [string]$CausationEventId,
    [string[]]$SourceEventId = @(),

    [ValidateSet('completed', 'failed', 'cancelled')]
    [string]$Status = 'completed'
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

    switch ($Action) {
        'participant' {
            if (-not $Participant -or -not $State) { throw 'participant requires -Participant and -State' }
            $arguments = @($bridge, 'participant', '--session-file', $SessionFile, '--participant', $Participant, '--state', $State, '--source-ref', $SourceRef)
            if ($Detail) { $arguments += @('--detail', $Detail) }
        }
        'message' {
            if (-not $Participant -or -not $Kind -or -not $Text) { throw 'message requires -Participant, -Kind, and -Text' }
            $arguments = @($bridge, 'message', '--session-file', $SessionFile, '--participant', $Participant, '--kind', $Kind, '--source-ref', $SourceRef, '--text', $Text)
            if ($CausationEventId) { $arguments += @('--causation-event-id', $CausationEventId) }
            foreach ($eventId in $SourceEventId) { $arguments += @('--source-event-id', $eventId) }
        }
        'finish' {
            $arguments = @($bridge, 'finish', '--session-file', $SessionFile, '--status', $Status, '--source-ref', $SourceRef)
            if ($Detail) { $arguments += @('--detail', $Detail) }
        }
    }

    $arguments += @('--plugin-protocol-version', $bridgeProtocolVersion)
    & $python @arguments
    $bridgeExitCode = $LASTEXITCODE
} finally {
    $env:PYTHONPATH = $previousPythonPath
}
exit $bridgeExitCode
