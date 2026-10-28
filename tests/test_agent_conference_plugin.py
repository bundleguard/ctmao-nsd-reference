"""Repository-contained Agent Conference plugin compatibility tests."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import unittest

from ctmao_nsd import CODEX_BRIDGE_PROTOCOL_VERSION


ROOT = Path(__file__).resolve().parents[1]
PLUGIN = ROOT / "plugins" / "agent-conference"


class AgentConferencePluginTests(unittest.TestCase):
    def test_manifest_uses_patch_release_and_bounded_listing_copy(self) -> None:
        manifest = json.loads(
            (PLUGIN / ".codex-plugin" / "plugin.json").read_text(encoding="utf-8")
        )
        self.assertEqual(manifest["name"], "agent-conference")
        self.assertEqual(manifest["version"], "0.4.2")
        self.assertLessEqual(len(manifest["interface"]["shortDescription"]), 30)

    def test_scripts_declare_protocol_and_preserve_pythonpath(self) -> None:
        start = (PLUGIN / "scripts" / "start-conference.ps1").read_text(
            encoding="utf-8"
        )
        event = (PLUGIN / "scripts" / "conference-event.ps1").read_text(
            encoding="utf-8"
        )
        resolver = (PLUGIN / "scripts" / "resolve-python.ps1").read_text(
            encoding="utf-8"
        )
        for script in (start, event):
            self.assertIn(
                f"$bridgeProtocolVersion = '{CODEX_BRIDGE_PROTOCOL_VERSION}'", script
            )
            self.assertIn("$env:PYTHONPATH", script)
            self.assertIn("[IO.Path]::PathSeparator", script)
            self.assertIn("$previousPythonPath", script)
            self.assertIn("finally", script)
        self.assertIn("--plugin-protocol-version", start)
        self.assertIn("--plugin-protocol-version", event)
        self.assertIn("[string]$RepositoryRoot", event)
        self.assertIn("WindowsApps", resolver)
        self.assertLess(resolver.index("$candidates.Add($bundled)"), resolver.index("Get-Command python"))

    def test_skill_requires_repository_bridge_and_causal_receipts(self) -> None:
        skill = (PLUGIN / "skills" / "agent-conference" / "SKILL.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("requires the CTMAO-NSD repository bridge", skill)
        self.assertIn(f"protocol `{CODEX_BRIDGE_PROTOCOL_VERSION}`", skill)
        self.assertIn("-CausationEventId", skill)
        self.assertIn("-SourceEventId", skill)
        self.assertIn("accepted_event_id", skill)

    @unittest.skipUnless(os.name == "nt", "PowerShell wrapper smoke is Windows-only")
    def test_windows_wrappers_complete_exact_four_role_flow(self) -> None:
        start_script = PLUGIN / "scripts" / "start-conference.ps1"
        event_script = PLUGIN / "scripts" / "conference-event.ps1"

        def quoted(value: object) -> str:
            return "'" + str(value).replace("'", "''") + "'"

        start_command = (
            f"& {quoted(start_script)} "
            "-Objective 'PowerShell wrapper receipt smoke' "
            f"-RepositoryRoot {quoted(ROOT)} -TerminalHoldSeconds 0.2"
        )
        server = subprocess.Popen(
            [
                "powershell.exe",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-Command",
                start_command,
            ],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        try:
            assert server.stdout is not None
            launch = json.loads(server.stdout.readline())
            session_file = launch["session_file"]

            def event(arguments: str) -> dict[str, object]:
                command = (
                    f"& {quoted(event_script)} {arguments} "
                    f"-SessionFile {quoted(session_file)} "
                    f"-RepositoryRoot {quoted(ROOT)}"
                )
                completed = subprocess.run(
                    [
                        "powershell.exe",
                        "-NoProfile",
                        "-ExecutionPolicy",
                        "Bypass",
                        "-Command",
                        command,
                    ],
                    cwd=ROOT,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    timeout=15,
                    check=False,
                )
                self.assertEqual(completed.returncode, 0, completed.stderr)
                lines = [line for line in completed.stdout.splitlines() if line.strip()]
                self.assertTrue(lines, completed.stderr)
                return json.loads(lines[-1])

            work: dict[str, str] = {}
            messages: dict[str, str] = {}
            for role in ("Researcher", "Builder", "Reviewer", "Moderator"):
                receipt = event(
                    "participant "
                    f"-Participant {role} -State working "
                    f"-SourceRef {quoted('wrapper/' + role + '/working')}"
                )
                work[role] = str(receipt["accepted_event_id"])
                upstream = {
                    "Researcher": (),
                    "Builder": (),
                    "Reviewer": ("Researcher", "Builder"),
                    "Moderator": ("Researcher", "Builder", "Reviewer"),
                }[role]
                source_argument = ""
                if upstream:
                    values = ",".join(quoted(messages[item]) for item in upstream)
                    source_argument = f" -SourceEventId @({values})"
                receipt = event(
                    "message "
                    f"-Participant {role} -Kind "
                    f"{'decision' if role == 'Moderator' else 'analysis'} "
                    f"-SourceRef {quoted('wrapper/' + role + '/message')} "
                    f"-Text {quoted(role + ' wrapper result')} "
                    f"-CausationEventId {quoted(work[role])}{source_argument}"
                )
                messages[role] = str(receipt["accepted_event_id"])

            terminal = event(
                "finish -Status completed -SourceRef 'wrapper/finish'"
            )
            self.assertEqual(terminal["status"], "completed")
            self.assertEqual(
                terminal["bridge_protocol_version"], CODEX_BRIDGE_PROTOCOL_VERSION
            )
            self.assertEqual(server.wait(timeout=10), 0)
        finally:
            if server.poll() is None:
                server.terminate()
                server.wait(timeout=5)
            if server.stdout is not None:
                server.stdout.close()
            if server.stderr is not None:
                server.stderr.close()


if __name__ == "__main__":
    unittest.main()
