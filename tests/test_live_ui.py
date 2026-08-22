"""Loopback dashboard security and state endpoint tests."""

from __future__ import annotations

import json
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from ctmao_nsd import EventBus
from ctmao_nsd.live_ui import LiveDashboard


class LiveDashboardTests(unittest.TestCase):
    def test_close_before_start_is_safe_and_start_after_close_is_rejected(self) -> None:
        bus = EventBus()
        dashboard = LiveDashboard(bus)
        dashboard.close()
        try:
            with self.assertRaises(RuntimeError):
                dashboard.start()
        finally:
            bus.close()

    def test_ipv6_loopback_is_rejected_until_v1_supports_it(self) -> None:
        bus = EventBus()
        try:
            with self.assertRaises(ValueError):
                LiveDashboard(bus, host="::1")
        finally:
            bus.close()

    def test_loopback_token_and_no_store_state_endpoint(self) -> None:
        bus = EventBus()
        dashboard = LiveDashboard(bus)
        dashboard.start()
        try:
            base = dashboard.url.rsplit("/", 1)[0]
            with self.assertRaises(HTTPError) as denied:
                urlopen(base + "/api/state?token=wrong", timeout=1.0)
            self.assertEqual(denied.exception.code, 403)
            response = urlopen(
                base + f"/api/state?token={dashboard.token}", timeout=1.0
            )
            self.assertEqual(response.headers["Cache-Control"], "no-store")
            payload = json.loads(response.read())
            self.assertEqual(payload["run_status"], "idle")
        finally:
            dashboard.close()
            bus.close()

    def test_dashboard_exposes_conference_controls_and_provenance(self) -> None:
        bus = EventBus()
        dashboard = LiveDashboard(bus)
        dashboard.start()
        try:
            with urlopen(dashboard.url, timeout=1.0) as response:
                page = response.read().decode("utf-8")
            self.assertIn("Start conference", page)
            self.assertIn("Conference conversation", page)
            self.assertIn("Technical log and delegation tree", page)
            self.assertIn("Voice for ${id}", page)
            self.assertIn("Provenance", page)
            self.assertNotIn("p.turns.forEach", page)
        finally:
            dashboard.close()
            bus.close()

    def test_authenticated_control_endpoint_starts_and_cancels(self) -> None:
        class Control:
            def __init__(self) -> None:
                self.objective = None

            def start(self, objective: str) -> dict[str, object]:
                self.objective = objective
                return {"status": "queued"}

            def cancel(self) -> dict[str, object]:
                return {"status": "cancelling"}

            def snapshot(self) -> dict[str, object]:
                return {"status": "idle", "provider": "test"}

        bus = EventBus()
        control = Control()
        dashboard = LiveDashboard(bus, control=control)
        dashboard.start()
        try:
            base = dashboard.url.rsplit("/", 1)[0]
            request = Request(
                base + f"/api/runs?token={dashboard.token}",
                data=json.dumps({"objective": "Discuss a safe release"}).encode(),
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urlopen(request, timeout=1.0) as response:
                self.assertEqual(response.status, 202)
            self.assertEqual(control.objective, "Discuss a safe release")

            cancel = Request(
                base + f"/api/runs/cancel?token={dashboard.token}",
                data=b"{}",
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urlopen(cancel, timeout=1.0) as response:
                self.assertEqual(json.loads(response.read())["status"], "cancelling")
        finally:
            dashboard.close()
            bus.close()

    def test_control_endpoint_rejects_foreign_origin(self) -> None:
        class Control:
            def start(self, objective: str) -> dict[str, object]:
                raise AssertionError("must not be called")

            def cancel(self) -> dict[str, object]:
                raise AssertionError("must not be called")

            def snapshot(self) -> dict[str, object]:
                return {}

        bus = EventBus()
        dashboard = LiveDashboard(bus, control=Control())
        dashboard.start()
        try:
            base = dashboard.url.rsplit("/", 1)[0]
            request = Request(
                base + f"/api/runs?token={dashboard.token}",
                data=b'{"objective":"x"}',
                method="POST",
                headers={
                    "Content-Type": "application/json",
                    "Origin": "https://attacker.invalid",
                },
            )
            with self.assertRaises(HTTPError) as denied:
                urlopen(request, timeout=1.0)
            self.assertEqual(denied.exception.code, 403)
        finally:
            dashboard.close()
            bus.close()


if __name__ == "__main__":
    unittest.main()
