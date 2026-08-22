"""Loopback dashboard security and state endpoint tests."""

from __future__ import annotations

import json
import unittest
from urllib.error import HTTPError
from urllib.request import urlopen

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
            self.assertIn("Delegation tree", page)
            self.assertIn("Play latest briefing", page)
            self.assertIn("Voice for ${id}", page)
            self.assertIn("Provenance", page)
            self.assertNotIn("if(terminal)resetSpeech()", page)
        finally:
            dashboard.close()
            bus.close()


if __name__ == "__main__":
    unittest.main()
