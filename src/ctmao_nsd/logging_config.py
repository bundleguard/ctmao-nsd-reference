"""Logging defaults for examples and applications."""

from __future__ import annotations

import logging


def configure_logging(level: int = logging.INFO) -> None:
    """Configure a concise log format including worker thread names."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(threadName)s] %(name)s: %(message)s",
    )
