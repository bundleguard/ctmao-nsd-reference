"""Tests for bounded delegation policy and lineage tracking."""

from __future__ import annotations

import unittest
from uuid import uuid4

from ctmao_nsd.config import RuntimeConfig
from ctmao_nsd.delegation import (
    CircularDelegationDetected,
    DelegationContext,
    DelegationDepthExceeded,
    DelegationWidthExceeded,
)


class DelegationContextTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = RuntimeConfig(max_delegation_depth=2, max_children_per_agent=2)
        self.root_id = uuid4()
        self.root = DelegationContext.root(self.root_id, "A", self.config)

    def test_root_starts_at_depth_zero(self) -> None:
        self.assertEqual(self.root.depth, 0)
        self.assertEqual(self.root.lineage, (self.root_id,))

    def test_exact_maximum_depth_is_allowed(self) -> None:
        child = self.root.for_child(uuid4(), "child")
        grandchild = child.for_child(uuid4(), "grandchild")
        self.assertEqual((child.depth, grandchild.depth), (1, 2))

    def test_beyond_maximum_depth_is_rejected(self) -> None:
        child = self.root.for_child(uuid4(), "child")
        grandchild = child.for_child(uuid4(), "grandchild")
        with self.assertRaises(DelegationDepthExceeded):
            grandchild.for_child(uuid4(), "too-deep")

    def test_width_is_checked_before_scheduling(self) -> None:
        self.root.validate_child_count(2)
        with self.assertRaises(DelegationWidthExceeded):
            self.root.validate_child_count(3)

    def test_circular_lineage_is_rejected(self) -> None:
        child = self.root.for_child(uuid4(), "child")
        with self.assertRaises(CircularDelegationDetected):
            child.for_child(self.root_id, "cycle")
