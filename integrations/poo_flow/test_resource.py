"""Fail-closed receipt and POO Flow tool scope contracts; no cloud needed."""
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from poo_flow_runtime import RuntimeGraphPlan, RuntimeGraphEdge, RuntimeGraphConditionalEdge
from mrr_data_resource import MrrSnapshotResource, MrrResourceError


class ResourceContracts(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name)
        self.worker = self.path / "worker"
        self.worker.touch()
        self.resource = MrrSnapshotResource(self.worker, self.path / "cache", {})
        self.root = "b" + "a" * 58

    def test_agent_cannot_override_owner_scope(self):
        tool = self.resource.query_tool(root=self.root, source="plan", revision="revision")
        with patch("subprocess.run") as run:
            with self.assertRaises(MrrResourceError):
                tool.invoke({"root": self.root, "S3_ENDPOINT": "https://elsewhere"})
            run.assert_not_called()

    def test_conditional_or_dangling_graph_is_not_silently_reduced(self):
        plans = [RuntimeGraphPlan(nodes=("a",), edges=(RuntimeGraphEdge("a", "unknown"),)),
                 RuntimeGraphPlan(nodes=("a",), edges=(), conditional_edges=(
                     RuntimeGraphConditionalEdge("a", "router", {}),))]
        for plan in plans:
            with self.assertRaises(MrrResourceError):
                self.resource.publish(plan, source="plan", revision="rev")

    def test_unbound_or_malformed_receipt_is_not_a_semantic_success(self):
        for raw in (b'{}', b'{"result":{"kind":"admitted"}}', b'null', b'x' * (1024 * 1024 + 1)):
            def fake_run(*args, **kwargs):
                kwargs["stdout"].write(raw)
                return subprocess.CompletedProcess(args, 0)
            with patch("subprocess.run", side_effect=fake_run):
                with self.assertRaises(MrrResourceError):
                    self.resource.query(root=self.root, source="plan", revision="rev")

    def test_timeout_or_failed_process_never_returns_receipt(self):
        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("worker", 1)):
            with self.assertRaises(MrrResourceError):
                self.resource.query(root=self.root, source="plan", revision="rev")
        with patch("subprocess.run", return_value=subprocess.CompletedProcess("worker", 1)):
            with self.assertRaises(MrrResourceError):
                self.resource.query(root=self.root, source="plan", revision="rev")


if __name__ == "__main__":
    unittest.main()
