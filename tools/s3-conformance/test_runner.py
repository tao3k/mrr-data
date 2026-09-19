"""Regression checks for evidence freshness, without network or a server build."""
import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("runner", Path(__file__).with_name("run.py"))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class ReceiptContracts(unittest.TestCase):
    def test_source_digest_includes_untracked_code_but_not_receipts(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            subprocess.run(["git", "init", root], check=True, capture_output=True)
            (root / "lib.rs").write_text("fn first() {}")
            with patch.object(runner, "ROOT", root):
                first = runner.workspace_digest()
                (root / "receipt.json").write_text('{"result":"passed"}')
                self.assertEqual(first, runner.workspace_digest())
                (root / "lib.rs").write_text("fn changed() {}")
                self.assertNotEqual(first, runner.workspace_digest())
                before_patch = runner.workspace_digest()
                (root / "server.patch").write_text("reviewed upstream fix")
                self.assertNotEqual(before_patch, runner.workspace_digest())
                before_attributes = runner.workspace_digest()
                (root / ".gitattributes").write_text("*.patch whitespace=-blank-at-eol")
                self.assertNotEqual(before_attributes, runner.workspace_digest())

    def test_failed_build_cannot_leave_old_success_receipt(self):
        with tempfile.TemporaryDirectory() as folder:
            receipt = Path(folder) / "receipt.json"
            receipt.write_text('{"result":"passed"}')
            with patch.object(sys, "argv", ["runner", "--receipt", str(receipt)]), \
                 patch.object(runner, "workspace_digest", return_value="source"), \
                 patch.object(runner, "build", side_effect=RuntimeError("build failed")):
                with self.assertRaisesRegex(RuntimeError, "build failed"):
                    runner.main()
            self.assertFalse(receipt.exists())


if __name__ == "__main__":
    unittest.main()
