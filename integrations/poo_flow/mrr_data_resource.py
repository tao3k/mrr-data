"""MRR snapshot resource for POO Flow's existing RuntimeGraphTool boundary.

The caller supplies a projected RuntimeGraphPlan and execution configuration.
This adapter neither schedules work nor admits semantic query results itself.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile
from typing import Mapping

from poo_flow_runtime import RuntimeGraphPlan, RuntimeGraphTool

PROFILE = "poo-flow.static-edges.v1"
MAX_BYTES = 1024 * 1024
HEX = re.compile(r"[0-9a-f]{64}\Z")
CID = re.compile(r"b[a-z2-7]{58}\Z")


class MrrResourceError(RuntimeError):
    """No accepted result is available for this invocation."""


@dataclass(frozen=True)
class MrrReceipt:
    root: str
    generation: str
    source: str
    revision: str
    rows: tuple[tuple[str, str], ...] | None
    admission_digest: str | None
    remote_operations: int
    charged_bytes: int
    elapsed_micros: int


class MrrSnapshotResource:
    """One trusted external executable and cache; runtime configuration is local."""

    def __init__(self, executable: Path, cache: Path, environment: Mapping[str, str], *, timeout: float = 40):
        if timeout <= 0:
            raise ValueError("positive worker timeout required")
        self.executable = executable.resolve(strict=True)
        self.cache = cache.resolve()
        self.environment = dict(environment, MRR_CACHE_DIR=str(self.cache))
        self.timeout = timeout

    def publish(self, plan: RuntimeGraphPlan, *, source: str, revision: str) -> MrrReceipt:
        if not isinstance(plan, RuntimeGraphPlan):
            raise TypeError("an existing POO Flow RuntimeGraphPlan projection is required")
        if plan.conditional_edges:
            raise MrrResourceError("conditional routes are outside the static-edge profile")
        if len(plan.edges) > 1024:
            raise MrrResourceError("edge budget exceeded")
        nodes = set(plan.nodes) | {"__start__", "__end__"}
        edges = [[edge.source, edge.target] for edge in plan.edges]
        if any(node not in nodes for edge in edges for node in edge):
            raise MrrResourceError("edge references an undeclared runtime node")
        return self._invoke(source, revision, {"kind": "publish", "edges": edges})

    def query(self, *, root: str, source: str, revision: str) -> MrrReceipt:
        if not isinstance(root, str) or not CID.fullmatch(root):
            raise MrrResourceError("canonical snapshot root required")
        return self._invoke(source, revision, {"kind": "query", "root": root})

    def query_tool(self, *, root: str, source: str, revision: str) -> RuntimeGraphTool:
        # Scope is fixed by the runtime owner. Agent arguments cannot override it
        # or change credentials, cache paths, budgets or the trusted executable.
        def invoke(arguments):
            if arguments:
                raise MrrResourceError("the snapshot query tool accepts no scope overrides")
            return self.query(root=root, source=source, revision=revision)
        return RuntimeGraphTool("mrr_static_edges", invoke)

    def _invoke(self, source, revision, operation):
        request = {"profile": PROFILE, "source": source, "revision": revision, "operation": operation}
        encoded = json.dumps(request, ensure_ascii=False, separators=(",", ":")).encode()
        if len(encoded) > MAX_BYTES:
            raise MrrResourceError("request exceeds 1 MiB")
        # File-backed output avoids an unbounded communicate() allocation. The
        # trusted worker is itself bounded; only one receipt may be returned.
        with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
            try:
                completed = subprocess.run([str(self.executable)], input=encoded, stdout=output,
                                           stderr=errors, env=self.environment, timeout=self.timeout,
                                           check=False)
            except subprocess.TimeoutExpired as error:
                raise MrrResourceError("MRR worker deadline exceeded; no receipt accepted") from error
            if completed.returncode:
                errors.seek(0)
                raise MrrResourceError(errors.read(4096).decode(errors="replace"))
            output.seek(0)
            raw = output.read(MAX_BYTES + 1)
        if len(raw) > MAX_BYTES:
            raise MrrResourceError("worker receipt exceeds 1 MiB")
        try:
            receipt = json.loads(raw)
            if not isinstance(receipt, dict) or set(receipt) != {
                "profile", "producer", "request_sha256", "source", "revision", "root",
                "generation", "result", "remote_operations", "charged_bytes", "elapsed_micros"
            }:
                raise ValueError("receipt fields")
            for field, expected in (("profile", PROFILE), ("producer", "mrr-data-poo-flow"),
                                    ("source", source), ("revision", revision),
                                    ("request_sha256", hashlib.sha256(encoded).hexdigest())):
                if receipt[field] != expected:
                    raise ValueError("receipt binding mismatch: " + field)
            if (not CID.fullmatch(receipt["root"]) or not isinstance(receipt["generation"], str)
                    or not receipt["generation"] or len(receipt["generation"]) > 256):
                raise ValueError("receipt identity")
            for key in ("remote_operations", "charged_bytes", "elapsed_micros"):
                if type(receipt[key]) is not int or receipt[key] < 0:
                    raise ValueError("invalid counter")
            result = receipt["result"]
            rows, digest = None, None
            if operation["kind"] == "publish":
                if result != {"kind": "published"}:
                    raise ValueError("publication receipt expected")
            else:
                if receipt["root"] != operation["root"] or set(result) != {"kind", "rows", "admission_digest"}:
                    raise ValueError("query receipt binding mismatch")
                if result["kind"] != "admitted" or not HEX.fullmatch(result["admission_digest"]):
                    raise ValueError("MRR admission receipt required")
                if not isinstance(result["rows"], list) or len(result["rows"]) > 1024:
                    raise ValueError("row budget")
                if any(not isinstance(row, list) or len(row) != 2 or
                       any(not isinstance(cell, str) or not cell or len(cell) > 256 for cell in row)
                       for row in result["rows"]):
                    raise ValueError("query result shape")
                rows = tuple(tuple(row) for row in result["rows"])
                digest = result["admission_digest"]
            return MrrReceipt(receipt["root"], receipt["generation"], source, revision,
                              rows, digest, receipt["remote_operations"], receipt["charged_bytes"],
                              receipt["elapsed_micros"])
        except (ValueError, KeyError, TypeError) as error:
            raise MrrResourceError("invalid MRR worker receipt") from error
