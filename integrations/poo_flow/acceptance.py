"""Exercise the installed POO Flow tool boundary against real S3 and MRR."""
import argparse
import base64
from dataclasses import asdict
import hashlib
import json
import os
import subprocess
from pathlib import Path
import tempfile

from poo_flow_runtime import (RuntimeGraphPlan, RuntimeGraphEdge, RuntimeGraphToolNode,
                              RuntimeGraphToolCall, ai_message)
from mrr_data_resource import MrrSnapshotResource, MrrResourceError


def run(executable):
    plan = RuntimeGraphPlan(nodes=("compile", "test", "package"), edges=(
        RuntimeGraphEdge("compile", "test"), RuntimeGraphEdge("test", "package")))
    scope = {"source": "poo-flow/build-plan", "revision": "build-plan-1"}
    with tempfile.TemporaryDirectory(prefix="poo-mrr-") as temporary:
        root = Path(temporary)
        offline = MrrSnapshotResource(executable, root / "offline-protected", {})
        protected = offline.protect(plan, **scope)
        assert protected.remote_operations == 0
        assert offline.pending_roots() == (protected.root,)
        offline_query = MrrSnapshotResource(executable, root / "offline-protected", {}).query(root=protected.root, **scope)
        assert offline_query.remote_operations == 0 and len(offline_query.rows) == 2
        synchronized = MrrSnapshotResource(executable, root / "offline-protected", os.environ).sync_pending()
        assert synchronized["attempted"] == synchronized["published"] == 1
        assert synchronized["failed_roots"] == []
        assert offline.pending_roots() == ()
        reprotected = offline.protect(plan, **scope)
        assert reprotected.root == protected.root
        manually_synced = MrrSnapshotResource(executable, root / "offline-protected", os.environ).sync(root=protected.root, **scope)
        assert manually_synced.root == protected.root and manually_synced.remote_operations > 0
        assert offline.pending_roots() == ()
        producer = MrrSnapshotResource(executable, root / "producer", os.environ)
        published = producer.publish(plan, **scope)
        reordered = producer.publish(RuntimeGraphPlan(nodes=plan.nodes, edges=tuple(reversed(plan.edges))), **scope)
        assert reordered.root == published.root
        local = producer.query(root=published.root, **scope)
        consumer = MrrSnapshotResource(executable, root / "consumer", os.environ)
        tool = consumer.query_tool(root=published.root, **scope)
        node = RuntimeGraphToolNode({tool.name: tool})
        request = {"messages": [ai_message("query dependencies", tool_calls=[
            RuntimeGraphToolCall(tool.name, {}, "static-edges-request")])]}
        cold = node(request)["messages"][0]
        assert cold.tool_call_id == "static-edges-request"
        cold = cold.content
        warm = node(request)["messages"][0].content
        restarted = MrrSnapshotResource(executable, root / "consumer", os.environ).query(root=published.root, **scope)
        assert len(cold.rows) == 2 and cold.remote_operations > 0
        assert local.remote_operations == warm.remote_operations == restarted.remote_operations == 0
        for receipt in (cold, warm, restarted):
            assert receipt.rows == local.rows
            assert receipt.admission_digest == local.admission_digest
            assert receipt.generation == published.generation
        constrained_dir = root / "capacity-limited"
        constrained_dir.mkdir()
        filler = b"x" * (1024 * 1024)
        filler_cid = "b" + base64.b32encode(b"\x01\x55\x12\x20" + hashlib.sha256(filler).digest()).decode().lower().rstrip("=")
        (constrained_dir / filler_cid).write_bytes(filler)
        constrained = MrrSnapshotResource(executable, constrained_dir,
                                          dict(os.environ, MRR_LOCAL_MAX_BYTES="1048576"))
        capacity_limited = constrained.query(root=published.root, **scope)
        capacity_limited_repeated = constrained.query(root=published.root, **scope)
        assert capacity_limited.rows == capacity_limited_repeated.rows == local.rows
        assert capacity_limited.remote_operations > 0 and capacity_limited_repeated.remote_operations > 0
        assert not (constrained_dir / published.root).exists()
        assert (constrained_dir / filler_cid).stat().st_size == len(filler)
        negatives = []
        for name, action in (
            ("stale-generation", lambda: consumer.query(root=published.root, **dict(scope, revision="build-plan-2"))),
            ("source-substitution", lambda: consumer.query(root=published.root, **dict(scope, source="other-plan"))),
            ("scope-override", lambda: tool.invoke({"root": published.root})),
            ("duplicate-edge", lambda: producer.publish(RuntimeGraphPlan(nodes=plan.nodes, edges=plan.edges + plan.edges), **scope)),
            ("transport-failure", lambda: MrrSnapshotResource(executable, root / "offline", dict(os.environ, S3_ENDPOINT="http://127.0.0.1:1")).query(root=published.root, **scope)),
            ("deadline", lambda: MrrSnapshotResource(executable, root / "deadline", os.environ, timeout=0.000001).query(root=published.root, **scope)),
        ):
            try:
                action()
            except MrrResourceError:
                negatives.append(name)
            else:
                raise AssertionError("negative case accepted: " + name)
        # Corrupt the actual S3 root independently. A fresh process/cache must
        # reject the content even though its credentials and TLS remain valid.
        endpoint = os.environ["S3_ENDPOINT"].rstrip("/")
        key = "/".join((os.environ["S3_BUCKET"], os.environ["S3_ROOT"].strip("/"), "blocks", published.root))
        subprocess.run(["curl", "--fail", "--silent", "--show-error", "--max-time", "10",
                        "--noproxy", "*", "--cacert", os.environ["S3_CA_PEM"],
                        "--aws-sigv4", "aws:amz:us-east-1:s3", "--user",
                        os.environ["AWS_ACCESS_KEY_ID"] + ":" + os.environ["AWS_SECRET_ACCESS_KEY"],
                        "-X", "PUT", "--data-binary", "corrupt root", endpoint + "/" + key],
                       check=True, capture_output=True)
        try:
            MrrSnapshotResource(executable, root / "corrupted", os.environ).query(root=published.root, **scope)
        except MrrResourceError:
            negatives.append("corrupt-root")
        else:
            raise AssertionError("corrupt root produced an accepted result")
        return {"profile": "poo-flow.static-edges.v1", "result": "passed",
                "protected": asdict(protected), "offline_query": asdict(offline_query),
                "synchronized": synchronized, "manually_synced": asdict(manually_synced),
                "local": asdict(local), "cold": asdict(cold), "warm": asdict(warm),
                "restarted": asdict(restarted), "capacity_limited": asdict(capacity_limited),
                "capacity_limited_repeated": asdict(capacity_limited_repeated),
                "negative_cases": negatives}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    args.receipt.unlink(missing_ok=True)
    result = run(args.worker)
    args.receipt.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))
