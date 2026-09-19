#!/usr/bin/env python3
"""Local-only production-adapter acceptance against pinned upstream s3s-fs."""
import argparse
import hashlib
import json
import os
import platform
from pathlib import Path
import select
import socket
import socketserver
import ssl
import subprocess
import tempfile
import threading
import time

REVISION = "8ac644246b5a11664eaed6cbb3a36b3874047586"
REPOSITORY = "https://github.com/s3s-project/s3s.git"
ROOT = Path(__file__).resolve().parents[2]
KEY, SECRET = "local-conformance-key", "local-conformance-secret"


def run(args, **kwargs):
    return subprocess.run([str(a) for a in args], check=True, timeout=900, **kwargs)


def build(cache):
    source = cache / REVISION
    if not source.exists():
        source.mkdir(parents=True)
        run(["git", "init", source], capture_output=True)
        run(["git", "-C", source, "fetch", "--depth=1", REPOSITORY, REVISION])
        run(["git", "-C", source, "checkout", "--detach", "FETCH_HEAD"], capture_output=True)
    actual = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
    if actual != REVISION:
        raise RuntimeError("conformance server revision mismatch")
    run(["git", "-C", source, "diff", "--exit-code", "HEAD"], capture_output=True)
    run(["cargo", "+1.96.0", "build", "--locked", "-p", "s3s-fs", "--features", "binary", "--bin", "s3s-fs", "--target-dir", source / "target"], cwd=source)
    return source / "target/debug/s3s-fs"


def workspace_digest():
    names = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=ROOT
    ).decode().split("\0")
    digest = hashlib.sha256()
    for name in sorted(set(names)):
        path = ROOT / name
        if not path.is_file() or path.suffix not in {".rs", ".toml", ".lock", ".py", ".yml"}:
            continue
        digest.update(name.encode() + b"\0" + path.read_bytes() + b"\0")
    return digest.hexdigest()


def certificates(root):
    ca, key, cert = root / "ca.pem", root / "server.key", root / "server.pem"
    def openssl(*args):
        run(["openssl", *args], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
            "-subj", "/CN=MRR local test CA", "-keyout", root / "ca.key", "-out", ca)
    openssl("req", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=127.0.0.1",
            "-keyout", key, "-out", root / "server.csr")
    (root / "extensions").write_text("subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth\n")
    openssl("x509", "-req", "-in", root / "server.csr", "-CA", ca,
            "-CAkey", root / "ca.key", "-CAcreateserial", "-days", "1",
            "-extfile", root / "extensions", "-out", cert)
    return ca, key, cert


class TLSProxy(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


class Relay(socketserver.BaseRequestHandler):
    def handle(self):
        # Only TLS termination and opaque byte forwarding. s3s owns all HTTP,
        # S3 and signature processing, including signed Host header validation.
        try:
            self.request.settimeout(10)
            with self.server.context.wrap_socket(self.request, server_side=True) as client:
                with socket.create_connection(self.server.upstream, timeout=10) as upstream:
                    while True:
                        ready = [client] if client.pending() else select.select([client, upstream], [], [], 10)[0]
                        if not ready:
                            return
                        for source in ready:
                            data = source.recv(65536)
                            if not data:
                                return
                            (upstream if source is client else client).sendall(data)
        except (OSError, ssl.SSLError):
            # Untrusted CA/hostname negative tests intentionally abort TLS.
            return


def example_smoke(root, endpoint, ca):
    environment = {k: v for k, v in os.environ.items() if not k.startswith("AWS_")}
    empty_config = root / "empty-aws-config"
    empty_config.write_text("")
    environment.update(S3_ENDPOINT=endpoint, S3_BUCKET="conformance", S3_REGION="us-east-1",
                       S3_CA_PEM=str(ca), AWS_ACCESS_KEY_ID=KEY, AWS_SECRET_ACCESS_KEY=SECRET,
                       AWS_EC2_METADATA_DISABLED="true", AWS_CONFIG_FILE=str(empty_config),
                       AWS_SHARED_CREDENTIALS_FILE=str(empty_config))
    payload = root / "example-input"
    payload.write_bytes(b"local TLS example roundtrip")
    for example in ["s3_cache", "s3_snapshot"]:
        environment["S3_ROOT"] = "examples/" + example
        command = ["cargo", "run", "-p", "mrr-data-cache", "--features", "kache,s3,blocking",
                   "--locked", "--example", example]
        if example == "s3_cache":
            command += ["--", str(payload)]
        run(command, cwd=ROOT, env=environment)
    with payload.open("wb") as file:
        file.truncate(64 * 1024 * 1024 + 1)
    oversized = subprocess.run(
        ["cargo", "run", "-p", "mrr-data-cache", "--features", "kache,s3,blocking",
         "--locked", "--example", "s3_cache", "--", str(payload)],
        cwd=ROOT, env=environment, capture_output=True, text=True, timeout=600)
    if oversized.returncode == 0 or "file exceeds 64 MiB" not in oversized.stderr:
        raise RuntimeError("single-file example did not reject oversized input")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-dir", type=Path, default=Path(tempfile.gettempdir()) / "mrr-s3-conformance")
    parser.add_argument("--receipt", type=Path)
    args = parser.parse_args()
    # Never leave an earlier success receipt at the requested output on failure.
    if args.receipt:
        args.receipt.unlink(missing_ok=True)
    source_digest = workspace_digest()
    binary = build(args.cache_dir.resolve())
    with tempfile.TemporaryDirectory(prefix="mrr-s3-tls-") as directory:
        root = Path(directory)
        ca, key, cert = certificates(root)
        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))
            port = reserved.getsockname()[1]
        data = root / "data"
        data.mkdir()
        log = (root / "server.log").open("w+")
        server = subprocess.Popen([str(binary), "--host", "127.0.0.1", "--port", str(port),
                                   "--access-key", KEY, "--secret-key", SECRET, str(data)],
                                  stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 20
            while True:
                if server.poll() is not None or time.monotonic() > deadline:
                    log.seek(0)
                    raise RuntimeError("S3 server failed to start: " + log.read()[-4000:])
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                        break
                except OSError:
                    time.sleep(0.05)
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(cert, key)
            with TLSProxy(("127.0.0.1", 0), Relay) as proxy:
                proxy.context, proxy.upstream = context, ("127.0.0.1", port)
                thread = threading.Thread(target=proxy.serve_forever, daemon=True)
                thread.start()
                endpoint = f"https://127.0.0.1:{proxy.server_address[1]}"
                try:
                    # curl is an independent maintained SigV4 client for bucket provisioning.
                    run(["curl", "--fail", "--silent", "--show-error", "--max-time", "10",
                         "--noproxy", "*", "--cacert", ca, "--aws-sigv4", "aws:amz:us-east-1:s3",
                         "--user", f"{KEY}:{SECRET}", "-X", "PUT", endpoint + "/conformance"])
                    environment = dict(os.environ, MRR_S3_CONFORMANCE_ENDPOINT=endpoint,
                                       MRR_S3_CONFORMANCE_CA=str(ca))
                    result = subprocess.run(
                        ["cargo", "test", "-p", "mrr-data-cache", "--features", "kache,s3,blocking",
                         "--locked", "local_s3_tls_conformance", "--", "--ignored", "--nocapture"],
                        cwd=ROOT, env=environment, timeout=600, text=True,
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    print(result.stdout, flush=True)
                    result.check_returncode()
                    if "test result: ok. 1 passed;" not in result.stdout:
                        raise RuntimeError("conformance test did not execute exactly once")
                    example_smoke(root, endpoint, ca)
                finally:
                    proxy.shutdown()
                    thread.join(timeout=5)
        finally:
            server.terminate()
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=5)
            log.close()
    if workspace_digest() != source_digest:
        raise RuntimeError("workspace source changed during acceptance; rerun before recording success")
    receipt = {"server": "s3s-fs", "revision": REVISION,
               "workspace_source_sha256": source_digest,
               "platform": platform.platform(),
               "workspace_rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
               "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
               "transport": "loopback TLS proxy; ephemeral CA; SigV4 verified by s3s",
               "examples": ["s3_cache", "s3_snapshot", "s3_cache oversized rejection"],
               "result": "passed"}
    if args.receipt:
        args.receipt.write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt))


if __name__ == "__main__":
    main()
