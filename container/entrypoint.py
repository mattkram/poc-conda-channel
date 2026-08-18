"""
Tiny HTTP server that IS the container's whole job:

  POST /ingest-batch {"channel": "main", "files": [
      {"filename": "foo-1.0-0.conda", "uploadedAt": 173..., "uploadedBy": "..."},
      ...
  ]}

  POST /reindex {"channel": "main", "subdir": "noarch"}
      Rebuild repodata for a subdir with no new package — used after a
      package deletion so repodata no longer references the removed file.

  POST /delete-package {"channel": "main", "subdir": "noarch", "filename": "foo-1.0-0.conda"}
      Delete a single package from R2 then reindex its subdir.
      The Worker calls /reindex directly after doing the R2 delete itself,
      so this endpoint exists mainly as a convenience for direct container
      access during testing.

  Handles both .conda (current format) and .tar.bz2 (legacy) packages —
  conda_package_streaming reads metadata from either uniformly, and
  conda-index indexes both into the same repodata.json without any special
  handling on our part.

  Called once per debounced batch by the ChannelQueue Durable Object (see
  src/worker.ts) rather than once per upload — so N near-simultaneous
  uploads to the same channel produce one conda-index run, not N.

  For each file:
    1. skip it if its staging object is already gone (means a previous,
       partially-failed batch already fully ingested it — idempotent no-op)
    2. otherwise download it, read info/index.json to find its real
       `subdir` (the client never supplies this), move it locally into place
  Then, once per distinct subdir touched by this batch:
    3. pull that subdir's conda-index cache + remaining packages, reindex
    4. upload the new package(s) + repodata + updated cache, delete staging

  Returns {"results": {filename: "ok" | "error: <msg>"}} so the caller can
  retry only what actually failed instead of the whole batch.
"""
import json
import os
import subprocess
import sys
import tempfile
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import boto3
import botocore.exceptions
from conda_package_streaming import package_streaming

R2_ACCOUNT_ID = os.environ["R2_ACCOUNT_ID"]
R2_ACCESS_KEY_ID = os.environ["R2_ACCESS_KEY_ID"]
R2_SECRET_ACCESS_KEY = os.environ["R2_SECRET_ACCESS_KEY"]
BUCKET = os.environ.get("R2_BUCKET_NAME", "conda-channel")

s3 = boto3.client(
    "s3",
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    region_name="auto",
)


# ---------------------------------------------------------------------------
# Structured logging — emits one JSON object per line to stdout.
# Cloudflare Containers captures stdout into Workers Logs.
# ---------------------------------------------------------------------------

def _mem_mb() -> float:
    """Resident set size in MB from /proc/self/status (Linux only)."""
    try:
        for line in open("/proc/self/status").readlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    except Exception:
        pass
    return 0.0


def _cpu_times() -> tuple[float, float]:
    """(user_s, system_s) from /proc/self/stat."""
    try:
        fields = open("/proc/self/stat").read().split()
        clk = os.sysconf("SC_CLK_TCK")
        return int(fields[13]) / clk, int(fields[14]) / clk
    except Exception:
        return 0.0, 0.0


def log(event: str, **kwargs) -> None:
    """Emit a structured log line to stdout."""
    cpu_u, cpu_s = _cpu_times()
    record = {
        "ts": time.time(),
        "event": event,
        "mem_mb": round(_mem_mb(), 1),
        "cpu_user_s": round(cpu_u, 2),
        "cpu_sys_s": round(cpu_s, 2),
        **kwargs,
    }
    print(json.dumps(record), flush=True)

s3 = boto3.client(
    "s3",
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    region_name="auto",
)


def _object_exists(key: str) -> bool:
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


def read_index_json(conda_path: str) -> dict:
    """Works for both package formats — .conda (zip of zstd-compressed tars)
    and legacy .tar.bz2 (a single bz2 tar) — via conda_package_streaming,
    the same library conda-index and conda-build use internally. No need to
    hand-parse either container format ourselves."""
    for tar, member in package_streaming.stream_conda_info(conda_path):
        if member.name == "info/index.json":
            return json.load(tar.extractfile(member))
    raise ValueError(f"info/index.json not found in {conda_path}")


def ingest_batch(channel: str, files: list[dict]) -> dict[str, str]:
    results: dict[str, str] = {}
    t_batch = time.time()
    log("ingest_batch.start", channel=channel, n_files=len(files))

    with tempfile.TemporaryDirectory() as tmp:
        channel_root = os.path.join(tmp, "channel")
        by_subdir: dict[str, list[str]] = {}

        # Phase 1: stage every file locally and figure out where it belongs.
        for f in files:
            filename = f["filename"]
            staging_key = f"{channel}/_incoming/{filename}"

            if not _object_exists(staging_key):
                results[filename] = "ok"
                continue

            try:
                staged_path = os.path.join(tmp, filename)
                t0 = time.time()
                s3.download_file(BUCKET, staging_key, staged_path)
                size = os.path.getsize(staged_path)
                log("download.staging", filename=filename,
                    size_bytes=size, elapsed_s=round(time.time() - t0, 3))

                index = read_index_json(staged_path)
                subdir = index.get("subdir")
                if not subdir:
                    raise ValueError("index.json has no `subdir` field")

                subdir_path = os.path.join(channel_root, subdir)
                os.makedirs(subdir_path, exist_ok=True)
                os.replace(staged_path, os.path.join(subdir_path, filename))
                by_subdir.setdefault(subdir, []).append(filename)
            except Exception as e:  # noqa: BLE001
                log("download.staging.error", filename=filename, error=str(e))
                results[filename] = f"error: {e}"

        # Phase 2: one conda-index run per distinct subdir touched.
        for subdir, filenames in by_subdir.items():
            subdir_path = os.path.join(channel_root, subdir)
            prefix = f"{channel}/{subdir}/"
            try:
                t0 = time.time()
                _download_cache(prefix, subdir_path)
                log("download.cache.done", subdir=subdir, elapsed_s=round(time.time() - t0, 3))

                t0 = time.time()
                n_downloaded = _download_subdir(prefix, subdir_path, skip=set(filenames))
                log("download.existing.done", subdir=subdir,
                    n_downloaded=n_downloaded, elapsed_s=round(time.time() - t0, 3))

                t0 = time.time()
                _run_conda_index(channel_root, subdir)
                log("conda_index.done", subdir=subdir,
                    n_new=len(filenames), elapsed_s=round(time.time() - t0, 3))

                t0 = time.time()
                for filename in filenames:
                    s3.upload_file(os.path.join(subdir_path, filename), BUCKET, f"{prefix}{filename}")
                _upload_cache(prefix, subdir_path)
                _upload_shards(subdir_path, prefix)
                _upload_repodata(subdir_path, prefix)
                log("upload.done", subdir=subdir,
                    n_files=len(filenames), elapsed_s=round(time.time() - t0, 3))

                for filename in filenames:
                    s3.delete_object(Bucket=BUCKET, Key=f"{channel}/_incoming/{filename}")
                    results[filename] = "ok"
            except Exception as e:  # noqa: BLE001
                log("ingest.error", subdir=subdir, error=str(e))
                for filename in filenames:
                    results[filename] = f"error: {e}"

    log("ingest_batch.done", channel=channel,
        n_ok=sum(1 for v in results.values() if v == "ok"),
        n_err=sum(1 for v in results.values() if v != "ok"),
        elapsed_s=round(time.time() - t_batch, 3))
    return results


PACKAGE_EXTENSIONS = (".conda", ".tar.bz2")


def _cached_filenames(subdir_path: str) -> set[str]:
    """Return the set of package filenames already in conda-index's sqlite cache.
    These don't need to be downloaded — conda-index will use cached metadata
    and never open the package files themselves."""
    import sqlite3
    db_path = os.path.join(subdir_path, ".cache", "cache.db")
    if not os.path.exists(db_path):
        return set()
    try:
        conn = sqlite3.connect(db_path)
        rows = conn.execute("SELECT path FROM index_json").fetchall()
        conn.close()
        return {row[0] for row in rows}
    except Exception:
        return set()


def _download_subdir(prefix: str, subdir_path: str, skip: set[str] | None = None) -> int:
    """Download existing packages that conda-index actually needs on disk.
    Returns the number of packages downloaded."""
    skip = (skip or set()) | _cached_filenames(subdir_path)
    paginator = s3.get_paginator("list_objects_v2")
    n = 0
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            fname = obj["Key"][len(prefix):]
            if not fname or fname in skip or not fname.endswith(PACKAGE_EXTENSIONS):
                continue
            s3.download_file(BUCKET, obj["Key"], os.path.join(subdir_path, fname))
            n += 1
    return n


def _download_cache(prefix: str, subdir_path: str) -> None:
    """Pull down conda-index's own sqlite cache (<subdir>/.cache/cache.db)
    from a previous run, if one exists. Without this, conda-index has no
    memory of which packages it already extracted metadata from, and
    re-extracts everything every single invocation — its incremental
    indexing only works if this survives between runs."""
    cache_prefix = f"{prefix}.cache/"
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=cache_prefix):
        for obj in page.get("Contents", []):
            rel = obj["Key"][len(cache_prefix):]
            if not rel:
                continue
            local_path = os.path.join(subdir_path, ".cache", rel)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            s3.download_file(BUCKET, obj["Key"], local_path)


def _upload_cache(prefix: str, subdir_path: str) -> None:
    """Push conda-index's updated cache back to R2 so the next invocation
    (a fresh container, fresh tempdir) can pick up where this one left off."""
    local_cache_dir = os.path.join(subdir_path, ".cache")
    if not os.path.isdir(local_cache_dir):
        return
    cache_prefix = f"{prefix}.cache/"
    for root, _, files in os.walk(local_cache_dir):
        for fname in files:
            local_path = os.path.join(root, fname)
            rel = os.path.relpath(local_path, local_cache_dir)
            s3.upload_file(local_path, BUCKET, f"{cache_prefix}{rel}")


def _run_conda_index(channel_root: str, subdir: str) -> None:
    # Community-standard repodata generation — same tool conda-build /
    # anaconda.org use, so output stays byte-compatible with any client.
    # Cache is on by default; --no-update-cache would disable the very
    # thing we're persisting, so don't pass it.
    # --write-shards additionally produces CEP-16 sharded repodata
    # (repodata_shards.msgpack.zst + one content-addressed shard per
    # package). --write-monolithic stays on (the default) so clients that
    # don't support shards still get a plain repodata.json.
    subprocess.run(
        ["python", "-m", "conda_index", channel_root, "--subdir", subdir, "--write-shards"],
        check=True,
    )


def _upload_shards(subdir_path: str, prefix: str) -> None:
    """Upload individual shard files (<sha256>.msgpack.zst). conda-index writes
    these directly into the subdir alongside repodata.json — NOT in a shards/
    subdirectory. Content-addressed by hash, so re-uploading is idempotent.
    Must happen before _upload_repodata so readers never see a shard index
    referencing a shard that isn't in R2 yet."""
    for fname in os.listdir(subdir_path):
        # Shard files are named <64-char hex sha256>.msgpack.zst
        # repodata_shards.msgpack.zst is the index and is handled by _upload_repodata
        if fname.endswith(".msgpack.zst") and not fname.startswith("repodata"):
            local_path = os.path.join(subdir_path, fname)
            if os.path.isfile(local_path):
                s3.upload_file(
                    local_path,
                    BUCKET,
                    f"{prefix}{fname}",
                    ExtraArgs={"CacheControl": "public, max-age=31536000, immutable"},
                )


def _upload_repodata(subdir_path: str, prefix: str) -> None:
    # Files conda clients actually consume:
    #   repodata.json                     — monolithic package index
    #   repodata_shards.msgpack.zst       — CEP-16 shard index
    #   patch_instructions.json           — repodata patches (hotfixes etc.)
    #
    # Intentionally skipped (not read by clients):
    #   index.html                        — human-browsable listing
    #   repodata_from_packages.json       — raw pre-patch repodata
    #   repodata_shards_from_packages.*   — raw pre-patch shard source
    UPLOAD_NAMES = {"repodata.json", "repodata_shards.msgpack.zst",
                    "patch_instructions.json", "index.html"}
    for fname in os.listdir(subdir_path):
        if fname in UPLOAD_NAMES:
            s3.upload_file(os.path.join(subdir_path, fname), BUCKET, f"{prefix}{fname}")


def reindex(channel: str, subdir: str) -> None:
    """Standalone reindex with no new package to place."""
    t0 = time.time()
    log("reindex.start", channel=channel, subdir=subdir)
    with tempfile.TemporaryDirectory() as tmp:
        channel_root = os.path.join(tmp, "channel")
        subdir_path = os.path.join(channel_root, subdir)
        os.makedirs(subdir_path, exist_ok=True)

        prefix = f"{channel}/{subdir}/"
        _download_cache(prefix, subdir_path)
        n = _download_subdir(prefix, subdir_path)
        _run_conda_index(channel_root, subdir)
        _upload_cache(prefix, subdir_path)
        _upload_shards(subdir_path, prefix)
        _upload_repodata(subdir_path, prefix)
    log("reindex.done", channel=channel, subdir=subdir,
        n_downloaded=n, elapsed_s=round(time.time() - t0, 3))


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("content-length", 0))
        payload = json.loads(self.rfile.read(length) or b"{}")

        if self.path == "/ingest-batch":
            channel = payload.get("channel")
            files = payload.get("files")
            if not channel or not files:
                self._respond(400, b"missing channel or files")
                return
            results = ingest_batch(channel, files)
            # 200 even with some per-file errors inside `results` — the caller
            # (ChannelQueue.alarm) inspects `results` itself to decide what to
            # retry. A non-2xx here means the whole call is retried from
            # scratch, which per-file idempotency already handles safely, but
            # per-file granularity avoids unnecessary repeated conda-index runs
            # for subdirs that already succeeded.
            self._respond(200, json.dumps({"results": results}).encode())

        elif self.path == "/reindex":
            channel = payload.get("channel")
            subdir = payload.get("subdir")
            if not channel or not subdir:
                self._respond(400, b"missing channel or subdir")
                return
            try:
                reindex(channel, subdir)
                self._respond(200, json.dumps({"ok": True}).encode())
            except Exception as e:  # noqa: BLE001
                self._respond(500, json.dumps({"error": str(e)}).encode())

        elif self.path == "/delete-package":
            channel = payload.get("channel")
            subdir = payload.get("subdir")
            filename = payload.get("filename")
            if not channel or not subdir or not filename:
                self._respond(400, b"missing channel, subdir, or filename")
                return
            try:
                key = f"{channel}/{subdir}/{filename}"
                s3.delete_object(Bucket=BUCKET, Key=key)
                reindex(channel, subdir)
                self._respond(200, json.dumps({"ok": True}).encode())
            except Exception as e:  # noqa: BLE001
                self._respond(500, json.dumps({"error": str(e)}).encode())

        else:
            self._respond(404, b"not found")

    def _respond(self, status: int, body: bytes, content_type: str = "application/json") -> None:
        self.send_response(status)
        self.send_header("content-type", content_type)
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    log("container.start", bucket=BUCKET)
    HTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
