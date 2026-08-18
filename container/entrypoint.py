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
import tempfile
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

    with tempfile.TemporaryDirectory() as tmp:
        channel_root = os.path.join(tmp, "channel")
        by_subdir: dict[str, list[str]] = {}

        # Phase 1: stage every file locally and figure out where it belongs.
        # Failures here are per-file and don't block the rest of the batch.
        for f in files:
            filename = f["filename"]
            staging_key = f"{channel}/_incoming/{filename}"

            if not _object_exists(staging_key):
                # Already fully ingested by a previous run of this same
                # batch (e.g. one subdir succeeded, another failed, and
                # this file's queue entry got retried) — nothing to do.
                results[filename] = "ok"
                continue

            try:
                staged_path = os.path.join(tmp, filename)
                s3.download_file(BUCKET, staging_key, staged_path)

                index = read_index_json(staged_path)
                subdir = index.get("subdir")
                if not subdir:
                    raise ValueError("index.json has no `subdir` field")

                subdir_path = os.path.join(channel_root, subdir)
                os.makedirs(subdir_path, exist_ok=True)
                os.replace(staged_path, os.path.join(subdir_path, filename))
                by_subdir.setdefault(subdir, []).append(filename)
            except Exception as e:  # noqa: BLE001 — reported per-file, not fatal to the batch
                results[filename] = f"error: {e}"

        # Phase 2: one conda-index run per distinct subdir touched, covering
        # every file destined for it in this batch at once.
        for subdir, filenames in by_subdir.items():
            subdir_path = os.path.join(channel_root, subdir)
            prefix = f"{channel}/{subdir}/"
            try:
                _download_cache(prefix, subdir_path)
                _download_subdir(prefix, subdir_path, skip=set(filenames))
                _run_conda_index(channel_root, subdir)

                # Only commit to R2 once conda-index has succeeded locally.
                # repodata.json — the object clients actually read — goes
                # last, after the packages it references are already durably
                # in R2, so no reader ever sees a repodata entry for a
                # package that isn't actually there yet.
                for filename in filenames:
                    s3.upload_file(os.path.join(subdir_path, filename), BUCKET, f"{prefix}{filename}")
                _upload_cache(prefix, subdir_path)
                _upload_shards(subdir_path, prefix)
                _upload_repodata(subdir_path, prefix)

                for filename in filenames:
                    s3.delete_object(Bucket=BUCKET, Key=f"{channel}/_incoming/{filename}")
                    results[filename] = "ok"
            except Exception as e:  # noqa: BLE001 — reported per-file for retry
                for filename in filenames:
                    results[filename] = f"error: {e}"

    return results


PACKAGE_EXTENSIONS = (".conda", ".tar.bz2")


def _download_subdir(prefix: str, subdir_path: str, skip: set[str] | None = None) -> None:
    """Download every existing *package* under prefix except those in `skip`
    (already on disk locally from this batch). Positive extension filter
    rather than blocklisting — otherwise this would also re-download
    repodata.json, the shard index, and every shard file on every single
    run, none of which are inputs to conda-index, only its own outputs."""
    skip = skip or set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            fname = obj["Key"][len(prefix):]
            if not fname or fname in skip or not fname.endswith(PACKAGE_EXTENSIONS):
                continue
            s3.download_file(BUCKET, obj["Key"], os.path.join(subdir_path, fname))


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
    # Covers both repodata.json (monolithic) and repodata_shards.msgpack.zst
    # (the shard index) — both start with "repodata". The index is
    # short-lived per CEP-16's own recommendation (seconds-to-hours), unlike
    # the shards themselves, so no immutable Cache-Control here.
    for fname in os.listdir(subdir_path):
        if fname.startswith("repodata"):
            s3.upload_file(os.path.join(subdir_path, fname), BUCKET, f"{prefix}{fname}")


def reindex(channel: str, subdir: str) -> None:
    """Standalone reindex with no new package to place — e.g. a manual
    rebuild trigger. Downloads the full subdir since there's no local
    file to seed from."""
    with tempfile.TemporaryDirectory() as tmp:
        channel_root = os.path.join(tmp, "channel")
        subdir_path = os.path.join(channel_root, subdir)
        os.makedirs(subdir_path, exist_ok=True)

        prefix = f"{channel}/{subdir}/"
        _download_cache(prefix, subdir_path)
        _download_subdir(prefix, subdir_path)
        _run_conda_index(channel_root, subdir)
        _upload_cache(prefix, subdir_path)
        _upload_shards(subdir_path, prefix)
        _upload_repodata(subdir_path, prefix)


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
    HTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
