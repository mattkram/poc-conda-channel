"""
Tier 1 metadata extractor for the shard-first ingestion pipeline.

  POST /extract-metadata
    Body: {"channel": "main", "filename": "foo-1.0-0.conda", "staging_key": "main/_incoming/foo-1.0-0.conda"}
    Returns: {"filename": "...", "subdir": "...", "name": "...", "entry": {...repodata fields...}}

  POST /reindex   (legacy full rebuild — used for deletions and reconciliation only)
    Body: {"channel": "main", "subdir": "noarch"}

  POST /delete-package  (legacy — delete + full reindex)
    Body: {"channel": "main", "subdir": "noarch", "filename": "foo-1.0-0.conda"}

The hot path (/extract-metadata) is strictly per-package:
  1. Download the staged package from R2
  2. Extract info/index.json + optional info/run_exports.json
  3. Compute md5, sha256, size
  4. Return the complete repodata entry — no conda-index run, no subdir scan

All shard assembly and repodata.json generation happens in Durable Objects
(PackageIngestor, SubdirIndexMerger) in the Worker — the container only needs
to read package bytes, which it must do anyway.
"""
import hashlib
import io
import json
import os
import subprocess
import tempfile
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import boto3
import botocore.exceptions
import msgpack
import zstandard as zstd
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
# Structured logging — one JSON object per line to stdout.
# Cloudflare Containers captures stdout into Workers Logs.
# ---------------------------------------------------------------------------

def _mem_mb() -> float:
    try:
        for line in open("/proc/self/status").readlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    except Exception:
        pass
    return 0.0


def _cpu_times() -> tuple[float, float]:
    try:
        fields = open("/proc/self/stat").read().split()
        clk = os.sysconf("SC_CLK_TCK")
        return int(fields[13]) / clk, int(fields[14]) / clk
    except Exception:
        return 0.0, 0.0


def log(event: str, **kwargs) -> None:
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


# ---------------------------------------------------------------------------
# Tier 1 — per-package metadata extraction (the hot path)
# ---------------------------------------------------------------------------

def _object_exists(key: str) -> bool:
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


def _extract_info(pkg_path: str) -> dict:
    """Extract info/index.json, info/run_exports.json, info/about.json from a
    package. Works for both .conda (zip of zstd tars) and .tar.bz2."""
    index_json = None
    run_exports = None
    about = None
    for tar, member in package_streaming.stream_conda_info(pkg_path):
        if member.name == "info/index.json":
            index_json = json.load(tar.extractfile(member))
        elif member.name == "info/run_exports.json":
            run_exports = json.load(tar.extractfile(member))
        elif member.name == "info/about.json":
            about = json.load(tar.extractfile(member))
        if index_json is not None and run_exports is not None and about is not None:
            break
    if index_json is None:
        raise ValueError(f"info/index.json not found in {pkg_path}")
    return index_json, run_exports, about


def _compute_checksums(path: str) -> tuple[str, str, int]:
    """Returns (md5_hex, sha256_hex, size_bytes)."""
    md5 = hashlib.md5()
    sha256 = hashlib.sha256()
    size = 0
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            md5.update(chunk)
            sha256.update(chunk)
            size += len(chunk)
    return md5.hexdigest(), sha256.hexdigest(), size


def extract_metadata(channel: str, filename: str, staging_key: str) -> dict:
    """
    Download the staged package, extract its repodata entry, delete staging.
    Returns the full repodata entry dict ready to be merged into a shard.
    Does NOT run conda-index. Does NOT touch any other package.
    """
    t0 = time.time()
    log("extract_metadata.start", channel=channel, filename=filename)

    if not _object_exists(staging_key):
        # Already ingested by a previous retry — the staging object was deleted
        # after successful ingest. Return a sentinel so the caller can skip.
        log("extract_metadata.already_ingested", filename=filename)
        return {"already_ingested": True}

    with tempfile.TemporaryDirectory() as tmp:
        local_path = os.path.join(tmp, filename)

        t1 = time.time()
        s3.download_file(BUCKET, staging_key, local_path)
        dl_time = time.time() - t1
        size = os.path.getsize(local_path)
        log("extract_metadata.downloaded", filename=filename,
            size_bytes=size, elapsed_s=round(dl_time, 3))

        t1 = time.time()
        index_json, run_exports, about = _extract_info(local_path)
        md5, sha256, _ = _compute_checksums(local_path)
        extract_time = time.time() - t1
        log("extract_metadata.extracted", filename=filename,
            elapsed_s=round(extract_time, 3))

        subdir = index_json.get("subdir")
        if not subdir:
            raise ValueError(f"info/index.json has no `subdir` field in {filename}")

        # Build the repodata entry — same fields conda-index produces.
        entry = {**index_json, "md5": md5, "sha256": sha256, "size": size}
        if run_exports:
            entry["run_exports"] = run_exports

    # Compact browse record — enough to render a listing row + support search.
    about = about or {}
    browse = {
        "name": index_json.get("name"),
        "version": index_json.get("version"),
        "summary": about.get("summary", ""),
        "license": index_json.get("license", ""),
        "home": about.get("home", ""),
    }

    log("extract_metadata.done", filename=filename, subdir=subdir,
        name=index_json.get("name"), elapsed_s=round(time.time() - t0, 3))

    return {
        "filename": filename,
        "subdir": subdir,
        "name": index_json.get("name"),
        "entry": entry,
        "browse": browse,
        "version": index_json.get("version"),
    }


# ---------------------------------------------------------------------------
# Shard read/merge/write — the CEP-16 per-name shard is a msgpack+zstd file
# keyed by content hash. This is Tier 1's actual write: append one build's
# entry to the shard for that package name.
# ---------------------------------------------------------------------------

_ZSTD_C = zstd.ZstdCompressor(level=19)
_ZSTD_D = zstd.ZstdDecompressor()


def _pack_shard(shard: dict) -> tuple[bytes, str]:
    """msgpack+zstd encode a shard dict. Returns (compressed_bytes, sha256_hex)."""
    raw = msgpack.packb(shard, use_bin_type=True)
    compressed = _ZSTD_C.compress(raw)
    digest = hashlib.sha256(compressed).hexdigest()
    return compressed, digest


def _unpack_shard(compressed: bytes) -> dict:
    raw = _ZSTD_D.decompress(compressed)
    return msgpack.unpackb(raw, raw=False)


def _empty_shard() -> dict:
    return {"packages": {}, "packages.conda": {}}


def ingest_package(channel: str, filename: str, staging_key: str) -> dict:
    """
    Tier 1 hot path. Extract metadata for one package, then read-modify-write
    the single shard for that package NAME (append this build's entry),
    producing a new content-addressed shard object in R2.

    Returns {name, subdir, new_hash, old_hash, filename} so the caller
    (PackageIngestor DO) can update the shard index and clean up the old shard.
    Concurrency for the same name is serialized by the PackageIngestor DO,
    so no lock is needed here.
    """
    t0 = time.time()
    meta = extract_metadata(channel, filename, staging_key)
    if meta.get("already_ingested"):
        return {"already_ingested": True}

    name = meta["name"]
    subdir = meta["subdir"]
    entry = meta["entry"]
    prefix = f"{channel}/{subdir}"

    # Load the existing shard for this name, if any. The shard index maps
    # name -> current shard hash; the DO passes it in so we avoid a second
    # index read here.
    old_hash = None
    shard = _empty_shard()
    # Discover the current shard via the name->hash pointer object we maintain
    # at <prefix>/_shardptr/<name> (tiny, non-content-addressed, last-writer-wins).
    ptr_key = f"{prefix}/_shardptr/{name}"
    try:
        ptr = s3.get_object(Bucket=BUCKET, Key=ptr_key)
        old_hash = ptr["Body"].read().decode().strip()
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] not in ("404", "NoSuchKey"):
            raise

    if old_hash:
        try:
            existing = s3.get_object(Bucket=BUCKET, Key=f"{prefix}/{old_hash}.msgpack.zst")
            shard = _unpack_shard(existing["Body"].read())
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] not in ("404", "NoSuchKey"):
                raise
            shard = _empty_shard()

    # Append this build. .conda vs .tar.bz2 goes in the matching sub-dict.
    bucket_key = "packages.conda" if filename.endswith(".conda") else "packages"
    shard.setdefault(bucket_key, {})[filename] = entry

    # Write the new content-addressed shard.
    compressed, new_hash = _pack_shard(shard)
    s3.put_object(
        Bucket=BUCKET, Key=f"{prefix}/{new_hash}.msgpack.zst",
        Body=compressed,
        CacheControl="public, max-age=31536000, immutable",
    )
    # Update the name pointer to the new hash (last-writer-wins; DO serializes).
    s3.put_object(Bucket=BUCKET, Key=ptr_key, Body=new_hash.encode())

    # Write/refresh the compact browse record for this name. Merge subdir into
    # the set of subdirs this name is available in. Last-writer-wins; the
    # merger aggregates all of these into browse-index.json.
    browse = meta.get("browse", {})
    browse_key = f"{channel}/_browse/{name}.json"
    subdirs_seen = {subdir}
    try:
        existing_browse = json.loads(
            s3.get_object(Bucket=BUCKET, Key=browse_key)["Body"].read()
        )
        subdirs_seen |= set(existing_browse.get("subdirs", []))
        # Keep the highest version seen for the listing row.
        if existing_browse.get("version", "") > (browse.get("version") or ""):
            browse["version"] = existing_browse["version"]
            browse["summary"] = existing_browse.get("summary", browse.get("summary", ""))
            browse["license"] = existing_browse.get("license", browse.get("license", ""))
            browse["home"] = existing_browse.get("home", browse.get("home", ""))
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] not in ("404", "NoSuchKey"):
            raise
    browse["subdirs"] = sorted(subdirs_seen)
    s3.put_object(Bucket=BUCKET, Key=browse_key,
                  Body=json.dumps(browse).encode(), ContentType="application/json")

    # Move the package file into its final location and drop staging.
    s3.copy_object(Bucket=BUCKET, Key=f"{prefix}/{filename}",
                   CopySource={"Bucket": BUCKET, "Key": staging_key})
    s3.delete_object(Bucket=BUCKET, Key=staging_key)

    log("ingest_package.done", channel=channel, filename=filename, name=name,
        subdir=subdir, old_hash=old_hash, new_hash=new_hash,
        elapsed_s=round(time.time() - t0, 3))

    return {
        "filename": filename, "name": name, "subdir": subdir,
        "new_hash": new_hash, "old_hash": old_hash,
    }


# ---------------------------------------------------------------------------
# Legacy full-rebuild path — used for deletions and reconciliation only.
# Not called in the normal per-package upload flow.
# ---------------------------------------------------------------------------

PACKAGE_EXTENSIONS = (".conda", ".tar.bz2")


def _cached_filenames(subdir_path: str) -> set[str]:
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
    subprocess.run(
        ["python", "-m", "conda_index", channel_root, "--subdir", subdir, "--write-shards"],
        check=True,
    )


def _upload_shards(subdir_path: str, prefix: str) -> None:
    for fname in os.listdir(subdir_path):
        if fname.endswith(".msgpack.zst") and not fname.startswith("repodata"):
            local_path = os.path.join(subdir_path, fname)
            if os.path.isfile(local_path):
                s3.upload_file(
                    local_path, BUCKET, f"{prefix}{fname}",
                    ExtraArgs={"CacheControl": "public, max-age=31536000, immutable"},
                )


def _upload_repodata(subdir_path: str, prefix: str) -> None:
    UPLOAD_NAMES = {"repodata.json", "repodata_shards.msgpack.zst",
                    "patch_instructions.json", "index.html"}
    for fname in os.listdir(subdir_path):
        if fname in UPLOAD_NAMES:
            s3.upload_file(os.path.join(subdir_path, fname), BUCKET, f"{prefix}{fname}")


def rebuild_index_and_repodata(channel: str, subdir: str) -> dict:
    """
    Tier 2 + Tier 3, assembled FROM shards (not raw packages).
      Tier 2: rebuild the shard index by reading every name pointer.
      Tier 3: assemble repodata.json by unpacking every current shard.
    No package bytes are read. Runs in the SubdirIndexMerger DO's debounced alarm.
    """
    t0 = time.time()
    prefix = f"{channel}/{subdir}"
    log("rebuild.start", channel=channel, subdir=subdir)

    # Read all name -> hash pointers.
    name_to_hash: dict[str, str] = {}
    paginator = s3.get_paginator("list_objects_v2")
    ptr_prefix = f"{prefix}/_shardptr/"
    for page in paginator.paginate(Bucket=BUCKET, Prefix=ptr_prefix):
        for obj in page.get("Contents", []):
            name = obj["Key"][len(ptr_prefix):]
            if not name:
                continue
            h = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read().decode().strip()
            name_to_hash[name] = h

    # Tier 2: shard index maps name -> raw sha256 bytes of the shard.
    shards_map = {name: bytes.fromhex(h) for name, h in name_to_hash.items()}
    index = {
        "version": 1,
        "info": {"base_url": "", "shards_base_url": "", "subdir": subdir,
                 "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())},
        "shards": shards_map,
    }
    index_compressed = _ZSTD_C.compress(msgpack.packb(index, use_bin_type=True))
    s3.put_object(Bucket=BUCKET, Key=f"{prefix}/repodata_shards.msgpack.zst",
                  Body=index_compressed, CacheControl="public, max-age=300")

    # Tier 3: assemble repodata.json from the shards.
    packages: dict[str, dict] = {}
    packages_conda: dict[str, dict] = {}
    for name, h in name_to_hash.items():
        try:
            shard_obj = s3.get_object(Bucket=BUCKET, Key=f"{prefix}/{h}.msgpack.zst")
        except botocore.exceptions.ClientError:
            continue
        shard = _unpack_shard(shard_obj["Body"].read())
        packages.update(shard.get("packages", {}))
        packages_conda.update(shard.get("packages.conda", {}))

    repodata = {
        "info": {"subdir": subdir},
        "packages": packages,
        "packages.conda": packages_conda,
        "repodata_version": 2,
    }
    s3.put_object(Bucket=BUCKET, Key=f"{prefix}/repodata.json",
                  Body=json.dumps(repodata).encode(),
                  CacheControl="public, max-age=300",
                  ContentType="application/json")

    # Rebuild the per-channel browse index from all _browse/ records.
    _rebuild_browse_index(channel)
    # Ensure this channel is registered in the global channels index.
    _register_channel(channel)

    log("rebuild.done", channel=channel, subdir=subdir, n_names=len(name_to_hash),
        n_packages=len(packages) + len(packages_conda),
        elapsed_s=round(time.time() - t0, 3))
    return {"n_names": len(name_to_hash),
            "n_packages": len(packages) + len(packages_conda)}


def _rebuild_browse_index(channel: str) -> None:
    """Aggregate every per-name browse record under <channel>/_browse/ into a
    single browse-index.json served to the browse UI. Small (low single-digit
    MB even at conda-forge scale)."""
    t0 = time.time()
    records = []
    paginator = s3.get_paginator("list_objects_v2")
    browse_prefix = f"{channel}/_browse/"
    for page in paginator.paginate(Bucket=BUCKET, Prefix=browse_prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".json"):
                continue
            try:
                rec = json.loads(s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read())
                records.append(rec)
            except Exception:  # noqa: BLE001
                continue
    records.sort(key=lambda r: (r.get("name") or "").lower())
    body = json.dumps({"packages": records}).encode()
    s3.put_object(Bucket=BUCKET, Key=f"{channel}/browse-index.json",
                  Body=body, CacheControl="public, max-age=300",
                  ContentType="application/json")
    log("browse_index.done", channel=channel, n=len(records),
        elapsed_s=round(time.time() - t0, 3))


def rebuild_browse_from_repodata(channel: str) -> dict:
    """
    Backfill/reconcile browse records for a channel from its existing
    repodata.json files (across all subdirs). Base fields (name, version,
    license, subdirs) come from repodata; summary/home stay empty unless a
    later per-package ingest fills them. Regenerates every _browse/<name>.json
    and then the aggregated browse-index.json.
    """
    t0 = time.time()
    log("rebuild_browse.start", channel=channel)

    # Discover subdirs via delimiter listing.
    subdirs = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=f"{channel}/", Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            sd = cp["Prefix"][len(channel) + 1:].rstrip("/")
            if sd and not sd.startswith("_") and not sd.startswith("browse"):
                subdirs.add(sd)

    # name -> {version, license, subdirs}, keeping the highest version seen.
    recs: dict[str, dict] = {}
    for subdir in subdirs:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=f"{channel}/{subdir}/repodata.json")
        except botocore.exceptions.ClientError:
            continue
        rd = json.loads(obj["Body"].read())
        allpkgs = {**rd.get("packages", {}), **rd.get("packages.conda", {})}
        for meta in allpkgs.values():
            name = meta.get("name")
            if not name:
                continue
            r = recs.setdefault(name, {"name": name, "version": "", "summary": "",
                                       "license": "", "home": "", "subdirs": set()})
            r["subdirs"].add(subdir)
            v = meta.get("version", "")
            if v > r["version"]:
                r["version"] = v
                r["license"] = meta.get("license", "") or r["license"]

    # Write per-name records (preserving any existing summary/home).
    for name, r in recs.items():
        r["subdirs"] = sorted(r["subdirs"])
        browse_key = f"{channel}/_browse/{name}.json"
        try:
            prev = json.loads(s3.get_object(Bucket=BUCKET, Key=browse_key)["Body"].read())
            r["summary"] = r["summary"] or prev.get("summary", "")
            r["home"] = r["home"] or prev.get("home", "")
        except botocore.exceptions.ClientError:
            pass
        s3.put_object(Bucket=BUCKET, Key=browse_key,
                      Body=json.dumps(r).encode(), ContentType="application/json")

    _rebuild_browse_index(channel)
    _register_channel(channel)
    log("rebuild_browse.done", channel=channel, n_names=len(recs),
        n_subdirs=len(subdirs), elapsed_s=round(time.time() - t0, 3))
    return {"n_names": len(recs), "n_subdirs": len(subdirs)}


def _register_channel(channel: str) -> None:
    """Add this channel to the global _channels-index.json (list of names).
    Read-modify-write; the merge is serialized enough in practice (rare vs.
    per-package writes) that last-writer-wins is acceptable — a missed add is
    repaired on the channel's next rebuild."""
    key = "_channels-index.json"
    names = set()
    try:
        existing = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
        names = set(existing.get("channels", []))
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] not in ("404", "NoSuchKey"):
            raise
    if channel not in names:
        names.add(channel)
        s3.put_object(Bucket=BUCKET, Key=key,
                      Body=json.dumps({"channels": sorted(names)}).encode(),
                      ContentType="application/json")


def reindex(channel: str, subdir: str) -> None:
    """Full reindex via conda-index — used for deletions and reconciliation only."""
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


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("content-length", 0))
        payload = json.loads(self.rfile.read(length) or b"{}")

        if self.path == "/ingest-package":
            channel = payload.get("channel")
            filename = payload.get("filename")
            staging_key = payload.get("staging_key")
            if not channel or not filename or not staging_key:
                self._respond(400, b"missing channel, filename, or staging_key")
                return
            try:
                result = ingest_package(channel, filename, staging_key)
                self._respond(200, json.dumps(result).encode())
            except Exception as e:  # noqa: BLE001
                log("ingest_package.error", filename=filename, error=str(e))
                self._respond(500, json.dumps({"error": str(e)}).encode())

        elif self.path == "/rebuild-index":
            channel = payload.get("channel")
            subdir = payload.get("subdir")
            if not channel or not subdir:
                self._respond(400, b"missing channel or subdir")
                return
            try:
                result = rebuild_index_and_repodata(channel, subdir)
                self._respond(200, json.dumps(result).encode())
            except Exception as e:  # noqa: BLE001
                log("rebuild.error", channel=channel, subdir=subdir, error=str(e))
                self._respond(500, json.dumps({"error": str(e)}).encode())

        elif self.path == "/rebuild-browse":
            channel = payload.get("channel")
            if not channel:
                self._respond(400, b"missing channel")
                return
            try:
                result = rebuild_browse_from_repodata(channel)
                self._respond(200, json.dumps(result).encode())
            except Exception as e:  # noqa: BLE001
                log("rebuild_browse.error", channel=channel, error=str(e))
                self._respond(500, json.dumps({"error": str(e)}).encode())

        elif self.path == "/extract-metadata":
            channel = payload.get("channel")
            filename = payload.get("filename")
            staging_key = payload.get("staging_key")
            if not channel or not filename or not staging_key:
                self._respond(400, b"missing channel, filename, or staging_key")
                return
            try:
                result = extract_metadata(channel, filename, staging_key)
                self._respond(200, json.dumps(result).encode())
            except Exception as e:  # noqa: BLE001
                log("extract_metadata.error", filename=filename, error=str(e))
                self._respond(500, json.dumps({"error": str(e)}).encode())

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
                s3.delete_object(Bucket=BUCKET, Key=f"{channel}/{subdir}/{filename}")
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
