#!/usr/bin/env python3
"""
Delete orphaned shard files from R2.

A shard file is orphaned when the _shardptr for its package name has moved to
a newer hash (every upload rewrites the shard and leaves the old one behind).
Only the hash currently pointed to by _shardptr/<name> is live; all others for
that subdir are garbage.

Usage:
  python purge_orphaned_shards.py [--channel CHANNEL] [--dry-run]

  --channel   Channel prefix to scan, e.g. "mattkram/anaconda-cloud".
              Defaults to scanning all channels found in the bucket.
  --dry-run   Print what would be deleted without actually deleting.

Credential resolution order (first wins):
  1. Environment variables / .dev.vars: R2_ACCOUNT_ID, R2_ACCESS_KEY_ID,
     R2_SECRET_ACCESS_KEY  →  uses boto3 against the R2 S3-compat endpoint.
  2. Wrangler OAuth token (~/.config/wrangler/config/default.toml or the
     macOS Library path)  →  uses the Cloudflare REST API directly.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import sys
import urllib.parse
import urllib.request
from collections import defaultdict

# ---------------------------------------------------------------------------
# Credential loading
# ---------------------------------------------------------------------------

dev_vars = pathlib.Path(__file__).parent.parent / ".dev.vars"
if dev_vars.exists():
    for line in dev_vars.read_text().splitlines():
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

BUCKET = os.environ.get("R2_BUCKET_NAME", "conda-channel")

# --- Try boto3 path first ---
_s3 = None
if os.environ.get("R2_ACCOUNT_ID") and os.environ.get("R2_ACCESS_KEY_ID"):
    import boto3
    from botocore.exceptions import ClientError as _ClientError

    _s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )
    print("Using boto3 / R2 S3-compat API")

# --- Fall back to Wrangler OAuth token ---
_cf_token: str | None = None
_cf_account_id: str | None = None

if _s3 is None:
    import tomllib

    _wrangler_config_candidates = [
        pathlib.Path.home() / "Library/Preferences/.wrangler/config/default.toml",
        pathlib.Path.home() / ".config/wrangler/config/default.toml",
    ]
    for _p in _wrangler_config_candidates:
        if _p.exists():
            with open(_p, "rb") as _f:
                _wc = tomllib.load(_f)
            _cf_token = _wc.get("oauth_token")
            break

    if _cf_token is None:
        print("ERROR: No R2 credentials found. Provide R2_ACCOUNT_ID/R2_ACCESS_KEY_ID/R2_SECRET_ACCESS_KEY in .dev.vars, or log in with `wrangler login`.", file=sys.stderr)
        sys.exit(1)

    # Discover account ID via Cloudflare API
    def _cf_get(path: str, params: dict | None = None) -> dict:
        url = f"https://api.cloudflare.com/client/v4{path}"
        if params:
            url += "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {_cf_token}"})
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())

    _accounts = _cf_get("/accounts")
    _cf_account_id = _accounts["result"][0]["id"]
    print(f"Using Cloudflare REST API (account {_cf_account_id})")


# ---------------------------------------------------------------------------
# Unified R2 operations — delegates to boto3 or CF REST API
# ---------------------------------------------------------------------------

def list_keys(prefix: str) -> list[str]:
    if _s3 is not None:
        keys = []
        paginator = _s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys
    else:
        # Cloudflare REST API — max 1000 per page but only returns 20 by default
        keys = []
        cursor = None
        while True:
            params: dict = {"prefix": prefix, "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            url = (
                f"https://api.cloudflare.com/client/v4/accounts/{_cf_account_id}"
                f"/r2/buckets/{BUCKET}/objects"
                f"?{urllib.parse.urlencode(params)}"
            )
            req = urllib.request.Request(url, headers={"Authorization": f"Bearer {_cf_token}"})
            with urllib.request.urlopen(req, timeout=30) as r:
                result = json.loads(r.read())
            for obj in result.get("result", []):
                keys.append(obj["key"])
            info = result.get("result_info", {})
            if not info.get("is_truncated") or not info.get("cursor"):
                break
            cursor = info["cursor"]
        return keys


def read_key(key: str) -> str | None:
    if _s3 is not None:
        try:
            return _s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode().strip()
        except Exception as e:
            if hasattr(e, "response") and e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return None
            raise
    else:
        url = (
            f"https://api.cloudflare.com/client/v4/accounts/{_cf_account_id}"
            f"/r2/buckets/{BUCKET}/objects/{urllib.parse.quote(key, safe='')}"
        )
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {_cf_token}"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.read().decode().strip()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise


def delete_keys(keys: list[str], dry_run: bool) -> int:
    """Delete a list of keys. Returns number successfully deleted."""
    if not keys:
        return 0
    if dry_run:
        for k in keys:
            print(f"  [dry-run] would delete: {k}")
        return len(keys)

    if _s3 is not None:
        # S3 batch delete — up to 1000 per call
        n_deleted = 0
        for i in range(0, len(keys), 1000):
            batch = keys[i : i + 1000]
            resp = _s3.delete_objects(
                Bucket=BUCKET,
                Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
            )
            errors = resp.get("Errors", [])
            for err in errors:
                print(f"  ERROR deleting {err['Key']}: {err['Code']} {err['Message']}", file=sys.stderr)
            n_deleted += len(batch) - len(errors)
        return n_deleted
    else:
        # CF REST API — one DELETE per object (no batch endpoint in this API)
        n_deleted = 0
        for key in keys:
            url = (
                f"https://api.cloudflare.com/client/v4/accounts/{_cf_account_id}"
                f"/r2/buckets/{BUCKET}/objects/{urllib.parse.quote(key, safe='')}"
            )
            req = urllib.request.Request(
                url,
                method="DELETE",
                headers={"Authorization": f"Bearer {_cf_token}"},
            )
            try:
                with urllib.request.urlopen(req, timeout=30) as r:
                    r.read()
                n_deleted += 1
            except urllib.error.HTTPError as e:
                print(f"  ERROR deleting {key}: HTTP {e.code}", file=sys.stderr)
        return n_deleted


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def find_subdirs(channel: str) -> list[str]:
    all_keys = list_keys(channel + "/")
    subdirs: set[str] = set()
    for key in all_keys:
        rel = key[len(channel) + 1:]
        parts = rel.split("/")
        if parts[0] not in ("_shardptr", "_browse", "_incoming") and len(parts) >= 2:
            subdirs.add(parts[0])
    return sorted(subdirs)


def purge_channel_subdir(channel: str, subdir: str, dry_run: bool) -> tuple[int, int]:
    """Find and delete orphaned shards. Returns (n_orphans, n_deleted)."""
    prefix = f"{channel}/{subdir}"

    # All content-addressed shard files (excludes repodata_shards.msgpack.zst index)
    all_shard_keys = [
        k for k in list_keys(prefix + "/")
        if k.endswith(".msgpack.zst") and not k.split("/")[-1].startswith("repodata")
    ]
    hash_to_key = {
        k.split("/")[-1].removesuffix(".msgpack.zst"): k
        for k in all_shard_keys
    }

    if not hash_to_key:
        return 0, 0

    # Current live hashes from _shardptr/<name> pointer files
    ptr_prefix = f"{prefix}/_shardptr/"
    live_hashes: set[str] = set()
    for ptr_key in list_keys(ptr_prefix):
        h = read_key(ptr_key)
        if h:
            live_hashes.add(h)

    orphan_keys = [key for h, key in hash_to_key.items() if h not in live_hashes]

    print(
        f"  {subdir}: {len(hash_to_key)} shards total, "
        f"{len(live_hashes)} live, {len(orphan_keys)} orphaned"
    )

    n_deleted = delete_keys(orphan_keys, dry_run)
    return len(orphan_keys), n_deleted


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--channel",
        help='Channel prefix to scan, e.g. "mattkram/anaconda-cloud"',
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be deleted without actually deleting",
    )
    args = parser.parse_args()

    if args.dry_run:
        print("DRY RUN — nothing will be deleted\n")

    if args.channel:
        channels = [args.channel.rstrip("/")]
    else:
        top_keys = list_keys("")
        prefixes: set[str] = set()
        for k in top_keys:
            parts = k.split("/")
            if len(parts) >= 3:
                prefixes.add(f"{parts[0]}/{parts[1]}")
        channels = sorted(prefixes)
        print(f"Auto-discovered channels: {channels}\n")

    total_orphans = 0
    total_deleted = 0

    for channel in channels:
        print(f"Channel: {channel}")
        subdirs = find_subdirs(channel)
        if not subdirs:
            print("  (no subdirs found)")
            continue
        for subdir in subdirs:
            n_orphans, n_deleted = purge_channel_subdir(channel, subdir, args.dry_run)
            total_orphans += n_orphans
            total_deleted += n_deleted

    print(f"\nTotal orphaned shards found: {total_orphans}")
    if args.dry_run:
        print(f"Would delete: {total_orphans}")
    else:
        print(f"Deleted: {total_deleted}")


if __name__ == "__main__":
    main()
