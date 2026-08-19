#!/usr/bin/env python3
"""
Migrate R2 objects from flat channel names to namespaced names.
  anaconda-cloud/...       → mattkram/anaconda-cloud/...
  anaconda-cloud-2/...     → mattkram/anaconda-cloud-2/...

Uses server-side copy (no data transfer through this machine).
Reads credentials from environment variables or .dev.vars file.
"""
from __future__ import annotations
import os, pathlib, sys, time
import boto3
from botocore.exceptions import ClientError

# --- Load credentials ---
dev_vars = pathlib.Path(__file__).parent.parent / ".dev.vars"
if dev_vars.exists():
    for line in dev_vars.read_text().splitlines():
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

ACCOUNT_ID      = os.environ["R2_ACCOUNT_ID"]
ACCESS_KEY_ID   = os.environ["R2_ACCESS_KEY_ID"]
SECRET_KEY      = os.environ["R2_SECRET_ACCESS_KEY"]
BUCKET          = os.environ.get("R2_BUCKET_NAME", "conda-channel")

MIGRATIONS = [
    ("anaconda-cloud",   "mattkram/anaconda-cloud"),
    ("anaconda-cloud-2", "mattkram/anaconda-cloud-2"),
]

s3 = boto3.client(
    "s3",
    endpoint_url=f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_KEY,
    region_name="auto",
)

def list_objects(prefix: str) -> list[str]:
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys

def migrate(src_prefix: str, dst_prefix: str, dry_run: bool = False) -> None:
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Migrating: {src_prefix}/ → {dst_prefix}/")
    keys = list_objects(src_prefix + "/")
    print(f"  Found {len(keys)} objects")
    if not keys:
        print("  Nothing to do.")
        return

    copied = deleted = errors = 0
    for i, src_key in enumerate(keys, 1):
        # Rewrite prefix
        rel = src_key[len(src_prefix):]   # e.g. "/noarch/pkg.conda"
        dst_key = dst_prefix + rel

        if (i % 100 == 0) or i == len(keys):
            print(f"  [{i}/{len(keys)}] ...", end="\r")

        if dry_run:
            print(f"  COPY {src_key} → {dst_key}")
            continue

        # Server-side copy
        try:
            s3.copy_object(
                Bucket=BUCKET,
                CopySource={"Bucket": BUCKET, "Key": src_key},
                Key=dst_key,
            )
            copied += 1
        except ClientError as e:
            print(f"\n  ERROR copying {src_key}: {e}")
            errors += 1
            continue

        # Delete original after confirmed copy
        try:
            s3.delete_object(Bucket=BUCKET, Key=src_key)
            deleted += 1
        except ClientError as e:
            print(f"\n  ERROR deleting {src_key}: {e}")
            errors += 1

    if not dry_run:
        print(f"\n  Done: {copied} copied, {deleted} deleted, {errors} errors")

if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv

    for src, dst in MIGRATIONS:
        # Check destination doesn't already exist to avoid double-migration
        existing_dst = list_objects(dst + "/")
        existing_src = list_objects(src + "/")

        if existing_dst and not existing_src:
            print(f"  SKIP {src} → already migrated to {dst} ({len(existing_dst)} objects, src gone)")
            continue
        if existing_dst and existing_src:
            print(f"  WARNING: both {src}/ ({len(existing_src)}) and {dst}/ ({len(existing_dst)}) exist — skipping to be safe")
            continue
        if not existing_src:
            print(f"  SKIP {src} → no objects found at source")
            continue

        migrate(src, dst, dry_run=dry_run)

    print("\nMigration complete.")
