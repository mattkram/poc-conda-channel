#!/usr/bin/env python3
"""
bulk_mirror.py — Mirror a curated list of conda packages into conda-wit.

Reads a JSON manifest of packages, downloads each directly from the source
channel, uploads to R2 via the conda-wit upload API, and waits for indexing.

Usage:
    python3 scripts/bulk_mirror.py --manifest /path/to/top100_results.json \\
        --server https://conda.matt-kramer.com \\
        --token <upload_token> \\
        [--workers 8] [--channels mattkram/conda-forge mattkram/bioconda] \\
        [--dry-run] [--resume]

The upload token can also be set via CONDA_WIT_TOKEN env var.
"""
import argparse
import json
import os
import sqlite3
import sys
import tempfile
import threading
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

_db_lock = threading.Lock()

# ---------------------------------------------------------------------------
# State DB — tracks progress so runs are resumable
# ---------------------------------------------------------------------------

def open_state_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS uploads (
            channel TEXT NOT NULL,
            filename TEXT NOT NULL,
            status TEXT NOT NULL,  -- 'ok', 'failed', 'skipped'
            error TEXT,
            ts REAL,
            PRIMARY KEY (channel, filename)
        )
    """)
    conn.commit()
    return conn


def already_done(conn: sqlite3.Connection, channel: str, filename: str) -> bool:
    with _db_lock:
        row = conn.execute(
            "SELECT status FROM uploads WHERE channel=? AND filename=?",
            (channel, filename)
        ).fetchone()
    return row is not None and row[0] == "ok"


def record(conn: sqlite3.Connection, channel: str, filename: str,
           status: str, error: str = ""):
    with _db_lock:
        conn.execute(
            "INSERT OR REPLACE INTO uploads (channel, filename, status, error, ts) VALUES (?,?,?,?,?)",
            (channel, filename, status, error, time.time())
        )
        conn.commit()


# ---------------------------------------------------------------------------
# Upload helpers
# ---------------------------------------------------------------------------

def api(server: str, path: str, body: dict, token: str) -> dict:
    """POST JSON to the conda-wit API."""
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"{server}{path}",
        data=data,
        headers={
            "content-type": "application/json",
            "authorization": f"Bearer {token}",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def upload_package(server: str, token: str, channel: str,
                   filename: str, source_url: str, dry_run: bool) -> str:
    """Download from source and upload to conda-wit. Returns 'ok' or raises."""
    if dry_run:
        print(f"  [dry-run] would upload {channel}/{filename}")
        return "ok"

    # Step 1 — get presigned PUT URL
    init = api(server, "/upload/init", {"channel": channel, "filename": filename}, token)
    upload_url = init["upload_url"]

    # Step 2 — stream from source directly to R2
    with urllib.request.urlopen(source_url, timeout=120) as src:
        data = src.read()

    put_req = urllib.request.Request(
        upload_url,
        data=data,
        headers={"content-type": "application/octet-stream"},
        method="PUT",
    )
    with urllib.request.urlopen(put_req, timeout=120):
        pass

    # Step 3 — notify complete
    api(server, "/upload/complete", {"channel": channel, "filename": filename}, token)
    return "ok"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Bulk mirror packages into conda-wit")
    parser.add_argument("--manifest", required=True, help="Path to top100_results.json")
    parser.add_argument("--server", default="https://conda.matt-kramer.com")
    parser.add_argument("--token", default=os.environ.get("CONDA_WIT_TOKEN", ""))
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--channels", nargs="*",
                        help="Limit to these channels (default: all)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--resume", action="store_true",
                        help="Skip already-uploaded packages (uses mirror_state.db)")
    parser.add_argument("--state-db", default="mirror_state.db")
    args = parser.parse_args()

    if not args.token and not args.dry_run:
        sys.exit("ERROR: --token or CONDA_WIT_TOKEN required")

    manifest = json.load(open(args.manifest))
    state = open_state_db(args.state_db)

    # Flatten manifest into list of tasks
    tasks = []
    for channel, packages in manifest.items():
        if args.channels and channel not in args.channels:
            continue
        for pkg in packages:
            tasks.append(pkg)

    print(f"Packages to process: {len(tasks)} across "
          f"{len(set(t['channel'] for t in tasks))} channels")
    if args.dry_run:
        print("DRY RUN — no actual uploads")

    ok = skipped = failed = 0

    def process(pkg):
        channel = pkg["channel"]
        filename = pkg["filename"]
        source_url = pkg["url"]

        if args.resume and already_done(state, channel, filename):
            return channel, filename, "skipped", ""

        try:
            upload_package(args.server, args.token, channel,
                           filename, source_url, args.dry_run)
            record(state, channel, filename, "ok")
            return channel, filename, "ok", ""
        except Exception as e:
            err = str(e)
            try:
                record(state, channel, filename, "failed", err)
            except Exception:
                pass  # don't crash the worker if DB write fails
            return channel, filename, "failed", err

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process, pkg): pkg for pkg in tasks}
        for i, future in enumerate(as_completed(futures), 1):
            try:
                channel, filename, status, error = future.result()
            except Exception as e:
                failed += 1
                print(f"[{i}/{len(tasks)}] ✗ unexpected error: {e}")
                continue
            if status == "ok":
                ok += 1
                print(f"[{i}/{len(tasks)}] ✓ {channel}/{filename}")
            elif status == "skipped":
                skipped += 1
                print(f"[{i}/{len(tasks)}] - {channel}/{filename} (already done)")
            else:
                failed += 1
                print(f"[{i}/{len(tasks)}] ✗ {channel}/{filename}: {error}")

    print(f"\nDone. ok={ok} skipped={skipped} failed={failed}")
    if failed > 0:
        print(f"Re-run with --resume to retry failed uploads.")
        sys.exit(1)


if __name__ == "__main__":
    main()
