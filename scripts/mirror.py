#!/usr/bin/env python3
"""
Mirror a random subset of repo.anaconda.com/pkgs/main noarch packages to our channel.

Fetches repodata from both source and destination, picks N random packages
that aren't already in the destination, uploads them, then waits for repodata
to reflect all of them.

Usage:
  python scripts/mirror.py --channel main --count 100
  python scripts/mirror.py --channel main --count 100 --workers 8 --no-wait

Performance flags:
  --workers N    Parallel upload threads (default: 4)
  --no-wait      Don't poll repodata after upload (fire-and-forget)
"""
from __future__ import annotations

import argparse
import json
import pathlib
import random
import sys
import tempfile
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from channel_client import ChannelClient, ChannelError, DEFAULT_WORKER_URL

SOURCE_REPODATA = "https://repo.anaconda.com/pkgs/main/noarch/repodata.json"
SOURCE_BASE     = "https://repo.anaconda.com/pkgs/main/noarch"
POLL_TIMEOUT    = 300   # 5 min — more packages means longer indexing


def fetch_repodata(url: str, label: str) -> dict:
    print(f"Fetching {label} repodata...")
    req = urllib.request.Request(url, headers={"user-agent": "conda-mirror/1.0"})
    try:
        with urllib.request.urlopen(req) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return {}
        raise


def all_filenames(repodata: dict) -> set[str]:
    """Return every package filename present in a repodata dict."""
    return (
        set(repodata.get("packages", {}).keys()) |
        set(repodata.get("packages.conda", {}).keys())
    )


def candidate_packages(repodata: dict) -> list[dict]:
    """
    Return one entry per unique package name — the latest version, preferring
    .conda over .tar.bz2. Used to build the pool to sample from.
    """
    all_pkgs: dict[str, dict] = {}
    # Process .tar.bz2 first so .conda overwrites for same name+version
    for fmt in ("packages", "packages.conda"):
        for fname, meta in repodata.get(fmt, {}).items():
            all_pkgs[fname] = {**meta, "filename": fname}

    latest: dict[str, dict] = {}
    for meta in all_pkgs.values():
        name = meta.get("name", "")
        existing = latest.get(name)
        if not existing:
            latest[name] = meta
        else:
            if meta.get("version", "") > existing.get("version", ""):
                latest[name] = meta
    return list(latest.values())


def pick_packages(source_repodata: dict, dest_repodata: dict, count: int, seed: int | None) -> list[dict]:
    """
    Pick `count` random packages from source that aren't already in dest.
    Uses the destination's full filename set so we never re-upload something
    that's already indexed (even under a different format).
    """
    already_have = all_filenames(dest_repodata)
    candidates = [
        m for m in candidate_packages(source_repodata)
        if m["filename"] not in already_have
    ]

    if not candidates:
        return []

    rng = random.Random(seed)
    rng.shuffle(candidates)
    return candidates[:count]


def download_package(meta: dict, dest_dir: pathlib.Path) -> pathlib.Path:
    filename = meta["filename"]
    url = f"{SOURCE_BASE}/{filename}"
    dest = dest_dir / filename
    if dest.exists():
        return dest
    req = urllib.request.Request(url, headers={"user-agent": "conda-mirror/1.0"})
    with urllib.request.urlopen(req) as resp:
        dest.write_bytes(resp.read())
    return dest


def upload_one(
    client: ChannelClient,
    channel: str,
    token: str,
    meta: dict,
    dest_dir: pathlib.Path,
) -> tuple[str, float, str | None]:
    """Download then upload one package. Returns (filename, elapsed_s, error|None)."""
    filename = meta["filename"]
    t0 = time.time()
    try:
        pkg_path = download_package(meta, dest_dir)
        client.upload(channel, pkg_path, token, progress=False)
        return filename, time.time() - t0, None
    except Exception as e:
        return filename, time.time() - t0, str(e)


def main() -> None:
    parser = argparse.ArgumentParser(description="Mirror random pkgs/main noarch packages to a channel")
    parser.add_argument("--channel", "-c", default="main", help="Destination channel (default: main)")
    parser.add_argument("--count", "-n", type=int, default=100, help="Number of packages to mirror (default: 100)")
    parser.add_argument("--workers", "-w", type=int, default=4, help="Parallel upload threads (default: 4)")
    parser.add_argument("--worker-url", default=DEFAULT_WORKER_URL, metavar="URL")
    parser.add_argument("--reauth", action="store_true", help="Force re-authentication")
    parser.add_argument("--no-wait", action="store_true", help="Skip repodata polling after upload")
    parser.add_argument("--keep", action="store_true", help="Keep downloaded packages after upload")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducible package selection")
    args = parser.parse_args()

    client = ChannelClient(worker_url=args.worker_url)

    # Auth
    print("Authenticating...")
    try:
        token = client.login(force=args.reauth)
        print("  Token ready.\n")
    except ChannelError as e:
        sys.exit(f"Auth failed: {e}")

    # Fetch source and destination repodata in parallel
    source_repodata = fetch_repodata(SOURCE_REPODATA, "source (pkgs/main)")
    dest_url = f"{args.worker_url.rstrip('/')}/{args.channel}/noarch/repodata.json"
    dest_repodata = fetch_repodata(dest_url, f"destination ({args.channel})")

    already = len(all_filenames(dest_repodata))
    print(f"  Destination already has {already} package(s).\n")

    # Select packages
    selected = pick_packages(source_repodata, dest_repodata, args.count, args.seed)
    if not selected:
        print("Nothing new to upload — destination already contains all candidates.")
        sys.exit(0)
    if len(selected) < args.count:
        print(f"Note: only {len(selected)} new packages available (requested {args.count}).")

    total_bytes = sum(m.get("size", 0) for m in selected)
    print(f"Selected {len(selected)} packages ({total_bytes / 1024 / 1024:.1f} MB total)")
    print(f"Uploading to '{args.channel}' with {args.workers} parallel workers\n")

    # Upload with thread pool
    with tempfile.TemporaryDirectory() as tmpdir:
        dest_dir = pathlib.Path(tmpdir)
        t_start = time.time()
        results: list[tuple[str, float, str | None]] = []
        done = 0
        errors = 0

        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {
                pool.submit(upload_one, client, args.channel, token, meta, dest_dir): meta
                for meta in selected
            }
            for future in as_completed(futures):
                filename, elapsed, error = future.result()
                done += 1
                if error:
                    errors += 1
                    status = f"ERROR: {error}"
                else:
                    size_kb = futures[future].get("size", 0) // 1024
                    status = f"{size_kb}kB in {elapsed:.1f}s"
                print(f"  [{done:>3}/{len(selected)}] {filename}  {status}")
                results.append((filename, elapsed, error))

        wall = time.time() - t_start
        ok_count = len(selected) - errors
        errored_names = {r[0] for r in results if r[2]}
        ok_bytes = sum(m.get("size", 0) for m in selected if m["filename"] not in errored_names)

        print(f"\n{'='*60}")
        print(f"Uploaded {ok_count}/{len(selected)} packages in {wall:.1f}s")
        if wall > 0:
            print(f"Throughput: {ok_bytes / 1024 / 1024 / wall:.2f} MB/s  "
                  f"({ok_count / wall:.1f} pkgs/s)")
        if errors:
            print(f"Errors ({errors}):")
            for fname, _, err in results:
                if err:
                    print(f"  {fname}: {err}")
        print(f"{'='*60}\n")

        if args.keep:
            import shutil
            keep_dir = pathlib.Path("scripts/mirror-cache")
            keep_dir.mkdir(exist_ok=True)
            for p in dest_dir.iterdir():
                shutil.copy2(p, keep_dir / p.name)
            print(f"Packages kept in {keep_dir}\n")

    # Poll repodata
    if not args.no_wait and ok_count > 0:
        uploaded_names = [r[0] for r in results if not r[2]]
        print(f"Waiting for indexing (up to {POLL_TIMEOUT}s)...")
        t_index = time.time()
        ok = client.poll_repodata(args.channel, uploaded_names, timeout=POLL_TIMEOUT)
        index_time = time.time() - t_index
        if ok:
            print(f"All {ok_count} packages indexed in {index_time:.1f}s.")
        else:
            print(f"WARNING: not all packages appeared in repodata within {POLL_TIMEOUT}s.")


if __name__ == "__main__":
    main()
