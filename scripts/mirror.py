#!/usr/bin/env python3
"""
Mirror a subset of repo.anaconda.com/pkgs/main noarch packages to our channel.

Fetches repodata from the source, picks the N smallest packages (latest
version per name only), downloads each one, uploads it to our Worker, then
waits for repodata to reflect all of them.

Usage:
  python scripts/mirror.py --channel main --count 100
  python scripts/mirror.py --channel main --count 100 --workers 8 --no-wait

Performance flags:
  --workers N    Parallel upload threads (default: 4)
  --no-wait      Don't poll repodata after upload (fire-and-forget)
"""
from __future__ import annotations

import argparse
import pathlib
import sys
import tempfile
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from channel_client import ChannelClient, ChannelError, DEFAULT_WORKER_URL

SOURCE_REPODATA = "https://repo.anaconda.com/pkgs/main/noarch/repodata.json"
SOURCE_BASE     = "https://repo.anaconda.com/pkgs/main/noarch"
POLL_TIMEOUT    = 300   # 5 min — more packages means longer indexing


def fetch_repodata(url: str) -> dict:
    print(f"Fetching repodata from {url} ...")
    req = urllib.request.Request(url, headers={"user-agent": "conda-mirror/1.0"})
    with urllib.request.urlopen(req) as resp:
        import json
        return json.load(resp)


def pick_packages(repodata: dict, count: int) -> list[dict]:
    """
    Return up to `count` packages: latest version per name, sorted by size
    ascending, then trimmed to `count`. Prefers .conda over .tar.bz2.
    """
    all_pkgs: dict[str, dict] = {}

    # Merge both package dicts; .conda entries overwrite .tar.bz2 for same name+version
    for fmt in ("packages", "packages.conda"):
        for fname, meta in repodata.get(fmt, {}).items():
            all_pkgs[fname] = {**meta, "filename": fname}

    # Keep only latest version per package name
    latest: dict[str, dict] = {}
    for meta in all_pkgs.values():
        name = meta.get("name", "")
        existing = latest.get(name)
        if not existing:
            latest[name] = meta
        else:
            # Compare version strings lexicographically (good enough for selection)
            if meta.get("version", "") > existing.get("version", ""):
                latest[name] = meta

    # Sort by size ascending, take first `count`
    selected = sorted(latest.values(), key=lambda m: m.get("size", 0))[:count]
    return selected


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


def upload_one(client: ChannelClient, channel: str, token: str,
               meta: dict, dest_dir: pathlib.Path) -> tuple[str, float, str | None]:
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
    parser = argparse.ArgumentParser(description="Mirror pkgs/main noarch packages to a channel")
    parser.add_argument("--channel", "-c", default="main", help="Destination channel (default: main)")
    parser.add_argument("--count", "-n", type=int, default=100, help="Number of packages to mirror (default: 100)")
    parser.add_argument("--workers", "-w", type=int, default=4, help="Parallel upload threads (default: 4)")
    parser.add_argument("--worker-url", default=DEFAULT_WORKER_URL, metavar="URL")
    parser.add_argument("--reauth", action="store_true", help="Force re-authentication")
    parser.add_argument("--no-wait", action="store_true", help="Skip repodata polling after upload")
    parser.add_argument("--keep", action="store_true", help="Keep downloaded packages after upload")
    args = parser.parse_args()

    client = ChannelClient(worker_url=args.worker_url)

    # Auth
    print("Authenticating...")
    try:
        token = client.login(force=args.reauth)
        print("  Token ready.\n")
    except ChannelError as e:
        sys.exit(f"Auth failed: {e}")

    # Fetch and select packages
    repodata = fetch_repodata(SOURCE_REPODATA)
    selected = pick_packages(repodata, args.count)
    total_bytes = sum(m.get("size", 0) for m in selected)
    print(f"Selected {len(selected)} packages ({total_bytes / 1024 / 1024:.1f} MB total)")
    print(f"Uploading to channel '{args.channel}' with {args.workers} parallel workers\n")

    # Upload with thread pool, downloading to a temp dir
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
        ok_bytes = sum(
            futures[f].get("size", 0)
            for f, meta in futures.items()
            # re-match by result — just sum all selected sizes minus errored
        )
        # simpler: sum sizes of non-errored packages
        errored_names = {r[0] for r in results if r[2]}
        ok_bytes = sum(m.get("size", 0) for m in selected if m["filename"] not in errored_names)

        print(f"\n{'='*60}")
        print(f"Uploaded {ok_count}/{len(selected)} packages in {wall:.1f}s")
        print(f"Throughput: {ok_bytes / 1024 / 1024 / wall:.2f} MB/s  "
              f"({ok_count / wall:.1f} pkgs/s)")
        if errors:
            print(f"Errors: {errors}")
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
