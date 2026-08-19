#!/usr/bin/env python3
"""
Mirror packages from any anaconda.org channel to our server.

Fetches repodata from both source and destination, picks packages
not already in the destination, uploads them.

Usage:
  # mirror latest-version noarch from pkgs/main (original behaviour)
  python scripts/mirror.py --channel main --count 100

  # mirror ALL versions from anaconda-cloud noarch + linux-64, fire-and-forget
  python scripts/mirror.py --channel anaconda-cloud --all-versions \\
      --source https://conda.anaconda.org/anaconda-cloud \\
      --subdirs noarch linux-64 --workers 16 --no-wait --stats stats-ac.json

Performance flags:
  --workers N    Parallel upload threads (default: 8)
  --no-wait      Don't poll repodata after upload (fire-and-forget)
  --all-versions Include every version/build, not just latest per name
  --subdirs      Space-separated list of subdirs to mirror (default: noarch)
  --source URL   Source channel base URL (default: https://repo.anaconda.com/pkgs/main)
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

DEFAULT_SOURCE  = "https://repo.anaconda.com/pkgs/main"
POLL_TIMEOUT    = 300


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


def candidate_packages(repodata: dict, all_versions: bool = False) -> list[dict]:
    """
    Return candidate packages from repodata.
    all_versions=False: one entry per name (latest version, .conda preferred).
    all_versions=True:  every version/build.
    """
    all_pkgs: dict[str, dict] = {}
    for fmt in ("packages", "packages.conda"):
        for fname, meta in repodata.get(fmt, {}).items():
            all_pkgs[fname] = {**meta, "filename": fname}

    if all_versions:
        return list(all_pkgs.values())

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


def pick_packages(source_repodata: dict, dest_repodata: dict,
                  count: int, seed: int | None,
                  all_versions: bool = False) -> list[dict]:
    """
    Pick `count` random packages from source that aren't already in dest.
    Uses the destination's full filename set so we never re-upload something
    that's already indexed (even under a different format).
    """
    already_have = all_filenames(dest_repodata)
    candidates = [
        m for m in candidate_packages(source_repodata, all_versions)
        if m["filename"] not in already_have
    ]

    if not candidates:
        return []

    if all_versions or count <= 0 or count >= len(candidates):
        return candidates  # take all

    rng = random.Random(seed)
    rng.shuffle(candidates)
    return candidates[:count]


def download_package(meta: dict, dest_dir: pathlib.Path, source_base: str, subdir: str) -> pathlib.Path:
    filename = meta["filename"]
    url = f"{source_base.rstrip('/')}/{subdir}/{filename}"
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
    source_base: str,
    subdir: str,
) -> tuple[str, float, str | None]:
    """Download then upload one package. Returns (filename, elapsed_s, error|None)."""
    filename = meta["filename"]
    t0 = time.time()
    try:
        pkg_path = download_package(meta, dest_dir, source_base, subdir)
        client.upload(channel, pkg_path, token, progress=False)
        return filename, time.time() - t0, None
    except Exception as e:
        return filename, time.time() - t0, str(e)


def main() -> None:
    parser = argparse.ArgumentParser(description="Mirror packages from any anaconda.org channel")
    parser.add_argument("--channel", "-c", default="main", help="Destination channel (default: main)")
    parser.add_argument("--source", default=DEFAULT_SOURCE,
                        help=f"Source channel base URL (default: {DEFAULT_SOURCE})")
    parser.add_argument("--subdirs", nargs="+", default=["noarch"],
                        metavar="SUBDIR", help="Subdirs to mirror (default: noarch)")
    parser.add_argument("--count", "-n", type=int, default=100,
                        help="Max packages per subdir; 0 = all (default: 100)")
    parser.add_argument("--all-versions", action="store_true",
                        help="Mirror every version/build, not just latest per name")
    parser.add_argument("--workers", "-w", type=int, default=8,
                        help="Parallel upload threads (default: 8)")
    parser.add_argument("--worker-url", default=DEFAULT_WORKER_URL, metavar="URL")
    parser.add_argument("--reauth", action="store_true", help="Force re-authentication")
    parser.add_argument("--no-wait", action="store_true", help="Skip repodata polling after upload")
    parser.add_argument("--keep", action="store_true", help="Keep downloaded packages after upload")
    parser.add_argument("--seed", type=int, default=None, help="Random seed (ignored with --all-versions)")
    parser.add_argument("--stats", metavar="FILE", default=None,
                        help="Write per-package upload stats to a JSON file")
    args = parser.parse_args()

    source_base = args.source.rstrip("/")
    client = ChannelClient(worker_url=args.worker_url)

    print("Authenticating...")
    try:
        token = client.login(force=args.reauth)
        print("  Token ready.\n")
    except ChannelError as e:
        sys.exit(f"Auth failed: {e}")

    # Collect packages across all subdirs, deduping against destination.
    all_selected: list[tuple[dict, str]] = []  # (meta, subdir)
    for subdir in args.subdirs:
        src_url = f"{source_base}/{subdir}/repodata.json"
        dst_url = f"{args.worker_url.rstrip('/')}/repo/{args.channel}/{subdir}/repodata.json"
        src = fetch_repodata(src_url, f"source {subdir}")
        dst = fetch_repodata(dst_url, f"dest {subdir}")
        already = len(all_filenames(dst))
        count = 0 if args.all_versions else args.count
        sel = pick_packages(src, dst, count, args.seed, args.all_versions)
        print(f"  {subdir}: {len(sel)} new packages to upload (dest has {already})")
        all_selected.extend((m, subdir) for m in sel)

    if not all_selected:
        print("\nNothing new to upload.")
        sys.exit(0)

    total_bytes = sum(m.get("size", 0) for m, _ in all_selected)
    print(f"\nTotal: {len(all_selected)} packages, {total_bytes/1024/1024:.1f} MB")
    print(f"Uploading to '{args.channel}' with {args.workers} parallel workers\n")

    with tempfile.TemporaryDirectory() as tmpdir:
        dest_dir = pathlib.Path(tmpdir)
        t_start = time.time()
        results: list[tuple[str, float, str | None]] = []
        done = 0
        errors = 0

        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {
                pool.submit(upload_one, client, args.channel, token, meta,
                            dest_dir, source_base, subdir): (meta, subdir)
                for meta, subdir in all_selected
            }
            for future in as_completed(futures):
                filename, elapsed, error = future.result()
                meta, subdir = futures[future]
                done += 1
                if error:
                    errors += 1
                    status = f"ERROR: {error}"
                else:
                    size_kb = meta.get("size", 0) // 1024
                    status = f"{size_kb}kB in {elapsed:.1f}s"
                print(f"  [{done:>{len(str(len(all_selected)))}}/{len(all_selected)}] {filename}  {status}")
                results.append((filename, elapsed, error))

        wall = time.time() - t_start
        ok_count = len(all_selected) - errors
        errored = {r[0] for r in results if r[2]}
        ok_bytes = sum(m.get("size", 0) for m, _ in all_selected if m["filename"] not in errored)

        print(f"\n{'='*60}")
        print(f"Uploaded {ok_count}/{len(all_selected)} packages in {wall:.1f}s")
        if wall > 0:
            print(f"Throughput: {ok_bytes/1024/1024/wall:.2f} MB/s  ({ok_count/wall:.1f} pkgs/s)")
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

        if args.stats:
            fname_to_meta = {m["filename"]: (m, sd) for m, sd in all_selected}
            stats = {
                "run": {
                    "channel": args.channel,
                    "source": source_base,
                    "subdirs": args.subdirs,
                    "n_selected": len(all_selected),
                    "n_ok": ok_count,
                    "n_err": errors,
                    "wall_s": round(wall, 3),
                    "ok_bytes": ok_bytes,
                    "throughput_mbps": round(ok_bytes/1024/1024/wall, 4) if wall > 0 else 0,
                    "pkgs_per_s": round(ok_count/wall, 3) if wall > 0 else 0,
                    "ts": time.time(),
                },
                "packages": [
                    {
                        "filename": fname,
                        "subdir": fname_to_meta.get(fname, ({}, ""))[1],
                        "size_bytes": fname_to_meta.get(fname, ({}, ""))[0].get("size", 0),
                        "elapsed_s": round(elapsed, 3),
                        "error": error,
                    }
                    for fname, elapsed, error in results
                ],
            }
            pathlib.Path(args.stats).write_text(json.dumps(stats, indent=2))
            print(f"Stats written to {args.stats}\n")

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
