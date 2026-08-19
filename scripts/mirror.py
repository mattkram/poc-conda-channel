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
  --dl-workers N Parallel download threads (default: same as --workers)
  --no-wait      Don't poll repodata after upload (fire-and-forget)
  --all-versions Include every version/build, not just latest per name
  --subdirs      Space-separated list of subdirs to mirror (default: noarch)
  --source URL   Source channel base URL (default: https://repo.anaconda.com/pkgs/main)

Architecture:
  Download and upload run in separate thread pools connected by a queue so
  downloads overlap with in-flight uploads. Each phase is timed independently
  and reported in the stats output.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import queue
import random
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from channel_client import ChannelClient, ChannelError, DEFAULT_WORKER_URL

DEFAULT_SOURCE  = "https://repo.anaconda.com/pkgs/main"
POLL_TIMEOUT    = 300
_SENTINEL = object()


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


def download_package(meta: dict, dest_dir: pathlib.Path,
                     source_base: str, subdir: str) -> tuple[pathlib.Path, float]:
    """Download one package. Returns (local_path, elapsed_s)."""
    filename = meta["filename"]
    url = f"{source_base.rstrip('/')}/{subdir}/{filename}"
    dest = dest_dir / filename
    t0 = time.time()
    if not dest.exists():
        req = urllib.request.Request(url, headers={"user-agent": "conda-mirror/1.0"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            dest.write_bytes(resp.read())
    return dest, time.time() - t0


def upload_one(
    client: ChannelClient,
    channel: str,
    token: str,
    pkg_path: pathlib.Path,
) -> tuple[float, float, float]:
    """Upload one already-downloaded package.
    Returns (init_s, put_s, complete_s)."""
    _, timing = client.upload(channel, pkg_path, token, progress=False)
    return timing["init_s"], timing["put_s"], timing["complete_s"]


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
    parser.add_argument("--dl-workers", type=int, default=None,
                        help="Parallel download threads (default: same as --workers)")
    parser.add_argument("--worker-url", default=DEFAULT_WORKER_URL, metavar="URL")
    parser.add_argument("--reauth", action="store_true", help="Force re-authentication")
    parser.add_argument("--no-wait", action="store_true", help="Skip repodata polling after upload")
    parser.add_argument("--keep", action="store_true", help="Keep downloaded packages after upload")
    parser.add_argument("--seed", type=int, default=None, help="Random seed (ignored with --all-versions)")
    parser.add_argument("--stats", metavar="FILE", default=None,
                        help="Write per-package upload stats to a JSON file")
    args = parser.parse_args()

    dl_workers = args.dl_workers or args.workers
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
    print(f"Uploading to '{args.channel}' with {args.workers} upload / {dl_workers} download workers\n")

    # Per-package results: {filename: {dl_s, init_s, put_s, complete_s, ul_s, error}}
    pkg_results: dict[str, dict] = {}
    results_lock = threading.Lock()
    done_count = 0
    error_count = 0
    n_total = len(all_selected)
    width = len(str(n_total))

    # Pipeline: download pool feeds a bounded queue; upload pool drains it.
    # Queue capacity = 2× upload workers so downloads stay ahead without
    # buffering the entire dataset on disk.
    ready_queue: queue.Queue = queue.Queue(maxsize=args.workers * 2)

    with tempfile.TemporaryDirectory() as tmpdir:
        dest_dir = pathlib.Path(tmpdir)
        t_start = time.time()

        def download_worker(meta: dict, subdir: str) -> None:
            filename = meta["filename"]
            try:
                path, dl_s = download_package(meta, dest_dir, source_base, subdir)
                ready_queue.put((meta, subdir, path, dl_s, None))
            except Exception as e:
                ready_queue.put((meta, subdir, None, 0.0, str(e)))

        def upload_worker() -> None:
            nonlocal done_count, error_count
            while True:
                item = ready_queue.get()
                if item is _SENTINEL:
                    ready_queue.put(_SENTINEL)  # re-enqueue for other upload workers
                    break
                meta, subdir, path, dl_s, dl_err = item
                filename = meta["filename"]

                if dl_err:
                    init_s = put_s = complete_s = ul_s = 0.0
                    error = f"download failed: {dl_err}"
                else:
                    try:
                        init_s, put_s, complete_s = upload_one(client, args.channel, token, path)
                        ul_s = init_s + put_s + complete_s
                        error = None
                    except Exception as e:
                        init_s = put_s = complete_s = ul_s = 0.0
                        error = str(e)

                with results_lock:
                    done_count += 1
                    if error:
                        error_count += 1
                    pkg_results[filename] = {
                        "dl_s":       round(dl_s, 3),
                        "init_s":     round(init_s, 3),
                        "put_s":      round(put_s, 3),
                        "complete_s": round(complete_s, 3),
                        "ul_s":       round(ul_s, 3),
                        "error":      error,
                    }
                    dc = done_count

                size_kb = meta.get("size", 0) // 1024
                if error:
                    status = f"ERROR: {error}"
                else:
                    status = f"{size_kb}kB  dl={dl_s:.1f}s  init={init_s:.1f}s  put={put_s:.1f}s  complete={complete_s:.1f}s"
                print(f"  [{dc:>{width}}/{n_total}] {filename}  {status}")

        # Start upload workers first (they'll block on the queue).
        upload_threads = [
            threading.Thread(target=upload_worker, daemon=True)
            for _ in range(args.workers)
        ]
        for t in upload_threads:
            t.start()

        # Run downloads in a thread pool, feeding the queue.
        with ThreadPoolExecutor(max_workers=dl_workers) as dl_pool:
            dl_futures = [
                dl_pool.submit(download_worker, meta, subdir)
                for meta, subdir in all_selected
            ]
            for f in as_completed(dl_futures):
                f.result()  # surface exceptions if any

        # Signal upload workers that downloads are done.
        ready_queue.put(_SENTINEL)
        for t in upload_threads:
            t.join()

        wall = time.time() - t_start
        ok_count = n_total - error_count
        errored = {fn for fn, r in pkg_results.items() if r["error"]}
        ok_bytes = sum(
            m.get("size", 0) for m, _ in all_selected
            if m["filename"] not in errored
        )

        # Aggregate timing
        ok_results = [r for r in pkg_results.values() if not r["error"]]
        dl_times    = [r["dl_s"]       for r in ok_results]
        init_times  = [r["init_s"]     for r in ok_results]
        put_times   = [r["put_s"]      for r in ok_results]
        comp_times  = [r["complete_s"] for r in ok_results]
        ul_times    = [r["ul_s"]       for r in ok_results]

        def avg(xs): return sum(xs)/len(xs) if xs else 0

        print(f"\n{'='*60}")
        print(f"Uploaded {ok_count}/{n_total} packages in {wall:.1f}s wall time")
        if wall > 0:
            print(f"Throughput: {ok_bytes/1024/1024/wall:.2f} MB/s  ({ok_count/wall:.1f} pkgs/s)")
        if ok_results:
            print(f"Avg per package (ok only):")
            print(f"  download:         {avg(dl_times):.2f}s  (from source)")
            print(f"  upload/init:      {avg(init_times):.2f}s  (Worker presign)")
            print(f"  upload/PUT:       {avg(put_times):.2f}s  (bytes → R2)")
            print(f"  upload/complete:  {avg(comp_times):.2f}s  (Worker enqueue)")
            print(f"  upload total:     {avg(ul_times):.2f}s")
        if error_count:
            print(f"Errors ({error_count}):")
            for fname, r in pkg_results.items():
                if r["error"]:
                    print(f"  {fname}: {r['error']}")
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
                    "channel":         args.channel,
                    "source":          source_base,
                    "subdirs":         args.subdirs,
                    "ul_workers":      args.workers,
                    "dl_workers":      dl_workers,
                    "n_selected":      n_total,
                    "n_ok":            ok_count,
                    "n_err":           error_count,
                    "wall_s":          round(wall, 3),
                    "ok_bytes":        ok_bytes,
                    "throughput_mbps": round(ok_bytes/1024/1024/wall, 4) if wall > 0 else 0,
                    "pkgs_per_s":      round(ok_count/wall, 3) if wall > 0 else 0,
                    "avg_dl_s":        round(avg(dl_times), 3),
                    "avg_init_s":      round(avg(init_times), 3),
                    "avg_put_s":       round(avg(put_times), 3),
                    "avg_complete_s":  round(avg(comp_times), 3),
                    "avg_ul_s":        round(avg(ul_times), 3),
                    "ts":              time.time(),
                },
                "packages": [
                    {
                        "filename":   fname,
                        "subdir":     fname_to_meta.get(fname, ({}, ""))[1],
                        "size_bytes": fname_to_meta.get(fname, ({}, ""))[0].get("size", 0),
                        "dl_s":       r["dl_s"],
                        "init_s":     r["init_s"],
                        "put_s":      r["put_s"],
                        "complete_s": r["complete_s"],
                        "ul_s":       r["ul_s"],
                        "elapsed_s":  round(r["dl_s"] + r["ul_s"], 3),
                        "error":      r["error"],
                    }
                    for fname, r in pkg_results.items()
                ],
            }
            pathlib.Path(args.stats).write_text(json.dumps(stats, indent=2))
            print(f"Stats written to {args.stats}\n")

    # Poll repodata
    if not args.no_wait and ok_count > 0:
        uploaded_names = [fn for fn, r in pkg_results.items() if not r["error"]]
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
