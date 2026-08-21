#!/usr/bin/env python3
"""
End-to-end test for conda-wit.

Flow:
  1. Auth (cached token or GitHub Device Flow)
  2. Upload all fixture packages to the test channel
  3. Poll repodata until all packages appear (or timeout)
  4. Cleanup: purge the test channel (unless --no-cleanup)

Usage:
  python scripts/e2e_test.py [--channel test] [--no-cleanup] [--reauth]
"""
import argparse
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from channel_client import ChannelClient, ChannelError, DEFAULT_WORKER_URL

FIXTURES_DIR = pathlib.Path(__file__).parent / "fixtures"
POLL_TIMEOUT = 120


def main() -> None:
    parser = argparse.ArgumentParser(description="End-to-end test for conda-wit")
    parser.add_argument("--channel", default="test", help="Channel to use (default: test)")
    parser.add_argument("--worker-url", default=DEFAULT_WORKER_URL, metavar="URL")
    parser.add_argument("--no-cleanup", action="store_true", help="Leave channel intact after test")
    parser.add_argument("--reauth", action="store_true", help="Force re-authentication")
    parser.add_argument("--fixtures", default=str(FIXTURES_DIR), help="Directory of test packages")
    args = parser.parse_args()

    fixtures = pathlib.Path(args.fixtures)
    packages = sorted(
        p for p in fixtures.rglob("*")
        if p.is_file() and (p.suffix == ".conda" or p.name.endswith(".tar.bz2"))
    )
    if not packages:
        sys.exit(f"No packages found under {fixtures}\nRun: bash scripts/download_test_packages.sh")

    client = ChannelClient(worker_url=args.worker_url)

    print(f"Worker:   {args.worker_url}")
    print(f"Channel:  {args.channel}")
    print(f"Packages: {[p.name for p in packages]}")

    # 1. Auth
    print("\n=== Step 1: Auth ===")
    try:
        token = client.login(force=args.reauth)
        print("  Token ready.")
    except ChannelError as e:
        sys.exit(f"FAIL: {e}")

    # 2. Upload
    print(f"\n=== Step 2: Uploading {len(packages)} package(s) to '{args.channel}' ===")
    uploaded = []
    for pkg in packages:
        try:
            fname = client.upload(args.channel, pkg, token)
            uploaded.append(fname)
        except ChannelError as e:
            sys.exit(f"FAIL: {e}")

    # 3. Poll repodata
    print(f"\n=== Step 3: Polling repodata (up to {POLL_TIMEOUT}s) ===")
    success = client.poll_repodata(args.channel, uploaded, timeout=POLL_TIMEOUT)

    # 4. Cleanup
    if not args.no_cleanup:
        print(f"\n=== Step 4: Cleanup ===")
        try:
            deleted = client.purge_channel(args.channel, token)
            print(f"  Deleted {deleted} object(s) from '{args.channel}'.")
        except ChannelError as e:
            print(f"  WARNING: cleanup failed: {e}", file=sys.stderr)
    else:
        print(f"\n(Skipping cleanup — '{args.channel}' left intact)")

    if success:
        print(f"\nPASS — all {len(uploaded)} package(s) indexed.")
        sys.exit(0)
    else:
        print(f"\nFAIL — not all packages appeared in repodata within {POLL_TIMEOUT}s.")
        sys.exit(1)


if __name__ == "__main__":
    main()
