#!/usr/bin/env python3
"""
conda-channel-manager — CLI for conda-channel-server.

Commands:
  login    Authenticate via GitHub Device Flow and cache the token.
  logout   Remove the cached token.
  upload   Upload one or more packages to a channel.
  delete   Remove a specific package from a channel and reindex.
  purge    Wipe an entire channel (all packages, repodata, cache).
  search   List packages in a channel (optionally filtered by name glob).

Examples:
  python channel_manager.py login
  python channel_manager.py upload --channel main dist/*.conda
  python channel_manager.py upload --channel main --wait foo-1.0-0.conda
  python channel_manager.py delete --channel main --subdir noarch foo-1.0-0.conda
  python channel_manager.py purge --channel test
  python channel_manager.py search --channel main ca-cert*
"""
from __future__ import annotations

import argparse
import fnmatch
import pathlib
import sys

# Allow running from the repo root or the scripts/ directory
sys.path.insert(0, str(pathlib.Path(__file__).parent))
from channel_client import ChannelClient, ChannelError, DEFAULT_WORKER_URL

POLL_TIMEOUT = 120


def _make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="channel_manager.py",
        description="Manage a conda-channel-server channel.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--worker-url",
        default=DEFAULT_WORKER_URL,
        metavar="URL",
        help="Base URL of the deployed Worker (default: %(default)s)",
    )
    parser.add_argument(
        "--reauth",
        action="store_true",
        help="Force re-authentication even if a cached token exists",
    )

    sub = parser.add_subparsers(dest="command", metavar="command")
    sub.required = True

    # login
    sub.add_parser("login", help="Authenticate via GitHub Device Flow")

    # logout
    sub.add_parser("logout", help="Remove the cached token")

    # upload
    p_upload = sub.add_parser("upload", help="Upload package(s) to a channel")
    p_upload.add_argument("--channel", "-c", required=True, help="Channel name")
    p_upload.add_argument(
        "--wait",
        action="store_true",
        help=f"Wait up to {POLL_TIMEOUT}s for packages to appear in repodata after upload",
    )
    p_upload.add_argument(
        "packages",
        nargs="+",
        metavar="FILE",
        help=".conda or .tar.bz2 package file(s) to upload",
    )

    # delete
    p_delete = sub.add_parser(
        "delete", help="Remove a package from a channel and reindex"
    )
    p_delete.add_argument("--channel", "-c", required=True, help="Channel name")
    p_delete.add_argument("--subdir", "-s", required=True,
                           help="Subdir the package lives in (e.g. noarch, linux-64)")
    p_delete.add_argument("filename", help="Package filename to delete")

    # purge
    p_purge = sub.add_parser(
        "purge", help="Delete ALL objects in a channel (test teardown)"
    )
    p_purge.add_argument("--channel", "-c", required=True, help="Channel to wipe")
    p_purge.add_argument(
        "--yes", "-y", action="store_true", help="Skip confirmation prompt"
    )

    # search
    p_search = sub.add_parser("search", help="List packages in a channel")
    p_search.add_argument("--channel", "-c", required=True, help="Channel name")
    p_search.add_argument(
        "--subdir", "-s", default="noarch",
        help="Subdir to query (default: noarch)"
    )
    p_search.add_argument(
        "pattern",
        nargs="?",
        default="*",
        help="Optional glob pattern to filter package names (e.g. 'ca-cert*')",
    )

    # info
    p_info = sub.add_parser("info", help="Show channel owner and visibility")
    p_info.add_argument("--channel", "-c", required=True, help="Channel name")

    # visibility
    p_vis = sub.add_parser("visibility", help="Set a channel public or private (owner only)")
    p_vis.add_argument("--channel", "-c", required=True, help="Channel name")
    p_vis.add_argument("setting", choices=["public", "private"], help="New visibility")

    return parser


def cmd_login(args, client: ChannelClient) -> None:
    print("Authenticating via GitHub Device Flow...")
    client.login(force=args.reauth)
    print("Login successful. Token cached.")


def cmd_logout(args, client: ChannelClient) -> None:
    client.logout()
    print("Logged out (token cache cleared).")


def cmd_upload(args, client: ChannelClient) -> None:
    # Resolve and validate paths
    paths: list[pathlib.Path] = []
    for pattern in args.packages:
        p = pathlib.Path(pattern)
        if p.is_file():
            paths.append(p)
        else:
            # Try glob expansion (for shells that don't expand globs)
            expanded = sorted(pathlib.Path(".").glob(pattern))
            if not expanded:
                print(f"  WARNING: no files matched '{pattern}', skipping.", file=sys.stderr)
            paths.extend(expanded)

    if not paths:
        sys.exit("No package files found.")

    invalid = [p for p in paths if not (p.suffix == ".conda" or p.name.endswith(".tar.bz2"))]
    if invalid:
        sys.exit(f"Not valid package files: {[str(p) for p in invalid]}\n"
                 f"Only .conda and .tar.bz2 are accepted.")

    token = client.login(force=args.reauth)

    print(f"Uploading {len(paths)} package(s) to channel '{args.channel}':")
    uploaded = []
    errors = []
    for pkg in paths:
        try:
            fname = client.upload(args.channel, pkg, token)
            uploaded.append(fname)
        except ChannelError as e:
            print(f"  ERROR: {e}", file=sys.stderr)
            errors.append(pkg.name)

    if errors:
        print(f"\n{len(errors)} upload(s) failed: {errors}", file=sys.stderr)

    if uploaded and args.wait:
        print(f"\nWaiting for indexing (up to {POLL_TIMEOUT}s)...")
        ok = client.poll_repodata(args.channel, uploaded, timeout=POLL_TIMEOUT)
        if not ok:
            sys.exit(f"Timed out waiting for packages to appear in repodata.")
        print("All packages indexed.")

    if errors:
        sys.exit(1)


def cmd_delete(args, client: ChannelClient) -> None:
    token = client.login(force=args.reauth)
    print(f"Deleting {args.filename} from {args.channel}/{args.subdir}...")
    try:
        client.delete_package(args.channel, args.subdir, args.filename, token)
        print("Deleted and reindexed.")
    except ChannelError as e:
        sys.exit(f"ERROR: {e}")


def cmd_purge(args, client: ChannelClient) -> None:
    if not args.yes:
        answer = input(f"Purge ALL objects in channel '{args.channel}'? [y/N] ").strip().lower()
        if answer != "y":
            print("Aborted.")
            return

    token = client.login(force=args.reauth)
    print(f"Purging channel '{args.channel}'...")
    try:
        deleted = client.purge_channel(args.channel, token)
        print(f"Deleted {deleted} object(s).")
    except ChannelError as e:
        sys.exit(f"ERROR: {e}")


def cmd_search(args, client: ChannelClient) -> None:
    try:
        repodata = client.get_repodata(args.channel, args.subdir)
    except ChannelError as e:
        sys.exit(f"ERROR: {e}")

    all_pkgs = {**repodata.get("packages", {}), **repodata.get("packages.conda", {})}
    matches = {
        fname: meta for fname, meta in all_pkgs.items()
        if fnmatch.fnmatch(meta.get("name", ""), args.pattern)
    }

    if not matches:
        print(f"No packages matching '{args.pattern}' in {args.channel}/{args.subdir}.")
        return

    # Print table
    col_w = max(len(f) for f in matches) + 2
    print(f"{'Filename':<{col_w}}  {'Version':<20}  {'Build'}")
    print("-" * (col_w + 40))
    for fname, meta in sorted(matches.items()):
        print(f"{fname:<{col_w}}  {meta.get('version', ''):<20}  {meta.get('build', '')}")


def cmd_info(args, client: ChannelClient) -> None:
    try:
        info = client.get_channel_info(args.channel)
    except ChannelError as e:
        sys.exit(f"ERROR: {e}")
    owner = info.get("owner") or "(unclaimed)"
    visibility = info.get("visibility", "public")
    print(f"Channel:    {args.channel}")
    print(f"Owner:      {owner}")
    print(f"Visibility: {visibility}")


def cmd_visibility(args, client: ChannelClient) -> None:
    token = client.login(force=args.reauth)
    try:
        result = client.set_visibility(args.channel, args.setting, token)
        print(f"Channel '{args.channel}' is now {result['visibility']}.")
    except ChannelError as e:
        sys.exit(f"ERROR: {e}")


COMMANDS = {
    "login": cmd_login,
    "logout": cmd_logout,
    "upload": cmd_upload,
    "delete": cmd_delete,
    "purge": cmd_purge,
    "search": cmd_search,
    "info": cmd_info,
    "visibility": cmd_visibility,
}


def main() -> None:
    parser = _make_parser()
    args = parser.parse_args()
    client = ChannelClient(worker_url=args.worker_url)
    try:
        COMMANDS[args.command](args, client)
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(1)


if __name__ == "__main__":
    main()
