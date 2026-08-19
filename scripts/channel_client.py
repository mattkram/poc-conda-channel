"""
Reusable client library for conda-channel-server.

Provides:
  ChannelClient   — all API calls (auth, upload, delete, repodata poll)
  TokenCache      — load/save the upload token to .token-cache (gitignored)

No third-party dependencies — stdlib only.
"""
from __future__ import annotations

import base64
import json
import pathlib
import sys
import time
import urllib.error
import urllib.request
import webbrowser

DEFAULT_WORKER_URL = "https://conda.matt-kramer.com"
DEFAULT_TOKEN_CACHE = pathlib.Path(__file__).parent.parent / ".token-cache"

USER_AGENT = "conda-channel-manager/1.0"
POLL_INTERVAL = 5   # seconds between repodata polls


# ---------------------------------------------------------------------------
# Token cache
# ---------------------------------------------------------------------------

class TokenCache:
    def __init__(self, path: pathlib.Path = DEFAULT_TOKEN_CACHE):
        self.path = path

    def load(self) -> str | None:
        """Return a cached token if it exists and isn't close to expiry."""
        try:
            data = json.loads(self.path.read_text())
            if data.get("exp", 0) > time.time() + 60:
                return data["token"]
        except (FileNotFoundError, json.JSONDecodeError, KeyError):
            pass
        return None

    def save(self, token: str) -> None:
        try:
            payload = json.loads(_b64url_decode(token.split(".")[0]))
            exp = payload.get("exp", int(time.time()) + 3600)
        except Exception:
            exp = int(time.time()) + 3600
        self.path.write_text(json.dumps({"token": token, "exp": exp}))
        self.path.chmod(0o600)

    def clear(self) -> None:
        if self.path.exists():
            self.path.unlink()


# ---------------------------------------------------------------------------
# Low-level HTTP helpers
# ---------------------------------------------------------------------------

def _b64url_decode(s: str) -> str:
    s += "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s).decode()


def _api(method: str, url: str, body=None, token: str | None = None) -> tuple[int, dict | str]:
    headers = {"content-type": "application/json", "user-agent": USER_AGENT}
    if token:
        headers["authorization"] = f"Bearer {token}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            raw = resp.read()
            ct = resp.headers.get("content-type", "")
            return resp.status, (json.loads(raw) if "json" in ct else raw.decode())
    except urllib.error.HTTPError as e:
        raw = e.read()
        ct = e.headers.get("content-type", "")
        return e.code, (json.loads(raw) if "json" in ct else raw.decode())


def _put_file(url: str, path: pathlib.Path) -> int:
    """PUT raw file bytes to a presigned URL — no auth header, no Content-Type."""
    req = urllib.request.Request(url, data=path.read_bytes(), method="PUT")
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code


def _get_json(url: str) -> tuple[int, dict | str]:
    req = urllib.request.Request(url, headers={"user-agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


# ---------------------------------------------------------------------------
# ChannelClient
# ---------------------------------------------------------------------------

class ChannelClient:
    """
    High-level client for conda-channel-server.

    All methods raise ChannelError on failure unless noted otherwise.
    """

    def __init__(
        self,
        worker_url: str = DEFAULT_WORKER_URL,
        token_cache: TokenCache | None = None,
    ):
        self.worker_url = worker_url.rstrip("/")
        self.cache = token_cache or TokenCache()

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def login(self, force: bool = False) -> str:
        """
        Return a valid upload token. Uses cached token if available.
        Runs GitHub Device Flow if needed (opens browser, polls until approved).
        Set force=True to ignore the cache and re-authenticate.
        """
        if force:
            self.cache.clear()
        else:
            cached = self.cache.load()
            if cached:
                return cached

        status, data = _api("POST", f"{self.worker_url}/auth/device/start")
        if status != 200:
            raise ChannelError(f"/auth/device/start failed ({status}): {data}")

        print(f"  Open this URL in your browser: {data['verification_uri']}")
        print(f"  Enter code: {data['user_code']}")
        webbrowser.open(data["verification_uri"])

        device_code = data["device_code"]
        interval = data.get("interval", 5)
        deadline = time.time() + data.get("expires_in", 900)

        print(f"  Waiting for approval", end="", flush=True)
        while time.time() < deadline:
            time.sleep(interval)
            status, poll = _api("POST", f"{self.worker_url}/auth/device/poll",
                                {"device_code": device_code})
            if status == 200 and isinstance(poll, dict) and "upload_token" in poll:
                print(" authenticated.")
                self.cache.save(poll["upload_token"])
                return poll["upload_token"]
            error = poll.get("error", "") if isinstance(poll, dict) else str(poll)
            if error not in ("authorization_pending", "slow_down"):
                raise ChannelError(f"Device flow error: {poll}")
            print(".", end="", flush=True)

        raise ChannelError("Device flow timed out.")

    def logout(self) -> None:
        """Remove the cached token."""
        self.cache.clear()

    # ------------------------------------------------------------------
    # Upload
    # ------------------------------------------------------------------

    def upload(
        self,
        channel: str,
        pkg_path: pathlib.Path,
        token: str,
        progress: bool = True,
    ) -> tuple[str, dict]:
        """
        Upload a single package file to a channel.
        Returns (filename, timing) where timing = {init_s, put_s, complete_s, ul_s}.
        Raises ChannelError on failure.
        """
        filename = pkg_path.name
        size_kb = pkg_path.stat().st_size // 1024

        if progress:
            print(f"  {filename} ({size_kb}kB) ... ", end="", flush=True)

        # 1. Get presigned PUT URL
        t0 = time.time()
        status, data = _api("POST", f"{self.worker_url}/upload/init",
                             {"channel": channel, "filename": filename}, token=token)
        if status != 200:
            raise ChannelError(f"upload/init failed ({status}): {data}")
        t_init = time.time() - t0

        # 2. PUT bytes directly to R2
        t0 = time.time()
        put_status = _put_file(data["upload_url"], pkg_path)
        if put_status not in (200, 204):
            raise ChannelError(f"PUT to R2 failed ({put_status})")
        t_put = time.time() - t0

        # 3. Notify Worker that the upload landed
        t0 = time.time()
        status, data = _api("POST", f"{self.worker_url}/upload/complete",
                             {"channel": channel, "filename": filename}, token=token)
        if status != 202:
            raise ChannelError(f"upload/complete failed ({status}): {data}")
        t_complete = time.time() - t0

        timing = {
            "init_s":     round(t_init, 3),
            "put_s":      round(t_put, 3),
            "complete_s": round(t_complete, 3),
            "ul_s":       round(t_init + t_put + t_complete, 3),
        }

        if progress:
            print(f"queued.  init={t_init:.2f}s  put={t_put:.2f}s  complete={t_complete:.2f}s")
        return filename, timing

    # ------------------------------------------------------------------
    # Delete
    # ------------------------------------------------------------------

    def delete_package(
        self,
        channel: str,
        subdir: str,
        filename: str,
        token: str,
    ) -> None:
        """Delete a single package from a channel and trigger a reindex."""
        status, data = _api(
            "DELETE",
            f"{self.worker_url}/channel/{channel}/{subdir}/{filename}",
            token=token,
        )
        if status != 200:
            raise ChannelError(f"delete failed ({status}): {data}")

    def purge_channel(self, channel: str, token: str) -> int:
        """
        Delete all objects in a channel. Returns the number of deleted objects.
        Intended for test teardown.
        """
        status, data = _api("DELETE", f"{self.worker_url}/channel/{channel}", token=token)
        if status != 200:
            raise ChannelError(f"purge failed ({status}): {data}")
        return data.get("deleted", 0)

    # ------------------------------------------------------------------
    # Repodata
    # ------------------------------------------------------------------

    def poll_repodata(
        self,
        channel: str,
        filenames: list[str],
        timeout: int = 120,
        subdirs: list[str] | None = None,
        progress: bool = True,
    ) -> bool:
        """
        Poll repodata.json until all filenames appear or timeout is reached.
        Returns True if all found, False on timeout.
        subdirs defaults to ["noarch"] — pass additional subdirs if needed.
        """
        check_subdirs = subdirs or ["noarch"]
        remaining = set(filenames)
        deadline = time.time() + timeout

        while time.time() < deadline and remaining:
            time.sleep(POLL_INTERVAL)
            for subdir in check_subdirs:
                url = f"{self.worker_url}/repo/{channel}/{subdir}/repodata.json"
                status, data = _get_json(url)
                if status != 200:
                    continue
                all_pkgs = {**data.get("packages", {}), **data.get("packages.conda", {})}
                found = {f for f in remaining if f in all_pkgs}
                for f in found:
                    if progress:
                        print(f"  indexed: {f}")
                remaining -= found
            if remaining and progress:
                elapsed = int(timeout - (deadline - time.time()))
                print(f"  waiting for indexing... ({elapsed}s)", end="\r", flush=True)

        if progress and not remaining:
            print()  # clear the \r line
        return len(remaining) == 0

    def get_channel_info(self, channel: str) -> dict:
        """Return {owner, visibility} for a channel."""
        status, data = _api("GET", f"{self.worker_url}/channel/{channel}")
        if status != 200:
            raise ChannelError(f"could not get channel info ({status}): {data}")
        return data

    def set_visibility(self, channel: str, visibility: str, token: str) -> dict:
        """Set a channel's visibility to 'public' or 'private'. Owner only."""
        if visibility not in ("public", "private"):
            raise ChannelError("visibility must be 'public' or 'private'")
        status, data = _api(
            "POST",
            f"{self.worker_url}/channel/{channel}/visibility",
            {"visibility": visibility},
            token=token,
        )
        if status != 200:
            raise ChannelError(f"set-visibility failed ({status}): {data}")
        return data
        """Fetch and return repodata.json for a channel/subdir."""
        url = f"{self.worker_url}/repo/{channel}/{subdir}/repodata.json"
        status, data = _get_json(url)
        if status != 200:
            raise ChannelError(f"repodata not found for {channel}/{subdir} ({status})")
        return data


# ---------------------------------------------------------------------------
# Error type
# ---------------------------------------------------------------------------

class ChannelError(Exception):
    pass
