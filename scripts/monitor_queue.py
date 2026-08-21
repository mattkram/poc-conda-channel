#!/usr/bin/env python3
"""Poll the pkgs-main ingest queue until staging and pending both hit 0."""
import json, time, urllib.request, sys

def get_token():
    with open(".token-cache") as f:
        return json.load(f)["token"]

def check():
    token = get_token()
    req = urllib.request.Request(
        "https://conda.matt-kramer.com/internal/queue-status/mattkram/pkgs-main",
        headers={"Authorization": f"Bearer {token}"}
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        d = json.load(r)
    iq = d["ingest_queue"]
    return iq["pending"], d["staging_objects"], iq["dead"]

while True:
    try:
        pending, staging, dead = check()
        ts = time.strftime("%H:%M:%S")
        print(f"{ts}  pending={pending}  staging={staging}  dead={dead}", flush=True)
        if pending == 0 and staging == 0:
            print("ALL CLEAR — queue fully drained")
            sys.exit(0)
    except Exception as e:
        print(f"  error: {e}", flush=True)
    time.sleep(60)
