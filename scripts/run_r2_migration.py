#!/usr/bin/env python3
"""Run R2 prefix migration via the Worker's /internal/migrate-r2-prefix endpoint."""
import json, time, urllib.request, pathlib, sys

TOKEN = json.load(open(pathlib.Path(__file__).parent.parent / ".token-cache"))["token"]
URL = "https://conda.matt-kramer.com/internal/migrate-r2-prefix"

MIGRATIONS = [
    ("anaconda-cloud",   "mattkram/anaconda-cloud"),
    ("anaconda-cloud-2", "mattkram/anaconda-cloud-2"),
]

def migrate(src, dst):
    print(f"Migrating {src} → {dst}")
    cursor = None
    total = {"copied": 0, "deleted": 0, "errors": 0}
    batch = 0
    while True:
        payload = {"src": src, "dst": dst}
        if cursor:
            payload["cursor"] = cursor
        body = json.dumps(payload).encode()
        req = urllib.request.Request(
            URL, data=body,
            headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=60) as r:
            resp = json.loads(r.read())
        batch += 1
        for k in ("copied", "deleted", "errors"):
            total[k] += resp[k]
        print(f"  batch {batch:3d}: copied={resp['copied']:4d}  deleted={resp['deleted']:4d}  errors={resp['errors']}  done={resp['done']}")
        if resp["done"]:
            break
        cursor = resp.get("next_cursor")
        time.sleep(0.2)
    print(f"  TOTAL : copied={total['copied']}  deleted={total['deleted']}  errors={total['errors']}\n")

for src, dst in MIGRATIONS:
    migrate(src, dst)

print("Migration complete.")
