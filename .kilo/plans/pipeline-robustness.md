# Pipeline Robustness Fixes

## Problems identified

1. **`ChannelQueue.alarm` deletes pending key before INGESTOR confirms success** — if INGESTOR throws, package is silently lost
2. **`SubdirIndexMerger.alarm` has no try/catch** — container capacity error throws, dirty flag stays true but alarm is gone → stuck forever
3. **`_register_channel` in entrypoint.py missing `x-internal-secret` header** → 401 every time, channel never auto-registers
4. **D1 bulk upsert is N individual HTTP calls** (threaded) — slow and fragile under load; needs a real batch endpoint
5. **`not_found_handling = "none"` not yet confirmed working** — site showing 404 at root

## Fixes

### Fix 1: `src/do/channel-queue.ts` — safe delete
```ts
// In alarm(), only delete key after ingestor.fetch() returns ok
entries.map(async ([key, upload]) => {
  try {
    const resp = await ingestor.fetch(...);
    if (resp.ok) await this.ctx.storage.delete(key);
    // else: leave in pending, retry on next alarm
  } catch {
    // leave in pending
  }
})
```

### Fix 2: `src/do/subdir-index-merger.ts` — catch container errors
```ts
async alarm(): Promise<void> {
  // ... existing dirty/channel/subdir reads ...
  await this.ctx.storage.put("dirty", false);
  try {
    const container = getContainer(this.env.INDEXER, `${channel}/${subdir}/_merge`);
    const resp = await container.fetch("http://container/rebuild-index", ...);
    if (!resp.ok) {
      await this.ctx.storage.put("dirty", true);
      await this.ctx.storage.setAlarm(Date.now() + 60_000);
      return;
    }
  } catch (err) {
    // Container unavailable — reschedule
    await this.ctx.storage.put("dirty", true);
    const isCapacity = String(err).includes("no container instance");
    await this.ctx.storage.setAlarm(Date.now() + (isCapacity ? 15_000 : 60_000));
    return;
  }
  // check stillDirty as before
}
```

### Fix 3: `container/entrypoint.py` — add secret to `_register_channel`
```python
def _register_channel(channel: str) -> None:
    req = urllib.request.Request(
        f"{WORKER_URL}/internal/upsert-package",
        data=body,
        headers={
            "content-type": "application/json",
            "x-internal-secret": INTERNAL_SECRET,  # ADD THIS
        },
        method="POST",
    )
```

### Fix 4: `src/handlers/internal.ts` — batch upsert endpoint
Add `POST /internal/upsert-packages` (plural) accepting `{packages: UpsertPackageBody[]}` and doing a D1 batch insert. Container calls this instead of N individual calls.

### Fix 5: `wrangler.toml` — verify not_found_handling = "none" works
Already committed; confirm root route returns 200 after deploy.

## Deploy order
1. Fix all TS files → `npx tsc --noEmit` → commit
2. Fix entrypoint.py → commit  
3. `npx wrangler deploy` (rebuilds container image)
4. Verify `curl -sI https://conda.matt-kramer.com/` returns 200
