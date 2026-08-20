# conda-channel-server

Lightweight conda channel: R2 for storage, one Worker for auth/upload, one
Cloudflare Container (running real `conda-index`) that wakes on upload and
sleeps 2 min later. Accepts both `.conda` (current format) and legacy
`.tar.bz2` packages — `conda_package_streaming` reads metadata from either
uniformly, and `conda-index` indexes both into the same `repodata.json`.

## Setup

```bash
npm install
wrangler secret put GITHUB_CLIENT_ID
wrangler secret put GITHUB_ORG
wrangler secret put UPLOAD_TOKEN_SECRET   # any random 32+ byte string
```

The Worker needs R2 S3-compatible credentials too — not to move bytes,
only to *sign* presigned URLs (aws4fetch does the SigV4 signing locally,
no extra network call), and to forward into the container's environment
(see below):

```bash
wrangler secret put R2_ACCESS_KEY_ID
wrangler secret put R2_SECRET_ACCESS_KEY
wrangler secret put R2_ACCOUNT_ID
```

`R2_BUCKET_NAME` isn't sensitive, so it's a plain `[vars]` entry already in
`wrangler.toml` rather than a secret — edit it there if your bucket name
differs from `conda-channel`.

**The container gets these same R2 credentials automatically** — not via
any wrangler.toml container config block (there isn't one for this), but
through `envVars` on the `IndexerContainer` class in `src/worker.ts`, which
references the Worker's own `env` at the module level:

```ts
import { env } from "cloudflare:workers";

export class IndexerContainer extends Container<Env> {
  envVars = {
    R2_ACCOUNT_ID: env.R2_ACCOUNT_ID,
    R2_ACCESS_KEY_ID: env.R2_ACCESS_KEY_ID,
    R2_SECRET_ACCESS_KEY: env.R2_SECRET_ACCESS_KEY,
    R2_BUCKET_NAME: env.R2_BUCKET_NAME,
  };
}
```

Nothing further to provision for the container — the same four secrets
above cover it.

## Flow

1. `POST /auth/device/start` -> user code + verification URL
2. user approves in browser, CLI polls `POST /auth/device/poll`
3. Worker checks GitHub org membership, mints a 1hr HMAC upload token
4. `POST /upload/init` `{"channel": "main", "filename": "pkg-1.0-0.conda"}`
   with `Authorization: Bearer <token>` -> Worker returns a presigned R2 PUT
   URL, valid 15 min. **No package bytes touch the Worker at this step.**
5. Client `PUT`s the package (`.conda` or legacy `.tar.bz2`) directly to that
   URL — straight to R2.
6. `POST /upload/complete` with the same `{channel, filename}` -> Worker
   confirms the object landed (a cheap `head()`, not a read of the bytes),
   then hands off to the `ChannelQueue` Durable Object for that channel and
   returns `202 Accepted` — indexing happens asynchronously from here.
7. `ChannelQueue` records the upload and, if no alarm is already set,
   schedules one ~5s out. Any other uploads to the same channel in that
   window join the same batch automatically.
8. When the alarm fires, `ChannelQueue` forwards each pending upload to
   `PackageIngestor` (one DO instance per file), which immediately enqueues
   it onto `ChannelIngestQueue` (one instance per channel).
9. `ChannelIngestQueue` drains its work items one at a time, calling the
   container's `/ingest-package` for each file. The container downloads the
   package, extracts its metadata using `conda_package_streaming` (the same
   library conda-index uses internally), computes checksums, and writes a
   per-package CEP-16 shard to R2. It does **not** call `conda-index` on the
   hot path — metadata is extracted per-package incrementally, not by
   scanning the whole subdir. After each successful ingest it notifies
   `SubdirIndexMerger` for the affected subdir.
10. `SubdirIndexMerger` (one instance per `channel/subdir`) debounces
    `rebuild-index` calls with a ~3s window — so if five packages land in
    `linux-64` within a few seconds, only one full rebuild runs, not five.
    When the alarm fires it calls the container's `/rebuild-index`, which
    reads all current per-package shards from R2, assembles them into a new
    `repodata_shards.msgpack.zst` (CEP-16 shard index) and regenerates the
    monolithic `repodata.json`. Again, no `conda-index` call — the rebuild
    assembles `repodata.json` directly from the shards, which already contain
    the complete per-file metadata.
11. The container also writes `_browse/<name>.json` for each ingested package
    and calls back to `POST /internal/upsert-package` on the Worker to keep
    D1 in sync — this is what powers the browse UI and search.

The only service that ever reads package bytes is the container (step 9),
and it has to — that's what makes metadata extraction work.

> **Note on conda-index:** `conda-index` is still present in the container
> image and is invoked on the **slow path** only: `DELETE`ing a package
> triggers a full `conda-index` reindex of the affected subdir (since
> removing a file requires scanning what's left). Normal uploads never call
> `conda-index` — we replicate what it produces (same `repodata.json` fields,
> same CEP-16 shard format) by extracting metadata per-package with
> `conda_package_streaming`. This means `patch_instructions.json` and
> `current_repodata.json` are not generated on the hot path; for private/org
> channels built from your own packages this is fine, but mirroring
> conda-forge would require the full reindex path.

### Durable Object roles

Four DOs collaborate to make the ingest pipeline serialised, batched, and
crash-safe:

| DO | Instance key | Role |
|---|---|---|
| `ChannelQueue` | one per channel | Debounces uploads (~5s window), owns channel `owner`/`visibility` state, prevents concurrent ingest runs |
| `PackageIngestor` | one per `channel/filename` | Thin relay — exists so `ChannelQueue.alarm()` can fan out to multiple files in parallel without coupling directly to `ChannelIngestQueue` |
| `ChannelIngestQueue` | one per channel | Serialises per-file container calls (one at a time), handles retries with back-off |
| `SubdirIndexMerger` | one per `channel/subdir` | Debounces `rebuild-index` calls (~3s), coalesces multiple near-simultaneous package ingests into a single conda-index run |

`PackageIngestor` is the thinnest of the four — it's a one-`fetch()` relay
that could in theory be inlined into `ChannelQueue`. It exists as a
separate DO so that `ChannelQueue.alarm()` can `await` multiple ingest
dispatches without coupling its state machine directly to
`ChannelIngestQueue`'s API.

The only service that ever reads package bytes is the container in step 8,
and it has to — that's what makes conda-index work.

### Atomicity, ordering, batching

- **Atomicity**: a Durable Object never runs two alarm invocations
  concurrently, and `ChannelQueue` is one instance per channel — so there is
  never more than one `/ingest-batch` call (and therefore one `conda-index`
  run) in flight for a given channel at a time. This is Cloudflare's own
  concurrency guarantee, not a lock we built.
- **Ordering**: pending uploads are stored under keys like
  `pending:<zero-padded-timestamp>:<filename>`, so listing them back out is
  already in upload order — no separate sort step.
- **Batching**: the ~5s debounce window coalesces near-simultaneous uploads
  into a single container wake + single conda-index run per affected subdir.
- **Partial failure**: `repodata.json` is uploaded last, after the packages
  it references are already durably in R2, and only after the packages,
  cache, and repodata for a subdir are all confirmed uploaded does that
  subdir's staged files get deleted — so a crash mid-batch never leaves
  repodata pointing at a package that isn't there, and never loses an
  upload (it just stays in `_incoming/` for the next retry).

Bucket layout ends up as:

```
main/noarch/repodata.json
main/noarch/repodata_shards.msgpack.zst    <- CEP-16 shard index
main/noarch/shards/<sha256>.msgpack.zst    <- one per package, immutable
main/noarch/.cache/cache.db                <- conda-index's own incremental cache, persisted
main/noarch/some-pkg-1.0-0.conda
main/linux-64/repodata.json
main/linux-64/repodata_shards.msgpack.zst
main/linux-64/shards/<sha256>.msgpack.zst
main/linux-64/.cache/cache.db
main/linux-64/some-pkg-1.0-0.conda
experimental/osx-arm64/repodata.json
...
```

Point conda at a specific channel with the custom domain + path, e.g.
`conda install -c https://channel.example.com/main some-pkg`.

## Deploy

```bash
wrangler deploy
```

## Not yet handled (fine for v1, revisit later)

- **conda-index's own sqlite cache (`<subdir>/.cache/cache.db`) is persisted
  in R2** alongside packages and repodata, downloaded before indexing and
  re-uploaded after. Without this, conda-index has no memory between
  container invocations and re-extracts every package's metadata on every
  single upload — the cache is what makes its incremental indexing actually
  incremental for us. Note this only saves extraction *CPU*, not download
  time: every package's bytes still get pulled from R2 each run since
  conda-index expects the files present locally. If a channel grows large
  enough that download time dominates, the next optimization is skipping
  downloads for files whose R2 ETag/size match a previous run, rather than
  redownloading the whole subdir every time.
- `ChannelQueue` guarantees serialization *per channel*, not globally — many
  different channels can still reindex concurrently (that's fine, they're
  independent), but a container has a real memory/CPU ceiling
  (`instance_type` in wrangler.toml), so if you have dozens of very active
  channels at once, `max_instances` on `[[containers]]` becomes the actual
  throughput limit, not this queue.
- Upload returns `202 Accepted`, not a final "reindexed" confirmation —
  indexing is asynchronous (debounced + batched). There's currently no
  status-check endpoint for the client to poll; if you want one, a simple
  `GET /channel/<channel>/status` that reads `ChannelQueue`'s pending count
  would do it.
- No package deletion/yanking UI — owners can delete individual files from
  the package detail page (`DELETE /channel/:channel/:subdir/:filename`),
  which triggers a reindex of the affected subdir. Bulk deletion and yanking
  (keeping the file but flagging it) are not yet implemented.
- If ingest fails partway (e.g. `index.json` is malformed), the file is left
  under `<channel>/_incoming/` rather than silently lost — but nothing
  currently sweeps that directory, so add a periodic cleanup if failed
  uploads pile up (distinct from the retry queue — this is for uploads that
  fail validation entirely, e.g. corrupt files, which retries can't fix).
- No signature/hash verification beyond what `conda-index` already embeds in
  repodata.
