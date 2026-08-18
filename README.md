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
7. `ChannelQueue` records the upload and, if nothing is already scheduled,
   sets an alarm ~5s out. Any other uploads to the same channel in that
   window join the same batch.
8. When the alarm fires, `ChannelQueue` sends the whole batch to the
   container's `/ingest-batch` in one call. The container reads each
   package's real `subdir` out of its metadata, groups files by subdir,
   and runs one `conda-index` pass per subdir covering everything in the
   batch — not one run per upload. This pass writes both the classic
   monolithic `repodata.json` and [CEP-16 sharded
   repodata](https://conda.org/learn/ceps/cep-0016/) (`repodata_shards.msgpack.zst`
   + one content-addressed shard per package under `shards/`), so clients
   that support shards only fetch metadata for packages they actually need,
   while older clients still get the plain `repodata.json`.
9. The container reports per-file success/failure. `ChannelQueue` clears
   only the entries that succeeded; anything that failed stays queued and
   the alarm's built-in exponential-backoff retry (a Durable Objects
   primitive, not something we wrote) picks it up again.

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
- Upload now returns `202 Accepted`, not a final "reindexed" confirmation —
  indexing is asynchronous (debounced + batched). There's currently no
  status-check endpoint for the client to poll; if you want one, a simple
  `GET /channel/<channel>/status` that reads `ChannelQueue`'s pending count
  would do it.
- No package deletion/yanking endpoint.
- If ingest fails partway (e.g. `index.json` is malformed), the file is left
  under `<channel>/_incoming/` rather than silently lost — but nothing
  currently sweeps that directory, so add a periodic cleanup if failed
  uploads pile up (distinct from the retry queue — this is for uploads that
  fail validation entirely, e.g. corrupt files, which retries can't fix).
- No signature/hash verification beyond what `conda-index` already embeds in
  repodata.
