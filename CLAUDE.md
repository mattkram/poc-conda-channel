# conda-wit

Lightweight conda package channel server. R2 for storage, one Cloudflare
Worker for auth + upload, one Cloudflare Container running real
`conda-index` that wakes on upload and sleeps when idle. See `README.md`
for the full request-flow write-up; this file is the bootstrap checklist
and the things most likely to trip up a fresh deploy.

## Architecture at a glance

- `src/worker.ts` — the Worker. Three things live here:
  - GitHub Device Flow auth (`/auth/device/start`, `/auth/device/poll`) ->
    mints a short-lived HMAC upload token, no JWT library.
  - Presigned-upload endpoints (`/upload/init`, `/upload/complete`) — the
    Worker never touches package bytes; the client PUTs straight to R2.
  - `ChannelQueue`, a plain Durable Object (one per channel) that debounces
    uploads for ~5s, batches them, and hands the batch to the container.
    This is what gives atomicity/ordering — see README's "Atomicity,
    ordering, batching" section before changing this class.
- `container/Dockerfile` + `container/entrypoint.py` — the actual indexer.
  Python, `conda-index` + `conda_package_streaming` + `boto3`. Runs
  `conda-index` for real, writes classic `repodata.json` AND CEP-16 sharded
  repodata. Talks to R2 over its S3-compatible API, not the Workers R2
  binding (it's a plain container process, not a Worker).
- `IndexerContainer` (in `worker.ts`) is the Durable Object wrapper Cloudflare
  Containers requires — it's what `ChannelQueue`'s alarm calls into.

## Before you can `wrangler deploy`

1. **Docker must be running locally.** `wrangler deploy` builds
   `container/Dockerfile` and pushes the image — there's no way around
   having Docker (or a compatible builder) available on the machine
   running the deploy.
2. **Create the R2 bucket:**
   ```bash
   wrangler r2 bucket create conda-channel
   ```
   (or edit `R2_BUCKET_NAME` in `wrangler.toml` `[vars]` first if you want
   a different name.)
3. **Create an R2 API token** (Cloudflare dashboard -> R2 -> Manage API
   Tokens -> Create API Token, read+write on the bucket above). This gives
   you an Access Key ID, Secret Access Key, and the R2 Account ID needed
   below — these are S3-compatible credentials, not a Cloudflare API
   token.
4. **Create a GitHub OAuth App** (github.com/settings/developers -> New
   OAuth App). Device Flow must be enabled on the app (there's a checkbox
   under "Enable Device Flow" in the app settings). You only need the
   Client ID from this — device flow doesn't use a client secret.
5. **Set secrets:**
   ```bash
   wrangler secret put GITHUB_CLIENT_ID
   wrangler secret put GITHUB_ORG          # the GitHub org upload access is gated on
   wrangler secret put UPLOAD_TOKEN_SECRET # any random 32+ byte string, e.g. `openssl rand -hex 32`
   wrangler secret put R2_ACCESS_KEY_ID
   wrangler secret put R2_SECRET_ACCESS_KEY
   wrangler secret put R2_ACCOUNT_ID
   ```
6. **Install deps and deploy:**
   ```bash
   npm install
   wrangler deploy
   ```

For local iteration, copy `.dev.vars.example` to `.dev.vars` and fill in
real values — `wrangler dev` reads secrets from there instead of
`wrangler secret put`. `.dev.vars` is gitignored; never commit real values.

## Things worth double-checking against current Cloudflare docs

Cloudflare Containers is a fast-moving product; a few specifics here were
correct as of when this was written but are exactly the kind of thing that
drifts:

- `instance_type` in `wrangler.toml` — currently `lite | basic |
  standard-1..4`. Set to `standard-1` here since conda-index is more
  memory- than CPU-bound; bump if you see OOM on large subdirs.
- The `envVars` field on the `IndexerContainer` class in `worker.ts` is how
  R2 credentials reach the container process (via `os.environ` inside
  `entrypoint.py`) — there is deliberately no container-specific block in
  `wrangler.toml` for this; that's not how the current API works.
- Whether Containers requires a specific Workers plan tier — check current
  pricing/plan docs before assuming a free-tier test account can deploy
  this as-is.

## Testing end to end

There's no CLI client in this repo yet — testing means driving the HTTP
endpoints directly:

```bash
# 1. Device flow (or skip auth for local testing by temporarily stubbing
#    verifyUploadToken — don't ship that stub)
curl -X POST https://<worker>/auth/device/start
# ... approve in browser, then poll with the device_code ...
curl -X POST https://<worker>/auth/device/poll -d '{"device_code":"..."}'

# 2. Get a presigned upload URL
curl -X POST https://<worker>/upload/init \
  -H "Authorization: Bearer <upload_token>" \
  -d '{"channel":"main","filename":"some-pkg-1.0-0.conda"}'

# 3. PUT the actual file to the returned upload_url

# 4. Tell the Worker it landed
curl -X POST https://<worker>/upload/complete \
  -H "Authorization: Bearer <upload_token>" \
  -d '{"channel":"main","filename":"some-pkg-1.0-0.conda"}'

# 5. Wait ~5-10s for the debounce + container run, then check:
curl https://<worker>/main/linux-64/repodata.json   # once a custom domain
                                                      # or public R2 access
                                                      # is wired up — see README
```

`wrangler tail` is the fastest way to watch the Worker and `ChannelQueue`
alarm logs during this. Container logs are separate — `wrangler dev`
streams them locally; in production check the Cloudflare dashboard's
Containers logs for `IndexerContainer`.

## Known gaps (see README's "Not yet handled" for the full list)

Most relevant for a test deploy: there's no status-check endpoint (upload
returns `202` immediately, indexing is async), and public read access to
the bucket (so `repodata.json` etc. are actually fetchable by a conda
client) needs a custom domain or public R2 bucket access wired up
separately — this repo only builds the write path.
