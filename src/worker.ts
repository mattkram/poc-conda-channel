import { Container, getContainer } from "@cloudflare/containers";
import { DurableObject, env } from "cloudflare:workers";
import { AwsClient } from "aws4fetch";

export interface Env {
  CHANNEL_BUCKET: R2Bucket;
  DB: D1Database;
  INDEXER: DurableObjectNamespace;
  QUEUE: DurableObjectNamespace;
  INGESTOR: DurableObjectNamespace;
  MERGER: DurableObjectNamespace;
  INGEST_QUEUE: DurableObjectNamespace;
  GITHUB_CLIENT_ID: string;
  GITHUB_ORG: string;
  UPLOAD_TOKEN_SECRET: string;
  // Same R2 API token the container uses (S3-compatible creds), needed here
  // only to *sign* presigned URLs — the Worker never touches file bytes with them.
  R2_ACCESS_KEY_ID: string;
  R2_SECRET_ACCESS_KEY: string;
  R2_ACCOUNT_ID: string;
  R2_BUCKET_NAME: string;
}

// ---------------------------------------------------------------------------
// Container definition — this class IS the "runtime" backing the Durable
// Object. Cloudflare starts the container on first fetch() and sleeps it
// after `sleepAfter` idle time. No always-on cost.
// ---------------------------------------------------------------------------
export class IndexerContainer extends Container<Env> {
  defaultPort = 8080;     // container's internal HTTP server (see container/entrypoint.py)
  sleepAfter = "2m";      // shut down 2 min after last request

  // Forwards these into the container's os.environ at start — this is the
  // real @cloudflare/containers API (a class field referencing the
  // module-level `env` import), not a wrangler.toml block. Same secrets the
  // Worker already has for presigning; no separate container-level secrets
  // to provision.
  envVars = {
    R2_ACCOUNT_ID: env.R2_ACCOUNT_ID,
    R2_ACCESS_KEY_ID: env.R2_ACCESS_KEY_ID,
    R2_SECRET_ACCESS_KEY: env.R2_SECRET_ACCESS_KEY,
    R2_BUCKET_NAME: env.R2_BUCKET_NAME,
    // Worker URL so the container can POST browse records to D1 via the Worker.
    WORKER_URL: "https://conda.matt-kramer.com",
  };
}

// ---------------------------------------------------------------------------
// ChannelQueue — one instance per channel, no container attached. Its whole
// job is: accumulate uploads for a debounce window, then hand the whole
// batch to the container in a single call.
//
// Why this gives us atomicity: a Durable Object's alarm() has guaranteed
// at-least-once execution, and Cloudflare never runs two invocations of the
// same DO's alarm concurrently. So no matter how many uploads land at once
// for a channel, there is only ever one /ingest-batch call — and therefore
// one conda-index run — in flight for that channel at a time. We don't have
// to build our own lock; the DO's own concurrency model is the lock.
//
// Ordering: pending entries are keyed `pending:<paddedTimestamp>:<filename>`,
// so ctx.storage.list() returns them in upload order for free — no separate
// sort step, no clock skew handling beyond what Date.now() already gives us.
// ---------------------------------------------------------------------------
const DEBOUNCE_MS = 5_000; // collect uploads for 5s before triggering a batch

interface PendingUpload {
  channel: string;
  filename: string;
  uploadedAt: number;
  uploadedBy: string;
}

export class ChannelQueue extends DurableObject<Env> {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/enqueue" && request.method === "POST") {
      const upload = await request.json<PendingUpload>();
      const paddedTs = String(upload.uploadedAt).padStart(15, "0");
      const key = `pending:${paddedTs}:${upload.filename}`;
      await this.ctx.storage.put(key, upload);

      const existingAlarm = await this.ctx.storage.getAlarm();
      if (existingAlarm === null) {
        await this.ctx.storage.setAlarm(Date.now() + DEBOUNCE_MS);
      }
      return new Response("queued", { status: 202 });
    }

    // Claim ownership of this channel for a login. First caller wins;
    // subsequent callers with the same login are allowed through; any
    // other login gets a 403. Called by handleUploadInit before enqueuing.
    if (url.pathname === "/claim" && request.method === "POST") {
      const { login } = await request.json<{ login: string }>();
      const existing = await this.ctx.storage.get<string>("owner");
      if (!existing) {
        await this.ctx.storage.put("owner", login);
        return Response.json({ owner: login, claimed: true });
      }
      if (existing === login) {
        return Response.json({ owner: existing, claimed: false });
      }
      return Response.json({ owner: existing, claimed: false }, { status: 403 });
    }

    // Return current owner and visibility (or null/public if unclaimed).
    if (url.pathname === "/owner" && request.method === "GET") {
      const owner = await this.ctx.storage.get<string>("owner") ?? null;
      const visibility = await this.ctx.storage.get<string>("visibility") ?? "public";
      return Response.json({ owner, visibility });
    }

    // Set visibility — only the owner may call this.
    if (url.pathname === "/set-visibility" && request.method === "POST") {
      const { login, visibility } = await request.json<{ login: string; visibility: string }>();
      if (visibility !== "public" && visibility !== "private") {
        return new Response("visibility must be 'public' or 'private'", { status: 400 });
      }
      const owner = await this.ctx.storage.get<string>("owner");
      if (!owner) {
        return new Response("channel has no owner yet", { status: 409 });
      }
      if (owner !== login) {
        return new Response("only the channel owner can change visibility", { status: 403 });
      }
      await this.ctx.storage.put("visibility", visibility);
      return Response.json({ owner, visibility });
    }

    // Check read access — returns {allowed: true} or 401/403.
    // Called by the Worker's read path for private channels.
    if (url.pathname === "/check-read" && request.method === "POST") {
      const { login } = await request.json<{ login: string | null }>();
      const visibility = await this.ctx.storage.get<string>("visibility") ?? "public";
      if (visibility === "public") {
        return Response.json({ allowed: true });
      }
      const owner = await this.ctx.storage.get<string>("owner") ?? null;
      if (login && login === owner) {
        return Response.json({ allowed: true });
      }
      return Response.json({ allowed: false, owner }, { status: 403 });
    }

    return new Response("not found", { status: 404 });
  }

  async alarm(): Promise<void> {
    const pending = await this.ctx.storage.list<PendingUpload>({ prefix: "pending:" });
    if (pending.size === 0) return;

    const entries = [...pending.entries()];

    // Fan out to PackageIngestors. Each one stores its work and returns 202
    // immediately — no blocking on container completion. We delete the queue
    // entry right away since the ingestor's own alarm+retry handles failures.
    await Promise.all(
      entries.map(async ([key, upload]) => {
        const id = this.env.INGESTOR.idFromName(`${upload.channel}/${upload.filename}`);
        const ingestor = this.env.INGESTOR.get(id);
        await ingestor.fetch("http://ingestor/ingest", {
          method: "POST",
          body: JSON.stringify(upload),
          headers: { "content-type": "application/json" },
        });
        await this.ctx.storage.delete(key);
      })
    );

    const remaining = await this.ctx.storage.list({ prefix: "pending:" });
    if (remaining.size > 0) {
      await this.ctx.storage.setAlarm(Date.now() + DEBOUNCE_MS);
    }
  }
}

// ---------------------------------------------------------------------------
// PackageIngestor — one instance per (channel, filename).
// Immediately enqueues work into the channel's ChannelIngestQueue and returns.
// No container call here — the queue owns all container interaction.
// ---------------------------------------------------------------------------
export class PackageIngestor extends DurableObject<Env> {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname !== "/ingest" || request.method !== "POST") {
      return new Response("not found", { status: 404 });
    }
    const upload = await request.json<PendingUpload>();

    // Forward to the per-channel ingest queue and return immediately.
    const queueId = this.env.INGEST_QUEUE.idFromName(upload.channel);
    const queue = this.env.INGEST_QUEUE.get(queueId);
    await queue.fetch("http://ingest-queue/enqueue", {
      method: "POST",
      body: JSON.stringify(upload),
      headers: { "content-type": "application/json" },
    });
    return new Response("queued", { status: 202 });
  }
}

// ---------------------------------------------------------------------------
// ChannelIngestQueue — one instance per channel. Tier 1 work queue.
//
// Serializes all container /ingest-package calls for a channel so the single
// Python HTTP server is never overwhelmed. Works like ChannelQueue but for
// container calls rather than upload notifications:
//
//   /enqueue — store work, arm alarm
//   alarm()  — pop one item, call container, notify merger, arm next alarm
//
// Concurrency is exactly 1 container call at a time per channel. The alarm
// auto-reschedules as long as there is pending work. Failed items are retried
// via the normal DO alarm backoff.
// ---------------------------------------------------------------------------
const INGEST_QUEUE_DRAIN_MS = 100; // near-immediate between items

export class ChannelIngestQueue extends DurableObject<Env> {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/enqueue" && request.method === "POST") {
      const upload = await request.json<PendingUpload>();
      // Key by padded timestamp + filename for stable ordering.
      const key = `work:${String(upload.uploadedAt).padStart(15, "0")}:${upload.filename}`;
      await this.ctx.storage.put(key, upload);
      const existing = await this.ctx.storage.getAlarm();
      if (existing === null) {
        await this.ctx.storage.setAlarm(Date.now() + INGEST_QUEUE_DRAIN_MS);
      }
      return new Response("queued", { status: 202 });
    }
    return new Response("not found", { status: 404 });
  }

  async alarm(): Promise<void> {
    // Pop the oldest item.
    const list = await this.ctx.storage.list<PendingUpload>({ prefix: "work:", limit: 1 });
    if (list.size === 0) return;
    const [[key, upload]] = list.entries();
    const { channel, filename } = upload;

    // Call the container — one at a time, no overload.
    const container = getContainer(this.env.INDEXER, channel);
    const stagingKey = `${channel}/_incoming/${filename}`;
    const resp = await container.fetch("http://container/ingest-package", {
      method: "POST",
      body: JSON.stringify({ channel, filename, staging_key: stagingKey }),
      headers: { "content-type": "application/json" },
    });

    if (!resp.ok) {
      // Requeue at back by using a future timestamp so other items drain first.
      const retryKey = `work:${String(Date.now() + 60_000).padStart(15, "0")}:${filename}`;
      await this.ctx.storage.put(retryKey, upload);
      await this.ctx.storage.delete(key);
    } else {
      const result = await resp.json<{ already_ingested?: boolean; subdir?: string; name?: string }>();
      await this.ctx.storage.delete(key);

      if (!result.already_ingested && result.subdir) {
        const mergerId = this.env.MERGER.idFromName(`${channel}/${result.subdir}`);
        const merger = this.env.MERGER.get(mergerId);
        await merger.fetch("http://merger/notify", {
          method: "POST",
          body: JSON.stringify({ channel, subdir: result.subdir, name: result.name }),
          headers: { "content-type": "application/json" },
        });
      }
    }

    // Re-arm if more work remains.
    const remaining = await this.ctx.storage.list({ prefix: "work:", limit: 1 });
    if (remaining.size > 0) {
      await this.ctx.storage.setAlarm(Date.now() + INGEST_QUEUE_DRAIN_MS);
    }
  }
}

// ---------------------------------------------------------------------------
// SubdirIndexMerger — one instance per (channel, subdir). Tier 2 + Tier 3.
//
// Receives "a shard changed" notifications from PackageIngestor, debounces
// them, then asks the container to rebuild the shard index
// (repodata_shards.msgpack.zst) and assemble repodata.json from shards.
// Single-writer per subdir, but the file it writes is small — not a
// throughput bottleneck.
// ---------------------------------------------------------------------------
const MERGE_DEBOUNCE_MS = 3_000;

export class SubdirIndexMerger extends DurableObject<Env> {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/notify" && request.method === "POST") {
      const { channel, subdir } = await request.json<{ channel: string; subdir: string }>();
      await this.ctx.storage.put("channel", channel);
      await this.ctx.storage.put("subdir", subdir);
      await this.ctx.storage.put("dirty", true);
      const existing = await this.ctx.storage.getAlarm();
      if (existing === null) {
        await this.ctx.storage.setAlarm(Date.now() + MERGE_DEBOUNCE_MS);
      }
      return new Response("noted", { status: 202 });
    }
    return new Response("not found", { status: 404 });
  }

  async alarm(): Promise<void> {
    const dirty = await this.ctx.storage.get<boolean>("dirty");
    if (!dirty) return;
    const channel = await this.ctx.storage.get<string>("channel");
    const subdir = await this.ctx.storage.get<string>("subdir");
    if (!channel || !subdir) return;

    // Clear dirty before the rebuild so notifications arriving during the
    // rebuild re-arm the alarm for another pass.
    await this.ctx.storage.put("dirty", false);

    const container = getContainer(this.env.INDEXER, `${channel}/${subdir}/_merge`);
    const resp = await container.fetch("http://container/rebuild-index", {
      method: "POST",
      body: JSON.stringify({ channel, subdir }),
      headers: { "content-type": "application/json" },
    });
    if (!resp.ok) {
      // Rebuild failed — mark dirty again and let alarm backoff retry.
      await this.ctx.storage.put("dirty", true);
      throw new Error(`rebuild-index failed for ${channel}/${subdir}: ${await resp.text()}`);
    }

    // If more notifications arrived during the rebuild, schedule another pass.
    const stillDirty = await this.ctx.storage.get<boolean>("dirty");
    if (stillDirty) {
      await this.ctx.storage.setAlarm(Date.now() + MERGE_DEBOUNCE_MS);
    }
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    // Homepage → channels listing.
    if (url.pathname === "/" && request.method === "GET") {
      return Response.redirect(new URL("/channels", url).toString(), 302);
    }

    if (url.pathname === "/auth/device/start" && request.method === "POST") {
      return startDeviceFlow(env);
    }
    if (url.pathname === "/auth/device/poll" && request.method === "POST") {
      return pollDeviceFlow(request, env);
    }
    if (url.pathname === "/upload/init" && request.method === "POST") {
      return handleUploadInit(request, env);
    }
    if (url.pathname === "/upload/complete" && request.method === "POST") {
      return handleUploadComplete(request, env);
    }

    // --- UI pages under /channels ---
    if (url.pathname === "/channels" || url.pathname === "/channels/") {
      return handleChannelsIndex(request, env);
    }
    // Channel name may be namespaced (owner/name) — capture up to one slash.
    const resultsMatch = url.pathname.match(/^\/channels\/([^/]+(?:\/[^/]+)?)\/results\/?$/);
    if (resultsMatch && request.method === "GET") {
      return handleBrowseResults(request, resultsMatch[1], url, env);
    }
    const detailMatch = url.pathname.match(/^\/channels\/([^/]+(?:\/[^/]+)?)\/package\/([^/]+)\/?$/);
    if (detailMatch && request.method === "GET") {
      return handleBrowsePackage(request, detailMatch[1], detailMatch[2], env);
    }
    const browsePageMatch = url.pathname.match(/^\/channels\/([^/]+(?:\/[^/]+)?)\/?$/);
    if (browsePageMatch && request.method === "GET") {
      const seg = browsePageMatch[1];
      // Single-segment path could be a namespace — check D1 for owner/name channels.
      if (!seg.includes("/")) {
        const nsChannels = await env.DB.prepare(
          `SELECT name, owner, visibility FROM channels WHERE name LIKE ? OR name = ? ORDER BY name`
        ).bind(`${seg}/%`, seg).all<{ name: string; owner: string | null; visibility: string }>();
        if (nsChannels.results.some(r => r.name.startsWith(`${seg}/`))) {
          return handleNamespacePage(seg, nsChannels.results);
        }
      }
      return handleBrowsePage(request, seg, url, env);
    }

    // --- conda client read path under /repo ---
    // GET /repo/<channel>/<subdir>/  or  /repo/<channel>/<subdir>  — subdir index
    const repoSubdirMatch = url.pathname.match(/^\/repo\/([^/]+(?:\/[^/]+)?)\/([^/]+)\/?$/);
    if (repoSubdirMatch && request.method === "GET") {
      return handleR2Get(request, repoSubdirMatch[1],
        `${repoSubdirMatch[1]}/${repoSubdirMatch[2]}/index.html`, env);
    }
    // GET /repo/<channel>/<subdir>/<path> — repodata, shards, packages
    const repoReadMatch = url.pathname.match(/^\/repo\/([^/]+(?:\/[^/]+)?)\/([^/]+)\/.+$/);
    if (repoReadMatch && request.method === "GET") {
      return handleR2Get(request, repoReadMatch[1], url.pathname.slice("/repo/".length), env);
    }
    // GET /repo/<channel>  or  /repo/<channel>/ — channel root listing
    const repoRootMatch = url.pathname.match(/^\/repo\/([^/]+(?:\/[^/]+)?)\/?$/);
    if (repoRootMatch && request.method === "GET") {
      return handleChannelRoot(request, repoRootMatch[1], env);
    }

    // DELETE /channel/<channel>/<subdir>/<filename> — remove one package + reindex
    const pkgMatch = url.pathname.match(/^\/channel\/([^/]+(?:\/[^/]+)?)\/([^/]+)\/([^/]+)$/);
    if (pkgMatch && request.method === "DELETE") {
      return handleDeletePackage(request, pkgMatch[1], pkgMatch[2], pkgMatch[3], env);
    }

    // POST /channel/<channel>/rebuild-browse — backfill browse data (owner)
    const rebuildBrowseMatch = url.pathname.match(/^\/channel\/([^/]+(?:\/[^/]+)?)\/rebuild-browse$/);
    if (rebuildBrowseMatch && request.method === "POST") {
      return handleRebuildBrowse(request, rebuildBrowseMatch[1], env);
    }

    // GET  /channel/<channel>         — return owner + visibility
    // POST /channel/<channel>/visibility — set public/private (owner only)
    // DELETE /channel/<channel>       — wipe entire channel
    const chanMatch = url.pathname.match(/^\/channel\/([^/]+(?:\/[^/]+)?)$/);
    if (chanMatch && request.method === "GET") {
      return handleGetChannelInfo(chanMatch[1], env);
    }
    if (chanMatch && request.method === "DELETE") {
      return handleDeleteChannel(request, chanMatch[1], env);
    }
    const visMatch = url.pathname.match(/^\/channel\/([^/]+(?:\/[^/]+)?)\/visibility$/);
    if (visMatch && request.method === "POST") {
      return handleSetVisibility(request, visMatch[1], env);
    }

    // POST /internal/upsert-package — called by container after R2 browse write.
    if (url.pathname === "/internal/upsert-package" && request.method === "POST") {
      return handleUpsertPackage(request, env);
    }
    // POST /internal/reconcile/<channel> — rebuild D1 from R2 _browse/* objects.
    const reconcileMatch = url.pathname.match(/^\/internal\/reconcile\/([^/]+(?:\/[^/]+)?)$/);
    if (reconcileMatch && request.method === "POST") {
      return handleReconcile(request, reconcileMatch[1], env);
    }

    // POST /internal/migrate-r2-prefix — copy R2 objects from one prefix to another.
    // Body: { src: "anaconda-cloud", dst: "mattkram/anaconda-cloud", cursor?: string }
    // Returns: { copied, deleted, errors, done, next_cursor? }
    // Runs in batches of 20; call repeatedly with next_cursor until done=true.
    if (url.pathname === "/internal/migrate-r2-prefix" && request.method === "POST") {
      return handleMigrateR2Prefix(request, env);
    }

    // POST /internal/delete-r2-prefix — bulk-delete all R2 objects under a prefix.
    // Body: { prefix: "main/" }  (trailing slash included or not — we normalise)
    // Returns: { deleted, done, next_cursor? }
    if (url.pathname === "/internal/delete-r2-prefix" && request.method === "POST") {
      return handleDeleteR2Prefix(request, env);
    }

    // --- Legacy redirects: flat channel names → namespaced ---
    // Handles /channels/anaconda-cloud*, /repo/anaconda-cloud*, /channel/anaconda-cloud*
    const legacyRedirects: Record<string, string> = {
      "anaconda-cloud":   "mattkram/anaconda-cloud",
      "anaconda-cloud-2": "mattkram/anaconda-cloud-2",
    };
    for (const [flat, namespaced] of Object.entries(legacyRedirects)) {
      const flatRe = new RegExp(`^(\/channels\/|\/repo\/|\/channel\/)${flat}(\\/|$)`);
      if (flatRe.test(url.pathname)) {
        const newPath = url.pathname.replace(`/${flat}/`, `/${namespaced}/`)
                                    .replace(`/${flat}`, `/${namespaced}`);
        const newUrl = new URL(request.url);
        newUrl.pathname = newPath;
        return Response.redirect(newUrl.toString(), 301);
      }
    }

    return new Response("not found", { status: 404 });
  },
};

// ---------------------------------------------------------------------------
// Channel metadata — owner + visibility.
//
// Source of truth for ownership/visibility is D1 (fast reads, no DO wakeup).
// The ChannelQueue DO still handles upload debouncing and the claim/write path
// (it writes to its own storage for the queue logic), but all read-path
// visibility checks go straight to D1.
// ---------------------------------------------------------------------------

interface ChannelRow {
  name: string;
  owner: string | null;
  visibility: string;
  created_at: number;
}

// Ensure a channel row exists in D1. Called lazily on first claim/upload.
// If the row already exists this is a no-op (INSERT OR IGNORE).
async function ensureChannelRow(channel: string, owner: string, env: Env): Promise<void> {
  await env.DB.prepare(
    `INSERT OR IGNORE INTO channels (name, owner, visibility, created_at)
     VALUES (?, ?, 'public', ?)`
  ).bind(channel, owner, Date.now()).run();
}

// Claim-or-verify write access. Returns a 403 Response on denial, null on ok.
// For namespaced channels (e.g. "mattkram/main"), the namespace must match
// the uploader's login — you cannot upload to someone else's namespace.
async function checkChannelAccess(channel: string, login: string, env: Env): Promise<Response | null> {
  // Enforce namespace ownership up-front — no DO round-trip needed.
  const ns = channelNamespace(channel);
  if (ns && ns !== login) {
    return new Response(
      `channel '${channel}' belongs to namespace '${ns}' — you are '${login}'`,
      { status: 403 }
    );
  }

  // Fast D1 check — if the row exists, validate ownership there.
  const row = await env.DB.prepare(
    `SELECT owner FROM channels WHERE name = ?`
  ).bind(channel).first<{ owner: string | null }>();

  if (row) {
    if (row.owner && row.owner !== login) {
      return new Response(
        `channel '${channel}' is owned by ${row.owner} — access denied`,
        { status: 403 }
      );
    }
    return null;
  }

  // Channel not in D1 yet — fall through to DO for atomic first-claim.
  const queueId = env.QUEUE.idFromName(channel);
  const queue = env.QUEUE.get(queueId);
  const resp = await queue.fetch("http://queue/claim", {
    method: "POST",
    body: JSON.stringify({ login }),
    headers: { "content-type": "application/json" },
  });
  if (resp.status === 403) {
    const { owner } = await resp.json<{ owner: string }>();
    return new Response(
      `channel '${channel}' is owned by ${owner} — access denied`,
      { status: 403 }
    );
  }
  await ensureChannelRow(channel, login, env);
  return null;
}

// Check read access for a channel. Returns 401 Response on denial, null on ok.
// For public channels this is a single fast D1 query with no DO round-trip.
// login may be null for unauthenticated requests to public channels.
async function checkReadAccess(channel: string, login: string | null, env: Env): Promise<Response | null> {
  const row = await env.DB.prepare(
    `SELECT visibility, owner FROM channels WHERE name = ?`
  ).bind(channel).first<{ visibility: string; owner: string | null }>();

  // Channel not in D1 yet — assume public (no uploads have completed yet,
  // so there's nothing to protect). The DO will be the authority once data lands.
  if (!row) return null;

  if (row.visibility === "public") return null;

  // Private: require a valid token that matches the owner.
  if (login && login === row.owner) return null;

  return new Response(
    `channel '${channel}' is private — provide a valid Bearer token`,
    { status: 401, headers: { "WWW-Authenticate": 'Bearer realm="conda-channel"' } }
  );
}

// GET /channel/<channel> — returns { owner, visibility }
async function handleGetChannelInfo(channel: string, env: Env): Promise<Response> {
  if (!CHANNEL_NAME_RE.test(channel)) return new Response("invalid channel name", { status: 400 });
  const row = await env.DB.prepare(
    `SELECT owner, visibility FROM channels WHERE name = ?`
  ).bind(channel).first<{ owner: string | null; visibility: string }>();
  if (!row) return Response.json({ owner: null, visibility: "public" });
  return Response.json(row);
}

// POST /channel/<channel>/visibility — set public or private (owner only)
async function handleSetVisibility(request: Request, channel: string, env: Env): Promise<Response> {
  if (!CHANNEL_NAME_RE.test(channel)) return new Response("invalid channel name", { status: 400 });

  const auth = request.headers.get("authorization") ?? "";
  const token = auth.replace(/^Bearer\s+/i, "");
  const claims = await verifyUploadToken(token, env.UPLOAD_TOKEN_SECRET);
  if (!claims) return new Response("unauthorized", { status: 401 });

  const { visibility } = await request.json<{ visibility: string }>();
  if (visibility !== "public" && visibility !== "private") {
    return new Response("visibility must be 'public' or 'private'", { status: 400 });
  }

  const row = await env.DB.prepare(
    `SELECT owner FROM channels WHERE name = ?`
  ).bind(channel).first<{ owner: string | null }>();

  if (!row) return new Response("channel not found", { status: 404 });
  if (row.owner !== claims.login) {
    return new Response("only the channel owner can change visibility", { status: 403 });
  }

  await env.DB.prepare(
    `UPDATE channels SET visibility = ? WHERE name = ?`
  ).bind(visibility, channel).run();

  // Mirror to DO so the upload queue's internal checks stay consistent.
  const queueId = env.QUEUE.idFromName(channel);
  const queue = env.QUEUE.get(queueId);
  queue.fetch("http://queue/set-visibility", {
    method: "POST",
    body: JSON.stringify({ login: claims.login, visibility }),
    headers: { "content-type": "application/json" },
  }).catch(() => { /* best-effort mirror */ });

  return Response.json({ owner: row.owner, visibility });
}

// ---------------------------------------------------------------------------
// Read path — serve any object under <channel>/<subdir>/ from R2.
// Public channels: no auth. Private channels: Bearer token required.
// ---------------------------------------------------------------------------
async function handleR2Get(request: Request, channel: string, key: string, env: Env): Promise<Response> {
  // Extract login from Bearer token if present (may be absent for public channels).
  const auth = request.headers.get("authorization") ?? "";
  const token = auth.replace(/^Bearer\s+/i, "");
  const claims = token ? await verifyUploadToken(token, env.UPLOAD_TOKEN_SECRET) : null;

  const denied = await checkReadAccess(channel, claims?.login ?? null, env);
  if (denied) return denied;

  const obj = await env.CHANNEL_BUCKET.get(key);
  if (!obj) return new Response("not found", { status: 404 });
  const headers = new Headers();
  obj.writeHttpMetadata(headers);
  headers.set("etag", obj.httpEtag);
  return new Response(obj.body, { headers });
}

// ---------------------------------------------------------------------------
// Channel root listing — GET /<channel>/ — returns an HTML page listing
// the subdirs present in the channel (discovered by listing R2 prefixes).
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Browse UI — anaconda.org-style package listing, search, sort, pagination.
// Reads the channel's browse-index.json (built by the container in Tier 2/3),
// filters/sorts/paginates in the Worker, renders HTML fragments for htmx.
// ---------------------------------------------------------------------------
interface BrowseRecord {
  name: string;
  version: string;
  summary: string;
  license: string;
  home: string;
  subdirs: string[];
}

const PAGE_SIZE = 25;

const BROWSE_CSS = `
  * { box-sizing: border-box; }
  body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; color: #1f2933; background: #f5f7fa; }
  header { background: #fff; border-bottom: 1px solid #e4e7eb; padding: 16px 24px; display: flex; align-items: center; gap: 8px; }
  header .brand { font-weight: 700; font-size: 18px; color: #2d7a1f; text-decoration: none; }
  header .chan-ns { color: #2d7a1f; font-size: 14px; font-weight: 600; text-decoration: none; }
  header .chan-ns:hover { text-decoration: underline; }
  header .chan-sep { color: #9aacb8; font-size: 14px; padding: 0 2px; }
  header .chan { color: #3d4f5c; font-size: 14px; font-weight: 600; }
  .wrap { max-width: 960px; margin: 0 auto; padding: 24px; }
  .controls { display: flex; gap: 12px; margin-bottom: 20px; flex-wrap: wrap; align-items: center; }
  .controls label.sr-only { position:absolute; width:1px; height:1px; padding:0; margin:-1px; overflow:hidden; clip:rect(0,0,0,0); white-space:nowrap; border:0; }
  .controls input[type=search] { flex: 1 1 320px; padding: 10px 14px; border: 1px solid #cbd2d9; border-radius: 6px; font-size: 15px; }
  .controls select { padding: 10px 12px; border: 1px solid #cbd2d9; border-radius: 6px; font-size: 14px; background: #fff; }
  .count { color: #3d4f5c; font-size: 13px; margin-bottom: 12px; }
  .pkg { background: #fff; border: 1px solid #e4e7eb; border-radius: 8px; padding: 16px 18px; margin-bottom: 10px; }
  .pkg:hover { border-color: #2d7a1f; }
  .pkg a.name { font-size: 16px; font-weight: 600; color: #1f6f18; text-decoration: none; }
  .pkg .ver { color: #52606d; font-size: 13px; margin-left: 8px; }
  .pkg .summary { color: #3d4f5c; font-size: 14px; margin: 6px 0 8px; }
  .pkg .meta { display: flex; gap: 14px; flex-wrap: wrap; font-size: 12px; color: #3d4f5c; }
  .pkg .badge { background: #c8eac2; color: #1a5c12; border-radius: 4px; padding: 2px 8px; font-size: 12px; }
  .pager { display: flex; gap: 8px; align-items: center; margin-top: 20px; }
  .pager a, .pager span { padding: 6px 12px; border: 1px solid #cbd2d9; border-radius: 6px; text-decoration: none; color: #1f2933; font-size: 14px; cursor: pointer; }
  .pager .cur { background: #2d7a1f; color: #fff; border-color: #2d7a1f; }
  .empty { color: #3d4f5c; padding: 40px; text-align: center; }
  code { background: #f0f2f5; padding: 2px 6px; border-radius: 4px; font-size: 13px; }
`;

function esc(s: string): string {
  return (s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

async function loadBrowseIndex(channel: string, env: Env, q?: string, sort?: string): Promise<BrowseRecord[]> {
  // Use D1 for fast filtered queries when possible.
  const sortCol = sort === "name-desc" ? "name DESC" : "name ASC";
  let stmt: D1PreparedStatement;
  if (q) {
    // FTS match — join back to packages for the full row.
    stmt = env.DB.prepare(
      `SELECT p.name, p.version, p.summary, p.license, p.home, p.subdirs
       FROM packages_fts f
       JOIN packages p ON p.rowid = f.rowid
       WHERE p.channel = ? AND packages_fts MATCH ?
       ORDER BY p.${sortCol}`
    ).bind(channel, `"${q.replace(/"/g, '""')}"*`);
  } else {
    stmt = env.DB.prepare(
      `SELECT name, version, summary, license, home, subdirs
       FROM packages WHERE channel = ? ORDER BY ${sortCol}`
    ).bind(channel);
  }
  const { results } = await stmt.all<{
    name: string; version: string; summary: string;
    license: string; home: string; subdirs: string;
  }>();
  return results.map((r) => ({ ...r, subdirs: JSON.parse(r.subdirs ?? "[]") as string[] }));
}

function filterSort(records: BrowseRecord[], q: string, sort: string): BrowseRecord[] {
  // Records already filtered and sorted by D1; this is a passthrough.
  // Kept for compatibility with renderResults signature.
  return records;
}

function renderResults(channel: string, records: BrowseRecord[], q: string, sort: string, page: number): string {
  const total = records.length;
  const pages = Math.max(1, Math.ceil(total / PAGE_SIZE));
  const cur = Math.min(Math.max(1, page), pages);
  const slice = records.slice((cur - 1) * PAGE_SIZE, cur * PAGE_SIZE);

  const rows = slice.length
    ? slice.map((r) => `
      <div class="pkg">
        <a class="name" href="/channels/${channel}/package/${encodeURIComponent(r.name)}">${esc(r.name)}</a>
        <span class="ver">${esc(r.version)}</span>
        ${r.summary ? `<div class="summary">${esc(r.summary)}</div>` : ""}
        <div class="meta">
          ${r.license ? `<span>${esc(r.license)}</span>` : ""}
          ${(r.subdirs ?? []).map((s) => `<span class="badge">${esc(s)}</span>`).join(" ")}
        </div>
      </div>`).join("")
    : `<div class="empty">No packages match &ldquo;${esc(q)}&rdquo;.</div>`;

  const canonicalQs = (p: number) => {
    const params = new URLSearchParams();
    if (q) params.set("q", q);
    if (sort !== "name-asc") params.set("sort", sort);
    if (p > 1) params.set("page", String(p));
    const s = params.toString();
    return s ? "?" + s : "";
  };
  // /results always needs all params for the server to render correctly
  const resultsQs = (p: number) =>
    `?q=${encodeURIComponent(q)}&sort=${encodeURIComponent(sort)}&page=${p}`;
  const pager = pages > 1 ? `
    <div class="pager">
      ${cur > 1 ? `<a href="/channels/${channel}${canonicalQs(cur - 1)}" hx-get="/channels/${channel}/results${resultsQs(cur - 1)}" hx-target="#results" hx-push-url="/channels/${channel}${canonicalQs(cur - 1)}">&lsaquo; Prev</a>` : ""}
      <span class="cur">${cur} / ${pages}</span>
      ${cur < pages ? `<a href="/channels/${channel}${canonicalQs(cur + 1)}" hx-get="/channels/${channel}/results${resultsQs(cur + 1)}" hx-target="#results" hx-push-url="/channels/${channel}${canonicalQs(cur + 1)}">Next &rsaquo;</a>` : ""}
    </div>` : "";

  return `<div class="count">${total} package${total === 1 ? "" : "s"}</div>${rows}${pager}`;
}

async function browseAuth(request: Request, channel: string, env: Env): Promise<Response | null> {
  if (!CHANNEL_NAME_RE.test(channel)) return new Response("not found", { status: 404 });
  const auth = request.headers.get("authorization") ?? "";
  const token = auth.replace(/^Bearer\s+/i, "");
  const claims = token ? await verifyUploadToken(token, env.UPLOAD_TOKEN_SECRET) : null;
  return checkReadAccess(channel, claims?.login ?? null, env);
}

// GET /channels — parent page listing all channels from D1.
async function handleChannelsIndex(request: Request, env: Env): Promise<Response> {
  const { results } = await env.DB.prepare(
    `SELECT name, owner, visibility FROM channels ORDER BY name`
  ).all<{ name: string; owner: string | null; visibility: string }>();

  const cards = results.map((ch) => {
    const lock = ch.visibility === "private"
      ? ' <span class="badge" style="background:#fdecea;color:#b42318">private</span>'
      : "";
    const ns = channelNamespace(ch.name);
    const displayName = ns
      ? `<span style="color:#9aacb8">${esc(ns)}/</span>${esc(ch.name.slice(ns.length + 1))}`
      : esc(ch.name);
    return `
      <div class="pkg">
        <a class="name" href="/channels/${ch.name}">${displayName}</a>${lock}
        <div class="meta">${ch.owner ? `<span>owner: ${esc(ch.owner)}</span>` : ""}<span>conda channel</span></div>
      </div>`;
  });

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="description" content="Browse all conda channels hosted on this server.">
<title>Channels &middot; conda-channel-server</title>
<style>${BROWSE_CSS}</style>
</head>
<body>
<header><a class="brand" href="/channels">conda-channel-server</a><span class="chan">channels</span></header>
<main>
<div class="wrap">
  <div class="count">${results.length} channel${results.length === 1 ? "" : "s"}</div>
  ${cards.join("") || `<div class="empty">No channels yet.</div>`}
</div>
</main>
</body>
</html>`;
  return new Response(html, { headers: { "content-type": "text/html;charset=utf-8" } });
}

// GET /channels/:namespace — list all channels in a namespace.
function handleNamespacePage(
  namespace: string,
  channels: Array<{ name: string; owner: string | null; visibility: string }>
): Response {
  const nsChannels = channels.filter(ch => ch.name.startsWith(`${namespace}/`));
  const cards = nsChannels.map((ch) => {
    const short = ch.name.slice(namespace.length + 1);
    const lock = ch.visibility === "private"
      ? ' <span class="badge" style="background:#fdecea;color:#b42318">private</span>'
      : "";
    return `
      <div class="pkg">
        <a class="name" href="/channels/${ch.name}">${esc(short)}</a>${lock}
        <div class="meta"><span>conda channel</span></div>
      </div>`;
  });
  const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="description" content="Channels owned by ${esc(namespace)}.">
<title>${esc(namespace)} &middot; channels</title>
<style>${BROWSE_CSS}</style>
</head>
<body>
<header>
  <a class="brand" href="/channels">conda-channel-server</a>
  <span class="chan">${esc(namespace)}</span>
</header>
<main>
<div class="wrap">
  <div class="count">${nsChannels.length} channel${nsChannels.length === 1 ? "" : "s"}</div>
  ${cards.join("") || `<div class="empty">No channels in this namespace.</div>`}
</div>
</main>
</body>
</html>`;
  return new Response(html, { headers: { "content-type": "text/html;charset=utf-8" } });
}

    // POST /channel/<channel>/rebuild-browse — backfill browse data (owner)
// ---------------------------------------------------------------------------
// D1 upsert — called by the container after it writes _browse/<name>.json to R2.
// Keeps D1 in sync as the authoritative read projection of R2 browse data.
// Internal endpoint; no auth token required but only reachable from the container
// (which runs on the same Cloudflare account and calls back via the Worker URL).
// ---------------------------------------------------------------------------

interface UpsertPackageBody {
  channel: string;
  name: string;
  version: string;
  summary?: string;
  license?: string;
  home?: string;
  subdirs: string[];
}

async function handleUpsertPackage(request: Request, env: Env): Promise<Response> {
  const body = await request.json<UpsertPackageBody>();
  const { channel, name, version, summary, license, home, subdirs } = body;

  if (!channel || !name || !version) {
    return new Response("missing required fields: channel, name, version", { status: 400 });
  }

  // Ensure the channel row exists before inserting the FK reference.
  await env.DB.prepare(
    `INSERT OR IGNORE INTO channels (name, owner, visibility, created_at) VALUES (?, NULL, 'public', ?)`
  ).bind(channel, Date.now()).run();

  await env.DB.prepare(
    `INSERT INTO packages (channel, name, version, summary, license, home, subdirs, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(channel, name) DO UPDATE SET
       version    = excluded.version,
       summary    = excluded.summary,
       license    = excluded.license,
       home       = excluded.home,
       subdirs    = excluded.subdirs,
       updated_at = excluded.updated_at`
  ).bind(
    channel, name, version,
    summary ?? null, license ?? null, home ?? null,
    JSON.stringify(subdirs ?? []),
    Date.now()
  ).run();

  return new Response("ok", { status: 200 });
}

// ---------------------------------------------------------------------------
// Reconciliation — rebuild D1 packages table from R2 _browse/* objects.
// Walks <channel>/_browse/ prefix in R2, upserts each record into D1.
// Run once after deploying D1, and again any time they drift.
// Auth: upload token (owner of the channel or any org member).
// ---------------------------------------------------------------------------
async function handleReconcile(request: Request, channel: string, env: Env): Promise<Response> {
  if (!CHANNEL_NAME_RE.test(channel)) return new Response("invalid channel name", { status: 400 });

  const auth = request.headers.get("authorization") ?? "";
  const token = auth.replace(/^Bearer\s+/i, "");
  const claims = await verifyUploadToken(token, env.UPLOAD_TOKEN_SECRET);
  if (!claims) return new Response("unauthorized", { status: 401 });

  // Ensure the channel row exists.
  await env.DB.prepare(
    `INSERT OR IGNORE INTO channels (name, owner, visibility, created_at) VALUES (?, ?, 'public', ?)`
  ).bind(channel, claims.login, Date.now()).run();

  const prefix = `${channel}/_browse/`;
  let cursor: string | undefined;
  let upserted = 0;
  let errors = 0;

  do {
    const list = await env.CHANNEL_BUCKET.list({ prefix, cursor });
    await Promise.all(list.objects.map(async (obj) => {
      try {
        const r2obj = await env.CHANNEL_BUCKET.get(obj.key);
        if (!r2obj) return;
        const rec = await r2obj.json<UpsertPackageBody>();
        if (!rec.name || !rec.version) return;
        await env.DB.prepare(
          `INSERT INTO packages (channel, name, version, summary, license, home, subdirs, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(channel, name) DO UPDATE SET
             version    = excluded.version,
             summary    = excluded.summary,
             license    = excluded.license,
             home       = excluded.home,
             subdirs    = excluded.subdirs,
             updated_at = excluded.updated_at`
        ).bind(
          channel, rec.name, rec.version,
          rec.summary ?? null, rec.license ?? null, rec.home ?? null,
          JSON.stringify(rec.subdirs ?? []),
          Date.now()
        ).run();
        upserted++;
      } catch {
        errors++;
      }
    }));
    cursor = list.truncated ? list.cursor : undefined;
  } while (cursor);

  return Response.json({ channel, upserted, errors });
}

// POST /internal/migrate-r2-prefix
// Copies R2 objects from one prefix to another in batches of 100, then deletes
// the originals. Call repeatedly with the returned next_cursor until done=true.
async function handleMigrateR2Prefix(request: Request, env: Env): Promise<Response> {
  const auth = request.headers.get("authorization") ?? "";
  const token = auth.replace(/^Bearer\s+/i, "");
  const claims = await verifyUploadToken(token, env.UPLOAD_TOKEN_SECRET);
  if (!claims) return new Response("unauthorized", { status: 401 });

  const body = await request.json<{ src: string; dst: string; cursor?: string }>();
  const { src, dst, cursor } = body;
  if (!src || !dst) return new Response("missing src or dst", { status: 400 });
  if (src === dst) return new Response("src and dst must differ", { status: 400 });

  const BATCH = 20;
  const srcPrefix = src.endsWith("/") ? src : src + "/";
  const dstPrefix = dst.endsWith("/") ? dst : dst + "/";

  const list = await env.CHANNEL_BUCKET.list({ prefix: srcPrefix, cursor, limit: BATCH });

  let copied = 0, deleted = 0, errors = 0;

  // Sequential to stay within CPU time limits — each object can be megabytes.
  for (const obj of list.objects) {
    const srcKey = obj.key;
    const dstKey = dstPrefix + srcKey.slice(srcPrefix.length);
    try {
      const srcObj = await env.CHANNEL_BUCKET.get(srcKey);
      if (!srcObj) { errors++; continue; }
      const body = await srcObj.arrayBuffer();
      await env.CHANNEL_BUCKET.put(dstKey, body, {
        httpMetadata: srcObj.httpMetadata,
        customMetadata: srcObj.customMetadata,
      });
      copied++;
      await env.CHANNEL_BUCKET.delete(srcKey);
      deleted++;
    } catch {
      errors++;
    }
  }

  const done = !list.truncated;
  return Response.json({
    copied, deleted, errors, done,
    ...(done ? {} : { next_cursor: list.cursor }),
  });
}

// POST /internal/delete-r2-prefix
// Bulk-deletes all R2 objects under an arbitrary prefix in batches of 100.
// Accepts any prefix string — not restricted to CHANNEL_NAME_RE.
// Body: { prefix: "main" | "main/" | "__count_only__/" | "_channels-index.json" }
async function handleDeleteR2Prefix(request: Request, env: Env): Promise<Response> {
  const auth = request.headers.get("authorization") ?? "";
  const token = auth.replace(/^Bearer\s+/i, "");
  const claims = await verifyUploadToken(token, env.UPLOAD_TOKEN_SECRET);
  if (!claims) return new Response("unauthorized", { status: 401 });

  const body = await request.json<{ prefix: string; cursor?: string }>();
  let { prefix, cursor } = body;
  if (!prefix) return new Response("missing prefix", { status: 400 });

  // Single-object delete (no trailing slash needed)
  if (!prefix.endsWith("/") && !prefix.includes("/")) {
    // Could be a single key like "_channels-index.json"
  }
  // Normalise: ensure trailing slash for directory-style prefixes unless it
  // looks like an exact key (contains a dot in the last segment).
  const isExactKey = !prefix.endsWith("/") && /\.[a-z0-9]+$/i.test(prefix.split("/").pop() ?? "");
  const listPrefix = isExactKey ? prefix : (prefix.endsWith("/") ? prefix : prefix + "/");

  const list = await env.CHANNEL_BUCKET.list({ prefix: listPrefix, cursor, limit: 100 });

  // Also catch the exact key itself
  const toDelete = isExactKey
    ? [prefix]
    : list.objects.map(o => o.key);

  await Promise.all(toDelete.map(k => env.CHANNEL_BUCKET.delete(k)));

  const done = isExactKey || !list.truncated;
  return Response.json({
    deleted: toDelete.length,
    done,
    ...(done ? {} : { next_cursor: list.cursor }),
  });
}

async function handleRebuildBrowse(request: Request, channel: string, env: Env): Promise<Response> {
  if (!CHANNEL_NAME_RE.test(channel)) return new Response("invalid channel name", { status: 400 });
  const auth = request.headers.get("authorization") ?? "";
  const token = auth.replace(/^Bearer\s+/i, "");
  const claims = await verifyUploadToken(token, env.UPLOAD_TOKEN_SECRET);
  if (!claims) return new Response("unauthorized", { status: 401 });
  const denied = await checkChannelAccess(channel, claims.login, env);
  if (denied) return denied;

  const container = getContainer(env.INDEXER, `${channel}/_rebuild-browse`);
  const resp = await container.fetch("http://container/rebuild-browse", {
    method: "POST",
    body: JSON.stringify({ channel }),
    headers: { "content-type": "application/json" },
  });
  if (!resp.ok) {
    return new Response(`rebuild-browse failed: ${await resp.text()}`, { status: 502 });
  }
  return new Response(await resp.text(), {
    status: 200, headers: { "content-type": "application/json" },
  });
}

async function handleBrowseResults(request: Request, channel: string, url: URL, env: Env): Promise<Response> {
  const denied = await browseAuth(request, channel, env);
  if (denied) return denied;
  const q = url.searchParams.get("q") ?? "";
  const sort = url.searchParams.get("sort") ?? "name-asc";
  const page = parseInt(url.searchParams.get("page") ?? "1", 10) || 1;
  const records = await loadBrowseIndex(channel, env, q, sort);

  // Build the canonical page URL so htmx pushes /channels/:channel?... into the
  // browser history rather than the /results partial URL.
  const canonicalParams = new URLSearchParams();
  if (q) canonicalParams.set("q", q);
  if (sort !== "name-asc") canonicalParams.set("sort", sort);
  if (page > 1) canonicalParams.set("page", String(page));
  const canonicalSearch = canonicalParams.toString();
  const pushUrl = `/channels/${channel}${canonicalSearch ? "?" + canonicalSearch : ""}`;

  return new Response(renderResults(channel, records, q, sort, page), {
    headers: {
      "content-type": "text/html;charset=utf-8",
      "HX-Push-Url": pushUrl,
    },
  });
}

async function handleBrowsePage(request: Request, channel: string, url: URL, env: Env): Promise<Response> {
  const denied = await browseAuth(request, channel, env);
  if (denied) return denied;
  const q = url.searchParams.get("q") ?? "";
  const sort = url.searchParams.get("sort") ?? "name-asc";
  const page = parseInt(url.searchParams.get("page") ?? "1", 10) || 1;
  const records = await loadBrowseIndex(channel, env, q, sort);
  const results = renderResults(channel, records, q, sort, page);

  const ns = channelNamespace(channel);
  const channelHeader = ns
    ? `<a class="chan-ns" href="/channels/${esc(ns)}">${esc(ns)}</a><span class="chan-sep">/</span><span class="chan">${esc(channel.slice(ns.length + 1))}</span>`
    : `<span class="chan">${esc(channel)}</span>`;

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="description" content="Browse packages in the ${esc(channel)} conda channel. Search, filter, and install packages.">
<title>${esc(channel)} &middot; packages</title>
<script src="https://unpkg.com/htmx.org@1.9.12" defer></script>
<style>${BROWSE_CSS}</style>
</head>
<body>
<header>
  <a class="brand" href="/channels">conda-channel-server</a>
  ${channelHeader}
</header>
<main>
<div class="wrap">
  <form class="controls" hx-get="/channels/${channel}/results" hx-target="#results" hx-trigger="input changed delay:250ms from:input[name='q'], change from:select">
    <label class="sr-only" for="pkg-search">Search packages</label>
    <input id="pkg-search" type="search" name="q" placeholder="Search packages&hellip;" value="${esc(q)}" autocomplete="off">
    <label class="sr-only" for="pkg-sort">Sort by</label>
    <select id="pkg-sort" name="sort" aria-label="Sort packages">
      <option value="name-asc"${sort === "name-asc" ? " selected" : ""}>Name A&rarr;Z</option>
      <option value="name-desc"${sort === "name-desc" ? " selected" : ""}>Name Z&rarr;A</option>
    </select>
  </form>
  <div id="results">${results}</div>
</div>
</main>
</body>
</html>`;
  return new Response(html, { headers: { "content-type": "text/html;charset=utf-8" } });
}

async function handleBrowsePackage(request: Request, channel: string, name: string, env: Env): Promise<Response> {
  const denied = await browseAuth(request, channel, env);
  if (denied) return denied;
  name = decodeURIComponent(name);

  // Look up package record from D1 — fast single-row read.
  const row = await env.DB.prepare(
    `SELECT name, version, summary, license, home, subdirs FROM packages WHERE channel = ? AND name = ?`
  ).bind(channel, name).first<{
    name: string; version: string; summary: string;
    license: string; home: string; subdirs: string;
  }>();
  if (!row) return new Response("package not found", { status: 404 });
  const rec: BrowseRecord = { ...row, subdirs: JSON.parse(row.subdirs ?? "[]") as string[] };

  const builds: { subdir: string; filename: string; version: string; build: string }[] = [];
  for (const subdir of rec.subdirs ?? []) {
    const obj = await env.CHANNEL_BUCKET.get(`${channel}/${subdir}/repodata.json`);
    if (!obj) continue;
    const rd = await obj.json<{ packages: Record<string, any>; "packages.conda": Record<string, any> }>();
    for (const [fn, meta] of Object.entries({ ...(rd.packages ?? {}), ...(rd["packages.conda"] ?? {}) })) {
      if ((meta as any).name === name) {
        builds.push({ subdir, filename: fn, version: (meta as any).version, build: (meta as any).build });
      }
    }
  }
  builds.sort((a, b) => b.version.localeCompare(a.version) || a.filename.localeCompare(b.filename));

  const buildRows = builds.map((b) => `
    <div class="pkg">
      <a class="name" href="/repo/${channel}/${b.subdir}/${encodeURIComponent(b.filename)}">${esc(b.filename)}</a>
      <div class="meta"><span class="badge">${esc(b.subdir)}</span><span>v${esc(b.version)}</span><span>${esc(b.build)}</span></div>
    </div>`).join("");

  const origin = new URL(request.url).origin;
  const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="description" content="${esc(rec.summary || `${name} package in the ${channel} conda channel`)}">
<title>${esc(name)} &middot; ${esc(channel)}</title>
<style>${BROWSE_CSS}</style>
</head>
<body>
<header>
  <a class="brand" href="/channels">conda-channel-server</a>
  <span class="chan">/ <a href="/channels/${channel}" style="color:#3d4f5c">${esc(channel)}</a> / ${esc(name)}</span>
</header>
<main>
<div class="wrap">
  <h1 style="margin:0 0 4px">${esc(name)} <span class="ver">${esc(rec.version)}</span></h1>
  ${rec.summary ? `<p class="summary">${esc(rec.summary)}</p>` : ""}
  <div class="meta" style="margin-bottom:20px">
    ${rec.license ? `<span>License: ${esc(rec.license)}</span>` : ""}
    ${rec.home ? `<span><a href="${esc(rec.home)}">${esc(rec.home)}</a></span>` : ""}
  </div>
  <p><strong>Install:</strong> <code>conda install -c ${esc(origin)}/repo/${esc(channel)} ${esc(name)}</code></p>
  <h2 style="font-size:16px;margin-top:24px">Files</h2>
  ${buildRows || `<div class="empty">No files.</div>`}
</div>
</main>
</body>
</html>`;
  return new Response(html, { headers: { "content-type": "text/html;charset=utf-8" } });
}

async function handleChannelRoot(request: Request, channel: string, env: Env): Promise<Response> {
  if (!CHANNEL_NAME_RE.test(channel)) return new Response("not found", { status: 404 });

  const auth = request.headers.get("authorization") ?? "";
  const token = auth.replace(/^Bearer\s+/i, "");
  const claims = token ? await verifyUploadToken(token, env.UPLOAD_TOKEN_SECRET) : null;
  const denied = await checkReadAccess(channel, claims?.login ?? null, env);
  if (denied) return denied;

  // Discover subdirs by listing objects and collecting unique top-level prefixes.
  const subdirs = new Set<string>();
  let cursor: string | undefined;
  do {
    const list = await env.CHANNEL_BUCKET.list({ prefix: `${channel}/`, cursor, delimiter: "/" });
    for (const prefix of (list as any).delimitedPrefixes ?? []) {
      const subdir = prefix.slice(channel.length + 1, -1); // strip "channel/" and trailing "/"
      if (subdir && !subdir.startsWith("_")) subdirs.add(subdir);
    }
    cursor = list.truncated ? list.cursor : undefined;
  } while (cursor);

  const rows = [...subdirs].sort().map(
    (s) => `<li><a href="/${channel}/${s}/">${s}/</a></li>`
  ).join("\n    ");

  const html = `<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>${channel}</title></head>
<body>
<h1>${channel}</h1>
<ul>
    ${rows || "<li><em>(empty)</em></li>"}
</ul>
</body>
</html>`;

  return new Response(html, { headers: { "content-type": "text/html;charset=utf-8" } });
}

// ---------------------------------------------------------------------------
// Delete one package from a channel, then ask the container to reindex that
// subdir so repodata.json no longer references the removed package.
//
// Route: DELETE /channel/<channel>/<subdir>/<filename>
// Auth: upload token (same token as upload — "can manage this channel")
// ---------------------------------------------------------------------------
async function handleDeletePackage(request: Request, channel: string, subdir: string, filename: string, env: Env): Promise<Response> {
  const auth = request.headers.get("authorization") ?? "";
  const token = auth.replace(/^Bearer\s+/i, "");
  const claims = await verifyUploadToken(token, env.UPLOAD_TOKEN_SECRET);
  if (!claims) return new Response("unauthorized", { status: 401 });

  const invalid = validateChannelAndFilename(channel, filename);
  if (invalid) return new Response(invalid, { status: 400 });

  const denied = await checkChannelAccess(channel, claims.login, env);
  if (denied) return denied;

  const key = `${channel}/${subdir}/${filename}`;
  const exists = await env.CHANNEL_BUCKET.head(key);
  if (!exists) return new Response(`${key} not found`, { status: 404 });

  await env.CHANNEL_BUCKET.delete(key);

  // Ask the container to reindex the subdir so repodata reflects the removal.
  const container = getContainer(env.INDEXER, channel);
  const resp = await container.fetch("http://container/reindex", {
    method: "POST",
    body: JSON.stringify({ channel, subdir }),
    headers: { "content-type": "application/json" },
  });
  if (!resp.ok) {
    return new Response(`deleted ${filename} but reindex failed: ${await resp.text()}`, { status: 500 });
  }

  return new Response(`deleted ${filename} and reindexed ${channel}/${subdir}`, { status: 200 });
}

// ---------------------------------------------------------------------------
// Wipe an entire channel — deletes every object under the channel prefix
// including packages, repodata, shards, cache, and any staging objects.
// Intended for test teardown; no reindex needed since the channel is gone.
//
// Route: DELETE /channel/<channel>
// Auth: upload token
// ---------------------------------------------------------------------------
async function handleDeleteChannel(request: Request, channel: string, env: Env): Promise<Response> {
  const auth = request.headers.get("authorization") ?? "";
  const token = auth.replace(/^Bearer\s+/i, "");
  const claims = await verifyUploadToken(token, env.UPLOAD_TOKEN_SECRET);
  if (!claims) return new Response("unauthorized", { status: 401 });

  if (!CHANNEL_NAME_RE.test(channel)) return new Response("invalid channel name", { status: 400 });

  const denied = await checkChannelAccess(channel, claims.login, env);
  if (denied) return denied;

  let deleted = 0;
  let cursor: string | undefined;
  do {
    const list = await env.CHANNEL_BUCKET.list({ prefix: `${channel}/`, cursor });
    await Promise.all(list.objects.map((o) => env.CHANNEL_BUCKET.delete(o.key)));
    deleted += list.objects.length;
    cursor = list.truncated ? list.cursor : undefined;
  } while (cursor);

  return Response.json({ deleted, channel });
}

// ---------------------------------------------------------------------------
// Upload, in two steps. Bytes never pass through the Worker:
//   1. /upload/init     -> Worker signs a presigned PUT URL, client uploads
//                          straight to R2 with it.
//   2. /upload/complete -> Worker confirms the object landed, then hands off
//                          to the container (which is the only thing that
//                          ever reads the package bytes, because it has to).
// ---------------------------------------------------------------------------
// Bare channel name (e.g. "main") or namespaced (e.g. "mattkram/main").
// Namespace: valid GitHub username (alphanumeric + hyphens, ≤39 chars).
const CHANNEL_NAME_RE = /^(?:[a-z0-9][a-z0-9-]{0,38}\/)?[a-z0-9][a-z0-9._-]{0,63}$/;
const PRESIGN_TTL_SECONDS = 900; // 15 min — plenty for a package upload, short-lived if leaked

// Extract the namespace from a channel name, or null if none.
// "mattkram/main" → "mattkram";  "main" → null
function channelNamespace(channel: string): string | null {
  const slash = channel.indexOf("/");
  return slash === -1 ? null : channel.slice(0, slash);
}

function r2Client(env: Env): AwsClient {
  return new AwsClient({
    service: "s3",
    region: "auto",
    accessKeyId: env.R2_ACCESS_KEY_ID,
    secretAccessKey: env.R2_SECRET_ACCESS_KEY,
  });
}

function stagingKeyFor(channel: string, filename: string): string {
  return `${channel}/_incoming/${filename}`;
}

function validateChannelAndFilename(channel: string, filename: string): string | null {
  if (!CHANNEL_NAME_RE.test(channel)) return "invalid channel name";
  if (filename.includes("/")) return "invalid filename";
  if (!filename.endsWith(".conda") && !filename.endsWith(".tar.bz2")) {
    return "only .conda or .tar.bz2 packages are accepted";
  }
  return null;
}

async function handleUploadInit(request: Request, env: Env): Promise<Response> {
  const auth = request.headers.get("authorization") ?? "";
  const token = auth.replace(/^Bearer\s+/i, "");
  const claims = await verifyUploadToken(token, env.UPLOAD_TOKEN_SECRET);
  if (!claims) return new Response("unauthorized", { status: 401 });

  const { channel, filename } = await request.json<{ channel: string; filename: string }>();
  const invalid = validateChannelAndFilename(channel, filename);
  if (invalid) return new Response(invalid, { status: 400 });

  // Claim or verify ownership before issuing a presigned URL.
  const denied = await checkChannelAccess(channel, claims.login, env);
  if (denied) return denied;

  const key = stagingKeyFor(channel, filename);
  const objectUrl = new URL(
    `https://${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com/${env.R2_BUCKET_NAME}/${key}`
  );
  objectUrl.searchParams.set("X-Amz-Expires", String(PRESIGN_TTL_SECONDS));

  // signQuery signs only the host — don't sign or require Content-Type,
  // or client-side PUTs from anywhere other than curl/aws4fetch itself
  // will fail signature validation. Let R2 infer content type on write.
  const signed = await r2Client(env).sign(new Request(objectUrl, { method: "PUT" }), {
    aws: { signQuery: true },
  });

  return Response.json({
    upload_url: signed.url,
    method: "PUT",
    expires_in: PRESIGN_TTL_SECONDS,
  });
}

async function handleUploadComplete(request: Request, env: Env): Promise<Response> {
  const auth = request.headers.get("authorization") ?? "";
  const token = auth.replace(/^Bearer\s+/i, "");
  const claims = await verifyUploadToken(token, env.UPLOAD_TOKEN_SECRET);
  if (!claims) return new Response("unauthorized", { status: 401 });

  const { channel, filename } = await request.json<{ channel: string; filename: string }>();
  const invalid = validateChannelAndFilename(channel, filename);
  if (invalid) return new Response(invalid, { status: 400 });

  const key = stagingKeyFor(channel, filename);

  // Confirm the client actually finished the direct-to-R2 PUT before we
  // enqueue anything — cheap metadata check via the native binding,
  // still no bytes read.
  const head = await env.CHANNEL_BUCKET.head(key);
  if (!head) {
    return new Response(`no object staged at ${key} — did the upload PUT succeed?`, { status: 409 });
  }

  // Hand off to the per-channel queue DO rather than the container directly.
  // It debounces for a few seconds (batching any other near-simultaneous
  // uploads to this channel) and guarantees only one ingest run happens at
  // a time — see ChannelQueue above.
  const queueId = env.QUEUE.idFromName(channel);
  const queue = env.QUEUE.get(queueId);
  await queue.fetch("http://queue/enqueue", {
    method: "POST",
    body: JSON.stringify({ channel, filename, uploadedAt: Date.now(), uploadedBy: claims.login }),
    headers: { "content-type": "application/json" },
  });

  return new Response(`queued ${filename} for channel ${channel}`, { status: 202 });
}

// ---------------------------------------------------------------------------
// Auth: GitHub Device Flow -> our own short-lived HMAC token.
// CLI never sees an R2/Cloudflare credential, and the GitHub token
// never touches the upload path.
// ---------------------------------------------------------------------------
async function startDeviceFlow(env: Env): Promise<Response> {
  const resp = await fetch("https://github.com/login/device/code", {
    method: "POST",
    headers: { accept: "application/json", "content-type": "application/json" },
    body: JSON.stringify({ client_id: env.GITHUB_CLIENT_ID, scope: "read:org" }),
  });
  const data = await resp.json();
  // data: { device_code, user_code, verification_uri, expires_in, interval }
  return Response.json(data);
}

async function pollDeviceFlow(request: Request, env: Env): Promise<Response> {
  const { device_code } = await request.json<{ device_code: string }>();

  const resp = await fetch("https://github.com/login/oauth/access_token", {
    method: "POST",
    headers: { accept: "application/json", "content-type": "application/json" },
    body: JSON.stringify({
      client_id: env.GITHUB_CLIENT_ID,
      device_code,
      grant_type: "urn:ietf:params:oauth:grant-type:device_code",
    }),
  });
  const data = await resp.json<{ access_token?: string; error?: string }>();
  if (!data.access_token) {
    // "authorization_pending" while user hasn't approved yet — CLI keeps polling
    return Response.json(data, { status: 202 });
  }

  const ghUser = await fetch("https://api.github.com/user", {
    headers: { authorization: `Bearer ${data.access_token}`, "user-agent": "conda-channel-server" },
  }).then((r) => r.json<{ login: string }>());

  const memberCheck = await fetch(
    `https://api.github.com/orgs/${env.GITHUB_ORG}/members/${ghUser.login}`,
    { headers: { authorization: `Bearer ${data.access_token}`, "user-agent": "conda-channel-server" } }
  );
  if (memberCheck.status !== 204) {
    return new Response(`${ghUser.login} is not a member of ${env.GITHUB_ORG}`, { status: 403 });
  }

  const uploadToken = await signUploadToken({ login: ghUser.login }, env.UPLOAD_TOKEN_SECRET);
  return Response.json({ upload_token: uploadToken, expires_in: 3600 });
}

// ---------------------------------------------------------------------------
// Minimal HMAC-signed token (header.payload.sig, base64url) — avoids a JWT
// dependency for something this small. 1hr expiry, scoped to nothing but
// "can call /upload".
// ---------------------------------------------------------------------------
async function signUploadToken(payload: { login: string }, secret: string): Promise<string> {
  const body = { ...payload, exp: Math.floor(Date.now() / 1000) + 3600 };
  const encoded = b64url(JSON.stringify(body));
  const sig = await hmac(encoded, secret);
  return `${encoded}.${sig}`;
}

async function verifyUploadToken(token: string, secret: string): Promise<{ login: string } | null> {
  const [encoded, sig] = token.split(".");
  if (!encoded || !sig) return null;
  const expected = await hmac(encoded, secret);
  if (expected !== sig) return null;
  const payload = JSON.parse(atob(encoded));
  if (payload.exp < Math.floor(Date.now() / 1000)) return null;
  return payload;
}

function b64url(s: string): string {
  return btoa(s).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

async function hmac(data: string, secret: string): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw", new TextEncoder().encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]
  );
  const sig = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(data));
  return b64url(String.fromCharCode(...new Uint8Array(sig)));
}
