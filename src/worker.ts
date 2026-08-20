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
  GITHUB_CLIENT_SECRET: string;
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

    if (url.pathname === "/purge" && request.method === "POST") {
      const all = await this.ctx.storage.list({ prefix: "pending:" });
      await this.ctx.storage.delete([...all.keys()]);
      await this.ctx.storage.deleteAlarm();
      return Response.json({ purged: all.size });
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
    if (url.pathname === "/purge" && request.method === "POST") {
      // Delete all pending work items and cancel the alarm.
      const all = await this.ctx.storage.list({ prefix: "work:" });
      await this.ctx.storage.delete([...all.keys()]);
      await this.ctx.storage.deleteAlarm();
      return Response.json({ purged: all.size });
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
      const result = await resp.json<{
        already_ingested?: boolean;
        subdir?: string;
        name?: string;
        old_hash?: string;
        new_hash?: string;
      }>();
      await this.ctx.storage.delete(key);

      if (!result.already_ingested && result.subdir) {
        // Delete the old shard now that the pointer has moved to the new one.
        // old_hash is absent on the very first upload for a name (no prior shard).
        if (result.old_hash && result.old_hash !== result.new_hash) {
          const oldShardKey = `${channel}/${result.subdir}/${result.old_hash}.msgpack.zst`;
          await this.env.CHANNEL_BUCKET.delete(oldShardKey);
        }

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
    if (url.pathname === "/auth/login" && request.method === "GET") {
      return handleBrowserLoginStart(request, env);
    }
    if (url.pathname === "/auth/callback" && request.method === "GET") {
      return handleBrowserLoginCallback(request, url, env);
    }
    if (url.pathname === "/auth/logout" && request.method === "GET") {
      return handleBrowserLogout(url);
    }
    if (url.pathname === "/upload/init" && request.method === "POST") {
      return handleUploadInit(request, env);
    }
    if (url.pathname === "/upload/complete" && request.method === "POST") {
      return handleUploadComplete(request, env);
    }
    if (url.pathname === "/upload/exchange-oidc" && request.method === "POST") {
      return handleOidcExchange(request, env);
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
          return handleNamespacePage(request, seg, nsChannels.results, env);
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
    // Trusted publisher rules (owner only)
    const tpMatch = url.pathname.match(/^\/channel\/([^/]+(?:\/[^/]+)?)\/trusted-publishers$/);
    if (tpMatch && request.method === "GET") {
      return handleListTrustedPublishers(request, tpMatch[1], env);
    }
    if (tpMatch && request.method === "POST") {
      return handleAddTrustedPublisher(request, tpMatch[1], env);
    }
    const tpDelMatch = url.pathname.match(/^\/channel\/([^/]+(?:\/[^/]+)?)\/trusted-publishers\/(\d+)$/);
    if (tpDelMatch && request.method === "DELETE") {
      return handleDeleteTrustedPublisher(request, tpDelMatch[1], Number(tpDelMatch[2]), env);
    }
    // Admin UI
    const adminMatch = url.pathname.match(/^\/channels\/([^/]+(?:\/[^/]+)?)\/admin\/?$/);
    if (adminMatch && request.method === "GET") {
      return handleAdminPage(request, adminMatch[1], env);
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

    // POST /internal/purge-queue/<channel> — clear all pending ingest work for a channel.
    // Stops the ChannelIngestQueue DO from retrying stale/deleted uploads forever.
    const purgeQueueMatch = url.pathname.match(/^\/internal\/purge-queue\/([^/]+(?:\/[^/]+)?)$/);
    if (purgeQueueMatch && request.method === "POST") {
      return handlePurgeQueue(request, purgeQueueMatch[1], env);
    }

    // POST /internal/abort-multipart — list and abort all in-progress multipart uploads.
    // Cleans up orphaned multipart uploads left by interrupted R2 migrations.
    if (url.pathname === "/internal/abort-multipart" && request.method === "POST") {
      return handleAbortMultipart(request, env);
    }

    // POST /internal/list-r2 — list R2 keys under a prefix.
    // Body: { prefix: string, cursor?: string, limit?: number }
    // Returns { keys: string[], done: bool, next_cursor? }
    if (url.pathname === "/internal/list-r2" && request.method === "POST") {
      const auth = request.headers.get("authorization") ?? "";
      const token = auth.replace(/^Bearer\s+/i, "");
      const claims = await verifyUploadToken(token, env.UPLOAD_TOKEN_SECRET);
      if (!claims) return new Response("unauthorized", { status: 401 });
      const body = await request.json<{ prefix?: string; cursor?: string; limit?: number }>();
      const list = await env.CHANNEL_BUCKET.list({
        prefix: body.prefix ?? "",
        cursor: body.cursor,
        limit: body.limit ?? 1000,
      });
      return Response.json({
        keys: list.objects.map(o => o.key),
        done: !list.truncated,
        ...(list.truncated ? { next_cursor: list.cursor } : {}),
      });
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
// Reads from D1 (packages table) — fast, no R2 round-trip.
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

const PKG_DETAIL_CSS = `
  .pkg-hero { background:#fff; border:1px solid #e4e7eb; border-radius:10px; padding:24px 28px; margin-bottom:24px; }
  .pkg-title { display:flex; align-items:baseline; gap:12px; margin-bottom:8px; }
  .pkg-title h1 { margin:0; font-size:24px; }
  .ver-badge { background:#2d7a1f; color:#fff; border-radius:6px; padding:3px 10px; font-size:14px; font-weight:600; }
  .pkg-summary { color:#3d4f5c; font-size:15px; margin:8px 0 14px; }
  .pkg-attrs { display:flex; flex-direction:column; gap:6px; font-size:13px; }
  .attr { display:flex; gap:8px; align-items:baseline; flex-wrap:wrap; }
  .attr-label { font-weight:600; color:#52606d; min-width:60px; }
  .detail-section { margin-bottom:28px; }
  .detail-section h2 { font-size:16px; font-weight:700; margin:0 0 12px; color:#1f2933; display:flex; align-items:center; gap:8px; }
  .ver-count { font-size:12px; font-weight:400; color:#52606d; }
  .install-block { display:flex; align-items:center; gap:12px; background:#f0f2f5; border-radius:6px; padding:12px 16px; flex-wrap:wrap; }
  .install-block code { flex:1; font-size:13px; background:none; padding:0; user-select:all; }
  .copy-btn { padding:6px 14px; background:#2d7a1f; color:#fff; border:none; border-radius:6px; font-size:13px; cursor:pointer; white-space:nowrap; }
  .copy-btn:hover { background:#246018; }
  details { border:1px solid #e4e7eb; border-radius:8px; margin-bottom:8px; overflow:hidden; }
  summary.ver-summary { display:flex; align-items:center; gap:10px; padding:12px 16px; cursor:pointer; background:#fff; list-style:none; user-select:none; }
  summary.ver-summary::-webkit-details-marker { display:none; }
  details[open] summary.ver-summary { border-bottom:1px solid #e4e7eb; }
  summary.ver-summary::before { content:"▶"; font-size:10px; color:#9aacb8; transition:transform .15s; flex-shrink:0; }
  details[open] summary.ver-summary::before { transform:rotate(90deg); }
  .ver-num { font-size:15px; font-weight:700; color:#1f6f18; }
  .ver-subdirs { display:flex; gap:4px; flex-wrap:wrap; margin-left:auto; }
  .files-table { width:100%; border-collapse:collapse; font-size:13px; }
  .files-table th { text-align:left; padding:8px 12px; background:#f5f7fa; color:#52606d; font-weight:600; border-bottom:1px solid #e4e7eb; }
  .files-table td { padding:8px 12px; border-bottom:1px solid #f0f2f5; vertical-align:middle; }
  .files-table tr:last-child td { border-bottom:none; }
  .files-table tr:hover td { background:#fafbfc; }
  a.dl-link { color:#1f6f18; text-decoration:none; font-weight:500; }
  a.dl-link:hover { text-decoration:underline; }
  .num { color:#52606d; white-space:nowrap; }
  .mono { font-family:monospace; }
  .deps-list { list-style:none; padding:0; margin:0; display:flex; flex-wrap:wrap; gap:6px; }
  .deps-list li code { font-size:12px; }
`;

const BROWSE_CSS = `
  * { box-sizing: border-box; }
  body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; color: #1f2933; background: #f5f7fa; }
  header { background: #fff; border-bottom: 1px solid #e4e7eb; padding: 16px 24px; display: flex; align-items: center; gap: 8px; }
  header .brand { font-weight: 700; font-size: 18px; color: #2d7a1f; text-decoration: none; }
  header .chan-ns { color: #2d7a1f; font-size: 14px; font-weight: 600; text-decoration: none; }
  header .chan-ns:hover { text-decoration: underline; }
  header .chan-sep { color: #9aacb8; font-size: 14px; padding: 0 2px; }
  header .chan { color: #3d4f5c; font-size: 14px; font-weight: 600; }
  .header-user { margin-left: auto; display: flex; align-items: center; gap: 10px; font-size: 13px; color: #3d4f5c; }
  .header-user a.login-btn { padding: 5px 14px; background: #2d7a1f; color: #fff; border-radius: 6px; text-decoration: none; font-weight: 600; }
  .header-user a.login-btn:hover { background: #246018; }
  .header-user a.logout-btn { color: #52606d; text-decoration: none; }
  .header-user a.logout-btn:hover { text-decoration: underline; }
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
  .lock-badge { background: #fdecea; color: #b42318; border-radius: 4px; padding: 2px 8px; font-size: 12px; margin-left: 4px; }
`;

function esc(s: string): string {
  return (s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function userWidget(login: string | null): string {
  if (login) {
    return `<div class="header-user">
      <span>👤 ${esc(login)}</span>
      <a class="logout-btn" href="/auth/logout">Log out</a>
    </div>`;
  }
  return `<div class="header-user">
    <a class="login-btn" href="/auth/login">Log in with GitHub</a>
  </div>`;
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

// Resolve the logged-in GitHub login from either a Bearer token (CLI) or
// the __session cookie (browser). Returns null if not authenticated.
async function resolveLogin(request: Request, secret: string): Promise<string | null> {
  // Bearer token takes priority (CLI / API clients).
  const auth = request.headers.get("authorization") ?? "";
  const bearer = auth.replace(/^Bearer\s+/i, "");
  if (bearer) {
    const claims = await verifyUploadToken(bearer, secret);
    if (claims) return claims.login;
  }
  // Fall back to session cookie (browser).
  return getSessionLogin(request, secret);
}

async function browseAuth(request: Request, channel: string, env: Env): Promise<Response | null> {
  if (!CHANNEL_NAME_RE.test(channel)) return new Response("not found", { status: 404 });
  const login = await resolveLogin(request, env.UPLOAD_TOKEN_SECRET);
  return checkReadAccess(channel, login, env);
}

// GET /channels — parent page listing all channels from D1.
async function handleChannelsIndex(request: Request, env: Env): Promise<Response> {
  const login = await resolveLogin(request, env.UPLOAD_TOKEN_SECRET);
  const { results } = await env.DB.prepare(
    `SELECT name, owner, visibility FROM channels ORDER BY name`
  ).all<{ name: string; owner: string | null; visibility: string }>();

  // Hide private channels the viewer doesn't own.
  const visible = results.filter(ch => ch.visibility === "public" || ch.owner === login);

  const cards = visible.map((ch) => {
    const isPrivate = ch.visibility === "private";
    const lock = isPrivate
      ? ' <span class="lock-badge">🔒 private</span>'
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
<header>
  <a class="brand" href="/channels">conda-channel-server</a>
  <span class="chan-sep">/</span><span class="chan">channels</span>
  ${userWidget(login)}
</header>
<main>
<div class="wrap">
  <div class="count">${visible.length} channel${visible.length === 1 ? "" : "s"}</div>
  ${cards.join("") || `<div class="empty">No channels yet.</div>`}
</div>
</main>
</body>
</html>`;
  return new Response(html, { headers: { "content-type": "text/html;charset=utf-8" } });
}

// GET /channels/:namespace — list all channels in a namespace.
async function handleNamespacePage(
  request: Request,
  namespace: string,
  channels: Array<{ name: string; owner: string | null; visibility: string }>,
  env: Env,
): Promise<Response> {
  const login = await resolveLogin(request, env.UPLOAD_TOKEN_SECRET);
  const nsChannels = channels
    .filter(ch => ch.name.startsWith(`${namespace}/`))
    .filter(ch => ch.visibility === "public" || ch.owner === login);

  const cards = nsChannels.map((ch) => {
    const short = ch.name.slice(namespace.length + 1);
    const lock = ch.visibility === "private"
      ? ' <span class="lock-badge">🔒 private</span>'
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
  <span class="chan-sep">/</span>
  <span class="chan">${esc(namespace)}</span>
  ${userWidget(login)}
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

// POST /internal/purge-queue/:channel — clear all pending work from both the
// ChannelQueue (upload debouncer) and ChannelIngestQueue (ingest serialiser) DOs.
async function handlePurgeQueue(request: Request, channel: string, env: Env): Promise<Response> {
  const auth = request.headers.get("authorization") ?? "";
  const token = auth.replace(/^Bearer\s+/i, "");
  const claims = await verifyUploadToken(token, env.UPLOAD_TOKEN_SECRET);
  if (!claims) return new Response("unauthorized", { status: 401 });
  // No ownership check — channel may have been deleted; we just need to stop alarms.

  const [queueResp, ingestResp] = await Promise.all([
    env.QUEUE.get(env.QUEUE.idFromName(channel))
      .fetch("http://queue/purge", { method: "POST" })
      .then(r => r.json<{ purged: number }>()),
    env.INGEST_QUEUE.get(env.INGEST_QUEUE.idFromName(channel))
      .fetch("http://queue/purge", { method: "POST" })
      .then(r => r.json<{ purged: number }>()),
  ]);

  return Response.json({
    channel,
    queue_purged: queueResp.purged,
    ingest_queue_purged: ingestResp.purged,
  });
}

// POST /internal/abort-multipart — list and abort all in-progress multipart uploads in R2.
async function handleAbortMultipart(request: Request, env: Env): Promise<Response> {
  const auth = request.headers.get("authorization") ?? "";
  const token = auth.replace(/^Bearer\s+/i, "");
  const claims = await verifyUploadToken(token, env.UPLOAD_TOKEN_SECRET);
  if (!claims) return new Response("unauthorized", { status: 401 });

  const body = await request.json<{ debug?: boolean }>().catch(() => ({}));

  const client = r2Client(env);
  const bucketUrl = `https://${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com/${env.R2_BUCKET_NAME}`;

  // List multipart uploads
  const listReq = await client.sign(
    new Request(`${bucketUrl}?uploads&max-uploads=1000`, { method: "GET" })
  );
  const listResp = await fetch(listReq);
  const listXml = await listResp.text();

  if (body.debug) {
    return new Response(listXml, { headers: { "content-type": "text/xml" } });
  }

  // Parse upload IDs and keys from XML — R2 emits <UploadId> before <Key>
  const uploads: Array<{ key: string; uploadId: string }> = [];
  const uploadBlocks = listXml.matchAll(/<Upload>([\s\S]*?)<\/Upload>/g);
  for (const block of uploadBlocks) {
    const keyMatch = block[1].match(/<Key>([^<]+)<\/Key>/);
    const idMatch  = block[1].match(/<UploadId>([^<]+)<\/UploadId>/);
    if (keyMatch && idMatch) {
      uploads.push({ key: keyMatch[1], uploadId: idMatch[1] });
    }
  }

  // Abort each one
  let aborted = 0, errors = 0;
  for (const { key, uploadId } of uploads) {
    try {
      const abortReq = await client.sign(
        new Request(
          `${bucketUrl}/${encodeURIComponent(key)}?uploadId=${encodeURIComponent(uploadId)}`,
          { method: "DELETE" }
        )
      );
      const abortResp = await fetch(abortReq);
      if (abortResp.ok || abortResp.status === 204) {
        aborted++;
      } else {
        errors++;
      }
    } catch {
      errors++;
    }
  }

  return Response.json({ found: uploads.length, aborted, errors });
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
  const login = await resolveLogin(request, env.UPLOAD_TOKEN_SECRET);
  const q = url.searchParams.get("q") ?? "";
  const sort = url.searchParams.get("sort") ?? "name-asc";
  const page = parseInt(url.searchParams.get("page") ?? "1", 10) || 1;
  const records = await loadBrowseIndex(channel, env, q, sort);
  const results = renderResults(channel, records, q, sort, page);

  const chanRow = await env.DB.prepare(
    `SELECT owner FROM channels WHERE name = ?`
  ).bind(channel).first<{ owner: string | null }>();
  const isOwner = !!login && login === chanRow?.owner;

  const ns = channelNamespace(channel);
  const channelHeader = ns
    ? `<a class="chan-ns" href="/channels/${esc(ns)}">${esc(ns)}</a><span class="chan-sep">/</span><span class="chan">${esc(channel.slice(ns.length + 1))}</span>`
    : `<span class="chan">${esc(channel)}</span>`;

  const adminLink = isOwner
    ? `<a class="admin-btn" href="/channels/${esc(channel)}/admin">&#9881; Settings</a>`
    : "";

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="description" content="Browse packages in the ${esc(channel)} conda channel. Search, filter, and install packages.">
<title>${esc(channel)} &middot; packages</title>
<script src="https://unpkg.com/htmx.org@1.9.12" defer></script>
<style>${BROWSE_CSS}
  .header-user a.admin-btn { color: #52606d; text-decoration: none; font-size: 13px; }
  .header-user a.admin-btn:hover { text-decoration: underline; }
</style>
</head>
<body>
<header>
  <a class="brand" href="/channels">conda-channel-server</a>
  ${channelHeader}
  <div class="header-user">
    ${adminLink}
    ${login
      ? `<span>👤 ${esc(login)}</span><a class="logout-btn" href="/auth/logout">Log out</a>`
      : `<a class="login-btn" href="/auth/login">Log in with GitHub</a>`}
  </div>
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

interface BuildEntry {
  subdir: string;
  filename: string;
  version: string;
  build: string;
  build_number: number;
  timestamp?: number;
  size?: number;
  md5?: string;
  sha256?: string;
  depends?: string[];
}

// Read all builds for a package name from the per-name shards stored in R2.
// One shard per subdir — fetched in parallel via _shardptr/<name> pointers.
// Read all builds for a package name from repodata.json files (one per subdir).
// Workers can't decode zstd natively, so we use repodata.json rather than the
// msgpack+zstd shards; both contain the same per-file metadata.
// Returns full metadata including deps, size, checksums, timestamp.
async function loadBuildsFromRepodata(channel: string, name: string, subdirs: string[], env: Env): Promise<BuildEntry[]> {
  const results = await Promise.all(subdirs.map(async (subdir) => {
    const obj = await env.CHANNEL_BUCKET.get(`${channel}/${subdir}/repodata.json`);
    if (!obj) return [];
    const rd = await obj.json<{ packages?: Record<string, any>; "packages.conda"?: Record<string, any> }>();
    const entries: BuildEntry[] = [];
    for (const [fn, meta] of Object.entries({ ...(rd.packages ?? {}), ...(rd["packages.conda"] ?? {}) })) {
      if (meta.name === name) {
        entries.push({
          subdir,
          filename: fn,
          version: meta.version ?? "",
          build: meta.build ?? "",
          build_number: meta.build_number ?? 0,
          timestamp: meta.timestamp,
          size: meta.size,
          md5: meta.md5,
          sha256: meta.sha256,
          depends: meta.depends ?? [],
        });
      }
    }
    return entries;
  }));
  return results.flat();
}

function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(1)} MB`;
}

function fmtDate(ts?: number): string {
  if (!ts) return "";
  // conda timestamps are in milliseconds
  return new Date(ts).toISOString().slice(0, 10);
}

async function handleBrowsePackage(request: Request, channel: string, name: string, env: Env): Promise<Response> {
  const denied = await browseAuth(request, channel, env);
  if (denied) return denied;
  const login = await resolveLogin(request, env.UPLOAD_TOKEN_SECRET);
  name = decodeURIComponent(name);

  // D1 lookup for summary, license, home, latest version, subdirs.
  const row = await env.DB.prepare(
    `SELECT name, version, summary, license, home, subdirs FROM packages WHERE channel = ? AND name = ?`
  ).bind(channel, name).first<{
    name: string; version: string; summary: string;
    license: string; home: string; subdirs: string;
  }>();
  if (!row) return new Response("package not found", { status: 404 });
  const rec: BrowseRecord = { ...row, subdirs: JSON.parse(row.subdirs ?? "[]") as string[] };

  // Load all builds from repodata.json (has full metadata: deps, size, sha256, etc.)
  const builds = await loadBuildsFromRepodata(channel, name, rec.subdirs ?? [], env);

  // Sort: version descending, then subdir, then filename.
  builds.sort((a, b) =>
    b.version.localeCompare(a.version, undefined, { numeric: true }) ||
    a.subdir.localeCompare(b.subdir) ||
    a.filename.localeCompare(b.filename)
  );

  // Group by version for the version history section.
  const byVersion = new Map<string, BuildEntry[]>();
  for (const b of builds) {
    if (!byVersion.has(b.version)) byVersion.set(b.version, []);
    byVersion.get(b.version)!.push(b);
  }

  // Latest version for the install snippet and dependency list.
  const latestVersion = builds[0]?.version ?? rec.version;
  const latestBuilds = byVersion.get(latestVersion) ?? [];
  // Collect unique deps from all builds of the latest version.
  const latestDeps = [...new Set(latestBuilds.flatMap((b) => b.depends ?? []))].sort();

  const origin = new URL(request.url).origin;
  const repoUrl = `${origin}/repo/${channel}`;
  const installCmd = `conda install -c ${repoUrl} ${name}`;

  // Render one file row inside a version group.
  const fileRow = (b: BuildEntry) => `
    <tr>
      <td><a class="dl-link" href="/repo/${channel}/${b.subdir}/${encodeURIComponent(b.filename)}" title="Download">${esc(b.filename)}</a></td>
      <td><span class="badge">${esc(b.subdir)}</span></td>
      <td>${esc(b.build)}</td>
      <td class="num">${b.size != null ? fmtBytes(b.size) : ""}</td>
      <td class="num mono" title="${b.sha256 ? `SHA256: ${b.sha256}` : ""}">${b.md5 ? b.md5.slice(0, 8) + "…" : ""}</td>
      <td class="num">${fmtDate(b.timestamp)}</td>
    </tr>`;

  // Render version group (collapsible <details>).
  const versionGroups = [...byVersion.entries()].map(([ver, vbuilds], i) => `
  <details${i === 0 ? " open" : ""}>
    <summary class="ver-summary">
      <span class="ver-num">${esc(ver)}</span>
      <span class="ver-count">${vbuilds.length} file${vbuilds.length === 1 ? "" : "s"}</span>
      <span class="ver-subdirs">${[...new Set(vbuilds.map((b) => b.subdir))].map((s) => `<span class="badge">${esc(s)}</span>`).join(" ")}</span>
    </summary>
    <table class="files-table">
      <thead><tr><th>Filename</th><th>Subdir</th><th>Build</th><th>Size</th><th>MD5</th><th>Date</th></tr></thead>
      <tbody>${vbuilds.map(fileRow).join("")}</tbody>
    </table>
  </details>`).join("");

  const depsSection = latestDeps.length ? `
  <section class="detail-section">
    <h2>Dependencies <span class="ver-count">(${esc(latestVersion)})</span></h2>
    <ul class="deps-list">
      ${latestDeps.map((d) => `<li><code>${esc(d)}</code></li>`).join("")}
    </ul>
  </section>` : "";

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="description" content="${esc(rec.summary || `${name} package in the ${channel} conda channel`)}">
<title>${esc(name)} &middot; ${esc(channel)}</title>
<style>${BROWSE_CSS}${PKG_DETAIL_CSS}</style>
</head>
<body>
<header>
  <a class="brand" href="/channels">conda-channel-server</a>
  <span class="chan-sep">/</span>
  <a class="chan-ns" href="/channels/${channel.split("/")[0]}">${esc(channel.split("/")[0])}</a>
  <span class="chan-sep">/</span>
  <a class="chan" href="/channels/${channel}">${esc(channel.split("/").slice(1).join("/"))}</a>
  <span class="chan-sep">/</span>
  <span class="chan">${esc(name)}</span>
  ${userWidget(login)}
</header>
<main>
<div class="wrap">

  <div class="pkg-hero">
    <div class="pkg-title">
      <h1>${esc(name)}</h1>
      <span class="ver-badge">${esc(latestVersion)}</span>
    </div>
    ${rec.summary ? `<p class="pkg-summary">${esc(rec.summary)}</p>` : ""}
    <div class="pkg-attrs">
      ${rec.license ? `<span class="attr"><span class="attr-label">License</span>${esc(rec.license)}</span>` : ""}
      ${rec.home ? `<span class="attr"><span class="attr-label">Home</span><a href="${esc(rec.home)}" target="_blank" rel="noopener">${esc(rec.home)}</a></span>` : ""}
      <span class="attr"><span class="attr-label">Subdirs</span>${(rec.subdirs ?? []).map((s) => `<span class="badge">${esc(s)}</span>`).join(" ")}</span>
    </div>
  </div>

  <section class="detail-section">
    <h2>Install</h2>
    <div class="install-block">
      <code id="install-cmd">${esc(installCmd)}</code>
      <button class="copy-btn" onclick="navigator.clipboard.writeText(document.getElementById('install-cmd').textContent).then(()=>{this.textContent='Copied!';setTimeout(()=>this.textContent='Copy',1500)})">Copy</button>
    </div>
  </section>

  ${depsSection}

  <section class="detail-section">
    <h2>Files <span class="ver-count">${builds.length} total across ${byVersion.size} version${byVersion.size === 1 ? "" : "s"}</span></h2>
    ${versionGroups || `<div class="empty">No files.</div>`}
  </section>

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

  // Enforce token scope if present.
  const scopeErr = checkTokenScope(claims, channel, filename);
  if (scopeErr) return new Response(scopeErr, { status: 403 });

  // Reject normal tokens if channel requires OIDC trusted publishing.
  if (!claims.channel) {  // no channel scope means it's a broad login token, not an OIDC-minted one
    const trusted = await isTrustedPublishingRequired(channel, env);
    if (trusted) return new Response(
      `channel '${channel}' requires trusted publishing — use POST /upload/exchange-oidc to get an upload token`,
      { status: 403 }
    );
  }

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

  // Enforce token scope if present.
  const scopeErr = checkTokenScope(claims, channel, filename);
  if (scopeErr) return new Response(scopeErr, { status: 403 });

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
// Token scope helpers
// ---------------------------------------------------------------------------

// Check that a token's optional channel/pkg scope permits uploading `filename`
// to `channel`. Returns an error string, or null if permitted.
function checkTokenScope(
  claims: { login: string; channel?: string; pkg?: string },
  channel: string,
  filename: string,
): string | null {
  if (claims.channel && claims.channel !== channel) {
    return `token is scoped to channel '${claims.channel}', cannot upload to '${channel}'`;
  }
  if (claims.pkg) {
    // filename format: <name>-<version>-<build>.<ext>
    // Extract package name = everything before the first '-' that is followed by a version digit.
    const pkgName = filename.replace(/[-_][\d].*$/, "");
    if (pkgName !== claims.pkg) {
      return `token is scoped to package '${claims.pkg}', cannot upload '${filename}'`;
    }
  }
  return null;
}

// Returns true if every trusted_publisher rule for the channel has require_trusted=1,
// meaning normal login tokens are not accepted.
async function isTrustedPublishingRequired(channel: string, env: Env): Promise<boolean> {
  const row = await env.DB.prepare(
    `SELECT COUNT(*) as total, SUM(require_trusted) as req FROM trusted_publishers WHERE channel = ?`
  ).bind(channel).first<{ total: number; req: number | null }>();
  if (!row || row.total === 0) return false;
  return (row.req ?? 0) >= row.total; // all rules require trusted = channel requires trusted
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
async function signUploadToken(payload: { login: string; channel?: string; pkg?: string }, secret: string, ttl = 3600): Promise<string> {
  const body = { ...payload, exp: Math.floor(Date.now() / 1000) + ttl };
  const encoded = b64url(JSON.stringify(body));
  const sig = await hmac(encoded, secret);
  return `${encoded}.${sig}`;
}

async function verifyUploadToken(token: string, secret: string): Promise<{ login: string; channel?: string; pkg?: string } | null> {
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

// ---------------------------------------------------------------------------
// Browser OAuth (GitHub web application flow)
//
// GET /auth/login   — redirect to GitHub with a state CSRF token
// GET /auth/callback — exchange code for access_token, verify org membership,
//                      set a signed HttpOnly session cookie, redirect to /channels
// GET /auth/logout  — clear the session cookie, redirect to /channels
//
// The session cookie is the same HMAC-signed token format as upload tokens
// (base64url(JSON({login,exp})).sig) so verifyUploadToken reuses for both.
// Cookie name: __session   Lifetime: 8 hours   Flags: HttpOnly; Secure; SameSite=Lax
// ---------------------------------------------------------------------------

const SESSION_COOKIE = "__session";
const SESSION_TTL    = 8 * 3600; // seconds

function getSessionToken(request: Request): string | null {
  const cookie = request.headers.get("cookie") ?? "";
  for (const part of cookie.split(";")) {
    const [k, ...v] = part.trim().split("=");
    if (k.trim() === SESSION_COOKIE) return decodeURIComponent(v.join("="));
  }
  return null;
}

async function getSessionLogin(request: Request, secret: string): Promise<string | null> {
  const token = getSessionToken(request);
  if (!token) return null;
  const claims = await verifyUploadToken(token, secret);
  return claims?.login ?? null;
}

function sessionCookieHeader(token: string, maxAge: number): string {
  return `${SESSION_COOKIE}=${encodeURIComponent(token)}; Path=/; Max-Age=${maxAge}; HttpOnly; Secure; SameSite=Lax`;
}

async function handleBrowserLoginStart(request: Request, env: Env): Promise<Response> {
  // Generate a CSRF state token — HMAC of a random nonce so we can verify it
  // on callback without storing server-side state.
  const nonce = b64url(String.fromCharCode(...crypto.getRandomValues(new Uint8Array(16))));
  const state = `${nonce}.${await hmac(nonce, env.UPLOAD_TOKEN_SECRET)}`;

  const redirectUri = new URL("/auth/callback", new URL(request.url).origin).toString();
  const params = new URLSearchParams({
    client_id: env.GITHUB_CLIENT_ID,
    redirect_uri: redirectUri,
    scope: "read:org",
    state,
  });
  const githubUrl = `https://github.com/login/oauth/authorize?${params}`;

  // Stash state in a short-lived cookie so callback can verify it.
  return new Response(null, {
    status: 302,
    headers: {
      location: githubUrl,
      "set-cookie": `__oauth_state=${encodeURIComponent(state)}; Path=/auth/callback; Max-Age=600; HttpOnly; Secure; SameSite=Lax`,
    },
  });
}

async function handleBrowserLoginCallback(request: Request, url: URL, env: Env): Promise<Response> {
  const code  = url.searchParams.get("code");
  const state = url.searchParams.get("state");

  const errorPage = (msg: string) => new Response(
    `<!DOCTYPE html><html><head><meta charset="utf-8"><title>Login error</title>` +
    `<style>body{font-family:sans-serif;padding:40px;color:#c0392b}</style></head>` +
    `<body><h2>Login failed</h2><p>${esc(msg)}</p><a href="/channels">Back</a></body></html>`,
    { status: 400, headers: { "content-type": "text/html;charset=utf-8" } }
  );

  if (!code || !state) return errorPage("Missing code or state from GitHub.");

  // Verify CSRF state — check the cookie matches what we sent.
  const cookieHeader = request.headers.get("cookie") ?? "";
  const stateCookie = cookieHeader.split(";").map(p => p.trim())
    .find(p => p.startsWith("__oauth_state="))
    ?.slice("__oauth_state=".length);
  if (!stateCookie || decodeURIComponent(stateCookie) !== state) {
    return errorPage("State mismatch — possible CSRF. Please try logging in again.");
  }
  // Also verify state HMAC
  const [nonce, sig] = state.split(".");
  if (!nonce || !sig || sig !== await hmac(nonce, env.UPLOAD_TOKEN_SECRET)) {
    return errorPage("Invalid state signature.");
  }

  // Exchange code for GitHub access token.
  const origin = new URL(request.url).origin;
  const tokenResp = await fetch("https://github.com/login/oauth/access_token", {
    method: "POST",
    headers: { accept: "application/json", "content-type": "application/json" },
    body: JSON.stringify({
      client_id: env.GITHUB_CLIENT_ID,
      client_secret: env.GITHUB_CLIENT_SECRET,
      code,
      redirect_uri: `${origin}/auth/callback`,
    }),
  });
  const tokenData = await tokenResp.json<{ access_token?: string; error?: string; error_description?: string }>();
  if (!tokenData.access_token) {
    return errorPage(`GitHub error: ${tokenData.error_description ?? tokenData.error ?? "unknown"}`);
  }

  // Fetch GitHub username.
  const ghUser = await fetch("https://api.github.com/user", {
    headers: { authorization: `Bearer ${tokenData.access_token}`, "user-agent": "conda-channel-server" },
  }).then(r => r.json<{ login: string }>());

  // Check org membership (same gate as Device Flow).
  const memberCheck = await fetch(
    `https://api.github.com/orgs/${env.GITHUB_ORG}/members/${ghUser.login}`,
    { headers: { authorization: `Bearer ${tokenData.access_token}`, "user-agent": "conda-channel-server" } }
  );
  if (memberCheck.status !== 204) {
    return errorPage(`${esc(ghUser.login)} is not a member of ${esc(env.GITHUB_ORG)}.`);
  }

  // Mint a session token (same format as upload token, longer TTL).
  const sessionToken = await signUploadToken({ login: ghUser.login }, env.UPLOAD_TOKEN_SECRET, SESSION_TTL);

  // Redirect to channels page, clear state cookie, set session cookie.
  return new Response(null, {
    status: 302,
    headers: new Headers([
      ["location", "/channels"],
      ["set-cookie", sessionCookieHeader(sessionToken, SESSION_TTL)],
      ["set-cookie", `__oauth_state=; Path=/auth/callback; Max-Age=0; HttpOnly; Secure; SameSite=Lax`],
    ]),
  });
}

function handleBrowserLogout(url: URL): Response {
  return new Response(null, {
    status: 302,
    headers: new Headers([
      ["location", "/channels"],
      ["set-cookie", sessionCookieHeader("", 0)],
    ]),
  });
}

// ---------------------------------------------------------------------------
// Trusted Publishers — OIDC exchange + CRUD
// ---------------------------------------------------------------------------

interface TrustedPublisherRow {
  id: number;
  channel: string;
  repository: string | null;
  workflow: string | null;
  environment: string | null;
  package_name: string | null;
  require_trusted: number;
  created_at: number;
  created_by: string;
}

// POST /upload/exchange-oidc
// Body: { oidc_token: string, channel: string }
// Verifies a GitHub Actions OIDC JWT, finds a matching trusted_publisher rule,
// and mints a short-lived scoped upload token.
async function handleOidcExchange(request: Request, env: Env): Promise<Response> {
  let body: { oidc_token?: string; channel?: string };
  try {
    body = await request.json();
  } catch {
    return new Response("invalid JSON body", { status: 400 });
  }
  const { oidc_token, channel } = body;
  if (!oidc_token || !channel) {
    return new Response("missing oidc_token or channel", { status: 400 });
  }
  if (!CHANNEL_NAME_RE.test(channel)) {
    return new Response("invalid channel name", { status: 400 });
  }

  // ── 1. Decode the JWT header + payload (no verify yet) ───────────────────
  const parts = oidc_token.split(".");
  if (parts.length !== 3) return new Response("malformed JWT", { status: 400 });

  let header: { alg?: string; kid?: string };
  let claims: {
    iss?: string;
    aud?: string | string[];
    exp?: number;
    repository?: string;
    workflow_ref?: string;
    environment?: string;
    repository_owner?: string;
  };
  try {
    header = JSON.parse(atob(parts[0].replace(/-/g, "+").replace(/_/g, "/")));
    claims = JSON.parse(atob(parts[1].replace(/-/g, "+").replace(/_/g, "/")));
  } catch {
    return new Response("failed to decode JWT", { status: 400 });
  }

  // ── 2. Validate basic claims before fetching JWKS ────────────────────────
  const OIDC_ISSUER = "https://token.actions.githubusercontent.com";
  if (claims.iss !== OIDC_ISSUER) {
    return new Response(`invalid iss: expected ${OIDC_ISSUER}`, { status: 401 });
  }
  const aud = Array.isArray(claims.aud) ? claims.aud : [claims.aud ?? ""];
  if (!aud.includes("conda-channel-server")) {
    return new Response(`invalid aud: expected 'conda-channel-server'`, { status: 401 });
  }
  if (!claims.exp || claims.exp < Math.floor(Date.now() / 1000)) {
    return new Response("JWT has expired", { status: 401 });
  }
  if (header.alg !== "RS256") {
    return new Response(`unsupported alg: ${header.alg}`, { status: 400 });
  }
  if (!header.kid) {
    return new Response("JWT missing kid", { status: 400 });
  }

  // ── 3. Fetch JWKS and find the matching key ───────────────────────────────
  const jwksResp = await fetch(`${OIDC_ISSUER}/.well-known/jwks`);
  if (!jwksResp.ok) {
    return new Response("failed to fetch JWKS", { status: 502 });
  }
  const jwks = await jwksResp.json<{ keys: Array<{ kid: string; [k: string]: unknown }> }>();
  const jwk = jwks.keys.find((k) => k.kid === header.kid);
  if (!jwk) {
    return new Response(`no JWKS key for kid=${header.kid}`, { status: 401 });
  }

  // ── 4. Verify RS256 signature using Web Crypto ───────────────────────────
  let cryptoKey: CryptoKey;
  try {
    cryptoKey = await crypto.subtle.importKey(
      "jwk",
      jwk as unknown as JsonWebKey,
      { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
      false,
      ["verify"],
    );
  } catch {
    return new Response("failed to import JWK", { status: 500 });
  }

  const sigInput = new TextEncoder().encode(`${parts[0]}.${parts[1]}`);
  const sigBytes = Uint8Array.from(
    atob(parts[2].replace(/-/g, "+").replace(/_/g, "/")),
    (c) => c.charCodeAt(0),
  );
  const valid = await crypto.subtle.verify("RSASSA-PKCS1-v1_5", cryptoKey, sigBytes, sigInput);
  if (!valid) {
    return new Response("JWT signature verification failed", { status: 401 });
  }

  // ── 5. Match against trusted_publisher rules ─────────────────────────────
  const { results: rules } = await env.DB.prepare(
    `SELECT * FROM trusted_publishers WHERE channel = ?`
  ).bind(channel).all<TrustedPublisherRow>();

  if (rules.length === 0) {
    return new Response(`no trusted publisher rules configured for channel '${channel}'`, { status: 403 });
  }

  const matchedRule = rules.find((rule) => matchesRule(rule, claims));
  if (!matchedRule) {
    return new Response("no matching trusted publisher rule for this workflow", { status: 403 });
  }

  // ── 6. Mint a scoped short-lived upload token ─────────────────────────────
  const tokenPayload: { login: string; channel: string; pkg?: string } = {
    login: claims.repository_owner ?? channel.split("/")[0],
    channel,
  };
  if (matchedRule.package_name) {
    tokenPayload.pkg = matchedRule.package_name;
  }

  const TTL = 900; // 15 minutes
  const upload_token = await signUploadToken(tokenPayload, env.UPLOAD_TOKEN_SECRET, TTL);
  return Response.json({ upload_token, expires_in: TTL });
}

// Returns true if all non-null fields of a rule match the JWT claims.
function matchesRule(
  rule: TrustedPublisherRow,
  claims: { repository?: string; workflow_ref?: string; environment?: string },
): boolean {
  if (rule.repository !== null && rule.repository !== claims.repository) return false;
  if (rule.workflow !== null) {
    // Prefix match: the rule's workflow is a prefix of workflow_ref
    if (!claims.workflow_ref?.startsWith(rule.workflow)) return false;
  }
  if (rule.environment !== null && rule.environment !== claims.environment) return false;
  return true;
}

// GET /channel/:channel/trusted-publishers — list rules (owner only)
async function handleListTrustedPublishers(request: Request, channel: string, env: Env): Promise<Response> {
  if (!CHANNEL_NAME_RE.test(channel)) return new Response("invalid channel name", { status: 400 });

  const login = await resolveLogin(request, env.UPLOAD_TOKEN_SECRET);
  if (!login) return new Response("unauthorized", { status: 401 });

  const denied = await requireChannelOwner(channel, login, env);
  if (denied) return denied;

  const { results } = await env.DB.prepare(
    `SELECT id, channel, repository, workflow, environment, package_name, require_trusted, created_at, created_by
     FROM trusted_publishers WHERE channel = ? ORDER BY id`
  ).bind(channel).all<TrustedPublisherRow>();

  return Response.json(results);
}

// POST /channel/:channel/trusted-publishers — add a rule (owner only)
async function handleAddTrustedPublisher(request: Request, channel: string, env: Env): Promise<Response> {
  if (!CHANNEL_NAME_RE.test(channel)) return new Response("invalid channel name", { status: 400 });

  const login = await resolveLogin(request, env.UPLOAD_TOKEN_SECRET);
  if (!login) return new Response("unauthorized", { status: 401 });

  const denied = await requireChannelOwner(channel, login, env);
  if (denied) return denied;

  // Accept both JSON (API clients) and form submissions (admin UI).
  const ct = request.headers.get("content-type") ?? "";
  let repository: string | null, workflow: string | null,
      environment: string | null, package_name: string | null,
      require_trusted: number;

  if (ct.includes("application/x-www-form-urlencoded") || ct.includes("multipart/form-data")) {
    const fd = await request.formData();
    const noe = (v: File | string | null) => (v && v.toString().trim()) ? v.toString().trim() : null;
    repository     = noe(fd.get("repository"));
    workflow       = noe(fd.get("workflow"));
    environment    = noe(fd.get("environment"));
    package_name   = noe(fd.get("package_name"));
    require_trusted = fd.get("require_trusted") ? 1 : 0;
  } else {
    let body: {
      repository?: string | null; workflow?: string | null;
      environment?: string | null; package_name?: string | null;
      require_trusted?: boolean | number;
    };
    try { body = await request.json(); }
    catch { return new Response("invalid JSON body", { status: 400 }); }
    const noe = (v: string | null | undefined) => (v && v.trim()) ? v.trim() : null;
    repository     = noe(body.repository);
    workflow       = noe(body.workflow);
    environment    = noe(body.environment);
    package_name   = noe(body.package_name);
    require_trusted = body.require_trusted ? 1 : 0;
  }

  const result = await env.DB.prepare(
    `INSERT INTO trusted_publishers (channel, repository, workflow, environment, package_name, require_trusted, created_at, created_by)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
  ).bind(
    channel,
    repository,
    workflow,
    environment,
    package_name,
    require_trusted,
    Date.now(),
    login,
  ).run();

  const id = result.meta.last_row_id;
  const row = await env.DB.prepare(
    `SELECT * FROM trusted_publishers WHERE id = ?`
  ).bind(id).first<TrustedPublisherRow>();

  // Form submissions expect a redirect; API clients get JSON.
  const ct2 = request.headers.get("content-type") ?? "";
  if (ct2.includes("application/x-www-form-urlencoded") || ct2.includes("multipart/form-data")) {
    return new Response(null, { status: 302, headers: { location: `/channels/${channel}/admin` } });
  }
  return Response.json(row, { status: 201 });
}

// DELETE /channel/:channel/trusted-publishers/:id — remove a rule (owner only)
async function handleDeleteTrustedPublisher(request: Request, channel: string, id: number, env: Env): Promise<Response> {
  if (!CHANNEL_NAME_RE.test(channel)) return new Response("invalid channel name", { status: 400 });

  const login = await resolveLogin(request, env.UPLOAD_TOKEN_SECRET);
  if (!login) return new Response("unauthorized", { status: 401 });

  const denied = await requireChannelOwner(channel, login, env);
  if (denied) return denied;

  const result = await env.DB.prepare(
    `DELETE FROM trusted_publishers WHERE id = ? AND channel = ?`
  ).bind(id, channel).run();

  if (result.meta.changes === 0) {
    return new Response("rule not found", { status: 404 });
  }
  return new Response(null, { status: 204 });
}

// GET /channels/:channel/admin — HTML admin page (owner only, session cookie)
async function handleAdminPage(request: Request, channel: string, env: Env): Promise<Response> {
  if (!CHANNEL_NAME_RE.test(channel)) return new Response("not found", { status: 404 });

  const login = await resolveLogin(request, env.UPLOAD_TOKEN_SECRET);
  if (!login) {
    // Not logged in — redirect to login page.
    const loginUrl = `/auth/login`;
    return new Response(null, { status: 302, headers: { location: loginUrl } });
  }

  const denied = await requireChannelOwner(channel, login, env);
  if (denied) return denied;

  const { results: rules } = await env.DB.prepare(
    `SELECT id, repository, workflow, environment, package_name, require_trusted, created_at, created_by
     FROM trusted_publishers WHERE channel = ? ORDER BY id`
  ).bind(channel).all<Omit<TrustedPublisherRow, "channel">>();

  const chanInfo = await env.DB.prepare(
    `SELECT visibility FROM channels WHERE name = ?`
  ).bind(channel).first<{ visibility: string }>();

  const ns = channelNamespace(channel);
  const channelHeader = ns
    ? `<a class="chan-ns" href="/channels/${esc(ns)}">${esc(ns)}</a><span class="chan-sep">/</span><a class="chan" href="/channels/${esc(channel)}">${esc(channel.slice(ns.length + 1))}</a>`
    : `<a class="chan" href="/channels/${esc(channel)}">${esc(channel)}</a>`;

  const rulesRows = rules.map((r) => {
    const wild = `<span class="wildcard">any</span>`;
    const code = (v: string | null) => v ? `<code class="val">${esc(v)}</code>` : wild;
    return `
    <tr>
      <td class="col-id">${r.id}</td>
      <td>${code(r.repository)}</td>
      <td>${code(r.workflow)}</td>
      <td>${code(r.environment)}</td>
      <td>${code(r.package_name)}</td>
      <td class="col-bool">${r.require_trusted ? '<span class="yes-badge">Yes</span>' : '<span class="no-badge">No</span>'}</td>
      <td>${esc(r.created_by)}</td>
      <td class="col-action">
        <form method="POST" action="/channel/${esc(channel)}/trusted-publishers/${r.id}" style="display:inline">
          <input type="hidden" name="_method" value="DELETE">
          <button type="submit" class="btn-danger" onclick="return confirm('Delete this rule?')">Delete</button>
        </form>
      </td>
    </tr>`;
  }).join("");

  const visibility = chanInfo?.visibility ?? "public";

  const ADMIN_CSS = `
    .admin-section { background:#fff; border:1px solid #e4e7eb; border-radius:8px; padding:20px 24px; margin-bottom:24px; }
    .admin-section h2 { font-size:16px; font-weight:700; margin:0 0 14px; color:#1f2933; }
    table.tp-table { width:100%; border-collapse:collapse; font-size:13px; table-layout:fixed; }
    .tp-table th { text-align:left; padding:8px 10px; background:#f5f7fa; color:#52606d; font-weight:600; border-bottom:1px solid #e4e7eb; white-space:nowrap; }
    .tp-table td { padding:8px 10px; border-bottom:1px solid #f0f2f5; vertical-align:middle; word-break:break-all; }
    .tp-table tr:last-child td { border-bottom:none; }
    .tp-table tr:hover td { background:#fafbfc; }
    .tp-table .col-id { width:36px; text-align:center; color:#9aacb8; word-break:normal; }
    .tp-table .col-bool { width:68px; text-align:center; word-break:normal; }
    .tp-table .col-action { width:72px; text-align:right; word-break:normal; }
    code.val { background:#f0f2f5; padding:2px 5px; border-radius:4px; font-size:12px; word-break:break-all; }
    .wildcard { color:#9aacb8; font-style:italic; font-size:12px; }
    .yes-badge { background:#fdecea; color:#b42318; border-radius:4px; padding:2px 7px; font-size:12px; font-weight:600; }
    .no-badge  { background:#f0f2f5; color:#52606d; border-radius:4px; padding:2px 7px; font-size:12px; }
    .form-grid { display:grid; grid-template-columns:1fr 1fr; gap:12px; margin-top:4px; }
    .form-grid label { display:flex; flex-direction:column; gap:4px; font-size:13px; font-weight:600; color:#52606d; }
    .form-grid input[type=text] { padding:8px 10px; border:1px solid #cbd2d9; border-radius:6px; font-size:13px; }
    .form-grid input[type=text]::placeholder { color:#9aacb8; font-weight:400; }
    .form-row { display:flex; align-items:center; gap:8px; font-size:13px; margin-top:4px; }
    .form-row label { font-weight:600; color:#52606d; }
    .btn { padding:8px 18px; border:none; border-radius:6px; font-size:13px; font-weight:600; cursor:pointer; }
    .btn-primary { background:#2d7a1f; color:#fff; margin-top:14px; }
    .btn-primary:hover { background:#246018; }
    .btn-danger { background:#fff; color:#b42318; border:1px solid #f5c2bf; padding:5px 12px; border-radius:6px; font-size:12px; cursor:pointer; white-space:nowrap; }
    .btn-danger:hover { background:#fdecea; }
    .vis-form { display:flex; align-items:center; gap:10px; }
    .vis-form select { padding:8px 12px; border:1px solid #cbd2d9; border-radius:6px; font-size:13px; background:#fff; }
    .empty-rules { color:#52606d; font-size:13px; font-style:italic; }
    .hint { font-size:12px; color:#9aacb8; font-weight:400; margin-left:4px; }
  `;

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Admin &middot; ${esc(channel)}</title>
<style>${BROWSE_CSS}${ADMIN_CSS}</style>
</head>
<body>
<header>
  <a class="brand" href="/channels">conda-channel-server</a>
  <span class="chan-sep">/</span>
  ${channelHeader}
  <span class="chan-sep">/</span>
  <span class="chan">admin</span>
  ${userWidget(login)}
</header>
<main>
<div class="wrap">

  <!-- Trusted Publishers -->
  <div class="admin-section">
    <h2>Trusted Publishers <span class="hint">(GitHub Actions OIDC keyless upload)</span></h2>
    ${rules.length > 0 ? `
    <table class="tp-table">
      <thead>
        <tr>
          <th>ID</th><th>Repository</th><th>Workflow ref prefix</th><th>Environment</th><th>Package</th><th>OIDC only?</th><th>Added by</th><th></th>
        </tr>
      </thead>
      <tbody>${rulesRows}</tbody>
    </table>` : `<p class="empty-rules">No trusted publisher rules yet.</p>`}

    <details style="margin-top:16px">
      <summary style="cursor:pointer;font-weight:600;font-size:13px;color:#2d7a1f">+ Add rule</summary>
      <form method="POST" action="/channel/${esc(channel)}/trusted-publishers" style="margin-top:12px">
        <div class="form-grid">
          <label>Repository <span class="hint">(e.g. owner/repo, blank = any)</span>
            <input type="text" name="repository" placeholder="owner/repo">
          </label>
          <label>Workflow ref prefix <span class="hint">(e.g. refs/heads/main, blank = any)</span>
            <input type="text" name="workflow" placeholder=".github/workflows/publish.yml@refs/heads/main">
          </label>
          <label>Environment <span class="hint">(e.g. production, blank = any)</span>
            <input type="text" name="environment" placeholder="production">
          </label>
          <label>Package scope <span class="hint">(leave blank to allow all packages)</span>
            <input type="text" name="package_name" placeholder="my-package">
          </label>
        </div>
        <div class="form-row" style="margin-top:10px">
          <input type="checkbox" id="require_trusted" name="require_trusted" value="1">
          <label for="require_trusted">Require OIDC — reject normal upload tokens for this channel</label>
        </div>
        <button type="submit" class="btn btn-primary">Add rule</button>
      </form>
    </details>
  </div>

  <!-- Visibility -->
  <div class="admin-section">
    <h2>Channel Visibility</h2>
    <p style="font-size:13px;color:#3d4f5c;margin:0 0 12px">
      Current: <strong>${esc(visibility)}</strong>
    </p>
    <form method="POST" action="/channel/${esc(channel)}/visibility" class="vis-form">
      <select name="visibility">
        <option value="public"${visibility === "public" ? " selected" : ""}>Public</option>
        <option value="private"${visibility === "private" ? " selected" : ""}>Private</option>
      </select>
      <button type="submit" class="btn btn-primary" style="margin-top:0">Save</button>
    </form>
    <p style="font-size:12px;color:#9aacb8;margin:8px 0 0">
      Private channels are only visible to you. Visibility changes take effect immediately.
    </p>
  </div>

</div>
</main>
<script>
// Handle DELETE via form _method override (browsers can't DELETE from forms)
document.querySelectorAll('form[method="POST"]').forEach(form => {
  const methodInput = form.querySelector('input[name="_method"]');
  if (!methodInput) return;
  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const method = methodInput.value;
    const url = form.action;
    try {
      const resp = await fetch(url, { method, credentials: 'same-origin' });
      if (resp.ok || resp.status === 204) {
        window.location.reload();
      } else {
        const text = await resp.text();
        alert('Error: ' + text);
      }
    } catch (err) {
      alert('Request failed: ' + err);
    }
  });
});
// Handle add-rule and visibility forms (POST — just submit but use fetch to handle JSON vs redirect)
</script>
</body>
</html>`;
  return new Response(html, { headers: { "content-type": "text/html;charset=utf-8" } });
}

// ---------------------------------------------------------------------------
// Helper: require that `login` is the channel owner; returns a 403 Response
// or null on success.
// ---------------------------------------------------------------------------
async function requireChannelOwner(channel: string, login: string, env: Env): Promise<Response | null> {
  const row = await env.DB.prepare(
    `SELECT owner FROM channels WHERE name = ?`
  ).bind(channel).first<{ owner: string | null }>();
  if (!row) return new Response("channel not found", { status: 404 });
  if (row.owner !== login) {
    return new Response(`only the channel owner can manage trusted publishers`, { status: 403 });
  }
  return null;
}
