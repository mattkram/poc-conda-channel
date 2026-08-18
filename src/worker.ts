import { Container, getContainer } from "@cloudflare/containers";
import { DurableObject, env } from "cloudflare:workers";
import { AwsClient } from "aws4fetch";

export interface Env {
  CHANNEL_BUCKET: R2Bucket;
  INDEXER: DurableObjectNamespace;
  QUEUE: DurableObjectNamespace;
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

    const entries = [...pending.entries()]; // already in upload order via key prefix
    const channel = entries[0][1].channel;
    const files = entries.map(([, upload]) => ({
      filename: upload.filename,
      uploadedAt: upload.uploadedAt,
      uploadedBy: upload.uploadedBy,
    }));

    const container = getContainer(this.env.INDEXER, channel);
    const resp = await container.fetch("http://container/ingest-batch", {
      method: "POST",
      body: JSON.stringify({ channel, files }),
      headers: { "content-type": "application/json" },
    });

    if (!resp.ok) {
      // Don't clear anything — leave every entry queued and let the alarm's
      // own exponential-backoff retry (built into Durable Objects) try again.
      throw new Error(`ingest-batch failed for ${channel}: ${await resp.text()}`);
    }

    // Container reports per-file outcome so a partial failure only retries
    // the files that actually failed, not ones already safely ingested.
    const { results } = await resp.json<{ results: Record<string, "ok" | string> }>();
    const stillFailing: [string, PendingUpload][] = [];
    for (const [key, upload] of entries) {
      if (results[upload.filename] === "ok") {
        await this.ctx.storage.delete(key);
      } else {
        stillFailing.push([key, upload]);
      }
    }

    if (stillFailing.length > 0) {
      throw new Error(
        `${stillFailing.length} file(s) still failing in ${channel}: ` +
          stillFailing.map(([, u]) => u.filename).join(", ")
      );
    }

    // More uploads may have arrived while this batch was in flight — if so,
    // an alarm was never scheduled for them (one was already set), so make
    // sure they get picked up.
    const remaining = await this.ctx.storage.list({ prefix: "pending:" });
    if (remaining.size > 0) {
      await this.ctx.storage.setAlarm(Date.now() + DEBOUNCE_MS);
    }
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

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

    // GET /<channel>/<subdir>/<path> — read path for conda clients.
    // Public channels: no auth. Private channels: Bearer token required.
    const readMatch = url.pathname.match(/^\/([^/]+)\/([^/]+)\/.+$/);
    if (readMatch && request.method === "GET") {
      return handleR2Get(request, readMatch[1], url.pathname.slice(1), env);
    }

    // GET /<channel>/<subdir>  or  GET /<channel>/<subdir>/  — subdir index
    const subdirMatch = url.pathname.match(/^\/([^/]+)\/([^/]+)\/?$/);
    if (subdirMatch && request.method === "GET") {
      return handleR2Get(request, subdirMatch[1], `${subdirMatch[1]}/${subdirMatch[2]}/index.html`, env);
    }

    // GET /<channel>  or  GET /<channel>/  — channel root listing
    const channelRootMatch = url.pathname.match(/^\/([^/]+)\/?$/);
    if (channelRootMatch && request.method === "GET"
        && !url.pathname.startsWith("/channel/")
        && !url.pathname.startsWith("/auth/")
        && !url.pathname.startsWith("/upload/")) {
      return handleChannelRoot(request, channelRootMatch[1], env);
    }

    // DELETE /channel/<channel>/<subdir>/<filename> — remove one package + reindex
    const pkgMatch = url.pathname.match(/^\/channel\/([^/]+)\/([^/]+)\/([^/]+)$/);
    if (pkgMatch && request.method === "DELETE") {
      return handleDeletePackage(request, pkgMatch[1], pkgMatch[2], pkgMatch[3], env);
    }

    // GET  /channel/<channel>         — return owner + visibility
    // POST /channel/<channel>/visibility — set public/private (owner only)
    // DELETE /channel/<channel>       — wipe entire channel
    const chanMatch = url.pathname.match(/^\/channel\/([^/]+)$/);
    if (chanMatch && request.method === "GET") {
      return handleGetChannelInfo(chanMatch[1], env);
    }
    if (chanMatch && request.method === "DELETE") {
      return handleDeleteChannel(request, chanMatch[1], env);
    }
    const visMatch = url.pathname.match(/^\/channel\/([^/]+)\/visibility$/);
    if (visMatch && request.method === "POST") {
      return handleSetVisibility(request, visMatch[1], env);
    }

    return new Response("not found", { status: 404 });
  },
};

// ---------------------------------------------------------------------------
// Channel metadata — owner + visibility, backed by ChannelQueue DO storage.
// ---------------------------------------------------------------------------

// Claim-or-verify write access. Returns a 403 Response on denial, null on ok.
async function checkChannelAccess(channel: string, login: string, env: Env): Promise<Response | null> {
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
  return null;
}

// Check read access for a channel. Returns 401/403 Response on denial, null on ok.
// login may be null for unauthenticated requests to public channels.
async function checkReadAccess(channel: string, login: string | null, env: Env): Promise<Response | null> {
  const queueId = env.QUEUE.idFromName(channel);
  const queue = env.QUEUE.get(queueId);
  const resp = await queue.fetch("http://queue/check-read", {
    method: "POST",
    body: JSON.stringify({ login }),
    headers: { "content-type": "application/json" },
  });
  if (resp.status === 403) {
    return new Response(
      `channel '${channel}' is private — provide a valid Bearer token`,
      { status: 401, headers: { "WWW-Authenticate": 'Bearer realm="conda-channel"' } }
    );
  }
  return null;
}

// GET /channel/<channel> — returns { owner, visibility }
async function handleGetChannelInfo(channel: string, env: Env): Promise<Response> {
  if (!CHANNEL_NAME_RE.test(channel)) return new Response("invalid channel name", { status: 400 });
  const queueId = env.QUEUE.idFromName(channel);
  const queue = env.QUEUE.get(queueId);
  return queue.fetch("http://queue/owner");
}

// POST /channel/<channel>/visibility — set public or private (owner only)
async function handleSetVisibility(request: Request, channel: string, env: Env): Promise<Response> {
  if (!CHANNEL_NAME_RE.test(channel)) return new Response("invalid channel name", { status: 400 });

  const auth = request.headers.get("authorization") ?? "";
  const token = auth.replace(/^Bearer\s+/i, "");
  const claims = await verifyUploadToken(token, env.UPLOAD_TOKEN_SECRET);
  if (!claims) return new Response("unauthorized", { status: 401 });

  const body = await request.json<{ visibility: string }>();
  const queueId = env.QUEUE.idFromName(channel);
  const queue = env.QUEUE.get(queueId);
  const resp = await queue.fetch("http://queue/set-visibility", {
    method: "POST",
    body: JSON.stringify({ login: claims.login, visibility: body.visibility }),
    headers: { "content-type": "application/json" },
  });
  return resp;
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
const CHANNEL_NAME_RE = /^[a-z0-9][a-z0-9._-]{0,63}$/;
const PRESIGN_TTL_SECONDS = 900; // 15 min — plenty for a package upload, short-lived if leaked

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
