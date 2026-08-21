import type { Env, UpsertPackageBody } from "../types.js";
import { CHANNEL_NAME_RE } from "./channel.js";
import { verifyUploadToken, resolveLogin } from "./auth.js";
import { r2Client } from "./upload.js";

// ---------------------------------------------------------------------------
// Auth helpers for internal routes
// ---------------------------------------------------------------------------

/**
 * Verifies the `X-Internal-Secret` header matches `env.INTERNAL_SECRET`.
 * Used for container→worker callbacks that don't have a user login context.
 */
function verifyInternalSecret(request: Request, env: Env): boolean {
  const header = request.headers.get("x-internal-secret") ?? "";
  return header.length > 0 && header === env.INTERNAL_SECRET;
}

/**
 * Returns a 403 Response if the caller is not the superadmin, null on ok.
 * Accepts both Bearer token and session cookie.
 */
async function requireSuperadmin(request: Request, env: Env): Promise<Response | null> {
  const login = await resolveLogin(request, env.UPLOAD_TOKEN_SECRET);
  if (!login) return new Response("unauthorized", { status: 401 });
  if (login !== env.SUPERADMIN_LOGIN) {
    return new Response(`superadmin only — you are '${login}'`, { status: 403 });
  }
  return null;
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

export async function handleUpsertPackage(request: Request, env: Env): Promise<Response> {
  // Called by the container after ingest. Authenticated via a shared secret
  // header rather than a user token — the container has no GitHub login.
  if (!verifyInternalSecret(request, env)) {
    return new Response("unauthorized", { status: 401 });
  }

  const body = await request.json<UpsertPackageBody>();
  const { channel, name, version, summary, license, home, subdirs } = body;

  if (!channel || !name || !version) {
    return new Response("missing required fields: channel, name, version", { status: 400 });
  }

  await env.DB.prepare(
    `INSERT OR IGNORE INTO channels (name, owner, visibility, created_at) VALUES (?, NULL, 'public', ?)`,
  )
    .bind(channel, Date.now())
    .run();

  await env.DB.prepare(
    `INSERT INTO packages (channel, name, version, summary, license, home, subdirs, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(channel, name) DO UPDATE SET
       version    = excluded.version,
       summary    = excluded.summary,
       license    = excluded.license,
       home       = excluded.home,
       subdirs    = (
         SELECT json_group_array(DISTINCT value)
         FROM (
           SELECT value FROM json_each(packages.subdirs)
           UNION
           SELECT value FROM json_each(excluded.subdirs)
         )
       ),
       updated_at = excluded.updated_at`,
  )
    .bind(
      channel,
      name,
      version,
      summary ?? null,
      license ?? null,
      home ?? null,
      JSON.stringify(subdirs ?? []),
      Date.now(),
    )
    .run();

  return new Response("ok", { status: 200 });
}

export async function handleUpsertPackageBulk(request: Request, env: Env): Promise<Response> {
  // Batch version of handleUpsertPackage. Accepts { packages: UpsertPackageBody[] }.
  // Uses a D1 batch() so all upserts are one round-trip instead of N.
  if (!verifyInternalSecret(request, env)) {
    return new Response("unauthorized", { status: 401 });
  }

  const { packages } = await request.json<{ packages: UpsertPackageBody[] }>();
  if (!Array.isArray(packages) || packages.length === 0) {
    return new Response("packages must be a non-empty array", { status: 400 });
  }

  const stmts = packages.flatMap((body) => {
    const { channel, name, version, summary, license, home, subdirs } = body;
    if (!channel || !name || !version) return [];
    return [
      env.DB.prepare(
        `INSERT OR IGNORE INTO channels (name, owner, visibility, created_at) VALUES (?, NULL, 'public', ?)`,
      ).bind(channel, Date.now()),
      env.DB.prepare(
        `INSERT INTO packages (channel, name, version, summary, license, home, subdirs, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(channel, name) DO UPDATE SET
           version    = excluded.version,
           summary    = excluded.summary,
           license    = excluded.license,
           home       = excluded.home,
           subdirs    = (
             SELECT json_group_array(DISTINCT value)
             FROM (
               SELECT value FROM json_each(packages.subdirs)
               UNION
               SELECT value FROM json_each(excluded.subdirs)
             )
           ),
           updated_at = excluded.updated_at`,
      ).bind(
        channel,
        name,
        version,
        summary ?? null,
        license ?? null,
        home ?? null,
        JSON.stringify(subdirs ?? []),
        Date.now(),
      ),
    ];
  });

  if (stmts.length === 0) {
    return new Response("no valid packages in request", { status: 400 });
  }

  await env.DB.batch(stmts);
  return Response.json({ upserted: packages.length });
}

export async function handleRegisterChannel(request: Request, env: Env): Promise<Response> {
  // Called by the container after rebuild-index to ensure the channel row exists in D1.
  // Only needs the channel name — no package metadata required.
  if (!verifyInternalSecret(request, env)) {
    return new Response("unauthorized", { status: 401 });
  }
  const { channel } = await request.json<{ channel: string }>();
  if (!channel) {
    return new Response("missing required field: channel", { status: 400 });
  }
  await env.DB.prepare(
    `INSERT OR IGNORE INTO channels (name, owner, visibility, created_at) VALUES (?, NULL, 'public', ?)`,
  )
    .bind(channel, Date.now())
    .run();
  return new Response("ok", { status: 200 });
}

export async function handleRequeueStaging(request: Request, channel: string, env: Env): Promise<Response> {
  // Re-enqueue all objects stuck in _incoming/ for a channel, bypassing upload auth.
  // Superadmin only. Used to recover from stuck ChannelIngestQueue state.
  if (!CHANNEL_NAME_RE.test(channel)) return new Response("invalid channel name", { status: 400 });
  const denied = await requireSuperadmin(request, env);
  if (denied) return denied;

  const prefix = `${channel}/_incoming/`;
  let cursor: string | undefined;
  let enqueued = 0;

  do {
    const list = await env.CHANNEL_BUCKET.list({ prefix, cursor });
    await Promise.all(list.objects.map(async (obj) => {
      const filename = obj.key.slice(prefix.length);
      if (!filename || filename.includes("/")) return;
      const queueId = env.QUEUE.idFromName(channel);
      const queue = env.QUEUE.get(queueId);
      await queue.fetch("http://queue/enqueue", {
        method: "POST",
        body: JSON.stringify({ channel, filename, uploadedAt: Date.now(), uploadedBy: env.SUPERADMIN_LOGIN }),
        headers: { "content-type": "application/json" },
      });
      enqueued++;
    }));
    cursor = list.truncated ? list.cursor : undefined;
  } while (cursor);

  return Response.json({ channel, enqueued });
}

export async function handleReconcile(
  request: Request,
  channel: string,
  env: Env,
): Promise<Response> {
  if (!CHANNEL_NAME_RE.test(channel)) return new Response("invalid channel name", { status: 400 });

  const denied = await requireSuperadmin(request, env);
  if (denied) return denied;

  await env.DB.prepare(
    `INSERT OR IGNORE INTO channels (name, owner, visibility, created_at) VALUES (?, ?, 'public', ?)`,
  )
    .bind(channel, env.SUPERADMIN_LOGIN, Date.now())
    .run();

  const prefix = `${channel}/_browse/`;
  let cursor: string | undefined;
  let upserted = 0;
  let errors = 0;

  do {
    const list = await env.CHANNEL_BUCKET.list({ prefix, cursor });
    await Promise.all(
      list.objects.map(async (obj) => {
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
               updated_at = excluded.updated_at`,
          )
            .bind(
              channel,
              rec.name,
              rec.version,
              rec.summary ?? null,
              rec.license ?? null,
              rec.home ?? null,
              JSON.stringify(rec.subdirs ?? []),
              Date.now(),
            )
            .run();
          upserted++;
        } catch {
          errors++;
        }
      }),
    );
    cursor = list.truncated ? list.cursor : undefined;
  } while (cursor);

  return Response.json({ channel, upserted, errors });
}

export async function handleMigrateR2Prefix(request: Request, env: Env): Promise<Response> {
  const denied = await requireSuperadmin(request, env);
  if (denied) return denied;

  const body = await request.json<{ src: string; dst: string; cursor?: string }>();
  const { src, dst, cursor } = body;
  if (!src || !dst) return new Response("missing src or dst", { status: 400 });
  if (src === dst) return new Response("src and dst must differ", { status: 400 });

  const BATCH = 20;
  const srcPrefix = src.endsWith("/") ? src : src + "/";
  const dstPrefix = dst.endsWith("/") ? dst : dst + "/";

  const list = await env.CHANNEL_BUCKET.list({ prefix: srcPrefix, cursor, limit: BATCH });
  let copied = 0,
    deleted = 0,
    errors = 0;

  for (const obj of list.objects) {
    const srcKey = obj.key;
    const dstKey = dstPrefix + srcKey.slice(srcPrefix.length);
    try {
      const srcObj = await env.CHANNEL_BUCKET.get(srcKey);
      if (!srcObj) {
        errors++;
        continue;
      }
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
    copied,
    deleted,
    errors,
    done,
    ...(done ? {} : { next_cursor: list.cursor }),
  });
}

export async function handleDeleteR2Prefix(request: Request, env: Env): Promise<Response> {
  const denied = await requireSuperadmin(request, env);
  if (denied) return denied;

  const body = await request.json<{ prefix: string; cursor?: string }>();
  const { prefix, cursor } = body;
  if (!prefix) return new Response("missing prefix", { status: 400 });

  const isExactKey =
    !prefix.endsWith("/") && /\.[a-z0-9]+$/i.test(prefix.split("/").pop() ?? "");
  const listPrefix = isExactKey
    ? prefix
    : prefix.endsWith("/")
      ? prefix
      : prefix + "/";

  const list = await env.CHANNEL_BUCKET.list({ prefix: listPrefix, cursor, limit: 100 });
  const toDelete = isExactKey ? [prefix] : list.objects.map((o) => o.key);

  await Promise.all(toDelete.map((k) => env.CHANNEL_BUCKET.delete(k)));

  const done = isExactKey || !list.truncated;
  return Response.json({
    deleted: toDelete.length,
    done,
    ...(done ? {} : { next_cursor: list.cursor }),
  });
}

export async function handlePurgeQueue(
  request: Request,
  channel: string,
  env: Env,
): Promise<Response> {
  const denied = await requireSuperadmin(request, env);
  if (denied) return denied;

  const [queueResp, ingestResp] = await Promise.all([
    env.QUEUE.get(env.QUEUE.idFromName(channel))
      .fetch("http://queue/purge", { method: "POST" })
      .then((r) => r.json<{ purged: number }>()),
    env.INGEST_QUEUE.get(env.INGEST_QUEUE.idFromName(channel))
      .fetch("http://queue/purge", { method: "POST" })
      .then((r) => r.json<{ purged: number }>()),
  ]);

  return Response.json({
    channel,
    queue_purged: queueResp.purged,
    ingest_queue_purged: ingestResp.purged,
  });
}

export async function handleAbortMultipart(request: Request, env: Env): Promise<Response> {
  const denied = await requireSuperadmin(request, env);
  if (denied) return denied;

  const body = await request.json<{ debug?: boolean }>().catch(() => ({}));
  const client = r2Client(env);
  const bucketUrl = `https://${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com/${env.R2_BUCKET_NAME}`;

  const listReq = await client.sign(
    new Request(`${bucketUrl}?uploads&max-uploads=1000`, { method: "GET" }),
  );
  const listResp = await fetch(listReq);
  const listXml = await listResp.text();

  if ((body as any).debug) {
    return new Response(listXml, { headers: { "content-type": "text/xml" } });
  }

  const uploads: Array<{ key: string; uploadId: string }> = [];
  const uploadBlocks = listXml.matchAll(/<Upload>([\s\S]*?)<\/Upload>/g);
  for (const block of uploadBlocks) {
    const keyMatch = block[1].match(/<Key>([^<]+)<\/Key>/);
    const idMatch = block[1].match(/<UploadId>([^<]+)<\/UploadId>/);
    if (keyMatch && idMatch) {
      uploads.push({ key: keyMatch[1], uploadId: idMatch[1] });
    }
  }

  let aborted = 0,
    errors = 0;
  for (const { key, uploadId } of uploads) {
    try {
      const abortReq = await client.sign(
        new Request(
          `${bucketUrl}/${encodeURIComponent(key)}?uploadId=${encodeURIComponent(uploadId)}`,
          { method: "DELETE" },
        ),
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
