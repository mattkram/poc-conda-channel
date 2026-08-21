import type { Env } from "../types.js";
import { channelNamespace } from "../utils.js";
import { verifyUploadToken, resolveLogin } from "./auth.js";

export { channelNamespace };

export const CHANNEL_NAME_RE = /^(?:[a-z0-9][a-z0-9-]{0,38}\/)?[a-z0-9][a-z0-9._-]{0,63}$/;

// ---------------------------------------------------------------------------
// D1 helpers
// ---------------------------------------------------------------------------

export async function ensureChannelRow(channel: string, owner: string, env: Env): Promise<void> {
  await env.DB.prepare(
    `INSERT OR IGNORE INTO channels (name, owner, visibility, created_at) VALUES (?, ?, 'public', ?)`,
  )
    .bind(channel, owner, Date.now())
    .run();
}

// ---------------------------------------------------------------------------
// Access control
// ---------------------------------------------------------------------------

/** Returns a 403 Response on denial, null on ok. */
export async function checkChannelAccess(
  channel: string,
  login: string,
  env: Env,
): Promise<Response | null> {
  const ns = channelNamespace(channel);
  if (ns && ns !== login) {
    return new Response(
      `channel '${channel}' belongs to namespace '${ns}' — you are '${login}'`,
      { status: 403 },
    );
  }

  const row = await env.DB.prepare(`SELECT owner FROM channels WHERE name = ?`)
    .bind(channel)
    .first<{ owner: string | null }>();

  if (row) {
    if (row.owner && row.owner !== login) {
      return new Response(
        `channel '${channel}' is owned by ${row.owner} — access denied`,
        { status: 403 },
      );
    }
    return null;
  }

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
      { status: 403 },
    );
  }
  await ensureChannelRow(channel, login, env);
  return null;
}

/** Returns a 401 Response on denial, null on ok. login may be null for public channels. */
export async function checkReadAccess(
  channel: string,
  login: string | null,
  env: Env,
): Promise<Response | null> {
  const row = await env.DB.prepare(`SELECT visibility, owner FROM channels WHERE name = ?`)
    .bind(channel)
    .first<{ visibility: string; owner: string | null }>();

  if (!row) return null;
  if (row.visibility === "public") return null;
  if (login && login === row.owner) return null;

  return new Response(
    `channel '${channel}' is private — provide a valid Bearer token`,
    { status: 401, headers: { "WWW-Authenticate": 'Bearer realm="conda-channel"' } },
  );
}

/** Returns a 403 Response if login is not the channel owner, null on ok. */
export async function requireChannelOwner(
  channel: string,
  login: string,
  env: Env,
): Promise<Response | null> {
  const row = await env.DB.prepare(`SELECT owner FROM channels WHERE name = ?`)
    .bind(channel)
    .first<{ owner: string | null }>();
  if (!row) return new Response("channel not found", { status: 404 });
  if (row.owner !== login) {
    return new Response("only the channel owner can manage trusted publishers", { status: 403 });
  }
  return null;
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

export async function handleGetChannelInfo(channel: string, env: Env): Promise<Response> {
  if (!CHANNEL_NAME_RE.test(channel)) return new Response("invalid channel name", { status: 400 });
  const row = await env.DB.prepare(`SELECT owner, visibility FROM channels WHERE name = ?`)
    .bind(channel)
    .first<{ owner: string | null; visibility: string }>();
  if (!row) return Response.json({ owner: null, visibility: "public" });
  return Response.json(row);
}

export async function handleSetVisibility(
  request: Request,
  channel: string,
  env: Env,
): Promise<Response> {
  if (!CHANNEL_NAME_RE.test(channel)) return new Response("invalid channel name", { status: 400 });

  const auth = request.headers.get("authorization") ?? "";
  const token = auth.replace(/^Bearer\s+/i, "");
  const claims = await verifyUploadToken(token, env.UPLOAD_TOKEN_SECRET);
  if (!claims) return new Response("unauthorized", { status: 401 });

  const { visibility } = await request.json<{ visibility: string }>();
  if (visibility !== "public" && visibility !== "private") {
    return new Response("visibility must be 'public' or 'private'", { status: 400 });
  }

  const row = await env.DB.prepare(`SELECT owner FROM channels WHERE name = ?`)
    .bind(channel)
    .first<{ owner: string | null }>();
  if (!row) return new Response("channel not found", { status: 404 });
  if (row.owner !== claims.login) {
    return new Response("only the channel owner can change visibility", { status: 403 });
  }

  await env.DB.prepare(`UPDATE channels SET visibility = ? WHERE name = ?`)
    .bind(visibility, channel)
    .run();

  const queueId = env.QUEUE.idFromName(channel);
  const queue = env.QUEUE.get(queueId);
  queue
    .fetch("http://queue/set-visibility", {
      method: "POST",
      body: JSON.stringify({ login: claims.login, visibility }),
      headers: { "content-type": "application/json" },
    })
    .catch(() => {});

  return Response.json({ owner: row.owner, visibility });
}

export async function handleSetRequireOidc(
  request: Request,
  channel: string,
  env: Env,
): Promise<Response> {
  if (!CHANNEL_NAME_RE.test(channel)) return new Response("invalid channel name", { status: 400 });

  const login = await resolveLogin(request, env.UPLOAD_TOKEN_SECRET);
  if (!login) return new Response("unauthorized", { status: 401 });

  const row = await env.DB.prepare(`SELECT owner FROM channels WHERE name = ?`)
    .bind(channel)
    .first<{ owner: string | null }>();
  if (!row) return new Response("channel not found", { status: 404 });
  if (row.owner !== login) return new Response("only the channel owner can change this setting", { status: 403 });

  const ct = request.headers.get("content-type") ?? "";
  let require_oidc: number;
  if (ct.includes("application/x-www-form-urlencoded") || ct.includes("multipart/form-data")) {
    const fd = await request.formData();
    require_oidc = fd.get("require_oidc") ? 1 : 0;
  } else {
    const body = await request.json<{ require_oidc?: boolean | number }>();
    require_oidc = body.require_oidc ? 1 : 0;
  }

  await env.DB.prepare(`UPDATE channels SET require_oidc = ? WHERE name = ?`)
    .bind(require_oidc, channel)
    .run();

  const isForm = ct.includes("application/x-www-form-urlencoded") || ct.includes("multipart/form-data");
  if (isForm) return new Response(null, { status: 302, headers: { location: `/channels/${channel}/admin` } });
  return Response.json({ require_oidc });
}

export async function handleDeleteChannel(
  request: Request,
  channel: string,
  env: Env,
): Promise<Response> {
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
// browseAuth — used by browse handlers
// ---------------------------------------------------------------------------

export async function browseAuth(
  request: Request,
  channel: string,
  env: Env,
): Promise<Response | null> {
  if (!CHANNEL_NAME_RE.test(channel)) return new Response("not found", { status: 404 });
  const login = await resolveLogin(request, env.UPLOAD_TOKEN_SECRET);
  return checkReadAccess(channel, login, env);
}
