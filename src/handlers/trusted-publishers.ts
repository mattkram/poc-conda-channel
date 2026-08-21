import type { Env, TrustedPublisherRow } from "../types.js";
import { CHANNEL_NAME_RE, requireChannelOwner } from "./channel.js";
import { resolveLogin, signUploadToken } from "./auth.js";
import { b64url } from "../utils.js";

// ---------------------------------------------------------------------------
// OIDC exchange
// ---------------------------------------------------------------------------

export async function handleOidcExchange(request: Request, env: Env): Promise<Response> {
  let body: { oidc_token?: string; channel?: string };
  try {
    body = await request.json();
  } catch {
    return new Response("invalid JSON body", { status: 400 });
  }
  const { oidc_token, channel } = body;
  if (!oidc_token || !channel) return new Response("missing oidc_token or channel", { status: 400 });
  if (!CHANNEL_NAME_RE.test(channel)) return new Response("invalid channel name", { status: 400 });

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

  const OIDC_ISSUER = "https://token.actions.githubusercontent.com";
  if (claims.iss !== OIDC_ISSUER)
    return new Response(`invalid iss: expected ${OIDC_ISSUER}`, { status: 401 });

  const aud = Array.isArray(claims.aud) ? claims.aud : [claims.aud ?? ""];
  if (!aud.includes("conda-wit"))
    return new Response(`invalid aud: expected 'conda-wit'`, { status: 401 });
  if (!claims.exp || claims.exp < Math.floor(Date.now() / 1000))
    return new Response("JWT has expired", { status: 401 });
  if (header.alg !== "RS256")
    return new Response(`unsupported alg: ${header.alg}`, { status: 400 });
  if (!header.kid) return new Response("JWT missing kid", { status: 400 });

  const jwksResp = await fetch(`${OIDC_ISSUER}/.well-known/jwks`);
  if (!jwksResp.ok) return new Response("failed to fetch JWKS", { status: 502 });
  const jwks = await jwksResp.json<{ keys: Array<{ kid: string; [k: string]: unknown }> }>();
  const jwk = jwks.keys.find((k) => k.kid === header.kid);
  if (!jwk) return new Response(`no JWKS key for kid=${header.kid}`, { status: 401 });

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
  if (!valid) return new Response("JWT signature verification failed", { status: 401 });

  const { results: rules } = await env.DB.prepare(
    `SELECT * FROM trusted_publishers WHERE channel = ?`,
  )
    .bind(channel)
    .all<TrustedPublisherRow>();

  if (rules.length === 0)
    return new Response(
      `no trusted publisher rules configured for channel '${channel}'`,
      { status: 403 },
    );

  const matchedRule = rules.find((rule) => matchesRule(rule, claims));
  if (!matchedRule)
    return new Response("no matching trusted publisher rule for this workflow", { status: 403 });

  const tokenPayload: { login: string; channel: string; pkg?: string } = {
    login: claims.repository_owner ?? channel.split("/")[0],
    channel,
  };
  if (matchedRule.package_name) tokenPayload.pkg = matchedRule.package_name;

  const TTL = 900;
  const upload_token = await signUploadToken(tokenPayload, env.UPLOAD_TOKEN_SECRET, TTL);
  return Response.json({ upload_token, expires_in: TTL });
}

export function matchesRule(
  rule: TrustedPublisherRow,
  claims: { repository?: string; workflow_ref?: string; environment?: string },
): boolean {
  if (rule.repository !== null && rule.repository !== claims.repository) return false;
  if (rule.workflow !== null && !claims.workflow_ref?.startsWith(rule.workflow)) return false;
  if (rule.environment !== null && rule.environment !== claims.environment) return false;
  return true;
}

// ---------------------------------------------------------------------------
// CRUD handlers
// ---------------------------------------------------------------------------

export async function handleListTrustedPublishers(
  request: Request,
  channel: string,
  env: Env,
): Promise<Response> {
  if (!CHANNEL_NAME_RE.test(channel)) return new Response("invalid channel name", { status: 400 });
  const login = await resolveLogin(request, env.UPLOAD_TOKEN_SECRET);
  if (!login) return new Response("unauthorized", { status: 401 });
  const denied = await requireChannelOwner(channel, login, env);
  if (denied) return denied;

  const { results } = await env.DB.prepare(
    `SELECT id, channel, repository, workflow, environment, package_name, require_trusted, created_at, created_by
     FROM trusted_publishers WHERE channel = ? ORDER BY id`,
  )
    .bind(channel)
    .all<TrustedPublisherRow>();

  return Response.json(results);
}

export async function handleAddTrustedPublisher(
  request: Request,
  channel: string,
  env: Env,
): Promise<Response> {
  if (!CHANNEL_NAME_RE.test(channel)) return new Response("invalid channel name", { status: 400 });
  const login = await resolveLogin(request, env.UPLOAD_TOKEN_SECRET);
  if (!login) return new Response("unauthorized", { status: 401 });
  const denied = await requireChannelOwner(channel, login, env);
  if (denied) return denied;

  const ct = request.headers.get("content-type") ?? "";
  let repository: string | null,
    workflow: string | null,
    environment: string | null,
    package_name: string | null,
    require_trusted: number;

  if (ct.includes("application/x-www-form-urlencoded") || ct.includes("multipart/form-data")) {
    const fd = await request.formData();
    const noe = (v: File | string | null) =>
      v && v.toString().trim() ? v.toString().trim() : null;
    repository = noe(fd.get("repository"));
    workflow = noe(fd.get("workflow"));
    environment = noe(fd.get("environment"));
    package_name = noe(fd.get("package_name"));
    require_trusted = fd.get("require_trusted") ? 1 : 0;
  } else {
    let body: {
      repository?: string | null;
      workflow?: string | null;
      environment?: string | null;
      package_name?: string | null;
      require_trusted?: boolean | number;
    };
    try {
      body = await request.json();
    } catch {
      return new Response("invalid JSON body", { status: 400 });
    }
    const noe = (v: string | null | undefined) => (v && v.trim() ? v.trim() : null);
    repository = noe(body.repository);
    workflow = noe(body.workflow);
    environment = noe(body.environment);
    package_name = noe(body.package_name);
    require_trusted = body.require_trusted ? 1 : 0;
  }

  const result = await env.DB.prepare(
    `INSERT INTO trusted_publishers (channel, repository, workflow, environment, package_name, require_trusted, created_at, created_by)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
  )
    .bind(channel, repository, workflow, environment, package_name, require_trusted, Date.now(), login)
    .run();

  const id = result.meta.last_row_id;
  const row = await env.DB.prepare(`SELECT * FROM trusted_publishers WHERE id = ?`)
    .bind(id)
    .first<TrustedPublisherRow>();

  const ct2 = request.headers.get("content-type") ?? "";
  if (ct2.includes("application/x-www-form-urlencoded") || ct2.includes("multipart/form-data")) {
    return new Response(null, {
      status: 302,
      headers: { location: `/channels/${channel}/admin` },
    });
  }
  return Response.json(row, { status: 201 });
}

export async function handleDeleteTrustedPublisher(
  request: Request,
  channel: string,
  id: number,
  env: Env,
): Promise<Response> {
  if (!CHANNEL_NAME_RE.test(channel)) return new Response("invalid channel name", { status: 400 });
  const login = await resolveLogin(request, env.UPLOAD_TOKEN_SECRET);
  if (!login) return new Response("unauthorized", { status: 401 });
  const denied = await requireChannelOwner(channel, login, env);
  if (denied) return denied;

  const result = await env.DB.prepare(
    `DELETE FROM trusted_publishers WHERE id = ? AND channel = ?`,
  )
    .bind(id, channel)
    .run();

  if (result.meta.changes === 0) return new Response("rule not found", { status: 404 });
  return new Response(null, { status: 204 });
}
