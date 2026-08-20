import { getContainer } from "@cloudflare/containers";
import type { Env, TrustedPublisherRow } from "./types.js";
import { esc, channelNamespace, userWidget } from "./utils.js";
import { resolveLogin } from "./auth.js";
import { CHANNEL_NAME_RE, requireChannelOwner } from "./channel.js";
import { verifyUploadToken } from "./auth.js";
import { BROWSE_CSS } from "./browse/ui.js";

const ADMIN_CSS = `
  .admin-section { background:#fff; border:1px solid #e4e7eb; border-radius:8px; padding:20px 24px; margin-bottom:24px; }
  .admin-section h2 { font-size:16px; font-weight:700; margin:0 0 14px; color:#1f2933; }
  .tp-wrap { overflow-x:auto; }
  table.tp-table { width:100%; border-collapse:collapse; font-size:13px; }
  .tp-table th { text-align:left; padding:8px 10px; background:#f5f7fa; color:#52606d; font-weight:600; border-bottom:1px solid #e4e7eb; white-space:nowrap; }
  .tp-table td { padding:8px 10px; border-bottom:1px solid #f0f2f5; vertical-align:middle; }
  .tp-table tr:last-child td { border-bottom:none; }
  .tp-table tr:hover td { background:#fafbfc; }
  .tp-table .col-id { width:40px; text-align:center; color:#9aacb8; }
  .tp-table .col-repo { min-width:140px; }
  .tp-table .col-workflow { min-width:180px; }
  .tp-table .col-env { min-width:80px; }
  .tp-table .col-pkg { min-width:120px; }
  .tp-table .col-bool { width:80px; text-align:center; white-space:nowrap; }
  .tp-table .col-by { min-width:80px; white-space:nowrap; }
  .tp-table .col-action { width:80px; text-align:right; white-space:nowrap; }
  code.val { background:#f0f2f5; padding:2px 5px; border-radius:4px; font-size:12px; overflow-wrap:anywhere; }
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

export async function handleAdminPage(
  request: Request,
  channel: string,
  env: Env,
): Promise<Response> {
  if (!CHANNEL_NAME_RE.test(channel)) return new Response("not found", { status: 404 });

  const login = await resolveLogin(request, env.UPLOAD_TOKEN_SECRET);
  if (!login) {
    return new Response(null, { status: 302, headers: { location: "/auth/login" } });
  }

  const denied = await requireChannelOwner(channel, login, env);
  if (denied) return denied;

  const { results: rules } = await env.DB.prepare(
    `SELECT id, repository, workflow, environment, package_name, require_trusted, created_at, created_by
     FROM trusted_publishers WHERE channel = ? ORDER BY id`,
  )
    .bind(channel)
    .all<Omit<TrustedPublisherRow, "channel">>();

  const chanInfo = await env.DB.prepare(`SELECT visibility FROM channels WHERE name = ?`)
    .bind(channel)
    .first<{ visibility: string }>();

  const ns = channelNamespace(channel);
  const channelHeader = ns
    ? `<a class="chan-ns" href="/channels/${esc(ns)}">${esc(ns)}</a><span class="chan-sep">/</span><a class="chan" href="/channels/${esc(channel)}">${esc(channel.slice(ns.length + 1))}</a>`
    : `<a class="chan" href="/channels/${esc(channel)}">${esc(channel)}</a>`;

  const rulesRows = rules
    .map((r) => {
      const wild = `<span class="wildcard">any</span>`;
      const code = (v: string | null) =>
        v ? `<code class="val">${esc(v)}</code>` : wild;
      return `
    <tr>
      <td class="col-id">${r.id}</td>
      <td class="col-repo">${code(r.repository)}</td>
      <td class="col-workflow">${code(r.workflow)}</td>
      <td class="col-env">${code(r.environment)}</td>
      <td class="col-pkg">${code(r.package_name)}</td>
      <td class="col-bool">${r.require_trusted ? '<span class="yes-badge">Yes</span>' : '<span class="no-badge">No</span>'}</td>
      <td class="col-by">${esc(r.created_by)}</td>
      <td class="col-action">
        <form method="POST" action="/channel/${esc(channel)}/trusted-publishers/${r.id}" style="display:inline">
          <input type="hidden" name="_method" value="DELETE">
          <button type="submit" class="btn-danger" onclick="return confirm('Delete this rule?')">Delete</button>
        </form>
      </td>
    </tr>`;
    })
    .join("");

  const visibility = chanInfo?.visibility ?? "public";

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
  <a class="brand" href="/">conda-wit</a>
  <span class="chan-sep">/</span>
  ${channelHeader}
  <span class="chan-sep">/</span>
  <span class="chan">admin</span>
  ${userWidget(login)}
</header>
<main>
<div class="wrap">

  <div class="admin-section">
    <h2>Trusted Publishers <span class="hint">(GitHub Actions OIDC keyless upload)</span></h2>
    ${rules.length > 0 ? `
    <div class="tp-wrap">
    <table class="tp-table">
      <thead>
        <tr>
          <th>ID</th><th>Repository</th><th>Workflow ref prefix</th><th>Environment</th><th>Package</th><th>OIDC only?</th><th>Added by</th><th></th>
        </tr>
      </thead>
      <tbody>${rulesRows}</tbody>
    </table>
    </div>` : `<p class="empty-rules">No trusted publisher rules yet.</p>`}

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
</script>
</body>
</html>`;
  return new Response(html, { headers: { "content-type": "text/html;charset=utf-8" } });
}

export async function handleRebuildBrowse(
  request: Request,
  channel: string,
  env: Env,
): Promise<Response> {
  if (!CHANNEL_NAME_RE.test(channel)) return new Response("invalid channel name", { status: 400 });
  const auth = request.headers.get("authorization") ?? "";
  const token = auth.replace(/^Bearer\s+/i, "");
  const claims = await verifyUploadToken(token, env.UPLOAD_TOKEN_SECRET);
  if (!claims) return new Response("unauthorized", { status: 401 });

  const { checkChannelAccess } = await import("./channel.js");
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
    status: 200,
    headers: { "content-type": "application/json" },
  });
}
