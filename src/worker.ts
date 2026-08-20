import { Container } from "@cloudflare/containers";
import { env } from "cloudflare:workers";
import type { Env } from "./types.js";

// ---------------------------------------------------------------------------
// Container DO — must be exported from the entrypoint for wrangler to find it.
// ---------------------------------------------------------------------------
export class IndexerContainer extends Container<Env> {
  defaultPort = 8080;
  sleepAfter = "2m";
  envVars = {
    R2_ACCOUNT_ID: env.R2_ACCOUNT_ID,
    R2_ACCESS_KEY_ID: env.R2_ACCESS_KEY_ID,
    R2_SECRET_ACCESS_KEY: env.R2_SECRET_ACCESS_KEY,
    R2_BUCKET_NAME: env.R2_BUCKET_NAME,
    WORKER_URL: "https://conda.matt-kramer.com",
  };
}

// ---------------------------------------------------------------------------
// Durable Object re-exports — wrangler needs these from the entrypoint.
// ---------------------------------------------------------------------------
export { ChannelQueue } from "./do/channel-queue.js";
export { PackageIngestor } from "./do/package-ingestor.js";
export { ChannelIngestQueue } from "./do/channel-ingest-queue.js";
export { SubdirIndexMerger } from "./do/subdir-index-merger.js";

// ---------------------------------------------------------------------------
// Handler imports
// ---------------------------------------------------------------------------
import { startDeviceFlow, pollDeviceFlow, handleBrowserLoginStart, handleBrowserLoginCallback, handleBrowserLogout } from "./auth.js";
import { handleGetChannelInfo, handleSetVisibility, handleDeleteChannel } from "./channel.js";
import { handleUploadInit, handleUploadComplete } from "./upload.js";
import { handleOidcExchange, handleListTrustedPublishers, handleAddTrustedPublisher, handleDeleteTrustedPublisher } from "./trusted-publishers.js";
import { handleHomepage, handleSearchResults, handleGlobalSearch, handleChannelsIndex, handleNamespacePage, handleBrowseResults, handleBrowsePage, handleBrowsePackage, handleR2Get, handleChannelRoot, handleDeletePackage } from "./browse/pages.js";
import { handleAdminPage, handleRebuildBrowse } from "./admin.js";
import { handleUpsertPackage, handleReconcile, handleMigrateR2Prefix, handleDeleteR2Prefix, handlePurgeQueue, handleAbortMultipart } from "./internal.js";
import { verifyUploadToken } from "./auth.js";

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const p = url.pathname;
    const m = request.method;

    if (p === "/" && m === "GET") return handleHomepage(request, url, env);
    if (p === "/search/results" && m === "GET") return handleSearchResults(request, url, env);
    if (p === "/search" && m === "GET") return handleGlobalSearch(request, url, env);

    if (p === "/auth/device/start" && m === "POST") return startDeviceFlow(env);
    if (p === "/auth/device/poll" && m === "POST") return pollDeviceFlow(request, env);
    if (p === "/auth/login" && m === "GET") return handleBrowserLoginStart(request, env);
    if (p === "/auth/callback" && m === "GET") return handleBrowserLoginCallback(request, url, env);
    if (p === "/auth/logout" && m === "GET") return handleBrowserLogout(url);

    if (p === "/upload/init" && m === "POST") return handleUploadInit(request, env);
    if (p === "/upload/complete" && m === "POST") return handleUploadComplete(request, env);
    if (p === "/upload/exchange-oidc" && m === "POST") return handleOidcExchange(request, env);

    if (p === "/channels" || p === "/channels/") return handleChannelsIndex(request, env);

    const resultsMatch = p.match(/^\/channels\/([^/]+(?:\/[^/]+)?)\/results\/?$/);
    if (resultsMatch && m === "GET") return handleBrowseResults(request, resultsMatch[1], url, env);

    const detailMatch = p.match(/^\/channels\/([^/]+(?:\/[^/]+)?)\/package\/([^/]+)\/?$/);
    if (detailMatch && m === "GET") return handleBrowsePackage(request, detailMatch[1], detailMatch[2], env);

    const adminMatch = p.match(/^\/channels\/([^/]+(?:\/[^/]+)?)\/admin\/?$/);
    if (adminMatch && m === "GET") return handleAdminPage(request, adminMatch[1], env);

    const browseMatch = p.match(/^\/channels\/([^/]+(?:\/[^/]+)?)\/?$/);
    if (browseMatch && m === "GET") {
      const seg = browseMatch[1];
      if (!seg.includes("/")) {
        const nsChannels = await env.DB.prepare(
          `SELECT name, owner, visibility FROM channels WHERE name LIKE ? OR name = ? ORDER BY name`,
        ).bind(`${seg}/%`, seg).all<{ name: string; owner: string | null; visibility: string }>();
        if (nsChannels.results.some((r) => r.name.startsWith(`${seg}/`))) {
          return handleNamespacePage(request, seg, nsChannels.results, env);
        }
      }
      return handleBrowsePage(request, seg, url, env);
    }

    const repoSubdirMatch = p.match(/^\/repo\/([^/]+(?:\/[^/]+)?)\/([^/]+)\/?$/);
    if (repoSubdirMatch && m === "GET") {
      return handleR2Get(request, repoSubdirMatch[1], `${repoSubdirMatch[1]}/${repoSubdirMatch[2]}/index.html`, env);
    }
    const repoReadMatch = p.match(/^\/repo\/([^/]+(?:\/[^/]+)?)\/([^/]+)\/.+$/);
    if (repoReadMatch && m === "GET") {
      return handleR2Get(request, repoReadMatch[1], p.slice("/repo/".length), env);
    }
    const repoRootMatch = p.match(/^\/repo\/([^/]+(?:\/[^/]+)?)\/?$/);
    if (repoRootMatch && m === "GET") return handleChannelRoot(request, repoRootMatch[1], env);

    const pkgMatch = p.match(/^\/channel\/([^/]+(?:\/[^/]+)?)\/([^/]+)\/([^/]+)$/);
    if (pkgMatch && m === "DELETE") return handleDeletePackage(request, pkgMatch[1], pkgMatch[2], pkgMatch[3], env);

    const rebuildMatch = p.match(/^\/channel\/([^/]+(?:\/[^/]+)?)\/rebuild-browse$/);
    if (rebuildMatch && m === "POST") return handleRebuildBrowse(request, rebuildMatch[1], env);

    const chanMatch = p.match(/^\/channel\/([^/]+(?:\/[^/]+)?)$/);
    if (chanMatch && m === "GET") return handleGetChannelInfo(chanMatch[1], env);
    if (chanMatch && m === "DELETE") return handleDeleteChannel(request, chanMatch[1], env);

    const visMatch = p.match(/^\/channel\/([^/]+(?:\/[^/]+)?)\/visibility$/);
    if (visMatch && m === "POST") return handleSetVisibility(request, visMatch[1], env);

    const tpMatch = p.match(/^\/channel\/([^/]+(?:\/[^/]+)?)\/trusted-publishers$/);
    if (tpMatch && m === "GET") return handleListTrustedPublishers(request, tpMatch[1], env);
    if (tpMatch && m === "POST") return handleAddTrustedPublisher(request, tpMatch[1], env);

    const tpDelMatch = p.match(/^\/channel\/([^/]+(?:\/[^/]+)?)\/trusted-publishers\/(\d+)$/);
    if (tpDelMatch && m === "DELETE") return handleDeleteTrustedPublisher(request, tpDelMatch[1], Number(tpDelMatch[2]), env);

    if (p === "/internal/upsert-package" && m === "POST") return handleUpsertPackage(request, env);

    const reconcileMatch = p.match(/^\/internal\/reconcile\/([^/]+(?:\/[^/]+)?)$/);
    if (reconcileMatch && m === "POST") return handleReconcile(request, reconcileMatch[1], env);

    if (p === "/internal/migrate-r2-prefix" && m === "POST") return handleMigrateR2Prefix(request, env);
    if (p === "/internal/delete-r2-prefix" && m === "POST") return handleDeleteR2Prefix(request, env);

    const purgeMatch = p.match(/^\/internal\/purge-queue\/([^/]+(?:\/[^/]+)?)$/);
    if (purgeMatch && m === "POST") return handlePurgeQueue(request, purgeMatch[1], env);

    if (p === "/internal/abort-multipart" && m === "POST") return handleAbortMultipart(request, env);

    if (p === "/internal/list-r2" && m === "POST") {
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
        keys: list.objects.map((o) => o.key),
        done: !list.truncated,
        ...(list.truncated ? { next_cursor: list.cursor } : {}),
      });
    }

    // Legacy channel name redirects
    const legacyRedirects: Record<string, string> = {
      "anaconda-cloud": "mattkram/anaconda-cloud",
      "anaconda-cloud-2": "mattkram/anaconda-cloud-2",
    };
    for (const [flat, namespaced] of Object.entries(legacyRedirects)) {
      const flatRe = new RegExp(`^(\\/channels\\/|\\/repo\\/|\\/channel\\/)${flat}(\\/|$)`);
      if (flatRe.test(p)) {
        const newPath = p.replace(`/${flat}/`, `/${namespaced}/`).replace(`/${flat}`, `/${namespaced}`);
        const newUrl = new URL(request.url);
        newUrl.pathname = newPath;
        return Response.redirect(newUrl.toString(), 301);
      }
    }

    return new Response("not found", { status: 404 });
  },
};
