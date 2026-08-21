import { getContainer } from "@cloudflare/containers";
import { DurableObject } from "cloudflare:workers";
import type { Env } from "../types.js";

const MERGE_DEBOUNCE_MS = 3_000;
// Extra delay after a container 500 — gives the cold-starting container time
// to fully initialize before the next rebuild attempt.
const CONTAINER_COLD_START_RETRY_MS = 30_000;

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

    await this.ctx.storage.put("dirty", false);

    try {
      const container = getContainer(this.env.INDEXER, `${channel}/${subdir}/_merge`);
      const resp = await container.fetch("http://container/rebuild-index", {
        method: "POST",
        body: JSON.stringify({ channel, subdir }),
        headers: { "content-type": "application/json" },
      });
      // 200 = synchronous success, 202 = async rebuild kicked off — both are ok.
      if (!resp.ok) {
        await this.ctx.storage.put("dirty", true);
        // 500 from a freshly-started container = still warming up; give it more time.
        // Other non-2xx = real error; retry at 60s.
        const retryMs = resp.status === 500 ? CONTAINER_COLD_START_RETRY_MS : 60_000;
        await this.ctx.storage.setAlarm(Date.now() + retryMs);
        return;
      }
    } catch (err) {
      await this.ctx.storage.put("dirty", true);
      const msg = String(err);
      const isCapacity = msg.includes("no container instance") || msg.includes("try again later");
      // Connection closed / port not ready = container is cold-starting, back off longer
      const isColdStart = msg.includes("connection closed") || msg.includes("port") || msg.includes("Network");
      const retryMs = isCapacity ? 15_000 : isColdStart ? CONTAINER_COLD_START_RETRY_MS : 60_000;
      await this.ctx.storage.setAlarm(Date.now() + retryMs);
      return;
    }

    const stillDirty = await this.ctx.storage.get<boolean>("dirty");
    if (stillDirty) {
      await this.ctx.storage.setAlarm(Date.now() + MERGE_DEBOUNCE_MS);
    }
  }
}
