import { getContainer } from "@cloudflare/containers";
import { DurableObject } from "cloudflare:workers";
import type { Env } from "../types.js";

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

    await this.ctx.storage.put("dirty", false);

    try {
      const container = getContainer(this.env.INDEXER, `${channel}/${subdir}/_merge`);
      const resp = await container.fetch("http://container/rebuild-index", {
        method: "POST",
        body: JSON.stringify({ channel, subdir }),
        headers: { "content-type": "application/json" },
      });
      if (!resp.ok) {
        await this.ctx.storage.put("dirty", true);
        await this.ctx.storage.setAlarm(Date.now() + 60_000);
        return;
      }
    } catch (err) {
      await this.ctx.storage.put("dirty", true);
      const isCapacity = String(err).includes("no container instance");
      await this.ctx.storage.setAlarm(Date.now() + (isCapacity ? 15_000 : 60_000));
      return;
    }

    const stillDirty = await this.ctx.storage.get<boolean>("dirty");
    if (stillDirty) {
      await this.ctx.storage.setAlarm(Date.now() + MERGE_DEBOUNCE_MS);
    }
  }
}
