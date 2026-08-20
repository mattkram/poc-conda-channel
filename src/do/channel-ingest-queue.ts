import { getContainer } from "@cloudflare/containers";
import { DurableObject } from "cloudflare:workers";
import type { Env, PendingUpload } from "../types.js";

const INGEST_QUEUE_DRAIN_MS = 100;
// Back-off when the container platform has no capacity.
const CONTAINER_UNAVAILABLE_RETRY_MS = 15_000;

export class ChannelIngestQueue extends DurableObject<Env> {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/enqueue" && request.method === "POST") {
      const upload = await request.json<PendingUpload>();
      const key = `work:${String(upload.uploadedAt).padStart(15, "0")}:${upload.filename}`;
      await this.ctx.storage.put(key, upload);
      const existing = await this.ctx.storage.getAlarm();
      if (existing === null) {
        await this.ctx.storage.setAlarm(Date.now() + INGEST_QUEUE_DRAIN_MS);
      }
      return new Response("queued", { status: 202 });
    }
    if (url.pathname === "/purge" && request.method === "POST") {
      const all = await this.ctx.storage.list({ prefix: "work:" });
      await this.ctx.storage.delete([...all.keys()]);
      await this.ctx.storage.deleteAlarm();
      return Response.json({ purged: all.size });
    }
    return new Response("not found", { status: 404 });
  }

  async alarm(): Promise<void> {
    const list = await this.ctx.storage.list<PendingUpload>({ prefix: "work:", limit: 1 });
    if (list.size === 0) return;
    const [[key, upload]] = list.entries();
    const { channel, filename } = upload;

    let resp: Response;
    try {
      const container = getContainer(this.env.INDEXER, channel);
      const stagingKey = `${channel}/_incoming/${filename}`;
      resp = await container.fetch("http://container/ingest-package", {
        method: "POST",
        body: JSON.stringify({ channel, filename, staging_key: stagingKey }),
        headers: { "content-type": "application/json" },
      });
    } catch (err: unknown) {
      // Container platform unavailable — back off and retry the whole queue.
      const msg = err instanceof Error ? err.message : String(err);
      const isCapacity = msg.includes("no container instance") || msg.includes("try again later");
      const retryMs = isCapacity ? CONTAINER_UNAVAILABLE_RETRY_MS : 60_000;
      await this.ctx.storage.setAlarm(Date.now() + retryMs);
      return;
    }

    if (!resp.ok) {
      // Container returned an error — requeue with a delay.
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

    const remaining = await this.ctx.storage.list({ prefix: "work:", limit: 1 });
    if (remaining.size > 0) {
      await this.ctx.storage.setAlarm(Date.now() + INGEST_QUEUE_DRAIN_MS);
    }
  }
}
