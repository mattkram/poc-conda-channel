import { getContainer } from "@cloudflare/containers";
import { DurableObject } from "cloudflare:workers";
import type { Env, PendingUpload } from "../types.js";

const INGEST_QUEUE_DRAIN_MS = 100;
// Back-off when the container platform has no capacity.
const CONTAINER_UNAVAILABLE_RETRY_MS = 15_000;
const CONTAINER_ERROR_RETRY_MS = 60_000;
// Maximum retries before giving up on a single package and logging it as dead.
const MAX_RETRIES = 10;

export class ChannelIngestQueue extends DurableObject<Env> {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/enqueue" && request.method === "POST") {
      const upload = await request.json<PendingUpload>();
      const key = `work:${String(upload.uploadedAt).padStart(15, "0")}:${upload.filename}`;
      // Only enqueue if not already present (idempotent).
      const existing = await this.ctx.storage.get(key);
      if (!existing) {
        await this.ctx.storage.put(key, { ...upload, retries: 0 });
      }
      if ((await this.ctx.storage.getAlarm()) === null) {
        await this.ctx.storage.setAlarm(Date.now() + INGEST_QUEUE_DRAIN_MS);
      }
      return new Response("queued", { status: 202 });
    }
    if (url.pathname === "/purge" && request.method === "POST") {
      const all = await this.ctx.storage.list({ prefix: "work:" });
      const dead = await this.ctx.storage.list({ prefix: "dead:" });
      await this.ctx.storage.delete([...all.keys(), ...dead.keys()]);
      await this.ctx.storage.deleteAlarm();
      return Response.json({ purged: all.size + dead.size });
    }
    if (url.pathname === "/status" && request.method === "GET") {
      const work = await this.ctx.storage.list({ prefix: "work:" });
      const dead = await this.ctx.storage.list({ prefix: "dead:" });
      return Response.json({ pending: work.size, dead: dead.size });
    }
    return new Response("not found", { status: 404 });
  }

  async alarm(): Promise<void> {
    const list = await this.ctx.storage.list<PendingUpload & { retries: number }>(
      { prefix: "work:", limit: 1 }
    );
    if (list.size === 0) return;
    const [[key, upload]] = list.entries();
    const { channel, filename } = upload;
    const retries = upload.retries ?? 0;

    // Dead-letter: too many retries, move to dead queue and log.
    if (retries >= MAX_RETRIES) {
      console.error(`[ChannelIngestQueue] dead-lettering ${channel}/${filename} after ${retries} retries`);
      await this.ctx.storage.put(`dead:${key.slice("work:".length)}`, upload);
      await this.ctx.storage.delete(key);
      await this._scheduleNext();
      return;
    }

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
      // Container platform unavailable — back off, keeping item at front of queue.
      const msg = err instanceof Error ? err.message : String(err);
      const isCapacity = msg.includes("no container instance") || msg.includes("try again later");
      const retryMs = isCapacity ? CONTAINER_UNAVAILABLE_RETRY_MS : CONTAINER_ERROR_RETRY_MS;
      await this.ctx.storage.put(key, { ...upload, retries: retries + 1 });
      await this.ctx.storage.setAlarm(Date.now() + retryMs);
      return;
    }

    if (!resp.ok) {
      // Container returned an error — increment retry count, move to back of queue.
      const newTs = String(Date.now() + CONTAINER_ERROR_RETRY_MS).padStart(15, "0");
      const retryKey = `work:${newTs}:${filename}`;
      await this.ctx.storage.put(retryKey, { ...upload, retries: retries + 1 });
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

    await this._scheduleNext();
  }

  private async _scheduleNext(): Promise<void> {
    const remaining = await this.ctx.storage.list({ prefix: "work:", limit: 1 });
    if (remaining.size > 0) {
      await this.ctx.storage.setAlarm(Date.now() + INGEST_QUEUE_DRAIN_MS);
    }
  }
}
