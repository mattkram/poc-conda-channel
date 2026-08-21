import { DurableObject } from "cloudflare:workers";
import type { Env, PendingUpload } from "../types.js";

const DEBOUNCE_MS = 5_000;

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

    if (url.pathname === "/owner" && request.method === "GET") {
      const owner = (await this.ctx.storage.get<string>("owner")) ?? null;
      const visibility = (await this.ctx.storage.get<string>("visibility")) ?? "public";
      return Response.json({ owner, visibility });
    }

    if (url.pathname === "/set-visibility" && request.method === "POST") {
      const { login, visibility } = await request.json<{ login: string; visibility: string }>();
      if (visibility !== "public" && visibility !== "private") {
        return new Response("visibility must be 'public' or 'private'", { status: 400 });
      }
      const owner = await this.ctx.storage.get<string>("owner");
      if (!owner) return new Response("channel has no owner yet", { status: 409 });
      if (owner !== login) {
        return new Response("only the channel owner can change visibility", { status: 403 });
      }
      await this.ctx.storage.put("visibility", visibility);
      return Response.json({ owner, visibility });
    }

    if (url.pathname === "/check-read" && request.method === "POST") {
      const { login } = await request.json<{ login: string | null }>();
      const visibility = (await this.ctx.storage.get<string>("visibility")) ?? "public";
      if (visibility === "public") return Response.json({ allowed: true });
      const owner = (await this.ctx.storage.get<string>("owner")) ?? null;
      if (login && login === owner) return Response.json({ allowed: true });
      return Response.json({ allowed: false, owner }, { status: 403 });
    }

    if (url.pathname === "/purge" && request.method === "POST") {
      const all = await this.ctx.storage.list({ prefix: "pending:" });
      await this.ctx.storage.delete([...all.keys()]);
      await this.ctx.storage.deleteAlarm();
      return Response.json({ purged: all.size });
    }

    return new Response("not found", { status: 404 });
  }

  async alarm(): Promise<void> {
    const pending = await this.ctx.storage.list<PendingUpload>({ prefix: "pending:" });
    if (pending.size === 0) return;

    const entries = [...pending.entries()];
    await Promise.all(
      entries.map(async ([key, upload]) => {
        const id = this.env.INGESTOR.idFromName(`${upload.channel}/${upload.filename}`);
        const ingestor = this.env.INGESTOR.get(id);
        try {
          const resp = await ingestor.fetch("http://ingestor/ingest", {
            method: "POST",
            body: JSON.stringify(upload),
            headers: { "content-type": "application/json" },
          });
          if (resp.ok) {
            await this.ctx.storage.delete(key);
          }
          // else: leave in pending, retry on next alarm
        } catch {
          // Network/DO error — leave in pending for retry
        }
      }),
    );

    const remaining = await this.ctx.storage.list({ prefix: "pending:" });
    if (remaining.size > 0) {
      await this.ctx.storage.setAlarm(Date.now() + DEBOUNCE_MS);
    }
  }
}
