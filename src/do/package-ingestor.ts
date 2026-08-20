import { DurableObject } from "cloudflare:workers";
import type { Env, PendingUpload } from "../types.js";

export class PackageIngestor extends DurableObject<Env> {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname !== "/ingest" || request.method !== "POST") {
      return new Response("not found", { status: 404 });
    }
    const upload = await request.json<PendingUpload>();
    const queueId = this.env.INGEST_QUEUE.idFromName(upload.channel);
    const queue = this.env.INGEST_QUEUE.get(queueId);
    await queue.fetch("http://ingest-queue/enqueue", {
      method: "POST",
      body: JSON.stringify(upload),
      headers: { "content-type": "application/json" },
    });
    return new Response("queued", { status: 202 });
  }
}
