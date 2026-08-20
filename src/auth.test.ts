import { describe, it, expect } from "vitest";
import { signUploadToken, verifyUploadToken } from "./handlers/auth.js";
import { matchesRule } from "./handlers/trusted-publishers.js";
import type { TrustedPublisherRow } from "./types.js";

const SECRET = "test-secret-at-least-32-bytes-long!!";

describe("signUploadToken / verifyUploadToken", () => {
  it("round-trips a basic login token", async () => {
    const token = await signUploadToken({ login: "alice" }, SECRET);
    const claims = await verifyUploadToken(token, SECRET);
    expect(claims?.login).toBe("alice");
    expect(claims?.channel).toBeUndefined();
    expect(claims?.pkg).toBeUndefined();
  });

  it("round-trips a scoped token with channel + pkg", async () => {
    const token = await signUploadToken(
      { login: "alice", channel: "alice/main", pkg: "my-pkg" },
      SECRET,
    );
    const claims = await verifyUploadToken(token, SECRET);
    expect(claims?.login).toBe("alice");
    expect(claims?.channel).toBe("alice/main");
    expect(claims?.pkg).toBe("my-pkg");
  });

  it("returns null for a tampered token", async () => {
    const token = await signUploadToken({ login: "alice" }, SECRET);
    const tampered = token.slice(0, -4) + "XXXX";
    expect(await verifyUploadToken(tampered, SECRET)).toBeNull();
  });

  it("returns null for a token with wrong secret", async () => {
    const token = await signUploadToken({ login: "alice" }, SECRET);
    expect(await verifyUploadToken(token, "different-secret-at-least-32-bytes!!")).toBeNull();
  });

  it("returns null for an expired token (ttl=0)", async () => {
    // ttl=-1 ensures exp is already in the past
    const token = await signUploadToken({ login: "alice" }, SECRET, -1);
    expect(await verifyUploadToken(token, SECRET)).toBeNull();
  });

  it("respects custom TTL", async () => {
    const before = Math.floor(Date.now() / 1000);
    const token = await signUploadToken({ login: "alice" }, SECRET, 7200);
    // Decode the payload directly to check exp without relying on the return type.
    const [encoded] = token.split(".");
    const payload = JSON.parse(atob(encoded.replace(/-/g, "+").replace(/_/g, "/")));
    expect(payload.exp).toBeGreaterThanOrEqual(before + 7200 - 1);
    // Also confirm it verifies successfully.
    expect(await verifyUploadToken(token, SECRET)).not.toBeNull();
  });
});

// ---------------------------------------------------------------------------
// matchesRule — pure function, no Workers APIs needed but put here alongside
// the auth tests for organization.
// ---------------------------------------------------------------------------
describe("matchesRule", () => {
  const base: TrustedPublisherRow = {
    id: 1,
    channel: "alice/pkg",
    repository: null,
    workflow: null,
    environment: null,
    package_name: null,
    require_trusted: 0,
    created_at: 0,
    created_by: "alice",
  };

  it("wildcard rule (all nulls) matches anything", () => {
    expect(
      matchesRule(base, {
        repository: "alice/repo",
        workflow_ref: "refs/heads/main",
        environment: "production",
      }),
    ).toBe(true);
  });

  it("matches when repository matches exactly", () => {
    expect(
      matchesRule({ ...base, repository: "alice/repo" }, { repository: "alice/repo" }),
    ).toBe(true);
  });

  it("rejects when repository does not match", () => {
    expect(
      matchesRule({ ...base, repository: "alice/repo" }, { repository: "bob/repo" }),
    ).toBe(false);
  });

  it("prefix-matches workflow_ref", () => {
    expect(
      matchesRule(
        { ...base, workflow: "alice/repo/.github/workflows/publish.yml" },
        {
          workflow_ref:
            "alice/repo/.github/workflows/publish.yml@refs/heads/main",
        },
      ),
    ).toBe(true);
  });

  it("rejects when workflow prefix does not match", () => {
    expect(
      matchesRule(
        { ...base, workflow: "alice/repo/.github/workflows/publish.yml" },
        { workflow_ref: "alice/repo/.github/workflows/other.yml@refs/heads/main" },
      ),
    ).toBe(false);
  });

  it("matches when environment matches exactly", () => {
    expect(
      matchesRule({ ...base, environment: "production" }, { environment: "production" }),
    ).toBe(true);
  });

  it("rejects when environment does not match", () => {
    expect(
      matchesRule({ ...base, environment: "production" }, { environment: "staging" }),
    ).toBe(false);
  });
});
