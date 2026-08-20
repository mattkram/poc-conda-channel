import { describe, it, expect } from "vitest";
import { esc, channelNamespace, fmtBytes, fmtDate, b64url } from "./utils.js";

describe("esc", () => {
  it("escapes HTML special characters", () => {
    expect(esc(`<script>alert("xss")</script>`)).toBe(
      "&lt;script&gt;alert(&quot;xss&quot;)&lt;/script&gt;",
    );
  });
  it("escapes ampersands", () => {
    expect(esc("a & b")).toBe("a &amp; b");
  });
  it("handles empty string", () => {
    expect(esc("")).toBe("");
  });
  it("handles null-like via ??", () => {
    expect(esc(undefined as any)).toBe("");
  });
});

describe("channelNamespace", () => {
  it("returns namespace for namespaced channel", () => {
    expect(channelNamespace("mattkram/main")).toBe("mattkram");
  });
  it("returns null for bare channel name", () => {
    expect(channelNamespace("main")).toBeNull();
  });
  it("handles multiple slashes (returns first segment)", () => {
    expect(channelNamespace("org/sub/channel")).toBe("org");
  });
});

describe("fmtBytes", () => {
  it("formats bytes", () => {
    expect(fmtBytes(512)).toBe("512 B");
  });
  it("formats kilobytes", () => {
    expect(fmtBytes(2048)).toBe("2.0 KB");
  });
  it("formats megabytes", () => {
    expect(fmtBytes(5 * 1024 * 1024)).toBe("5.0 MB");
  });
});

describe("fmtDate", () => {
  it("formats a unix timestamp in ms as ISO date", () => {
    // 2024-01-15 in ms
    expect(fmtDate(1705276800000)).toBe("2024-01-15");
  });
  it("returns empty string for undefined", () => {
    expect(fmtDate(undefined)).toBe("");
  });
  it("returns empty string for 0", () => {
    expect(fmtDate(0)).toBe("");
  });
});

describe("b64url", () => {
  it("encodes to base64url (no +/=)", () => {
    const result = b64url("hello world");
    expect(result).not.toContain("+");
    expect(result).not.toContain("/");
    expect(result).not.toContain("=");
  });
  it("round-trips through atob", () => {
    const input = JSON.stringify({ login: "alice", exp: 9999999999 });
    const encoded = b64url(input);
    const decoded = atob(encoded.replace(/-/g, "+").replace(/_/g, "/"));
    expect(decoded).toBe(input);
  });
});
