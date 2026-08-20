import { describe, it, expect } from "vitest";
import { validateChannelAndFilename, checkTokenScope } from "./upload.js";

describe("validateChannelAndFilename", () => {
  it("accepts valid bare channel + .conda file", () => {
    expect(validateChannelAndFilename("main", "numpy-1.0-py310_0.conda")).toBeNull();
  });
  it("accepts valid namespaced channel + .tar.bz2 file", () => {
    expect(validateChannelAndFilename("mattkram/main", "numpy-1.0-py310_0.tar.bz2")).toBeNull();
  });
  it("rejects channel with invalid characters", () => {
    expect(validateChannelAndFilename("BAD_CHANNEL!", "numpy-1.0-py310_0.conda")).toBe(
      "invalid channel name",
    );
  });
  it("rejects filename with path separator", () => {
    expect(validateChannelAndFilename("main", "sub/dir/file.conda")).toBe("invalid filename");
  });
  it("rejects unsupported file extension", () => {
    expect(validateChannelAndFilename("main", "package.zip")).toBe(
      "only .conda or .tar.bz2 packages are accepted",
    );
  });
});

describe("checkTokenScope", () => {
  const base = { login: "alice", exp: 9999999999 };

  it("allows when no scope is set", () => {
    expect(checkTokenScope(base, "main", "numpy-1.0-py310_0.conda")).toBeNull();
  });

  it("allows when channel scope matches", () => {
    expect(
      checkTokenScope({ ...base, channel: "mattkram/main" }, "mattkram/main", "pkg-1.0-0.conda"),
    ).toBeNull();
  });

  it("rejects when channel scope does not match", () => {
    const result = checkTokenScope(
      { ...base, channel: "mattkram/main" },
      "mattkram/other",
      "pkg-1.0-0.conda",
    );
    expect(result).toContain("scoped to channel");
  });

  it("allows when pkg scope matches filename prefix", () => {
    expect(
      checkTokenScope({ ...base, pkg: "numpy" }, "main", "numpy-1.26.0-py310_0.conda"),
    ).toBeNull();
  });

  it("rejects when pkg scope does not match filename", () => {
    const result = checkTokenScope({ ...base, pkg: "numpy" }, "main", "scipy-1.0-py310_0.conda");
    expect(result).toContain("scoped to package");
  });
});
