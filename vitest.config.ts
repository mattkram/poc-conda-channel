import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // All tests run in Node — crypto.subtle is available in Node 18+,
    // which covers all our pure-function tests including HMAC token logic.
    // Workers-runtime integration tests can be added later with a compatible
    // version of @cloudflare/vitest-pool-workers.
    include: ["src/**/*.test.ts"],
    environment: "node",
  },
});
