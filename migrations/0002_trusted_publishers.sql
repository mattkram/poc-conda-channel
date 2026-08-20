-- Trusted publishers: GitHub Actions OIDC rules for keyless upload auth.
--
-- A rule matches an incoming OIDC token when ALL non-null fields match the
-- corresponding JWT claim. NULL means "any value accepted" (wildcard).
--
-- Matching logic (see worker.ts matchesRule):
--   repository  matches jwt.repository        (e.g. "anaconda/conda-channel")
--   workflow    matches jwt.workflow_ref       (e.g. "refs/heads/main" prefix or exact)
--   environment matches jwt.environment        (e.g. "production")
--   package_name is NOT a JWT claim — it scopes the token minted on success,
--               restricting uploads to that package name only.
--
-- require_trusted: if TRUE, normal Bearer upload tokens are rejected for this
-- channel; OIDC exchange is the only valid upload path. Default FALSE so
-- existing channels keep working unchanged.

CREATE TABLE IF NOT EXISTS trusted_publishers (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  channel       TEXT NOT NULL REFERENCES channels(name) ON DELETE CASCADE,
  -- GitHub OIDC claim matchers (NULL = wildcard / match any)
  repository    TEXT,         -- e.g. "anaconda/my-repo"
  workflow      TEXT,         -- prefix-matched against workflow_ref, e.g. ".github/workflows/publish.yml"
  environment   TEXT,         -- e.g. "production"
  -- Optional upload scope restriction
  package_name  TEXT,         -- if set, token minted is scoped to this package only
  -- Channel-level policy
  require_trusted INTEGER NOT NULL DEFAULT 0  -- boolean: 1 = OIDC-only, 0 = OIDC or token
    CHECK (require_trusted IN (0, 1)),
  created_at    INTEGER NOT NULL,             -- unix ms
  created_by    TEXT NOT NULL                 -- GitHub login of the owner who added the rule
);

CREATE INDEX IF NOT EXISTS idx_tp_channel ON trusted_publishers(channel);
