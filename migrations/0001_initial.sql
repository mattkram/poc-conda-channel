-- Channels: source of truth for ownership and visibility.
-- Replaces _channels-index.json in R2 and owner/visibility in ChannelQueue DO storage.
CREATE TABLE IF NOT EXISTS channels (
  name        TEXT PRIMARY KEY,
  owner       TEXT,                          -- GitHub login, NULL if unclaimed
  visibility  TEXT NOT NULL DEFAULT 'public'
                CHECK (visibility IN ('public', 'private')),
  created_at  INTEGER NOT NULL               -- unix ms
);

-- Packages: read-optimized projection of R2 _browse/<name>.json files.
-- One row per (channel, package name). Rebuilt from R2 by reconciliation job.
-- Source of truth remains R2; this table is always safely rebuildable.
CREATE TABLE IF NOT EXISTS packages (
  channel     TEXT NOT NULL REFERENCES channels(name) ON DELETE CASCADE,
  name        TEXT NOT NULL,
  version     TEXT NOT NULL,
  summary     TEXT,
  license     TEXT,
  home        TEXT,
  subdirs     TEXT NOT NULL DEFAULT '[]',    -- JSON array, e.g. '["linux-64","noarch"]'
  updated_at  INTEGER NOT NULL,              -- unix ms; used by incremental reconciliation
  PRIMARY KEY (channel, name)
);

-- Index for fast per-channel listing (also covered by PK but explicit for clarity).
CREATE INDEX IF NOT EXISTS idx_packages_channel ON packages(channel);

-- Index for case-insensitive name prefix search.
CREATE INDEX IF NOT EXISTS idx_packages_name ON packages(channel, name COLLATE NOCASE);

-- Full-text search over package name and summary.
CREATE VIRTUAL TABLE IF NOT EXISTS packages_fts USING fts5(
  name,
  summary,
  content=packages,
  content_rowid=rowid
);

-- Keep FTS in sync with the packages table via triggers.
CREATE TRIGGER IF NOT EXISTS packages_fts_insert
  AFTER INSERT ON packages BEGIN
    INSERT INTO packages_fts(rowid, name, summary)
    VALUES (new.rowid, new.name, new.summary);
  END;

CREATE TRIGGER IF NOT EXISTS packages_fts_delete
  AFTER DELETE ON packages BEGIN
    INSERT INTO packages_fts(packages_fts, rowid, name, summary)
    VALUES ('delete', old.rowid, old.name, old.summary);
  END;

CREATE TRIGGER IF NOT EXISTS packages_fts_update
  AFTER UPDATE ON packages BEGIN
    INSERT INTO packages_fts(packages_fts, rowid, name, summary)
    VALUES ('delete', old.rowid, old.name, old.summary);
    INSERT INTO packages_fts(rowid, name, summary)
    VALUES (new.rowid, new.name, new.summary);
  END;
