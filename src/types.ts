// ---------------------------------------------------------------------------
// Shared types used across modules.
// ---------------------------------------------------------------------------

export interface Env {
  CHANNEL_BUCKET: R2Bucket;
  DB: D1Database;
  INDEXER: DurableObjectNamespace;
  QUEUE: DurableObjectNamespace;
  INGESTOR: DurableObjectNamespace;
  MERGER: DurableObjectNamespace;
  INGEST_QUEUE: DurableObjectNamespace;
  GITHUB_CLIENT_ID: string;
  GITHUB_CLIENT_SECRET: string;
  GITHUB_ORG: string;
  UPLOAD_TOKEN_SECRET: string;
  R2_ACCESS_KEY_ID: string;
  R2_SECRET_ACCESS_KEY: string;
  R2_ACCOUNT_ID: string;
  R2_BUCKET_NAME: string;
}

export interface PendingUpload {
  channel: string;
  filename: string;
  uploadedAt: number;
  uploadedBy: string;
}

export interface ChannelRow {
  name: string;
  owner: string | null;
  visibility: string;
  created_at: number;
}

export interface BrowseRecord {
  name: string;
  version: string;
  summary: string;
  license: string;
  home: string;
  subdirs: string[];
}

export interface BuildEntry {
  subdir: string;
  filename: string;
  version: string;
  build: string;
  depends?: string[];
  size?: number;
  md5?: string;
  sha256?: string;
  timestamp?: number;
}

export interface UpsertPackageBody {
  channel: string;
  name: string;
  version: string;
  summary?: string | null;
  license?: string | null;
  home?: string | null;
  subdirs: string[];
}

export interface TrustedPublisherRow {
  id: number;
  channel: string;
  repository: string | null;
  workflow: string | null;
  environment: string | null;
  package_name: string | null;
  require_trusted: number;
  created_at: number;
  created_by: string;
}

export interface TokenClaims {
  login: string;
  channel?: string;
  pkg?: string;
  exp: number;
}
