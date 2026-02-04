-- SQLite schema for RSS storage
-- Execute once on the server:
--   sqlite3 /path/to/rss_cache.db < db/schema.sql

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS feed_sources (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT NOT NULL UNIQUE,
  display_name TEXT,
  profile_url TEXT UNIQUE,
  include_replies INTEGER NOT NULL DEFAULT 1,
  max_reply_depth INTEGER NOT NULL DEFAULT 1,
  is_active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS posts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_id INTEGER NOT NULL,
  post_id TEXT NOT NULL UNIQUE,
  url TEXT NOT NULL,
  text TEXT,
  created_at TEXT,
  scraped_at TEXT NOT NULL,
  is_reply INTEGER NOT NULL DEFAULT 0,
  parent_post_id TEXT,
  FOREIGN KEY (source_id) REFERENCES feed_sources(id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS tokens (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  token TEXT NOT NULL UNIQUE,
  scope TEXT NOT NULL,
  created_at TEXT NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS rss_token_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  token_id INTEGER NOT NULL,
  username TEXT NOT NULL,
  ip TEXT,
  user_agent TEXT,
  status_code INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (token_id) REFERENCES tokens(id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS rss_rate_limits (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  window_seconds INTEGER NOT NULL DEFAULT 300,
  max_requests INTEGER NOT NULL DEFAULT 60,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rss_token_counters (
  token_id INTEGER NOT NULL,
  window_start INTEGER NOT NULL,
  count INTEGER NOT NULL,
  PRIMARY KEY (token_id, window_start),
  FOREIGN KEY (token_id) REFERENCES tokens(id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS rss_feed_cache (
  username TEXT NOT NULL,
  limit_count INTEGER NOT NULL,
  etag TEXT NOT NULL,
  last_modified TEXT,
  xml TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (username, limit_count)
);

CREATE TABLE IF NOT EXISTS rss_cache_policy (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  enabled INTEGER NOT NULL DEFAULT 1,
  ttl_seconds INTEGER NOT NULL DEFAULT 300,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rss_invalid_token_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  token_hash TEXT NOT NULL,
  username TEXT,
  ip TEXT,
  user_agent TEXT,
  status_code INTEGER NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rss_schedule (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  is_active INTEGER NOT NULL DEFAULT 1,
  interval_minutes INTEGER NOT NULL DEFAULT 30,
  start_time TEXT NOT NULL DEFAULT '09:00',
  last_run_at TEXT,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_posts_source_id ON posts(source_id);
CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at);
CREATE INDEX IF NOT EXISTS idx_posts_scraped_at ON posts(scraped_at);
CREATE INDEX IF NOT EXISTS idx_tokens_scope ON tokens(scope);
CREATE INDEX IF NOT EXISTS idx_rss_token_logs_token_id ON rss_token_logs(token_id);
CREATE INDEX IF NOT EXISTS idx_rss_token_logs_created_at ON rss_token_logs(created_at);
CREATE INDEX IF NOT EXISTS idx_rss_token_counters_token_id ON rss_token_counters(token_id);
