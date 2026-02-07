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
  media_json TEXT,
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

CREATE TABLE IF NOT EXISTS curation_config (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  candidate_generation_enabled INTEGER NOT NULL DEFAULT 1,
  auto_publish_enabled INTEGER NOT NULL DEFAULT 1,
  rss_public_enabled INTEGER NOT NULL DEFAULT 1,
  target_daily_spend_usd REAL NOT NULL DEFAULT 1.0,
  daily_run_time_kst TEXT NOT NULL DEFAULT '06:00',
  publish_delay_minutes INTEGER NOT NULL DEFAULT 60,
  feed_size INTEGER NOT NULL DEFAULT 10,
  window_hours INTEGER NOT NULL DEFAULT 24,
  candidate_pool_size INTEGER NOT NULL DEFAULT 20,
  w_impact REAL NOT NULL DEFAULT 0.35,
  w_novelty REAL NOT NULL DEFAULT 0.25,
  w_utility REAL NOT NULL DEFAULT 0.25,
  w_accessibility REAL NOT NULL DEFAULT 0.15,
  recommend_threshold INTEGER NOT NULL DEFAULT 75,
  min_utility INTEGER NOT NULL DEFAULT 60,
  min_accessibility INTEGER NOT NULL DEFAULT 55,
  pick_easy INTEGER NOT NULL DEFAULT 6,
  pick_deep INTEGER NOT NULL DEFAULT 2,
  pick_action INTEGER NOT NULL DEFAULT 2,
  openrouter_model TEXT NOT NULL DEFAULT 'openai/gpt-4o-mini',
  input_cost_per_1m REAL NOT NULL DEFAULT 0.15,
  output_cost_per_1m REAL NOT NULL DEFAULT 0.60,
  prompt_version TEXT NOT NULL DEFAULT 'v1',
  llm_enabled INTEGER NOT NULL DEFAULT 1,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS curation_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_date_kst TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  total_posts INTEGER NOT NULL DEFAULT 0,
  rule_pass_count INTEGER NOT NULL DEFAULT 0,
  llm_eval_count INTEGER NOT NULL DEFAULT 0,
  llm_input_tokens INTEGER NOT NULL DEFAULT 0,
  llm_output_tokens INTEGER NOT NULL DEFAULT 0,
  llm_spend_usd REAL NOT NULL DEFAULT 0.0,
  budget_over_target INTEGER NOT NULL DEFAULT 0,
  budget_note TEXT,
  error_message TEXT
);

CREATE TABLE IF NOT EXISTS curation_candidates (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id INTEGER NOT NULL,
  post_id TEXT NOT NULL,
  source_id INTEGER,
  username TEXT,
  url TEXT NOT NULL,
  created_at TEXT,
  full_text TEXT,
  full_text_hash TEXT,
  representative_image_url TEXT,
  rule_score REAL NOT NULL DEFAULT 0,
  llm_total_score REAL NOT NULL DEFAULT 0,
  impact REAL NOT NULL DEFAULT 0,
  novelty REAL NOT NULL DEFAULT 0,
  utility REAL NOT NULL DEFAULT 0,
  accessibility REAL NOT NULL DEFAULT 0,
  difficulty INTEGER NOT NULL DEFAULT 1,
  flags_json TEXT,
  reasons_json TEXT,
  recommended_tier TEXT NOT NULL DEFAULT 'borderline',
  status TEXT NOT NULL DEFAULT 'pending',
  rank_order INTEGER NOT NULL DEFAULT 9999,
  generated_title TEXT,
  generated_summary TEXT,
  edited_title TEXT,
  edited_summary TEXT,
  edited_image_url TEXT,
  review_note TEXT,
  reviewed_by TEXT,
  reviewed_at TEXT,
  selected_for_publish INTEGER NOT NULL DEFAULT 1,
  FOREIGN KEY (run_id) REFERENCES curation_runs(id) ON DELETE CASCADE,
  UNIQUE(run_id, post_id)
);

CREATE TABLE IF NOT EXISTS curation_llm_cache (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  post_id TEXT NOT NULL,
  full_text_hash TEXT NOT NULL,
  model TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  task TEXT NOT NULL,
  result_json TEXT NOT NULL,
  input_tokens INTEGER NOT NULL DEFAULT 0,
  output_tokens INTEGER NOT NULL DEFAULT 0,
  cost_usd REAL NOT NULL DEFAULT 0.0,
  created_at TEXT NOT NULL,
  UNIQUE(post_id, full_text_hash, model, prompt_version, task)
);

CREATE TABLE IF NOT EXISTS curation_publications (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id INTEGER NOT NULL,
  feed_key TEXT NOT NULL DEFAULT 'ai-daily',
  published_at TEXT NOT NULL,
  publish_mode TEXT NOT NULL,
  published_by TEXT,
  item_count INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY (run_id) REFERENCES curation_runs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS curation_publication_items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  publication_id INTEGER NOT NULL,
  position INTEGER NOT NULL,
  post_id TEXT NOT NULL,
  username TEXT,
  url TEXT NOT NULL,
  created_at TEXT,
  title TEXT NOT NULL,
  summary TEXT NOT NULL,
  image_url TEXT,
  FOREIGN KEY (publication_id) REFERENCES curation_publications(id) ON DELETE CASCADE,
  UNIQUE(publication_id, position)
);

CREATE TABLE IF NOT EXISTS curated_rss_cache (
  feed_type TEXT NOT NULL,
  limit_count INTEGER NOT NULL,
  etag TEXT NOT NULL,
  last_modified TEXT,
  xml TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (feed_type, limit_count)
);

CREATE INDEX IF NOT EXISTS idx_posts_source_id ON posts(source_id);
CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at);
CREATE INDEX IF NOT EXISTS idx_posts_scraped_at ON posts(scraped_at);
CREATE INDEX IF NOT EXISTS idx_tokens_scope ON tokens(scope);
CREATE INDEX IF NOT EXISTS idx_rss_token_logs_token_id ON rss_token_logs(token_id);
CREATE INDEX IF NOT EXISTS idx_rss_token_logs_created_at ON rss_token_logs(created_at);
CREATE INDEX IF NOT EXISTS idx_rss_token_counters_token_id ON rss_token_counters(token_id);
CREATE INDEX IF NOT EXISTS idx_curation_runs_date ON curation_runs(run_date_kst);
CREATE INDEX IF NOT EXISTS idx_curation_candidates_run ON curation_candidates(run_id, rank_order);
CREATE INDEX IF NOT EXISTS idx_curation_candidates_status ON curation_candidates(run_id, status);
CREATE INDEX IF NOT EXISTS idx_curation_publications_run ON curation_publications(run_id);
