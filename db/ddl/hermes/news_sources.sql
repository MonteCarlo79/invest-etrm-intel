-- hermes schema + news_sources table
-- Run once; news_screener.py also calls _init_db() on startup so this is for
-- manual bootstrapping or documentation purposes.

CREATE SCHEMA IF NOT EXISTS hermes;

CREATE TABLE IF NOT EXISTS hermes.news_sources (
    id                   SERIAL PRIMARY KEY,
    name                 TEXT NOT NULL,
    url                  TEXT NOT NULL,
    source_type          TEXT NOT NULL DEFAULT 'wechat',   -- 'wechat' | 'web' | 'rss'
    biz_id               TEXT,                             -- WeChat __biz param
    region_bucket        TEXT,                             -- 华北|华东|华南|西北|西南|东北|全国
    category_hint        TEXT,                             -- policy|market_rules|market_analytics|technology|industry_news|other
    scrape_config        JSONB,                            -- optional per-source selectors for web sources
    active               BOOLEAN NOT NULL DEFAULT TRUE,
    consecutive_failures INT NOT NULL DEFAULT 0,
    last_scraped_at      TIMESTAMPTZ,
    created_at           TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(name, url)
);

-- Add AI-metadata columns to staging.spot_knowledge_docs
ALTER TABLE staging.spot_knowledge_docs
    ADD COLUMN IF NOT EXISTS region_bucket   TEXT,
    ADD COLUMN IF NOT EXISTS region_province TEXT,
    ADD COLUMN IF NOT EXISTS source_name     TEXT,
    ADD COLUMN IF NOT EXISTS relevance_score INT,
    ADD COLUMN IF NOT EXISTS ai_summary      TEXT,
    ADD COLUMN IF NOT EXISTS published_at    TIMESTAMPTZ;
