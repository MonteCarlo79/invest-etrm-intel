-- staging.spot_report_summaries
-- AI-generated daily market summaries for China spot electricity market reports
--
-- One row per report_date.  Regenerating for the same date replaces the row.

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.spot_report_summaries (
    id                bigserial    PRIMARY KEY,
    report_date       date         NOT NULL UNIQUE,
    summary_text      text         NOT NULL,
    model             text,
    prompt_tokens     int,
    completion_tokens int,
    source_pdf        text,
    created_at        timestamptz  DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_spot_report_summaries_date
    ON staging.spot_report_summaries (report_date);
