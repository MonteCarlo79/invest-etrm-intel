CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.mengxi_agent4_status (
    pipeline_name            text PRIMARY KEY,
    province                 text NOT NULL,
    trust_state              text NOT NULL,
    last_run_status          text,
    failure_class            text,
    evidence_summary         text,
    heuristic_summary        text,
    recommended_action       text,
    latest_success_file_date date,
    latest_quality_date      date,
    expected_file_date       date,
    freshness_lag_days       integer,
    status_payload           jsonb,
    source_log_group         text,
    source_log_stream        text,
    source_event_time        timestamptz,
    updated_at               timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.mengxi_agent4_alert_state (
    incident_key         text PRIMARY KEY,
    pipeline_name        text NOT NULL,
    failure_class        text NOT NULL,
    trust_state          text NOT NULL,
    first_observed_at    timestamptz NOT NULL DEFAULT now(),
    last_observed_at     timestamptz NOT NULL DEFAULT now(),
    last_alert_sent_at   timestamptz,
    resolved_at          timestamptz,
    occurrence_count     integer NOT NULL DEFAULT 1,
    evidence_summary     text,
    recommended_action   text,
    status_payload       jsonb
);

CREATE INDEX IF NOT EXISTS idx_mengxi_agent4_alert_state_active
ON ops.mengxi_agent4_alert_state (pipeline_name, failure_class, resolved_at);
