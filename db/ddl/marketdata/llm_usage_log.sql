-- Per-call LLM token usage for cost attribution (written by shared/usage_meter.py).
CREATE TABLE IF NOT EXISTS marketdata.llm_usage_log (
    id            BIGSERIAL PRIMARY KEY,
    ts            TIMESTAMPTZ NOT NULL DEFAULT now(),
    caller        TEXT NOT NULL,
    model         TEXT NOT NULL,
    input_tokens  INT,
    output_tokens INT
);

CREATE INDEX IF NOT EXISTS llm_usage_log_ts_idx ON marketdata.llm_usage_log (ts);
CREATE INDEX IF NOT EXISTS llm_usage_log_caller_ts_idx ON marketdata.llm_usage_log (caller, ts);
