-- Schema definition for LiteLLM Spend Aggregator

CREATE TABLE IF NOT EXISTS cost_logs (
    id BIGSERIAL PRIMARY KEY,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    cost_usd FLOAT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cost_logs_provider_model
ON cost_logs(provider, model);

CREATE TABLE IF NOT EXISTS spend_summary (
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    total_cost FLOAT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (provider, model)
);

ALTER TABLE spend_summary ADD CONSTRAINT uniq_provider_model UNIQUE (provider, model);
