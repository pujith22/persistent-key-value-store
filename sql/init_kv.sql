-- SQL init script for persistent key-value store
-- Creates table, inserts test rows, and selects them for verification

CREATE TABLE IF NOT EXISTS kv_store (
    key integer PRIMARY KEY,
    value text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO kv_store (key, value) VALUES (1, 'foo') ON CONFLICT (key) DO NOTHING;
INSERT INTO kv_store (key, value) VALUES (2, 'bar') ON CONFLICT (key) DO NOTHING;

-- Show a few rows for verification
SELECT * FROM kv_store LIMIT 10;
