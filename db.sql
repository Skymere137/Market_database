CREATE TABLE IF NOT EXISTS candle_meta (
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    table_name TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT NOW()
);