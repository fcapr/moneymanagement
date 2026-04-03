CREATE TABLE IF NOT EXISTS transactions (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL,
    profile VARCHAR(64) NOT NULL,
    category VARCHAR(128) NOT NULL,
    subcategory VARCHAR(128) DEFAULT '',
    amount NUMERIC(14, 2) NOT NULL,
    currency VARCHAR(8) NOT NULL DEFAULT 'EUR',
    amount_eur NUMERIC(14, 2) NOT NULL,
    type VARCHAR(16) NOT NULL CHECK (type IN ('Expense', 'Income')),
    notes TEXT DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(date DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_profile ON transactions(profile);
CREATE INDEX IF NOT EXISTS idx_transactions_type ON transactions(type);
CREATE INDEX IF NOT EXISTS idx_transactions_category ON transactions(category);

CREATE TABLE IF NOT EXISTS portfolio_holdings (
    id BIGSERIAL PRIMARY KEY,
    profile VARCHAR(64) NOT NULL,
    ticker VARCHAR(32) NOT NULL,
    isin VARCHAR(32),
    quantity NUMERIC(16, 6) NOT NULL DEFAULT 0,
    purchase_price NUMERIC(16, 6),
    ter NUMERIC(8, 4)
);

CREATE TABLE IF NOT EXISTS app_config (
    key TEXT PRIMARY KEY,
    value JSONB NOT NULL
);
