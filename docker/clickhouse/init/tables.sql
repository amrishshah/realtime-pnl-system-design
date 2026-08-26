CREATE TABLE IF NOT EXISTS ticks (
    symbol String,
    price Decimal64(4),
    event_time DateTime64(3),
    ingested_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree
ORDER BY (symbol, event_time);

-- Stage 13: durable audit trail of every PnL value the live path ever
-- computed and pushed. Without this, there's nothing to reconcile against
-- at EOD -- the `ticks` table above holds prices, not computed PnL.
CREATE TABLE IF NOT EXISTS pnl_audit (
    client_id String,
    symbol String,
    pnl Decimal64(4),
    price Decimal64(4),
    computed_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree
ORDER BY (client_id, symbol, computed_at);
