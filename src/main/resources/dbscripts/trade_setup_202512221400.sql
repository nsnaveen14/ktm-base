-- Trade Setup Table for storing trade recommendations with entry, target, and stop-loss
-- Created: December 22, 2025

-- Create sequence for trade_setup table
CREATE SEQUENCE IF NOT EXISTS trade_setup_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Create trade_setup table
CREATE TABLE IF NOT EXISTS trade_setup (
    id BIGINT PRIMARY KEY DEFAULT nextval('trade_setup_seq'),

    -- Basic trade info
    instrument_token BIGINT,
    trade_direction VARCHAR(10),        -- BUY, SELL, NONE
    setup_type VARCHAR(50),             -- ORDER_BLOCK, CHANNEL_BOUNCE, FIB_RETRACEMENT, CONFLUENCE
    confidence DOUBLE PRECISION,        -- 0-100 confidence score

    -- Entry details
    entry_price DOUBLE PRECISION,
    entry_type VARCHAR(20),             -- LIMIT, MARKET, STOP
    entry_reason VARCHAR(500),

    -- Target details
    target1 DOUBLE PRECISION,           -- First target (conservative)
    target2 DOUBLE PRECISION,           -- Second target (moderate)
    target3 DOUBLE PRECISION,           -- Third target (aggressive)
    target_reason VARCHAR(500),

    -- Stop-loss details
    stop_loss DOUBLE PRECISION,
    trailing_stop_distance DOUBLE PRECISION,
    stop_loss_reason VARCHAR(500),

    -- Risk-Reward analysis
    risk_points DOUBLE PRECISION,
    reward_points1 DOUBLE PRECISION,
    risk_reward_ratio1 DOUBLE PRECISION,
    risk_reward_ratio2 DOUBLE PRECISION,

    -- Option trading specific
    suggested_option_type VARCHAR(5),   -- CE, PE
    suggested_strike INTEGER,
    option_strategy VARCHAR(20),        -- BUY_CE, BUY_PE, SELL_CE, SELL_PE

    -- Market context at setup time
    current_price DOUBLE PRECISION,
    market_trend VARCHAR(20),           -- BULLISH, BEARISH, NEUTRAL
    smc_bias VARCHAR(20),               -- Smart Money Concepts bias
    smc_confidence DOUBLE PRECISION,
    channel_type VARCHAR(20),           -- ASCENDING, DESCENDING, HORIZONTAL
    nearest_fib_level VARCHAR(20),
    pcr_value DOUBLE PRECISION,
    max_pain_strike INTEGER,

    -- Validity and tracking
    created_at TIMESTAMP,
    valid_until TIMESTAMP,
    is_valid BOOLEAN DEFAULT true,
    invalid_reason VARCHAR(500),

    -- Outcome tracking
    is_executed BOOLEAN DEFAULT false,
    execution_price DOUBLE PRECISION,
    execution_time TIMESTAMP,
    is_closed BOOLEAN DEFAULT false,
    exit_price DOUBLE PRECISION,
    exit_time TIMESTAMP,
    exit_reason VARCHAR(50),            -- TARGET1_HIT, TARGET2_HIT, TARGET3_HIT, STOPLOSS_HIT, MANUAL, EXPIRED
    profit_loss DOUBLE PRECISION,
    profit_loss_percent DOUBLE PRECISION
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_trade_setup_instrument_token ON trade_setup(instrument_token);
CREATE INDEX IF NOT EXISTS idx_trade_setup_created_at ON trade_setup(created_at);
CREATE INDEX IF NOT EXISTS idx_trade_setup_valid_until ON trade_setup(valid_until);
CREATE INDEX IF NOT EXISTS idx_trade_setup_is_valid ON trade_setup(is_valid);
CREATE INDEX IF NOT EXISTS idx_trade_setup_is_closed ON trade_setup(is_closed);
CREATE INDEX IF NOT EXISTS idx_trade_setup_trade_direction ON trade_setup(trade_direction);

-- Grant permissions (adjust as needed for your setup)
-- GRANT ALL PRIVILEGES ON TABLE trade_setup TO your_user;
-- GRANT USAGE, SELECT ON SEQUENCE trade_setup_seq TO your_user;

COMMENT ON TABLE trade_setup IS 'Stores trade setup recommendations with entry, target, and stop-loss levels';
COMMENT ON COLUMN trade_setup.setup_type IS 'Type of setup: ORDER_BLOCK, CHANNEL_BOUNCE, FIB_RETRACEMENT, CONFLUENCE';
COMMENT ON COLUMN trade_setup.exit_reason IS 'Reason for exit: TARGET1_HIT, TARGET2_HIT, TARGET3_HIT, STOPLOSS_HIT, MANUAL, EXPIRED';

