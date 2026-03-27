-- Create prediction_deviation table for tracking prediction accuracy and deviations
-- This table stores deviation statistics calculated every 15 minutes during market hours

-- Create sequence for primary key
CREATE SEQUENCE IF NOT EXISTS prediction_deviation_seq START 1 INCREMENT 1;

-- Create the prediction_deviation table
CREATE TABLE IF NOT EXISTS prediction_deviation (
    id BIGINT PRIMARY KEY DEFAULT nextval('prediction_deviation_seq'),
    instrument_token BIGINT NOT NULL,
    verification_time TIMESTAMP NOT NULL,
    trading_date DATE NOT NULL,

    -- Batch info
    batch_id VARCHAR(20),
    predictions_verified INTEGER,

    -- Close price deviation metrics
    avg_close_deviation DOUBLE PRECISION,
    avg_close_deviation_percent DOUBLE PRECISION,
    max_close_deviation DOUBLE PRECISION,
    min_close_deviation DOUBLE PRECISION,
    close_deviation_std DOUBLE PRECISION,

    -- High/Low price deviation metrics
    avg_high_deviation DOUBLE PRECISION,
    avg_high_deviation_percent DOUBLE PRECISION,
    avg_low_deviation DOUBLE PRECISION,
    avg_low_deviation_percent DOUBLE PRECISION,

    -- Directional accuracy
    direction_accuracy_percent DOUBLE PRECISION,
    bullish_predictions INTEGER,
    bearish_predictions INTEGER,
    neutral_predictions INTEGER,
    correct_bullish INTEGER,
    correct_bearish INTEGER,

    -- Bias tracking
    systematic_bias DOUBLE PRECISION,
    bias_direction VARCHAR(20),

    -- Volatility comparison
    predicted_avg_volatility DOUBLE PRECISION,
    actual_avg_volatility DOUBLE PRECISION,
    volatility_underestimate_ratio DOUBLE PRECISION,

    -- Market context
    avg_pcr DOUBLE PRECISION,
    avg_vix DOUBLE PRECISION,
    market_trend VARCHAR(20),

    -- Time context
    market_hour INTEGER,
    is_opening_hour BOOLEAN,
    is_closing_hour BOOLEAN,
    day_of_week INTEGER,
    is_expiry_day BOOLEAN,

    -- Correction factors
    suggested_close_correction DOUBLE PRECISION,
    suggested_volatility_correction DOUBLE PRECISION,

    -- Cumulative tracking
    cumulative_sessions INTEGER,
    cumulative_avg_deviation DOUBLE PRECISION,
    cumulative_accuracy DOUBLE PRECISION,

    -- Sequence-wise deviations (1-5 candles)
    seq1_avg_deviation DOUBLE PRECISION,
    seq2_avg_deviation DOUBLE PRECISION,
    seq3_avg_deviation DOUBLE PRECISION,
    seq4_avg_deviation DOUBLE PRECISION,
    seq5_avg_deviation DOUBLE PRECISION,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_pred_dev_instrument_token ON prediction_deviation(instrument_token);
CREATE INDEX IF NOT EXISTS idx_pred_dev_trading_date ON prediction_deviation(trading_date);
CREATE INDEX IF NOT EXISTS idx_pred_dev_verification_time ON prediction_deviation(verification_time);
CREATE INDEX IF NOT EXISTS idx_pred_dev_market_hour ON prediction_deviation(market_hour);
CREATE INDEX IF NOT EXISTS idx_pred_dev_is_expiry_day ON prediction_deviation(is_expiry_day);

-- Add comment to table
COMMENT ON TABLE prediction_deviation IS 'Stores prediction deviation statistics calculated every 15 minutes for analysis and correction of prediction algorithms';

