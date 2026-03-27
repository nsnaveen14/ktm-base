package com.trading.kalyani.KTManager.service;

import com.trading.kalyani.KTManager.entity.CandleStick;
import com.trading.kalyani.KTManager.entity.IndexLTP;
import com.trading.kalyani.KTManager.entity.MiniDelta;
import com.trading.kalyani.KTManager.entity.PredictedCandleStick;
import com.trading.kalyani.KTManager.entity.PredictionDeviation;
import com.trading.kalyani.KTManager.entity.TradeSetupEntity;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Service for predicting the next five 1-minute candles for NIFTY 50.
 * Uses technical analysis, option chain data, and market microstructure
 * to generate predictions for intraday option trading.
 */
public interface CandlePredictionService {

    /**
     * Generate predictions for the next 5 one-minute candles.
     * This is the main entry point for candle prediction.
     *
     * @param appJobConfigNum The job configuration number
     * @param startTime
     * @param endTime
     * @return List of 5 predicted candles
     */
    List<PredictedCandleStick> predictNextFiveCandles(Integer appJobConfigNum, LocalDateTime startTime, LocalDateTime endTime);

    /**
     * Get the latest predictions for NIFTY without regenerating.
     *
     * @return List of most recent predictions
     */
    List<PredictedCandleStick> getLatestPredictions();

    /**
     * Verify past predictions against actual candle data and update accuracy metrics.
     *
     * @return Number of predictions verified
     */
    int verifyPastPredictions();

    /**
     * Get prediction accuracy for the current trading session.
     *
     * @return Average prediction accuracy as percentage
     */
    Double getSessionAccuracy();

    /**
     * Analyze the current market structure based on option chain.
     * Returns the bias direction: BULLISH, BEARISH, or NEUTRAL.
     *
     * @param miniDeltaList Option chain data
     * @param indexLTP Current index LTP and market parameters
     * @return Market bias direction
     */
    String analyzeMarketBias(List<MiniDelta> miniDeltaList, IndexLTP indexLTP);

    /**
     * Calculate expected volatility for the next candles based on
     * ATR, option premiums, and recent price action.
     *
     * @param recentCandles Recent candle data
     * @param indexLTP Current index parameters
     * @return Expected volatility in points
     */
    Double calculateExpectedVolatility(List<CandleStick> recentCandles, IndexLTP indexLTP);

    /**
     * Get the recommended trade direction based on predictions.
     *
     * @return BULLISH, BEARISH, or NEUTRAL
     */
    String getTradeRecommendation();

    /**
     * Clean up old prediction data to maintain database performance.
     *
     * @param daysToRetain Number of days of predictions to retain
     * @return Number of records deleted
     */
    int cleanupOldPredictions(int daysToRetain);

    /**
     * Get the latest valid trade setup for NIFTY.
     *
     * @return Optional containing the latest valid trade setup, or empty if none exists
     */
    Optional<TradeSetupEntity> getLatestTradeSetup();

    /**
     * Get all trade setups for today.
     *
     * @return List of trade setups created today
     */
    List<TradeSetupEntity> getTodayTradeSetups();

    /**
     * Get trade setup performance metrics.
     *
     * @return Map containing win rate, total P/L, and other metrics
     */
    java.util.Map<String, Object> getTradeSetupPerformance();

    // ========== Automated Prediction Job Methods ==========

    /**
     * Execute prediction job - called every minute during market hours.
     * Generates predictions for the next 5 minutes and saves them to the database.
     */
    void executePredictionJob();

    /**
     * Verify predictions and calculate deviations - called every 15 minutes.
     * Compares predicted vs actual candles and stores deviation statistics.
     *
     * @return PredictionDeviation containing the calculated statistics
     */
    PredictionDeviation verifyAndCalculateDeviation();

    /**
     * Verify predictions and calculate deviations with force option.
     * When forceCalculation is true, bypasses ticker check and uses wider time window.
     *
     * @param forceCalculation if true, bypasses normal checks and uses wider time window
     * @return PredictionDeviation containing the calculated statistics
     */
    PredictionDeviation verifyAndCalculateDeviation(boolean forceCalculation);

    /**
     * Get the latest deviation statistics.
     *
     * @return Optional containing the latest deviation record
     */
    Optional<PredictionDeviation> getLatestDeviation();

    /**
     * Get deviation statistics for today's trading session.
     *
     * @return List of all deviation records for today
     */
    List<PredictionDeviation> getTodayDeviations();

    /**
     * Get correction factors based on historical deviations.
     * These factors should be applied to improve future predictions.
     *
     * @return Map containing correction factors for different parameters
     */
    java.util.Map<String, Double> getCorrectionFactors();

    /**
     * Check if prediction job is currently active/running.
     *
     * @return true if the job is active
     */
    boolean isPredictionJobActive();

    /**
     * Start the automated prediction job.
     */
    void startPredictionJob();

    /**
     * Stop the automated prediction job.
     */
    void stopPredictionJob();

    /**
     * Get prediction job statistics including total predictions,
     * verifications, average accuracy, etc.
     *
     * @return Map containing job statistics
     */
    java.util.Map<String, Object> getPredictionJobStats();

    /**
     * Get live tick data for NIFTY 50 and ATM CE/PE strike prices.
     *
     * @return Map containing live LTP data for NIFTY, ATM CE, and ATM PE
     */
    Map<String, Object> getLiveTickData();

    /**
     * Get EMA chart data for displaying 9 EMA and 40 EMA lines.
     *
     * @return Map containing EMA series data with timestamps
     */
    Map<String, Object> getEMAChartData();

    /**
     * Get rolling chart data for predicted vs actual candles.
     * Returns 30-minute window with:
     * - First 25 minutes: Both actual and predicted candle close prices
     * - Last 5 minutes: Only predicted candles (future predictions)
     *
     * @return Map containing actualSeries, predictedSeries, and metadata
     */
    Map<String, Object> getRollingChartData();

    /**
     * Request subscription for a list of instrument tokens so live ticks for these tokens are delivered to the job ticker.
     * Implementations should be resilient (no-op if ticker not available).
     * @param tokens List of instrument tokens to subscribe
     */
    void subscribeTokenForJob(java.util.List<Long> tokens);

    /**
     * Return the latest LTP for a given instrument token (if available). Implementations should return null if
     * the token is not subscribed or tick not yet arrived.
     * @param token instrument token
     * @return last traded price or null
     */
    Double getLTPForToken(Long token);

    /**
     * Return the latest tick timestamp (epoch millis) for a given instrument token if available.
     * This helps callers decide whether the price is fresh enough to use for exits.
     * @param token instrument token
     * @return epoch millis of the tick or null
     */
    Long getTickTimestampForToken(Long token);
}
