package com.trading.kalyani.KTManager.service;

import com.trading.kalyani.KTManager.entity.MarketStructure;
import com.trading.kalyani.KTManager.model.HistoricalDataResponse.HistoricalCandle;

import java.util.List;
import java.util.Map;

/**
 * Service for Market Structure Analysis (SMC-based).
 *
 * Provides:
 * - Trend identification (HH/HL vs LH/LL)
 * - Change of Character (CHoCH) detection
 * - Premium/Discount zone calculation
 * - Market phase identification
 */
public interface MarketStructureService {

    // ==================== Core Analysis ====================

    /**
     * Analyze market structure for an instrument
     */
    MarketStructure analyzeMarketStructure(Long instrumentToken, String timeframe);

    /**
     * Analyze market structure using provided candles
     */
    MarketStructure analyzeMarketStructure(Long instrumentToken, String timeframe, List<HistoricalCandle> candles);

    /**
     * Get latest market structure analysis
     */
    MarketStructure getLatestAnalysis(Long instrumentToken, String timeframe);

    /**
     * Analyze all configured indices
     */
    Map<String, MarketStructure> analyzeAllIndices();

    // ==================== Trend Analysis ====================

    /**
     * Get current trend direction
     */
    String getTrendDirection(Long instrumentToken, String timeframe);

    /**
     * Get trend strength
     */
    String getTrendStrength(Long instrumentToken, String timeframe);

    /**
     * Check if price is making higher highs and higher lows (uptrend)
     */
    boolean isUptrend(Long instrumentToken, String timeframe);

    /**
     * Check if price is making lower highs and lower lows (downtrend)
     */
    boolean isDowntrend(Long instrumentToken, String timeframe);

    /**
     * Get trend alignment across multiple timeframes
     */
    Map<String, String> getMultiTimeframeTrend(Long instrumentToken, List<String> timeframes);

    // ==================== CHoCH Detection ====================

    /**
     * Check for Change of Character (trend reversal)
     */
    boolean hasChochOccurred(Long instrumentToken, String timeframe);

    /**
     * Get latest CHoCH details
     */
    Map<String, Object> getLatestChoch(Long instrumentToken, String timeframe);

    /**
     * Get CHoCH history
     */
    List<Map<String, Object>> getChochHistory(Long instrumentToken, String timeframe, int limit);

    // ==================== Premium/Discount Zones ====================

    /**
     * Get current price zone (PREMIUM/DISCOUNT/EQUILIBRIUM)
     */
    String getPriceZone(Long instrumentToken, String timeframe);

    /**
     * Get premium/discount zone levels
     */
    Map<String, Double> getZoneLevels(Long instrumentToken, String timeframe);

    /**
     * Check if price is in discount zone (good for longs)
     */
    boolean isInDiscountZone(Long instrumentToken, String timeframe);

    /**
     * Check if price is in premium zone (good for shorts)
     */
    boolean isInPremiumZone(Long instrumentToken, String timeframe);

    // ==================== Market Phase ====================

    /**
     * Get current market phase
     */
    String getMarketPhase(Long instrumentToken, String timeframe);

    /**
     * Get market phase with confidence
     */
    Map<String, Object> getMarketPhaseDetails(Long instrumentToken, String timeframe);

    /**
     * Check if in accumulation phase (bullish setup)
     */
    boolean isAccumulationPhase(Long instrumentToken, String timeframe);

    /**
     * Check if in distribution phase (bearish setup)
     */
    boolean isDistributionPhase(Long instrumentToken, String timeframe);

    // ==================== Trade Bias ====================

    /**
     * Get overall trade bias based on market structure
     */
    String getOverallBias(Long instrumentToken, String timeframe);

    /**
     * Get bias with confidence score
     */
    Map<String, Object> getBiasWithConfidence(Long instrumentToken, String timeframe);

    /**
     * Check if IOB trade direction aligns with market structure
     */
    boolean isTradeAlignedWithStructure(Long instrumentToken, String timeframe, String tradeDirection);

    // ==================== Swing Points ====================

    /**
     * Get recent swing high levels
     */
    List<Map<String, Object>> getSwingHighs(Long instrumentToken, String timeframe, int limit);

    /**
     * Get recent swing low levels
     */
    List<Map<String, Object>> getSwingLows(Long instrumentToken, String timeframe, int limit);

    /**
     * Get key support/resistance levels from swing points
     */
    Map<String, List<Double>> getKeyLevels(Long instrumentToken, String timeframe);

    // ==================== Dashboard ====================

    /**
     * Get comprehensive market structure dashboard
     */
    Map<String, Object> getDashboard(Long instrumentToken);

    /**
     * Get market structure summary for multiple instruments
     */
    Map<String, Map<String, Object>> getMultiInstrumentSummary(List<Long> instrumentTokens);
}
