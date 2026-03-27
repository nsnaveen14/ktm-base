package com.trading.kalyani.KTManager.service;

import com.trading.kalyani.KTManager.entity.RiskManagement;
import com.trading.kalyani.KTManager.entity.InternalOrderBlock;
import com.trading.kalyani.KTManager.model.HistoricalDataResponse.HistoricalCandle;

import java.util.List;
import java.util.Map;

/**
 * Service for Risk Management.
 *
 * Provides:
 * - Position sizing based on account risk percentage
 * - Dynamic SL placement based on ATR
 * - Maximum daily loss limits
 * - Portfolio heat tracking
 * - Correlation-based exposure limits
 */
public interface RiskManagementService {

    // ==================== Account Configuration ====================

    /**
     * Initialize or update account configuration
     */
    RiskManagement initializeAccount(Double accountCapital, Double riskPerTradePercent,
                                     Double maxDailyLossPercent, Double maxPortfolioHeatPercent);

    /**
     * Get current account configuration
     */
    RiskManagement getAccountConfig();

    /**
     * Update account capital
     */
    void updateAccountCapital(Double newCapital);

    /**
     * Update risk parameters
     */
    void updateRiskParameters(Double riskPerTradePercent, Double maxDailyLossPercent,
                              Double maxPortfolioHeatPercent);

    // ==================== Position Sizing ====================

    /**
     * Calculate position size based on risk parameters
     */
    Map<String, Object> calculatePositionSize(Long instrumentToken, Double entryPrice,
                                               Double stopLoss, Integer lotSize);

    /**
     * Calculate position size for an IOB trade
     */
    Map<String, Object> calculatePositionSizeForIOB(InternalOrderBlock iob);

    /**
     * Get maximum affordable position size
     */
    Integer getMaxPositionSize(Long instrumentToken, Double entryPrice, Double stopLoss);

    /**
     * Calculate risk amount for a position
     */
    Double calculateRiskAmount(Integer quantity, Double entryPrice, Double stopLoss);

    // ==================== ATR-Based Risk ====================

    /**
     * Calculate Average True Range (ATR)
     */
    Double calculateATR(List<HistoricalCandle> candles, int period);

    /**
     * Calculate ATR for an instrument
     */
    Double calculateATR(Long instrumentToken, String timeframe, int period);

    /**
     * Calculate dynamic stop loss based on ATR
     */
    Double calculateDynamicStopLoss(Long instrumentToken, String tradeDirection,
                                    Double entryPrice, Double atrMultiplier);

    /**
     * Get ATR as percentage of price (volatility measure)
     */
    Double getATRPercent(Long instrumentToken, String timeframe);

    /**
     * Adjust position size based on volatility
     */
    Integer adjustPositionSizeForVolatility(Integer basePositionSize, Double currentATRPercent,
                                            Double normalATRPercent);

    // ==================== Daily Loss Limits ====================

    /**
     * Check if daily loss limit has been reached
     */
    boolean isDailyLossLimitReached();

    /**
     * Get remaining daily loss allowance
     */
    Double getRemainingDailyLoss();

    /**
     * Record a trade P&L
     */
    void recordTradePnl(Double pnl, boolean isRealized);

    /**
     * Update daily P&L from open positions
     */
    void updateUnrealizedPnl(Double unrealizedPnl);

    /**
     * Get today's P&L summary
     */
    Map<String, Object> getDailyPnlSummary();

    /**
     * Check if daily trade limit reached
     */
    boolean isDailyTradeLimitReached();

    /**
     * Increment daily trade count
     */
    void incrementDailyTradeCount();

    // ==================== Portfolio Heat ====================

    /**
     * Calculate current portfolio heat
     */
    Double calculatePortfolioHeat();

    /**
     * Check if portfolio heat limit exceeded
     */
    boolean isPortfolioHeatExceeded();

    /**
     * Get remaining risk capacity
     */
    Double getRemainingRiskCapacity();

    /**
     * Add position to portfolio tracking
     */
    void addOpenPosition(Long instrumentToken, Double riskAmount, Integer quantity);

    /**
     * Remove position from portfolio tracking
     */
    void removeOpenPosition(Long instrumentToken);

    /**
     * Get all open positions with risk
     */
    Map<Long, Map<String, Object>> getOpenPositionsRisk();

    /**
     * Get portfolio heat summary
     */
    Map<String, Object> getPortfolioHeatSummary();

    // ==================== Exposure Limits ====================

    /**
     * Check instrument exposure limit
     */
    boolean isInstrumentExposureLimitReached(Long instrumentToken, Double additionalExposure);

    /**
     * Check correlated exposure limit (e.g., all indices)
     */
    boolean isCorrelatedExposureLimitReached(String correlationGroup, Double additionalExposure);

    /**
     * Get current exposure by correlation group
     */
    Map<String, Double> getExposureByCorrelationGroup();

    /**
     * Get instrument correlation group
     */
    String getCorrelationGroup(Long instrumentToken);

    // ==================== Risk Metrics ====================

    /**
     * Calculate win rate from trade history
     */
    Double calculateWinRate(int tradeDays);

    /**
     * Calculate average risk-reward achieved
     */
    Double calculateAverageRRAchieved(int tradeDays);

    /**
     * Calculate maximum drawdown
     */
    Double calculateMaxDrawdown(int tradeDays);

    /**
     * Calculate current drawdown from peak
     */
    Double calculateCurrentDrawdown();

    /**
     * Calculate profit factor
     */
    Double calculateProfitFactor(int tradeDays);

    /**
     * Get comprehensive risk metrics
     */
    Map<String, Object> getRiskMetrics(int tradeDays);

    // ==================== Trade Approval ====================

    /**
     * Check if trading is allowed based on all risk parameters
     */
    boolean isTradingAllowed();

    /**
     * Get trading status with reason if blocked
     */
    Map<String, Object> getTradingStatus();

    /**
     * Validate a potential trade
     */
    Map<String, Object> validateTrade(Long instrumentToken, String tradeDirection,
                                       Double entryPrice, Double stopLoss, Integer quantity);

    /**
     * Get risk assessment score (0-100, higher = safer)
     */
    Double getRiskAssessmentScore();

    /**
     * Pre-trade risk check for IOB
     */
    Map<String, Object> preTradeRiskCheck(InternalOrderBlock iob);

    // ==================== Dashboard ====================

    /**
     * Get comprehensive risk management dashboard
     */
    Map<String, Object> getDashboard();

    /**
     * Get daily risk report
     */
    Map<String, Object> getDailyRiskReport();

    /**
     * Get risk alerts (limit warnings)
     */
    List<Map<String, Object>> getRiskAlerts();

    // ==================== Lifecycle ====================

    /**
     * Reset daily metrics (call at start of trading day)
     */
    void resetDailyMetrics();

    /**
     * End of day processing
     */
    void endOfDayProcessing();

    /**
     * Save current risk state
     */
    void saveRiskState();
}
