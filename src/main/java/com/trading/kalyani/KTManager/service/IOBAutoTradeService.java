package com.trading.kalyani.KTManager.service;

import com.trading.kalyani.KTManager.entity.IOBTradeResult;
import com.trading.kalyani.KTManager.entity.InternalOrderBlock;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Service interface for IOB-based automated trading.
 * Handles trade execution, position management, and performance tracking.
 */
public interface IOBAutoTradeService {

    // ==================== Auto Trading Control ====================

    /**
     * Enable or disable IOB auto-trading
     */
    void setAutoTradingEnabled(boolean enabled);

    /**
     * Check if IOB auto-trading is enabled
     */
    boolean isAutoTradingEnabled();

    /**
     * Get auto-trading configuration
     */
    Map<String, Object> getAutoTradeConfig();

    /**
     * Update auto-trading configuration
     */
    void updateAutoTradeConfig(Map<String, Object> config);

    // ==================== Trade Entry ====================

    /**
     * Check if an IOB qualifies for auto-trade entry
     */
    boolean shouldEnterTrade(InternalOrderBlock iob, Double currentPrice);

    /**
     * Execute auto-trade entry for an IOB
     */
    IOBTradeResult enterTrade(InternalOrderBlock iob, Double currentPrice, String entryTrigger);

    /**
     * Manually enter a trade based on IOB
     */
    IOBTradeResult manualEnterTrade(Long iobId);

    // ==================== Trade Management ====================

    /**
     * Monitor all open IOB trades and check for exit conditions
     */
    List<IOBTradeResult> monitorOpenTrades();

    /**
     * Update trailing stop losses for open trades
     */
    void updateTrailingStopLosses(Double currentPrice);

    /**
     * Check and process trade exits based on price movement
     */
    List<IOBTradeResult> processExits(Long instrumentToken, Double currentPrice);

    /**
     * Exit a specific trade
     */
    IOBTradeResult exitTrade(Long tradeResultId, String exitReason, Double exitPrice);

    /**
     * Exit all open trades (e.g., at market close)
     */
    List<IOBTradeResult> exitAllOpenTrades(String exitReason);

    // ==================== Real-Time Processing ====================

    /**
     * Process a price tick for IOB trading
     * Called on each price update from WebSocket
     */
    void processPriceTick(Long instrumentToken, Double currentPrice, LocalDateTime tickTime);

    /**
     * Check for new IOB entries at current price
     */
    List<IOBTradeResult> checkForEntries(Long instrumentToken, Double currentPrice);

    // ==================== Performance Tracking ====================

    /**
     * Get today's IOB trading summary
     */
    Map<String, Object> getTodaysSummary();

    /**
     * Get open IOB trades
     */
    List<IOBTradeResult> getOpenTrades();

    /**
     * Get today's IOB trades
     */
    List<IOBTradeResult> getTodaysTrades();

    /**
     * Get trade history for date range
     */
    List<IOBTradeResult> getTradeHistory(LocalDate startDate, LocalDate endDate);

    /**
     * Get comprehensive IOB trading statistics
     */
    Map<String, Object> getPerformanceStatistics();

    /**
     * Get performance statistics for a specific period
     */
    Map<String, Object> getPerformanceStatistics(LocalDateTime startDate, LocalDateTime endDate);

    /**
     * Get win rate for closed trades
     */
    Double getWinRate(LocalDateTime startDate, LocalDateTime endDate);

    /**
     * Get average risk-reward achieved
     */
    Double getAverageRR(LocalDateTime startDate, LocalDateTime endDate);

    // ==================== Backtesting ====================

    /**
     * Run backtest on historical data
     */
    Map<String, Object> runBacktest(Long instrumentToken, LocalDate startDate, LocalDate endDate, String timeframe);

    /**
     * Run backtest with custom parameters
     */
    Map<String, Object> runBacktest(Long instrumentToken, LocalDate startDate, LocalDate endDate,
                                     String timeframe, Map<String, Object> params);

    /**
     * Get backtest results for an instrument
     */
    List<IOBTradeResult> getBacktestResults(Long instrumentToken);

    /**
     * Clear backtest results
     */
    void clearBacktestResults(Long instrumentToken);

    // ==================== Risk Management ====================

    /**
     * Check if daily loss limit is reached
     */
    boolean isDailyLossLimitReached();

    /**
     * Check if maximum open trades limit is reached
     */
    boolean isMaxOpenTradesReached();

    /**
     * Get current portfolio heat (total risk across open positions)
     */
    Double getPortfolioHeat();

    /**
     * Calculate position size based on risk parameters
     */
    Integer calculatePositionSize(Double entryPrice, Double stopLoss, Double riskAmount);
}
