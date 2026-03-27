package com.trading.kalyani.KTManager.service;

import com.trading.kalyani.KTManager.entity.InternalOrderBlock;

import java.util.List;
import java.util.Map;

/**
 * Service for automated IOB-based trading.
 * Handles entry condition monitoring, order placement, and position management.
 */
public interface AutoTradingService {

    // ==================== Auto Trading Configuration ====================

    /**
     * Enable/disable auto trading globally
     */
    void setAutoTradingEnabled(boolean enabled);

    /**
     * Check if auto trading is enabled
     */
    boolean isAutoTradingEnabled();

    /**
     * Get current auto trading configuration
     */
    Map<String, Object> getAutoTradingConfig();

    /**
     * Update auto trading configuration
     */
    void updateAutoTradingConfig(Map<String, Object> config);

    /**
     * Set instruments for auto trading
     */
    void setAutoTradingInstruments(List<Long> instrumentTokens);

    /**
     * Get instruments enabled for auto trading
     */
    List<Long> getAutoTradingInstruments();

    // ==================== Entry Condition Monitoring ====================

    /**
     * Check if an IOB has valid entry conditions
     */
    Map<String, Object> checkEntryConditions(Long iobId, Double currentPrice);

    /**
     * Check for zone touch (price enters IOB zone)
     */
    boolean isZoneTouched(InternalOrderBlock iob, Double currentPrice);

    /**
     * Check for confirmation candle (optional additional confirmation)
     */
    boolean hasConfirmationCandle(InternalOrderBlock iob);

    /**
     * Get IOBs ready for entry
     */
    List<InternalOrderBlock> getIOBsReadyForEntry(Long instrumentToken, Double currentPrice);

    // ==================== Order Management ====================

    /**
     * Place an order based on IOB signal
     */
    Map<String, Object> placeIOBOrder(Long iobId, Double entryPrice, Integer quantity);

    /**
     * Place a market order for immediate entry
     */
    Map<String, Object> placeMarketOrder(Long iobId, Integer quantity);

    /**
     * Place a limit order at zone entry
     */
    Map<String, Object> placeLimitOrder(Long iobId, Double limitPrice, Integer quantity);

    /**
     * Cancel a pending order
     */
    Map<String, Object> cancelOrder(String orderId);

    /**
     * Get order status
     */
    Map<String, Object> getOrderStatus(String orderId);

    /**
     * Get all pending orders
     */
    List<Map<String, Object>> getPendingOrders();

    // ==================== Position Management ====================

    /**
     * Get all open positions from auto trading
     */
    List<Map<String, Object>> getOpenPositions();

    /**
     * Update trailing stop for a position
     */
    Map<String, Object> updateTrailingStop(String positionId, Double newStopLoss);

    /**
     * Book partial profits at target
     */
    Map<String, Object> bookPartialProfits(String positionId, int percentage, Double price);

    /**
     * Close a position
     */
    Map<String, Object> closePosition(String positionId, Double exitPrice, String reason);

    /**
     * Close all positions for an instrument
     */
    Map<String, Object> closeAllPositions(Long instrumentToken);

    // ==================== Auto Trade Execution Loop ====================

    /**
     * Process auto trades (called by scheduler)
     */
    void processAutoTrades(Map<Long, Double> currentPrices);

    /**
     * Monitor and manage existing positions
     */
    void manageOpenPositions(Map<Long, Double> currentPrices);

    /**
     * Check and execute pending entry orders
     */
    void processPendingEntries(Map<Long, Double> currentPrices);

    // ==================== Statistics & Reports ====================

    /**
     * Get auto trading statistics
     */
    Map<String, Object> getAutoTradingStats();

    /**
     * Get today's auto trades
     */
    List<Map<String, Object>> getTodaysAutoTrades();

    /**
     * Get auto trading activity log
     */
    List<Map<String, Object>> getActivityLog(int limit);
}
