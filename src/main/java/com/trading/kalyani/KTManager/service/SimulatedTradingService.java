package com.trading.kalyani.KTManager.service;

import com.trading.kalyani.KTManager.entity.SimulatedTrade;
import com.trading.kalyani.KTManager.entity.TradingLedger;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

/**
 * Service interface for simulated options trading.
 * Handles trade signal detection, automatic trade placement,
 * trade management (entry/exit), and P&L calculation.
 */
public interface SimulatedTradingService {

    // ============= Trade Signal Detection =============

    /**
     * Check for trade signals from both trade setup and EMA crossover.
     * Returns signal details if a valid signal is detected.
     */
    Map<String, Object> checkForTradeSignals();

    /**
     * Check if current EMA crossover generates a trade signal.
     */
    Map<String, Object> checkEMACrossoverSignal();

    /**
     * Check if current trade setup generates a trade signal.
     */
    Map<String, Object> checkTradeSetupSignal();

    /**
     * Check for IOB (Internal Order Block) based trade signals.
     * Automatically detects fresh IOBs and creates trade signals when:
     * - IOB has confidence >= 50%
     * - Price is within or near the IOB zone
     * - IOB is not yet mitigated
     *
     * Target selection based on confidence:
     * - 85%+ confidence: Target 3
     * - 70-84% confidence: Target 2
     * - 50-69% confidence: Target 1
     */
    Map<String, Object> checkIOBSignal();

    /**
     * Place a trade based on IOB signal with appropriate targets.
     * @param iob The Internal Order Block to trade
     * @return The created trade or null if placement failed
     */
    SimulatedTrade placeIOBTrade(com.trading.kalyani.KTManager.entity.InternalOrderBlock iob);

    // ============= Trade Placement =============

    /**
     * Place a simulated trade based on signal.
     * @param signalType BUY or SELL
     * @param signalSource TRADE_SETUP or EMA_CROSSOVER
     * @param signalStrength STRONG, MODERATE, or WEAK
     * @return The created trade or null if placement failed
     */
    SimulatedTrade placeTrade(String signalType, String signalSource, String signalStrength);

    /**
     * Automatically place trade based on detected signals.
     * Called periodically to check and execute trades.
     */
    Map<String, Object> autoPlaceTrade();

    // ============= Trade Management =============

    /**
     * Monitor all open trades and check for exit conditions.
     * Exits trades that hit target or stop loss.
     */
    List<SimulatedTrade> monitorAndManageOpenTrades();

    /**
     * Exit a specific trade.
     * @param tradeId Trade ID to exit
     * @param exitReason Reason for exit
     * @param exitPrice Exit price
     * @return Updated trade
     */
    SimulatedTrade exitTrade(String tradeId, String exitReason, Double exitPrice);

    /**
     * Exit all open trades (e.g., at market close).
     */
    List<SimulatedTrade> exitAllOpenTrades(String exitReason);

    /**
     * Update trailing stop loss for all open trades.
     */
    void updateTrailingStopLosses();

    // ============= P&L and Reporting =============

    /**
     * Get today's trading summary.
     */
    Map<String, Object> getTodaysSummary();

    /**
     * Get all open trades.
     */
    List<SimulatedTrade> getOpenTrades();

    /**
     * Get today's trades.
     */
    List<SimulatedTrade> getTodaysTrades();

    /**
     * Get trade history for a specific date range.
     */
    List<SimulatedTrade> getTradeHistory(LocalDate startDate, LocalDate endDate);

    /**
     * Get today's ledger entry.
     */
    TradingLedger getTodaysLedger();

    /**
     * Get ledger for date range.
     */
    List<TradingLedger> getLedgerHistory(LocalDate startDate, LocalDate endDate);

    /**
     * Update today's ledger based on closed trades.
     */
    TradingLedger updateTodaysLedger();

    /**
     * Get comprehensive trading statistics.
     */
    Map<String, Object> getTradingStatistics();

    // ============= Configuration =============

    /**
     * Get current trading configuration.
     */
    Map<String, Object> getTradingConfig();

    /**
     * Update trading configuration.
     */
    void updateTradingConfig(Map<String, Object> config);

    /**
     * Enable or disable auto-trading.
     */
    void setAutoTradingEnabled(boolean enabled);

    /**
     * Check if auto-trading is enabled.
     */
    boolean isAutoTradingEnabled();

    // ============= Manual Controls =============

    /**
     * Manually trigger a BUY trade.
     */
    SimulatedTrade manualBuyTrade(String source);

    /**
     * Manually trigger a SELL trade.
     */
    SimulatedTrade manualSellTrade(String source);

    /**
     * Cancel a pending trade.
     */
    SimulatedTrade cancelTrade(String tradeId);

    /**
     * Discard trades by trade IDs from today's records and recalculate ledger.
     * Discarded trades will be flagged (status = 'DISCARDED') and excluded from ledger P&L.
     */
    List<SimulatedTrade> discardTrades(List<String> tradeIds);

    /**
     * Close any OPEN trades left over from a previous trading day (carry-over).
     * These stale trades block new signals for their signal source and can never
     * be properly exited because the underlying option has already expired.
     * Returns the list of trades that were closed.
     */
    List<SimulatedTrade> closeStaleOpenTrades();

    // ============= Performance Statistics =============

    /**
     * Get performance statistics by signal source for a given period.
     * @param period "daily", "weekly", or "monthly"
     * @return Map containing performance data for each signal source
     */
    Map<String, Object> getPerformanceBySignalSource(String period);

    /**
     * Get comprehensive performance statistics for charting.
     * Includes daily, weekly, and monthly breakdowns for all signal sources.
     */
    Map<String, Object> getPerformanceChartData();
}
