package com.trading.kalyani.KTManager.service;

import com.trading.kalyani.KTManager.entity.InternalOrderBlock;
import com.trading.kalyani.KTManager.entity.PerformanceMetrics;
import com.trading.kalyani.KTManager.entity.TradeResult;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Service for tracking IOB trade performance and calculating metrics.
 */
public interface PerformanceTrackingService {

    // ==================== Trade Result Management ====================

    /**
     * Create a new trade result from an IOB
     */
    TradeResult createTradeFromIOB(InternalOrderBlock iob, Double entryPrice,
                                   Integer quantity, String tradeType, String entryReason);

    /**
     * Update trade with entry details
     */
    TradeResult recordTradeEntry(Long tradeResultId, Double entryPrice,
                                 LocalDateTime entryTime, Integer quantity);

    /**
     * Close a trade with exit details
     */
    TradeResult closeTradeResult(Long tradeResultId, Double exitPrice,
                                 LocalDateTime exitTime, String exitReason);

    /**
     * Update trailing stop for a trade
     */
    TradeResult updateTrailingStop(Long tradeResultId, Double trailingStop);

    /**
     * Get all open trades
     */
    List<TradeResult> getOpenTrades();

    /**
     * Get open trades for an instrument
     */
    List<TradeResult> getOpenTradesByInstrument(Long instrumentToken);

    /**
     * Get trade result by IOB ID
     */
    TradeResult getTradeByIOBId(Long iobId);

    /**
     * Get trade result by trade ID
     */
    TradeResult getTradeByTradeId(String tradeId);

    /**
     * Get recent closed trades
     */
    List<TradeResult> getRecentTrades(int limit);

    /**
     * Get trades in date range
     */
    List<TradeResult> getTradesInRange(LocalDateTime startDate, LocalDateTime endDate);

    // ==================== Performance Metrics ====================

    /**
     * Calculate and update all-time performance metrics
     */
    PerformanceMetrics calculateAllTimeMetrics();

    /**
     * Calculate daily performance metrics
     */
    PerformanceMetrics calculateDailyMetrics(LocalDate date);

    /**
     * Calculate weekly performance metrics
     */
    PerformanceMetrics calculateWeeklyMetrics(LocalDate weekStartDate);

    /**
     * Calculate monthly performance metrics
     */
    PerformanceMetrics calculateMonthlyMetrics(int year, int month);

    /**
     * Get latest all-time metrics
     */
    PerformanceMetrics getLatestAllTimeMetrics();

    /**
     * Get daily metrics for last N days
     */
    List<PerformanceMetrics> getDailyMetrics(int days);

    /**
     * Get performance metrics for an instrument
     */
    PerformanceMetrics getInstrumentMetrics(Long instrumentToken);

    // ==================== Dashboard Data ====================

    /**
     * Get complete performance dashboard data
     */
    Map<String, Object> getPerformanceDashboard();

    /**
     * Get equity curve data
     */
    List<Map<String, Object>> getEquityCurve(LocalDateTime startDate, LocalDateTime endDate);

    /**
     * Get win/loss distribution
     */
    Map<String, Object> getWinLossDistribution();

    /**
     * Get performance by IOB type (bullish vs bearish)
     */
    Map<String, Object> getPerformanceByIOBType();

    /**
     * Get performance by confidence level
     */
    Map<String, Object> getPerformanceByConfidence();

    /**
     * Get performance by time of day
     */
    Map<String, Object> getPerformanceByTimeOfDay();

    /**
     * Get drawdown analysis
     */
    Map<String, Object> getDrawdownAnalysis();

    // ==================== Trade Outcome Checking ====================

    /**
     * Check and update outcomes for open trades based on current prices
     */
    void checkTradeOutcomes(Map<Long, Double> currentPrices);

    /**
     * Check if a trade hit any target or stop loss
     */
    Map<String, Object> checkTradeStatus(Long tradeResultId, Double currentPrice);

    // ==================== Scheduled Tasks ====================

    /**
     * Daily metrics calculation job
     */
    void runDailyMetricsCalculation();

    /**
     * Recalculate all metrics
     */
    void recalculateAllMetrics();
}
