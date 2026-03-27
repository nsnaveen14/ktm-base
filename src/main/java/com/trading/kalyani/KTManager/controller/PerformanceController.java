package com.trading.kalyani.KTManager.controller;

import com.trading.kalyani.KTManager.entity.PerformanceMetrics;
import com.trading.kalyani.KTManager.entity.TradeResult;
import com.trading.kalyani.KTManager.service.BacktestingService;
import com.trading.kalyani.KTManager.service.PerformanceTrackingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REST Controller for Performance Tracking and Backtesting.
 */
@RestController
@RequestMapping("/api/performance")
@CrossOrigin(origins = "*")
public class PerformanceController {

    private static final Logger logger = LoggerFactory.getLogger(PerformanceController.class);

    @Autowired
    private PerformanceTrackingService performanceService;

    @Autowired
    private BacktestingService backtestingService;

    // ==================== Performance Dashboard ====================

    /**
     * Get complete performance dashboard
     */
    @GetMapping("/dashboard")
    public ResponseEntity<Map<String, Object>> getDashboard() {
        try {
            Map<String, Object> dashboard = performanceService.getPerformanceDashboard();
            return ResponseEntity.ok(dashboard);
        } catch (Exception e) {
            logger.error("Error getting dashboard: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Get all-time performance metrics
     */
    @GetMapping("/metrics/all-time")
    public ResponseEntity<Map<String, Object>> getAllTimeMetrics() {
        try {
            PerformanceMetrics metrics = performanceService.getLatestAllTimeMetrics();
            if (metrics == null) {
                metrics = performanceService.calculateAllTimeMetrics();
            }
            return ResponseEntity.ok(convertMetricsToMap(metrics));
        } catch (Exception e) {
            logger.error("Error getting all-time metrics: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Get daily metrics for last N days
     */
    @GetMapping("/metrics/daily")
    public ResponseEntity<List<PerformanceMetrics>> getDailyMetrics(
            @RequestParam(defaultValue = "30") int days) {
        try {
            List<PerformanceMetrics> metrics = performanceService.getDailyMetrics(days);
            return ResponseEntity.ok(metrics);
        } catch (Exception e) {
            logger.error("Error getting daily metrics: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Calculate metrics for a specific date
     */
    @PostMapping("/metrics/calculate-daily")
    public ResponseEntity<PerformanceMetrics> calculateDailyMetrics(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date) {
        try {
            PerformanceMetrics metrics = performanceService.calculateDailyMetrics(date);
            return ResponseEntity.ok(metrics);
        } catch (Exception e) {
            logger.error("Error calculating daily metrics: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Recalculate all metrics
     */
    @PostMapping("/metrics/recalculate")
    public ResponseEntity<Map<String, Object>> recalculateMetrics() {
        try {
            performanceService.recalculateAllMetrics();
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "message", "Metrics recalculation completed"
            ));
        } catch (Exception e) {
            logger.error("Error recalculating metrics: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    // ==================== Trade Results ====================

    /**
     * Get open trades
     */
    @GetMapping("/trades/open")
    public ResponseEntity<List<TradeResult>> getOpenTrades() {
        try {
            List<TradeResult> trades = performanceService.getOpenTrades();
            return ResponseEntity.ok(trades);
        } catch (Exception e) {
            logger.error("Error getting open trades: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get recent closed trades
     */
    @GetMapping("/trades/recent")
    public ResponseEntity<List<TradeResult>> getRecentTrades(
            @RequestParam(defaultValue = "20") int limit) {
        try {
            List<TradeResult> trades = performanceService.getRecentTrades(limit);
            return ResponseEntity.ok(trades);
        } catch (Exception e) {
            logger.error("Error getting recent trades: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get trades in date range
     */
    @GetMapping("/trades/range")
    public ResponseEntity<List<TradeResult>> getTradesInRange(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime startDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime endDate) {
        try {
            List<TradeResult> trades = performanceService.getTradesInRange(startDate, endDate);
            return ResponseEntity.ok(trades);
        } catch (Exception e) {
            logger.error("Error getting trades: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get trade by IOB ID
     */
    @GetMapping("/trades/by-iob/{iobId}")
    public ResponseEntity<TradeResult> getTradeByIOB(@PathVariable Long iobId) {
        try {
            TradeResult trade = performanceService.getTradeByIOBId(iobId);
            if (trade == null) {
                return ResponseEntity.notFound().build();
            }
            return ResponseEntity.ok(trade);
        } catch (Exception e) {
            logger.error("Error getting trade: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Close a trade manually
     */
    @PostMapping("/trades/{tradeId}/close")
    public ResponseEntity<TradeResult> closeTrade(
            @PathVariable Long tradeId,
            @RequestParam Double exitPrice,
            @RequestParam(defaultValue = "MANUAL") String exitReason) {
        try {
            TradeResult trade = performanceService.closeTradeResult(tradeId, exitPrice,
                    LocalDateTime.now(), exitReason);
            return ResponseEntity.ok(trade);
        } catch (Exception e) {
            logger.error("Error closing trade: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    // ==================== Analysis Endpoints ====================

    /**
     * Get equity curve data
     */
    @GetMapping("/analysis/equity-curve")
    public ResponseEntity<List<Map<String, Object>>> getEquityCurve(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime startDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime endDate) {
        try {
            List<Map<String, Object>> curve = performanceService.getEquityCurve(startDate, endDate);
            return ResponseEntity.ok(curve);
        } catch (Exception e) {
            logger.error("Error getting equity curve: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get win/loss distribution
     */
    @GetMapping("/analysis/win-loss-distribution")
    public ResponseEntity<Map<String, Object>> getWinLossDistribution() {
        try {
            Map<String, Object> distribution = performanceService.getWinLossDistribution();
            return ResponseEntity.ok(distribution);
        } catch (Exception e) {
            logger.error("Error getting distribution: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get performance by IOB type
     */
    @GetMapping("/analysis/by-iob-type")
    public ResponseEntity<Map<String, Object>> getPerformanceByIOBType() {
        try {
            Map<String, Object> analysis = performanceService.getPerformanceByIOBType();
            return ResponseEntity.ok(analysis);
        } catch (Exception e) {
            logger.error("Error getting analysis: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get performance by confidence level
     */
    @GetMapping("/analysis/by-confidence")
    public ResponseEntity<Map<String, Object>> getPerformanceByConfidence() {
        try {
            Map<String, Object> analysis = performanceService.getPerformanceByConfidence();
            return ResponseEntity.ok(analysis);
        } catch (Exception e) {
            logger.error("Error getting analysis: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get performance by time of day
     */
    @GetMapping("/analysis/by-time-of-day")
    public ResponseEntity<Map<String, Object>> getPerformanceByTimeOfDay() {
        try {
            Map<String, Object> analysis = performanceService.getPerformanceByTimeOfDay();
            return ResponseEntity.ok(analysis);
        } catch (Exception e) {
            logger.error("Error getting analysis: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get drawdown analysis
     */
    @GetMapping("/analysis/drawdown")
    public ResponseEntity<Map<String, Object>> getDrawdownAnalysis() {
        try {
            Map<String, Object> analysis = performanceService.getDrawdownAnalysis();
            return ResponseEntity.ok(analysis);
        } catch (Exception e) {
            logger.error("Error getting drawdown analysis: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    // ==================== Backtesting ====================

    /**
     * Run a backtest for a specific instrument
     */
    @PostMapping("/backtest/run")
    public ResponseEntity<Map<String, Object>> runBacktest(
            @RequestParam Long instrumentToken,
            @RequestParam(defaultValue = "5min") String timeframe,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime startDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime endDate,
            @RequestBody(required = false) Map<String, Object> parameters) {
        try {
            logger.info("Starting backtest for token {} from {} to {}",
                    instrumentToken, startDate, endDate);

            Map<String, Object> result = backtestingService.runBacktest(
                    instrumentToken, timeframe, startDate, endDate, parameters);

            return ResponseEntity.ok(result);
        } catch (Exception e) {
            logger.error("Error running backtest: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Run backtest for all indices
     */
    @PostMapping("/backtest/run-all")
    public ResponseEntity<Map<String, Object>> runBacktestAll(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime startDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime endDate,
            @RequestBody(required = false) Map<String, Object> parameters) {
        try {
            Map<String, Object> result = backtestingService.runBacktestAllIndices(
                    startDate, endDate, parameters);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            logger.error("Error running backtest: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Get backtest results
     */
    @GetMapping("/backtest/{backtestId}")
    public ResponseEntity<Map<String, Object>> getBacktestResults(@PathVariable String backtestId) {
        try {
            Map<String, Object> result = backtestingService.getBacktestResults(backtestId);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            logger.error("Error getting backtest results: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get backtest history
     */
    @GetMapping("/backtest/history")
    public ResponseEntity<List<Map<String, Object>>> getBacktestHistory() {
        try {
            List<Map<String, Object>> history = backtestingService.getBacktestHistory();
            return ResponseEntity.ok(history);
        } catch (Exception e) {
            logger.error("Error getting backtest history: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Compare two backtests
     */
    @GetMapping("/backtest/compare")
    public ResponseEntity<Map<String, Object>> compareBacktests(
            @RequestParam String backtestId1,
            @RequestParam String backtestId2) {
        try {
            Map<String, Object> comparison = backtestingService.compareBacktests(backtestId1, backtestId2);
            return ResponseEntity.ok(comparison);
        } catch (Exception e) {
            logger.error("Error comparing backtests: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Optimize parameters
     */
    @PostMapping("/backtest/optimize")
    public ResponseEntity<Map<String, Object>> optimizeParameters(
            @RequestParam Long instrumentToken,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime startDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime endDate) {
        try {
            Map<String, Object> result = backtestingService.optimizeParameters(
                    instrumentToken, startDate, endDate);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            logger.error("Error optimizing parameters: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    // ==================== Helper Methods ====================

    private Map<String, Object> convertMetricsToMap(PerformanceMetrics metrics) {
        if (metrics == null) return new HashMap<>();

        Map<String, Object> map = new HashMap<>();
        map.put("id", metrics.getId());
        map.put("metricDate", metrics.getMetricDate());
        map.put("periodType", metrics.getPeriodType());
        map.put("totalTrades", metrics.getTotalTrades());
        map.put("winningTrades", metrics.getWinningTrades());
        map.put("losingTrades", metrics.getLosingTrades());
        map.put("winRate", metrics.getWinRate());
        map.put("lossRate", metrics.getLossRate());
        map.put("avgWinAmount", metrics.getAvgWinAmount());
        map.put("avgLossAmount", metrics.getAvgLossAmount());
        map.put("largestWin", metrics.getLargestWin());
        map.put("largestLoss", metrics.getLargestLoss());
        map.put("winLossRatio", metrics.getWinLossRatio());
        map.put("expectancy", metrics.getExpectancy());
        map.put("profitFactor", metrics.getProfitFactor());
        map.put("totalGrossPnl", metrics.getTotalGrossPnl());
        map.put("totalNetPnl", metrics.getTotalNetPnl());
        map.put("avgPnlPerTrade", metrics.getAvgPnlPerTrade());
        map.put("avgPointsPerTrade", metrics.getAvgPointsPerTrade());
        map.put("maxDrawdown", metrics.getMaxDrawdown());
        map.put("maxDrawdownPercent", metrics.getMaxDrawdownPercent());
        map.put("currentDrawdown", metrics.getCurrentDrawdown());
        map.put("maxWinStreak", metrics.getMaxWinStreak());
        map.put("maxLossStreak", metrics.getMaxLossStreak());
        map.put("currentWinStreak", metrics.getCurrentWinStreak());
        map.put("currentLossStreak", metrics.getCurrentLossStreak());
        map.put("avgRRPlanned", metrics.getAvgRRPlanned());
        map.put("avgRRAchieved", metrics.getAvgRRAchieved());
        map.put("target1HitRate", metrics.getTarget1HitRate());
        map.put("target2HitRate", metrics.getTarget2HitRate());
        map.put("target3HitRate", metrics.getTarget3HitRate());
        map.put("bullishWinRate", metrics.getBullishWinRate());
        map.put("bearishWinRate", metrics.getBearishWinRate());
        map.put("fvgConfluenceWinRate", metrics.getFvgConfluenceWinRate());
        map.put("trendAlignedWinRate", metrics.getTrendAlignedWinRate());
        map.put("highConfidenceWinRate", metrics.getHighConfidenceWinRate());
        map.put("avgTradeDurationMinutes", metrics.getAvgTradeDurationMinutes());
        map.put("calculationTimestamp", metrics.getCalculationTimestamp());

        return map;
    }
}
