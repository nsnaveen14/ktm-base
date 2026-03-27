package com.trading.kalyani.KTManager.controller;

import com.trading.kalyani.KTManager.entity.IOBTradeResult;
import com.trading.kalyani.KTManager.service.IOBAutoTradeService;
import com.trading.kalyani.KTManager.service.InternalOrderBlockService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.NIFTY_INSTRUMENT_TOKEN;


/**
 * REST Controller for IOB Auto Trading and Performance Tracking.
 * Provides endpoints for automated trading control, performance statistics, and backtesting.
 */
@RestController
@RequestMapping("/api/iob/auto-trade")
@CrossOrigin(origins = "*")
public class IOBAutoTradeController {

    private static final Logger logger = LoggerFactory.getLogger(IOBAutoTradeController.class);

    @Autowired
    private IOBAutoTradeService autoTradeService;

    @Autowired
    private InternalOrderBlockService iobService;

    // ==================== Auto Trading Control ====================

    /**
     * Get auto-trading status and configuration
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        try {
            Map<String, Object> status = autoTradeService.getAutoTradeConfig();
            status.put("success", true);
            return ResponseEntity.ok(status);
        } catch (Exception e) {
            logger.error("Error getting auto-trade status: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Enable auto-trading
     */
    @PostMapping("/enable")
    public ResponseEntity<Map<String, Object>> enableAutoTrading() {
        try {
            autoTradeService.setAutoTradingEnabled(true);
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "autoTradingEnabled", true,
                    "message", "IOB Auto-trading enabled"
            ));
        } catch (Exception e) {
            logger.error("Error enabling auto-trade: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Disable auto-trading
     */
    @PostMapping("/disable")
    public ResponseEntity<Map<String, Object>> disableAutoTrading() {
        try {
            autoTradeService.setAutoTradingEnabled(false);
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "autoTradingEnabled", false,
                    "message", "IOB Auto-trading disabled"
            ));
        } catch (Exception e) {
            logger.error("Error disabling auto-trade: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Update auto-trade configuration
     */
    @PostMapping("/config")
    public ResponseEntity<Map<String, Object>> updateConfig(@RequestBody Map<String, Object> config) {
        try {
            autoTradeService.updateAutoTradeConfig(config);
            Map<String, Object> updatedConfig = autoTradeService.getAutoTradeConfig();
            updatedConfig.put("success", true);
            updatedConfig.put("message", "Configuration updated");
            return ResponseEntity.ok(updatedConfig);
        } catch (Exception e) {
            logger.error("Error updating config: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    // ==================== Manual Trade Control ====================

    /**
     * Manually enter a trade based on IOB
     */
    @PostMapping("/enter/{iobId}")
    public ResponseEntity<Map<String, Object>> enterTrade(@PathVariable Long iobId) {
        try {
            IOBTradeResult trade = autoTradeService.manualEnterTrade(iobId);
            if (trade != null) {
                return ResponseEntity.ok(Map.of(
                        "success", true,
                        "trade", tradeToMap(trade),
                        "message", "Trade entered successfully"
                ));
            } else {
                return ResponseEntity.badRequest().body(Map.of(
                        "success", false,
                        "error", "Failed to enter trade. IOB may not exist or already traded."
                ));
            }
        } catch (Exception e) {
            logger.error("Error entering trade: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Exit a specific trade
     */
    @PostMapping("/exit/{tradeId}")
    public ResponseEntity<Map<String, Object>> exitTrade(
            @PathVariable Long tradeId,
            @RequestParam String reason,
            @RequestParam Double exitPrice) {
        try {
            IOBTradeResult trade = autoTradeService.exitTrade(tradeId, reason, exitPrice);
            if (trade != null) {
                return ResponseEntity.ok(Map.of(
                        "success", true,
                        "trade", tradeToMap(trade),
                        "message", "Trade exited successfully"
                ));
            } else {
                return ResponseEntity.notFound().build();
            }
        } catch (Exception e) {
            logger.error("Error exiting trade: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Exit all open trades
     */
    @PostMapping("/exit-all")
    public ResponseEntity<Map<String, Object>> exitAllTrades(@RequestParam(defaultValue = "MANUAL_EXIT") String reason) {
        try {
            List<IOBTradeResult> exitedTrades = autoTradeService.exitAllOpenTrades(reason);
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "exitedCount", exitedTrades.size(),
                    "message", "All open trades exited"
            ));
        } catch (Exception e) {
            logger.error("Error exiting all trades: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    // ==================== Trade Monitoring ====================

    /**
     * Get all open trades
     */
    @GetMapping("/open-trades")
    public ResponseEntity<Map<String, Object>> getOpenTrades() {
        try {
            List<IOBTradeResult> openTrades = autoTradeService.getOpenTrades();
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "count", openTrades.size(),
                    "trades", openTrades.stream().map(this::tradeToMap).toList()
            ));
        } catch (Exception e) {
            logger.error("Error getting open trades: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Get today's trades
     */
    @GetMapping("/today")
    public ResponseEntity<Map<String, Object>> getTodaysTrades() {
        try {
            List<IOBTradeResult> trades = autoTradeService.getTodaysTrades();
            Map<String, Object> summary = autoTradeService.getTodaysSummary();
            summary.put("trades", trades.stream().map(this::tradeToMap).toList());
            summary.put("success", true);
            return ResponseEntity.ok(summary);
        } catch (Exception e) {
            logger.error("Error getting today's trades: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Get trade history
     */
    @GetMapping("/history")
    public ResponseEntity<Map<String, Object>> getTradeHistory(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate startDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate endDate) {
        try {
            List<IOBTradeResult> trades = autoTradeService.getTradeHistory(startDate, endDate);
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "period", Map.of("start", startDate, "end", endDate),
                    "count", trades.size(),
                    "trades", trades.stream().map(this::tradeToMap).toList()
            ));
        } catch (Exception e) {
            logger.error("Error getting trade history: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    // ==================== Performance Statistics ====================

    /**
     * Get performance statistics (last 30 days)
     */
    @GetMapping("/performance")
    public ResponseEntity<Map<String, Object>> getPerformance() {
        try {
            Map<String, Object> stats = autoTradeService.getPerformanceStatistics();
            stats.put("success", true);
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            logger.error("Error getting performance: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Get risk metrics
     */
    @GetMapping("/risk")
    public ResponseEntity<Map<String, Object>> getRiskMetrics() {
        try {
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "dailyLossLimitReached", autoTradeService.isDailyLossLimitReached(),
                    "maxOpenTradesReached", autoTradeService.isMaxOpenTradesReached(),
                    "portfolioHeat", autoTradeService.getPortfolioHeat(),
                    "openTradesCount", autoTradeService.getOpenTrades().size()
            ));
        } catch (Exception e) {
            logger.error("Error getting risk metrics: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    // ==================== Backtesting ====================

    /**
     * Run backtest
     */
    @PostMapping("/backtest")
    public ResponseEntity<Map<String, Object>> runBacktest(
            @RequestParam(defaultValue = "256265") Long instrumentToken,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate startDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate endDate,
            @RequestParam(defaultValue = "5min") String timeframe) {
        try {
            logger.info("Running backtest: {} from {} to {} on {}",
                    instrumentToken, startDate, endDate, timeframe);

            Map<String, Object> results = autoTradeService.runBacktest(
                    instrumentToken, startDate, endDate, timeframe);

            return ResponseEntity.ok(results);
        } catch (Exception e) {
            logger.error("Error running backtest: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Get backtest results
     */
    @GetMapping("/backtest/results/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getBacktestResults(@PathVariable Long instrumentToken) {
        try {
            List<IOBTradeResult> results = autoTradeService.getBacktestResults(instrumentToken);
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "instrumentToken", instrumentToken,
                    "count", results.size(),
                    "trades", results.stream().map(this::tradeToMap).toList()
            ));
        } catch (Exception e) {
            logger.error("Error getting backtest results: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Clear backtest results
     */
    @DeleteMapping("/backtest/results/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> clearBacktestResults(@PathVariable Long instrumentToken) {
        try {
            autoTradeService.clearBacktestResults(instrumentToken);
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "message", "Backtest results cleared"
            ));
        } catch (Exception e) {
            logger.error("Error clearing backtest results: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    // ==================== Multi-Timeframe Analysis ====================

    /**
     * Get MTF analysis for an instrument
     */
    @GetMapping("/mtf/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getMTFAnalysis(@PathVariable Long instrumentToken) {
        try {
            Map<String, Object> analysis = iobService.getMTFAnalysis(instrumentToken);
            analysis.put("success", true);
            return ResponseEntity.ok(analysis);
        } catch (Exception e) {
            logger.error("Error getting MTF analysis: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Get MTF analysis for all indices
     */
    @GetMapping("/mtf/all")
    public ResponseEntity<Map<String, Object>> getMTFAnalysisAll() {
        try {
            Map<String, Object> result = new HashMap<>();
            result.put("NIFTY", iobService.getMTFAnalysis(NIFTY_INSTRUMENT_TOKEN));
            result.put("success", true);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            logger.error("Error getting MTF analysis: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Get HTF-aligned IOBs
     */
    @GetMapping("/htf-aligned/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getHTFAlignedIOBs(@PathVariable Long instrumentToken) {
        try {
            var alignedIOBs = iobService.getHTFAlignedIOBs(instrumentToken);
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "instrumentToken", instrumentToken,
                    "count", alignedIOBs.size(),
                    "iobs", alignedIOBs
            ));
        } catch (Exception e) {
            logger.error("Error getting HTF-aligned IOBs: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Get high-confidence IOBs (>= 85%) ready for auto-trading
     */
    @GetMapping("/high-confidence/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getHighConfidenceIOBs(@PathVariable Long instrumentToken) {
        try {
            var tradableIOBs = iobService.getValidTradableIOBs(instrumentToken);
            // Filter for 85%+ confidence
            var highConfidenceIOBs = tradableIOBs.stream()
                    .filter(iob -> {
                        Double confidence = iob.getEnhancedConfidence() != null ?
                                iob.getEnhancedConfidence() : iob.getSignalConfidence();
                        return confidence != null && confidence >= 85.0;
                    })
                    .toList();

            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "instrumentToken", instrumentToken,
                    "count", highConfidenceIOBs.size(),
                    "minConfidenceThreshold", 85.0,
                    "autoTradingEnabled", autoTradeService.isAutoTradingEnabled(),
                    "iobs", highConfidenceIOBs
            ));
        } catch (Exception e) {
            logger.error("Error getting high-confidence IOBs: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Trigger auto-trade check for a specific instrument at current price
     */
    @PostMapping("/trigger-check/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> triggerAutoTradeCheck(
            @PathVariable Long instrumentToken,
            @RequestParam Double currentPrice) {
        try {
            if (!autoTradeService.isAutoTradingEnabled()) {
                return ResponseEntity.ok(Map.of(
                        "success", false,
                        "message", "Auto-trading is not enabled"
                ));
            }

            var trades = autoTradeService.checkForEntries(instrumentToken, currentPrice);

            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "instrumentToken", instrumentToken,
                    "currentPrice", currentPrice,
                    "tradesEntered", trades.size(),
                    "trades", trades.stream().map(this::tradeToMap).toList()
            ));
        } catch (Exception e) {
            logger.error("Error triggering auto-trade check: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    // ==================== Helper Methods ====================

    private Map<String, Object> tradeToMap(IOBTradeResult trade) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", trade.getId());
        map.put("tradeId", trade.getTradeId());
        map.put("iobId", trade.getIobId());
        map.put("instrumentName", trade.getInstrumentName());
        map.put("timeframe", trade.getTimeframe());
        map.put("iobType", trade.getIobType());
        map.put("tradeDirection", trade.getTradeDirection());
        map.put("signalConfidence", trade.getSignalConfidence());
        map.put("hasFvg", trade.getHasFvg());
        map.put("zoneHigh", trade.getZoneHigh());
        map.put("zoneLow", trade.getZoneLow());
        map.put("plannedEntry", trade.getPlannedEntry());
        map.put("actualEntry", trade.getActualEntry());
        map.put("entryTime", trade.getEntryTime());
        map.put("entryTrigger", trade.getEntryTrigger());
        map.put("plannedStopLoss", trade.getPlannedStopLoss());
        map.put("actualStopLoss", trade.getActualStopLoss());
        map.put("target1", trade.getTarget1());
        map.put("target2", trade.getTarget2());
        map.put("target3", trade.getTarget3());
        map.put("exitPrice", trade.getExitPrice());
        map.put("exitTime", trade.getExitTime());
        map.put("exitReason", trade.getExitReason());
        map.put("pointsCaptured", trade.getPointsCaptured());
        map.put("achievedRR", trade.getAchievedRR());
        map.put("isWinner", trade.getIsWinner());
        map.put("targetHit", trade.getTargetHit());
        map.put("grossPnl", trade.getGrossPnl());
        map.put("netPnl", trade.getNetPnl());
        map.put("status", trade.getStatus());
        map.put("tradeMode", trade.getTradeMode());
        return map;
    }
}
