package com.trading.kalyani.KTManager.controller;

import com.trading.kalyani.KTManager.entity.SimulatedTrade;
import com.trading.kalyani.KTManager.entity.TradingLedger;
import com.trading.kalyani.KTManager.service.SimulatedTradingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REST Controller for Simulated Trading functionality.
 * Provides endpoints for trade management, monitoring, and reporting.
 */
@RestController
@RequestMapping("/api/trading")
@CrossOrigin(origins = "*")
public class SimulatedTradingController {

    @Autowired
    private SimulatedTradingService tradingService;

    // ============= Trade Signal & Placement =============

    /**
     * Check for current trade signals without placing trade.
     */
    @GetMapping("/signals/check")
    public ResponseEntity<Map<String, Object>> checkSignals() {
        return ResponseEntity.ok(tradingService.checkForTradeSignals());
    }

    /**
     * Get EMA crossover signal status.
     */
    @GetMapping("/signals/ema")
    public ResponseEntity<Map<String, Object>> getEMASignal() {
        return ResponseEntity.ok(tradingService.checkEMACrossoverSignal());
    }

    /**
     * Get trade setup signal status.
     */
    @GetMapping("/signals/setup")
    public ResponseEntity<Map<String, Object>> getTradeSetupSignal() {
        return ResponseEntity.ok(tradingService.checkTradeSetupSignal());
    }

    /**
     * Get IOB (Internal Order Block) signal status.
     * Checks for fresh IOBs that can generate trade signals.
     */
    @GetMapping("/signals/iob")
    public ResponseEntity<Map<String, Object>> getIOBSignal() {
        return ResponseEntity.ok(tradingService.checkIOBSignal());
    }

    /**
     * Trigger auto trade placement based on current signals.
     */
    @PostMapping("/auto-trade")
    public ResponseEntity<Map<String, Object>> triggerAutoTrade() {
        return ResponseEntity.ok(tradingService.autoPlaceTrade());
    }

    /**
     * Manually place a BUY trade.
     */
    @PostMapping("/manual/buy")
    public ResponseEntity<Map<String, Object>> manualBuy(@RequestParam(required = false) String source) {
        Map<String, Object> response = new HashMap<>();
        SimulatedTrade trade = tradingService.manualBuyTrade(source);
        if (trade != null) {
            response.put("success", true);
            response.put("trade", trade);
            response.put("message", "BUY trade placed successfully");
        } else {
            response.put("success", false);
            response.put("message", "Failed to place BUY trade");
        }
        return ResponseEntity.ok(response);
    }

    /**
     * Manually place a SELL trade.
     */
    @PostMapping("/manual/sell")
    public ResponseEntity<Map<String, Object>> manualSell(@RequestParam(required = false) String source) {
        Map<String, Object> response = new HashMap<>();
        SimulatedTrade trade = tradingService.manualSellTrade(source);
        if (trade != null) {
            response.put("success", true);
            response.put("trade", trade);
            response.put("message", "SELL trade placed successfully");
        } else {
            response.put("success", false);
            response.put("message", "Failed to place SELL trade");
        }
        return ResponseEntity.ok(response);
    }

    // ============= Trade Management =============

    /**
     * Monitor and manage open trades (check for exits).
     */
    @PostMapping("/monitor")
    public ResponseEntity<Map<String, Object>> monitorTrades() {
        Map<String, Object> response = new HashMap<>();
        List<SimulatedTrade> exitedTrades = tradingService.monitorAndManageOpenTrades();
        response.put("exitedCount", exitedTrades.size());
        response.put("exitedTrades", exitedTrades);
        return ResponseEntity.ok(response);
    }

    /**
     * Exit a specific trade.
     */
    @PostMapping("/exit/{tradeId}")
    public ResponseEntity<Map<String, Object>> exitTrade(
            @PathVariable String tradeId,
            @RequestParam String exitReason,
            @RequestParam Double exitPrice) {
        Map<String, Object> response = new HashMap<>();
        SimulatedTrade trade = tradingService.exitTrade(tradeId, exitReason, exitPrice);
        if (trade != null) {
            response.put("success", true);
            response.put("trade", trade);
            response.put("message", "Trade exited successfully");
        } else {
            response.put("success", false);
            response.put("message", "Failed to exit trade");
        }
        return ResponseEntity.ok(response);
    }

    /**
     * Exit all open trades.
     */
    @PostMapping("/exit-all")
    public ResponseEntity<Map<String, Object>> exitAllTrades(
            @RequestParam(defaultValue = "MANUAL") String exitReason) {
        Map<String, Object> response = new HashMap<>();
        List<SimulatedTrade> exitedTrades = tradingService.exitAllOpenTrades(exitReason);
        response.put("exitedCount", exitedTrades.size());
        response.put("exitedTrades", exitedTrades);
        response.put("message", exitedTrades.size() + " trades exited");
        return ResponseEntity.ok(response);
    }

    /**
     * Close stale carry-over open trades from previous trading days.
     * These trades block new signals and can never be properly exited because
     * their underlying options have already expired.
     */
    @PostMapping("/close-stale")
    public ResponseEntity<Map<String, Object>> closeStaleOpenTrades() {
        Map<String, Object> response = new HashMap<>();
        List<SimulatedTrade> closed = tradingService.closeStaleOpenTrades();
        response.put("closedCount", closed.size());
        response.put("closedTrades", closed);
        response.put("message", closed.isEmpty()
                ? "No stale open trades found"
                : closed.size() + " stale trade(s) closed");
        return ResponseEntity.ok(response);
    }

    /**
     * Cancel a pending trade.
     */
    @PostMapping("/cancel/{tradeId}")
    public ResponseEntity<Map<String, Object>> cancelTrade(@PathVariable String tradeId) {
        Map<String, Object> response = new HashMap<>();
        SimulatedTrade trade = tradingService.cancelTrade(tradeId);
        if (trade != null) {
            response.put("success", true);
            response.put("trade", trade);
            response.put("message", "Trade cancelled");
        } else {
            response.put("success", false);
            response.put("message", "Failed to cancel trade or trade not in PENDING status");
        }
        return ResponseEntity.ok(response);
    }

    /**
     * Discard selected trades from today's records and recalculate ledger.
     */
    @PostMapping("/discard")
    public ResponseEntity<Map<String, Object>> discardTrades(@RequestBody Object tradeIdsObj) {
        Map<String, Object> response = new HashMap<>();
        try {
            List<String> tradeIds = new ArrayList<>();

            if (tradeIdsObj == null) {
                response.put("error", "Empty payload");
                return ResponseEntity.badRequest().body(response);
            }

            // Handle if client sends { "tradeIds": ["id1","id2"] }
            if (tradeIdsObj instanceof Map) {
                Map map = (Map) tradeIdsObj;
                Object v = map.get("tradeIds");
                if (v instanceof List) {
                    for (Object o : (List) v) {
                        if (o != null) tradeIds.add(o.toString());
                    }
                }
            } else if (tradeIdsObj instanceof List) {
                for (Object o : (List) tradeIdsObj) {
                    if (o != null) tradeIds.add(o.toString());
                }
            } else {
                // try to coerce single id string
                tradeIds.add(tradeIdsObj.toString());
            }

            if (tradeIds.isEmpty()) {
                response.put("error", "No tradeIds found in payload");
                return ResponseEntity.badRequest().body(response);
            }

            List<SimulatedTrade> discarded = tradingService.discardTrades(tradeIds);
            response.put("discardedCount", discarded.size());
            response.put("message", discarded.size() + " trade(s) discarded");
            response.put("discardedTrades", discarded);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            response.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    // ============= Trade Queries =============

    /**
     * Get all open trades.
     */
    @GetMapping("/open")
    public ResponseEntity<List<SimulatedTrade>> getOpenTrades() {
        return ResponseEntity.ok(tradingService.getOpenTrades());
    }

    /**
     * Get today's trades.
     */
    @GetMapping("/today")
    public ResponseEntity<List<SimulatedTrade>> getTodaysTrades() {
        return ResponseEntity.ok(tradingService.getTodaysTrades());
    }

    /**
     * Get trade history for date range.
     */
    @GetMapping("/history")
    public ResponseEntity<List<SimulatedTrade>> getTradeHistory(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate startDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate endDate) {
        return ResponseEntity.ok(tradingService.getTradeHistory(startDate, endDate));
    }

    // ============= Reporting & Summary =============

    /**
     * Get today's trading summary.
     */
    @GetMapping("/summary/today")
    public ResponseEntity<Map<String, Object>> getTodaysSummary() {
        return ResponseEntity.ok(tradingService.getTodaysSummary());
    }

    /**
     * Get comprehensive trading statistics.
     */
    @GetMapping("/statistics")
    public ResponseEntity<Map<String, Object>> getStatistics() {
        return ResponseEntity.ok(tradingService.getTradingStatistics());
    }

    /**
     * Get today's ledger entry.
     */
    @GetMapping("/ledger/today")
    public ResponseEntity<TradingLedger> getTodaysLedger() {
        TradingLedger ledger = tradingService.getTodaysLedger();
        if (ledger != null) {
            return ResponseEntity.ok(ledger);
        }
        return ResponseEntity.noContent().build();
    }

    /**
     * Get ledger history for date range.
     */
    @GetMapping("/ledger/history")
    public ResponseEntity<List<TradingLedger>> getLedgerHistory(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate startDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate endDate) {
        return ResponseEntity.ok(tradingService.getLedgerHistory(startDate, endDate));
    }

    /**
     * Manually update/recalculate today's ledger.
     */
    @PostMapping("/ledger/update")
    public ResponseEntity<TradingLedger> updateLedger() {
        TradingLedger ledger = tradingService.updateTodaysLedger();
        if (ledger != null) {
            return ResponseEntity.ok(ledger);
        }
        return ResponseEntity.internalServerError().build();
    }

    // ============= Configuration =============

    /**
     * Get trading configuration.
     */
    @GetMapping("/config")
    public ResponseEntity<Map<String, Object>> getConfig() {
        return ResponseEntity.ok(tradingService.getTradingConfig());
    }

    /**
     * Get trailing SL runtime configuration
     */
    @GetMapping("/trailing-config")
    public ResponseEntity<Map<String, Object>> getTrailingConfig() {
        Map<String, Object> cfg = new HashMap<>();
        Map<String, Object> full = tradingService.getTradingConfig();
        cfg.put("activationThresholdPercent", full.get("trailingActivationThresholdPercent"));
        cfg.put("trailPercentOfProfit", full.get("trailingTrailPercentOfProfit"));
        return ResponseEntity.ok(cfg);
    }

    /**
     * Update trailing SL runtime configuration
     */
    @PostMapping("/trailing-config")
    public ResponseEntity<Map<String, Object>> updateTrailingConfig(@RequestBody Map<String, Object> cfg) {
        Map<String, Object> resp = new HashMap<>();
        try {
            Map<String, Object> update = new HashMap<>();
            if (cfg.containsKey("activationThresholdPercent")) {
                update.put("trailingActivationThresholdPercent", cfg.get("activationThresholdPercent"));
            }
            if (cfg.containsKey("trailPercentOfProfit")) {
                update.put("trailingTrailPercentOfProfit", cfg.get("trailPercentOfProfit"));
            }
            tradingService.updateTradingConfig(update);
            resp.put("success", true);
            resp.put("config", tradingService.getTradingConfig());
            return ResponseEntity.ok(resp);
        } catch (Exception e) {
            resp.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(resp);
        }
    }

    /**
     * Update trading configuration.
     */
    @PostMapping("/config")
    public ResponseEntity<Map<String, Object>> updateConfig(@RequestBody Map<String, Object> config) {
        tradingService.updateTradingConfig(config);
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("config", tradingService.getTradingConfig());
        return ResponseEntity.ok(response);
    }

    /**
     * Enable auto-trading.
     */
    @PostMapping("/auto-trading/enable")
    public ResponseEntity<Map<String, Object>> enableAutoTrading() {
        tradingService.setAutoTradingEnabled(true);
        Map<String, Object> response = new HashMap<>();
        response.put("autoTradingEnabled", true);
        response.put("message", "Auto-trading enabled");
        return ResponseEntity.ok(response);
    }

    /**
     * Disable auto-trading.
     */
    @PostMapping("/auto-trading/disable")
    public ResponseEntity<Map<String, Object>> disableAutoTrading() {
        tradingService.setAutoTradingEnabled(false);
        Map<String, Object> response = new HashMap<>();
        response.put("autoTradingEnabled", false);
        response.put("message", "Auto-trading disabled");
        return ResponseEntity.ok(response);
    }

    /**
     * Get auto-trading status.
     */
    @GetMapping("/auto-trading/status")
    public ResponseEntity<Map<String, Object>> getAutoTradingStatus() {
        Map<String, Object> response = new HashMap<>();
        response.put("autoTradingEnabled", tradingService.isAutoTradingEnabled());
        return ResponseEntity.ok(response);
    }

    // ============= Performance Statistics =============

    /**
     * Get performance statistics by signal source for a given period.
     * @param period "daily", "weekly", or "monthly"
     */
    @GetMapping("/performance/{period}")
    public ResponseEntity<Map<String, Object>> getPerformanceByPeriod(
            @PathVariable String period) {
        return ResponseEntity.ok(tradingService.getPerformanceBySignalSource(period));
    }

    /**
     * Get comprehensive performance chart data.
     * Includes daily, weekly, monthly breakdowns and trend data.
     */
    @GetMapping("/performance/chart-data")
    public ResponseEntity<Map<String, Object>> getPerformanceChartData() {
        return ResponseEntity.ok(tradingService.getPerformanceChartData());
    }
}
