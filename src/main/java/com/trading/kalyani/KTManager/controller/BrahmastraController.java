package com.trading.kalyani.KTManager.controller;

import com.trading.kalyani.KTManager.dto.brahmastra.*;
import com.trading.kalyani.KTManager.entity.BrahmastraSignal;
import com.trading.kalyani.KTManager.service.BrahmastraService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * REST Controller for Brahmastra Triple Confirmation Trading Strategy.
 *
 * Provides endpoints for:
 * - Signal Generation (historical and real-time)
 * - Backtesting with comprehensive metrics
 * - Live scanning across multiple symbols
 * - Dashboard summary and analytics
 */
@RestController
@RequestMapping("/api/brahmastra")
@CrossOrigin(origins = "*")
@Tag(name = "Brahmastra Strategy", description = "Triple Confirmation Intraday Trading Strategy APIs")
public class BrahmastraController {

    private static final Logger logger = LoggerFactory.getLogger(BrahmastraController.class);

    @Autowired
    private BrahmastraService brahmastraService;

    // ==================== Signal Generation ====================

    /**
     * Generate trading signals for a given symbol and timeframe.
     * Signals are generated when Supertrend, MACD, and VWAP all confirm.
     */
    @PostMapping("/signals/generate")
    @Operation(summary = "Generate trading signals",
               description = "Generate buy/sell signals using triple confirmation (Supertrend + MACD + VWAP)")
    public ResponseEntity<Map<String, Object>> generateSignals(@RequestBody SignalRequest request) {
        try {
            logger.info("Signal generation request: {}", request);

            // Validate request
            if (request.getSymbol() == null || request.getSymbol().isEmpty()) {
                return ResponseEntity.badRequest().body(Map.of(
                        "success", false,
                        "error", "Symbol is required"
                ));
            }

            // Set defaults if not provided
            if (request.getTimeframe() == null) request.setTimeframe("5m");
            if (request.getFromDate() == null) request.setFromDate(LocalDate.now().minusDays(7));
            if (request.getToDate() == null) request.setToDate(LocalDate.now());
            if (request.getUsePCR() == null) request.setUsePCR(true);

            List<SignalDTO> signals = brahmastraService.generateSignals(request);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("symbol", request.getSymbol());
            response.put("timeframe", request.getTimeframe());
            response.put("fromDate", request.getFromDate());
            response.put("toDate", request.getToDate());
            response.put("usePCR", request.getUsePCR());
            response.put("totalSignals", signals.size());
            response.put("buySignals", signals.stream().filter(s -> "BUY".equals(s.getSignalType())).count());
            response.put("sellSignals", signals.stream().filter(s -> "SELL".equals(s.getSignalType())).count());
            response.put("signals", signals);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error generating signals: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Async version of signal generation for heavy computations.
     */
    @PostMapping("/signals/generate-async")
    @Operation(summary = "Generate signals asynchronously",
               description = "Async signal generation for large date ranges")
    public CompletableFuture<ResponseEntity<Map<String, Object>>> generateSignalsAsync(@RequestBody SignalRequest request) {
        return brahmastraService.generateSignalsAsync(request)
                .thenApply(signals -> {
                    Map<String, Object> response = new HashMap<>();
                    response.put("success", true);
                    response.put("signals", signals);
                    response.put("count", signals.size());
                    return ResponseEntity.ok(response);
                })
                .exceptionally(e -> {
                    logger.error("Async signal generation failed: {}", e.getMessage());
                    return ResponseEntity.internalServerError().body(Map.of(
                            "success", false,
                            "error", e.getMessage()
                    ));
                });
    }

    // ==================== Backtesting ====================

    /**
     * Run backtest for a given symbol with specified parameters.
     * Returns comprehensive metrics including win rate, Sharpe ratio, drawdown, etc.
     */
    @PostMapping("/backtest/run")
    @Operation(summary = "Run backtest",
               description = "Backtest the strategy with specified parameters and get detailed metrics")
    public ResponseEntity<Map<String, Object>> runBacktest(@RequestBody BacktestRequest request) {
        try {
            logger.info("Backtest request: {}", request);

            // Validate request
            if (request.getSymbol() == null || request.getSymbol().isEmpty()) {
                return ResponseEntity.badRequest().body(Map.of(
                        "success", false,
                        "error", "Symbol is required"
                ));
            }

            // Set defaults
            if (request.getTimeframe() == null) request.setTimeframe("5m");
            if (request.getFromDate() == null) request.setFromDate(LocalDate.now().minusDays(30));
            if (request.getToDate() == null) request.setToDate(LocalDate.now());
            if (request.getInitialCapital() == null) request.setInitialCapital(100000.0);
            if (request.getRiskPerTrade() == null) request.setRiskPerTrade(1.0);
            if (request.getUsePCR() == null) request.setUsePCR(true);

            BacktestResult result = brahmastraService.runBacktest(request);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("result", result);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error running backtest: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Async version of backtest for heavy computations.
     */
    @PostMapping("/backtest/run-async")
    @Operation(summary = "Run backtest asynchronously",
               description = "Async backtest for large date ranges")
    public CompletableFuture<ResponseEntity<Map<String, Object>>> runBacktestAsync(@RequestBody BacktestRequest request) {
        return brahmastraService.runBacktestAsync(request)
                .thenApply(result -> {
                    Map<String, Object> response = new HashMap<>();
                    response.put("success", true);
                    response.put("result", result);
                    return ResponseEntity.ok(response);
                })
                .exceptionally(e -> {
                    logger.error("Async backtest failed: {}", e.getMessage());
                    return ResponseEntity.internalServerError().body(Map.of(
                            "success", false,
                            "error", e.getMessage()
                    ));
                });
    }

    // ==================== Live Scanning ====================

    /**
     * Scan for live signals across multiple symbols.
     */
    @GetMapping("/scan/live")
    @Operation(summary = "Live signal scanner",
               description = "Scan multiple symbols for real-time trading signals")
    public ResponseEntity<Map<String, Object>> scanLive(
            @RequestParam(defaultValue = "NIFTY") String symbols) {
        try {
            List<String> symbolList = Arrays.asList(symbols.split(","));

            logger.info("Live scan request for symbols: {}", symbolList);

            List<LiveScanResult> results = brahmastraService.scanLiveSignals(symbolList);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("scannedAt", java.time.LocalDateTime.now());
            response.put("symbolsScanned", symbolList.size());
            response.put("signalsFound", results.size());
            response.put("results", results);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error in live scan: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Scan a single symbol for current signal.
     */
    @GetMapping("/scan/{symbol}")
    @Operation(summary = "Scan single symbol",
               description = "Scan a specific symbol for current trading signal")
    public ResponseEntity<Map<String, Object>> scanSymbol(
            @PathVariable String symbol,
            @RequestParam(defaultValue = "5m") String timeframe) {
        try {
            logger.info("Scanning {} on {} timeframe", symbol, timeframe);

            LiveScanResult result = brahmastraService.scanSymbol(symbol, timeframe);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("result", result);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error scanning {}: {}", symbol, e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    // ==================== Dashboard ====================

    /**
     * Get aggregated dashboard summary across all assets.
     */
    @GetMapping("/dashboard/summary")
    @Operation(summary = "Dashboard summary",
               description = "Get aggregated performance summary across all assets")
    public ResponseEntity<Map<String, Object>> getDashboardSummary() {
        try {
            logger.info("Fetching dashboard summary");

            DashboardSummary summary = brahmastraService.getDashboardSummary();

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("summary", summary);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error fetching dashboard: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Get symbol-specific summary.
     */
    @GetMapping("/dashboard/symbol/{symbol}")
    @Operation(summary = "Symbol summary",
               description = "Get performance summary for a specific symbol")
    public ResponseEntity<Map<String, Object>> getSymbolSummary(@PathVariable String symbol) {
        try {
            logger.info("Fetching summary for {}", symbol);

            SymbolSummary summary = brahmastraService.getSymbolSummary(symbol);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("summary", summary);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error fetching symbol summary: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    // ==================== Signal Management ====================

    /**
     * Get all active signals.
     */
    @GetMapping("/signals/active")
    @Operation(summary = "Get active signals",
               description = "Get all currently active trading signals")
    public ResponseEntity<Map<String, Object>> getActiveSignals() {
        try {
            List<SignalDTO> signals = brahmastraService.getActiveSignals();

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("count", signals.size());
            response.put("signals", signals);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error fetching active signals: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Get signals by date range.
     */
    @GetMapping("/signals/history")
    @Operation(summary = "Get signal history",
               description = "Get historical signals for a symbol within date range")
    public ResponseEntity<Map<String, Object>> getSignalHistory(
            @RequestParam String symbol,
            @RequestParam LocalDate fromDate,
            @RequestParam LocalDate toDate) {
        try {
            List<SignalDTO> signals = brahmastraService.getSignalsByDateRange(symbol, fromDate, toDate);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("symbol", symbol);
            response.put("fromDate", fromDate);
            response.put("toDate", toDate);
            response.put("count", signals.size());
            response.put("signals", signals);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error fetching signal history: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Get signal by ID.
     */
    @GetMapping("/signals/{id}")
    @Operation(summary = "Get signal by ID",
               description = "Get details of a specific signal")
    public ResponseEntity<Map<String, Object>> getSignalById(@PathVariable Long id) {
        try {
            SignalDTO signal = brahmastraService.getSignalById(id);

            if (signal == null) {
                return ResponseEntity.notFound().build();
            }

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("signal", signal);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error fetching signal {}: {}", id, e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Update signal status (for trade management).
     */
    @PutMapping("/signals/{id}/status")
    @Operation(summary = "Update signal status",
               description = "Update status of a signal (close, stop out, etc.)")
    public ResponseEntity<Map<String, Object>> updateSignalStatus(
            @PathVariable Long id,
            @RequestParam String status,
            @RequestParam(required = false) Double exitPrice,
            @RequestParam(required = false) String exitReason) {
        try {
            BrahmastraSignal signal = brahmastraService.updateSignalStatus(id, status, exitPrice, exitReason);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Signal status updated");
            response.put("signal", signal);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error updating signal {}: {}", id, e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    // ==================== PCR Integration ====================

    /**
     * Get current PCR values for all symbols.
     */
    @GetMapping("/pcr/current")
    @Operation(summary = "Get current PCR",
               description = "Get current Put-Call Ratio for all symbols")
    public ResponseEntity<Map<String, Object>> getCurrentPCR() {
        try {
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("niftyPCR", brahmastraService.getCurrentPCR("NIFTY"));
            response.put("niftyBias", brahmastraService.getPCRBias(brahmastraService.getCurrentPCR("NIFTY")));

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error fetching PCR: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    // ==================== Health Check ====================

    /**
     * Health check endpoint.
     */
    @GetMapping("/health")
    @Operation(summary = "Health check",
               description = "Check if the Brahmastra service is running")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "UP");
        response.put("service", "Brahmastra Triple Confirmation Strategy");
        response.put("timestamp", java.time.LocalDateTime.now());
        return ResponseEntity.ok(response);
    }

    // ==================== Indicator Metrics ====================

    /**
     * Get real-time indicator metrics for a specific symbol.
     * Returns Supertrend, MACD, and VWAP values with historical data for charts.
     */
    @GetMapping("/indicators/{symbol}")
    @Operation(summary = "Get indicator metrics for a symbol",
               description = "Get real-time Supertrend, MACD, and VWAP metrics with chart data")
    public ResponseEntity<Map<String, Object>> getIndicatorMetrics(
            @PathVariable String symbol,
            @RequestParam(defaultValue = "5m") String timeframe,
            @RequestParam(defaultValue = "50") int historyBars) {
        try {
            logger.info("Indicator metrics request for {} on {} timeframe with {} history bars", symbol, timeframe, historyBars);

            IndicatorMetrics metrics = brahmastraService.getIndicatorMetrics(symbol.toUpperCase(), timeframe, historyBars);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("metrics", metrics);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error fetching indicator metrics for {}: {}", symbol, e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Get real-time indicator metrics for all tracked symbols.
     */
    @GetMapping("/indicators/all")
    @Operation(summary = "Get indicator metrics for all symbols",
               description = "Get Supertrend, MACD, and VWAP metrics for NIFTY")
    public ResponseEntity<Map<String, Object>> getAllIndicatorMetrics(
            @RequestParam(defaultValue = "5m") String timeframe,
            @RequestParam(defaultValue = "50") int historyBars) {
        try {
            logger.info("Fetching indicator metrics for all symbols on {} timeframe", timeframe);

            List<IndicatorMetrics> allMetrics = brahmastraService.getAllIndicatorMetrics(timeframe, historyBars);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("timeframe", timeframe);
            response.put("symbolsCount", allMetrics.size());
            response.put("metrics", allMetrics);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error fetching all indicator metrics: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    // ==================== Option Chain Integration ====================

    /**
     * Get option chain metrics including Max Pain, OI Analysis, and Gamma Exposure for a symbol.
     */
    @GetMapping("/option-chain/{symbol}")
    @Operation(summary = "Get option chain metrics",
               description = "Get Max Pain, OI Analysis, and Gamma Exposure data for signal confirmation")
    public ResponseEntity<Map<String, Object>> getOptionChainMetrics(@PathVariable String symbol) {
        try {
            logger.info("Option chain metrics request for {}", symbol);

            OptionChainMetrics metrics = brahmastraService.getOptionChainMetrics(symbol.toUpperCase());

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("metrics", metrics);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error fetching option chain metrics for {}: {}", symbol, e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Get option chain metrics for all tracked symbols.
     */
    @GetMapping("/option-chain/all")
    @Operation(summary = "Get option chain metrics for all symbols",
               description = "Get Max Pain, OI Analysis, and Gamma Exposure for NIFTY")
    public ResponseEntity<Map<String, Object>> getAllOptionChainMetrics() {
        try {
            logger.info("Fetching option chain metrics for all symbols");

            List<OptionChainMetrics> allMetrics = brahmastraService.getAllOptionChainMetrics();

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("symbolsCount", allMetrics.size());
            response.put("metrics", allMetrics);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error fetching all option chain metrics: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Check if option chain data confirms a given signal.
     */
    @GetMapping("/option-chain/{symbol}/confirm/{signalType}")
    @Operation(summary = "Check option chain signal confirmation",
               description = "Validate if Max Pain, OI, and GEX support the given signal type")
    public ResponseEntity<Map<String, Object>> checkOptionChainConfirmation(
            @PathVariable String symbol,
            @PathVariable String signalType) {
        try {
            logger.info("Checking option chain confirmation for {} signal on {}", signalType, symbol);

            boolean confirms = brahmastraService.doesOptionChainConfirmSignal(symbol.toUpperCase(), signalType.toUpperCase());
            String optionChainSignal = brahmastraService.getOptionChainSignal(symbol.toUpperCase());
            OptionChainMetrics metrics = brahmastraService.getOptionChainMetrics(symbol.toUpperCase());

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("symbol", symbol.toUpperCase());
            response.put("signalType", signalType.toUpperCase());
            response.put("confirms", confirms);
            response.put("optionChainSignal", optionChainSignal);
            response.put("optionChainBias", metrics.getOptionChainBias());
            response.put("optionChainConfidence", metrics.getOptionChainConfidence());
            response.put("recommendation", metrics.getRecommendedAction());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Error checking option chain confirmation: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }
}

