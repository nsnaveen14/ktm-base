package com.trading.kalyani.KTManager.service;

import com.trading.kalyani.KTManager.dto.brahmastra.*;
import com.trading.kalyani.KTManager.entity.BrahmastraSignal;

import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Service interface for Brahmastra Triple Confirmation Trading Strategy.
 *
 * The strategy combines:
 * 1. Supertrend (ATR-based) - For trend direction
 * 2. MACD (12,26,9) - For momentum confirmation
 * 3. VWAP - For value area confirmation
 * 4. PCR (optional) - For market bias filtering
 */
public interface BrahmastraService {


    // ==================== Signal Generation ====================

    /**
     * Generate trading signals for a given symbol and timeframe.
     * Signals are generated when all three indicators confirm (Supertrend, MACD, VWAP).
     */
    List<SignalDTO> generateSignals(SignalRequest request);

    /**
     * Async version of signal generation for heavy computations.
     */
    CompletableFuture<List<SignalDTO>> generateSignalsAsync(SignalRequest request);

    /**
     * Scan for live signals across multiple symbols.
     */
    List<LiveScanResult> scanLiveSignals(List<String> symbols);

    /**
     * Scan a single symbol for current signal.
     */
    LiveScanResult scanSymbol(String symbol, String timeframe);

    // ==================== Backtesting ====================

    /**
     * Run backtest for a given symbol with specified parameters.
     */
    BacktestResult runBacktest(BacktestRequest request);

    /**
     * Async version of backtest for heavy computations.
     */
    CompletableFuture<BacktestResult> runBacktestAsync(BacktestRequest request);

    // ==================== Dashboard ====================

    /**
     * Get aggregated dashboard summary across all assets.
     */
    DashboardSummary getDashboardSummary();

    /**
     * Get symbol-specific summary.
     */
    SymbolSummary getSymbolSummary(String symbol);

    // ==================== Signal Management ====================

    /**
     * Get active signals.
     */
    List<SignalDTO> getActiveSignals();

    /**
     * Get signals for a date range.
     */
    List<SignalDTO> getSignalsByDateRange(String symbol, LocalDate from, LocalDate to);

    /**
     * Update signal status (for trade management).
     */
    BrahmastraSignal updateSignalStatus(Long signalId, String status, Double exitPrice, String exitReason);

    /**
     * Get signal by ID.
     */
    SignalDTO getSignalById(Long id);

    // ==================== Real-time Scanning ====================

    /**
     * Process live tick for signal detection.
     * Called from DailyJobServiceImpl during job execution.
     */
    void processLiveTick(Long instrumentToken, Double ltp, Integer appJobConfigNum);

    /**
     * Check if triple confirmation is met for current candle.
     */
    boolean isTripleConfirmation(Long instrumentToken, String signalType);

    // ==================== PCR Integration ====================

    /**
     * Get current PCR for a symbol.
     */
    Double getCurrentPCR(String symbol);

    /**
     * Determine market bias from PCR.
     */
    String getPCRBias(Double pcr);

    /**
     * Check if signal is allowed based on PCR bias.
     */
    boolean isSignalAllowedByPCR(String signalType, Double pcr);

    // ==================== Indicator Metrics ====================

    /**
     * Get real-time indicator metrics for a symbol.
     * Returns current values and historical data for Supertrend, MACD, and VWAP.
     */
    IndicatorMetrics getIndicatorMetrics(String symbol, String timeframe, int historyBars);

    /**
     * Get indicator metrics for all tracked symbols.
     */
    List<IndicatorMetrics> getAllIndicatorMetrics(String timeframe, int historyBars);

    // ==================== Option Chain Integration ====================

    /**
     * Get option chain metrics including Max Pain, OI Analysis, and Gamma Exposure.
     * Provides additional confirmation signals for the Brahmastra strategy.
     */
    OptionChainMetrics getOptionChainMetrics(String symbol);

    /**
     * Get option chain metrics for all tracked symbols.
     */
    List<OptionChainMetrics> getAllOptionChainMetrics();

    /**
     * Check if option chain data confirms the trading signal.
     * Uses Max Pain, OI changes, and Gamma Exposure for validation.
     */
    boolean doesOptionChainConfirmSignal(String symbol, String signalType);

    /**
     * Get combined signal from option chain analysis.
     * Returns BUY, SELL, or NEUTRAL based on Max Pain, OI, and GEX.
     */
    String getOptionChainSignal(String symbol);
}

