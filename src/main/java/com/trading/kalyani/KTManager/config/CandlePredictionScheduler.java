package com.trading.kalyani.KTManager.config;

import com.trading.kalyani.KTManager.entity.SimulatedTrade;
import com.trading.kalyani.KTManager.service.CandlePredictionService;
import com.trading.kalyani.KTManager.service.LiquiditySweepService;
import com.trading.kalyani.KTManager.service.SimulatedTradingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

/**
 * Scheduler for automated candle prediction and verification jobs.
 *
 * Prediction Job: Runs every minute during market hours (9:16 AM to 3:29 PM IST)
 * Verification Job: Runs every 15 minutes during market hours to verify predictions
 *                   and calculate deviation statistics for analysis and correction.
 */
@Component
public class CandlePredictionScheduler {

    private static final Logger logger = LoggerFactory.getLogger(CandlePredictionScheduler.class);

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final LocalTime MARKET_OPEN = LocalTime.of(9, 15);
    private static final LocalTime MARKET_CLOSE = LocalTime.of(15, 30);
    /** Auto-trading stops at 15:26 so closeAllTradesBeforeMarketClose at 15:27 is the sole final action. */
    private static final LocalTime AUTO_TRADE_CUTOFF = LocalTime.of(15, 26);

    @Autowired
    private CandlePredictionService candlePredictionService;

    @Autowired
    private SimulatedTradingService simulatedTradingService;

    @Autowired
    private LiquiditySweepService liquiditySweepService;

    @Value("${candle.prediction.job.enabled:true}")
    private boolean predictionJobEnabled;

    @Value("${candle.prediction.verification.enabled:true}")
    private boolean verificationJobEnabled;

    @Value("${simulated.trading.auto.enabled:true}")
    private boolean simulatedAutoTradingEnabled;

    @Value("${liquidity.sweep.analysis.enabled:true}")
    private boolean liquiditySweepAnalysisEnabled;

    /**
     * Prediction Job - Runs every minute during market hours (9:15 AM to 3:30 PM IST).
     * Generates predictions for the next 5 one-minute candles.
     * Time guard in executePredictionJob filters out-of-hours fires.
     * Note: 9:15 AM guard ensures first candle data is available before prediction runs.
     */
    @Scheduled(cron = "0 * 9-15 * * MON-FRI", zone = "Asia/Kolkata")
    public void runPredictionJob() {
        executePredictionJob();
    }

    /**
     * Common method to execute prediction job with proper checks
     */
    private void executePredictionJob() {
        if (!predictionJobEnabled) {
            logger.debug("Prediction job is disabled via configuration");
            return;
        }

        LocalTime now = LocalTime.now(IST);
        if (now.isBefore(MARKET_OPEN) || now.isAfter(MARKET_CLOSE)) {
            return;
        }

        try {
            if (candlePredictionService.isPredictionJobActive()) {
                candlePredictionService.executePredictionJob();
            }
        } catch (Exception e) {
            logger.error("Error in prediction scheduler: {}", e.getMessage(), e);
        }
    }

    /**
     * Verification Job - Runs every 15 minutes during market hours.
     * Verifies past predictions and calculates deviation statistics.
     *
     * Cron: At minute 0, 15, 30, 45 of every hour, Monday to Friday
     */
    @Scheduled(cron = "0 0,15,30,45 9-15 * * MON-FRI", zone = "Asia/Kolkata")
    public void runVerificationJob() {
        if (!verificationJobEnabled) {
            logger.debug("Verification job is disabled via configuration");
            return;
        }

        LocalTime now = LocalTime.now(IST);
        // Only run after market has been open for at least 15 minutes
        if (now.isBefore(MARKET_OPEN.plusMinutes(15))) {
            return;
        }

        try {
            if (candlePredictionService.isPredictionJobActive()) {
                logger.info("Executing verification job at {}", now);
                var deviation = candlePredictionService.verifyAndCalculateDeviation();

                if (deviation != null) {
                    logger.info("Verification completed - Batch: {}, Avg Deviation: {}%, Direction Accuracy: {}%",
                            deviation.getBatchId(),
                            String.format("%.2f", deviation.getAvgCloseDeviationPercent()),
                            String.format("%.2f", deviation.getDirectionAccuracyPercent()));
                }
            }
        } catch (Exception e) {
            logger.error("Error in verification scheduler: {}", e.getMessage(), e);
        }
    }

    /**
     * End of Day Summary - Runs at 3:35 PM to generate daily summary
     */
    @Scheduled(cron = "0 35 15 * * MON-FRI", zone = "Asia/Kolkata")
    public void runEndOfDaySummary() {
        if (!predictionJobEnabled) {
            return;
        }

        try {
            logger.info("Generating end of day prediction summary");

            // Get today's statistics
            var stats = candlePredictionService.getPredictionJobStats();

            logger.info("=== END OF DAY PREDICTION SUMMARY ===");
            logger.info("Total Prediction Runs: {}", stats.get("totalPredictionRuns"));
            logger.info("Total Verifications: {}", stats.get("todayVerificationCount"));
            logger.info("Average Deviation: {}%", stats.get("todayAvgDeviationPercent"));
            logger.info("Direction Accuracy: {}%", stats.get("todayDirectionAccuracyPercent"));
            logger.info("Session Accuracy: {}%", stats.get("sessionAccuracy"));

            // Log correction factors for next session
            @SuppressWarnings("unchecked")
            var corrections = (Map<String, Double>) stats.get("correctionFactors");
            if (corrections != null) {
                logger.info("Suggested Corrections for Next Session:");
                logger.info("  Close Correction: {} points",
                        String.format("%.2f", corrections.getOrDefault("closeCorrection", 0.0)));
                logger.info("  Volatility Multiplier: {}",
                        String.format("%.2f", corrections.getOrDefault("volatilityMultiplier", 1.0)));
            }

            logger.info("=====================================");

            // Stop the prediction job at end of day
            if (candlePredictionService.isPredictionJobActive()) {
                candlePredictionService.stopPredictionJob();
            }

        } catch (Exception e) {
            logger.error("Error generating end of day summary: {}", e.getMessage(), e);
        }
    }

    /**
     * Morning Initialization - Runs at 9:14 AM to prepare for market open
     */
    @Scheduled(cron = "0 14 9 * * MON-FRI", zone = "Asia/Kolkata")
    public void runMorningInitialization() {
        if (!predictionJobEnabled) {
            return;
        }

        try {
            logger.info("Initializing prediction job for market open");

            // Start the prediction job
            candlePredictionService.startPredictionJob();

            logger.info("Prediction job activated for today's trading session");

        } catch (Exception e) {
            logger.error("Error in morning initialization: {}", e.getMessage(), e);
        }
    }

    /**
     * Market Close Trade Exit - Runs at 3:27 PM to close all open trades before market close.
     * Offset to 3:27 so it runs after the last auto-trading cycle (3:26:30), acting as a
     * pure safety net with no overlap.
     */
    @Scheduled(cron = "0 27 15 * * MON-FRI", zone = "Asia/Kolkata")
    public void closeAllTradesBeforeMarketClose() {
        if (!simulatedAutoTradingEnabled) {
            return;
        }
        try {
            logger.info("=== MARKET CLOSE TRADE EXIT ===");
            logger.info("Closing all open trades before market close at 3:30 PM IST");

            List<SimulatedTrade> openTrades = simulatedTradingService.getOpenTrades();

            if (openTrades.isEmpty()) {
                logger.info("No open trades to close");
                return;
            }

            logger.info("Found {} open trade(s) to close", openTrades.size());

            // Exit all open trades with reason MARKET_CLOSE
            List<SimulatedTrade> exitedTrades = simulatedTradingService.exitAllOpenTrades("MARKET_CLOSE");

            if (!exitedTrades.isEmpty()) {
                double totalPnl = exitedTrades.stream()
                        .filter(t -> t.getNetPnl() != null)
                        .mapToDouble(SimulatedTrade::getNetPnl)
                        .sum();

                logger.info("Successfully closed {} trade(s) with total P&L: {}",
                        exitedTrades.size(), String.format("%.2f", totalPnl));

                for (SimulatedTrade trade : exitedTrades) {
                    logger.info("  Trade {} ({} {}) - Entry: {} Exit: {} P&L: {}",
                            trade.getTradeId(),
                            trade.getOptionType(),
                            trade.getSignalType(),
                            String.format("%.2f", trade.getEntryPrice()),
                            String.format("%.2f", trade.getExitPrice()),
                            String.format("%.2f", trade.getNetPnl()));
                }
            }

            logger.info("================================");

        } catch (Exception e) {
            logger.error("Error closing trades before market close: {}", e.getMessage(), e);
        }
    }

    // ============= Auto Trading Scheduler =============

    /**
     * Auto Trading Job - Runs every minute during market hours (9:15 AM to 3:30 PM IST).
     * Checks for trade signals and automatically places trades when signals are detected.
     * Also monitors and manages open trades (checks SL/Target/Reverse signals).
     * Time guard in executeAutoTradingJob filters out-of-hours fires.
     */
    @Scheduled(cron = "0 * 9-15 * * MON-FRI", zone = "Asia/Kolkata")
    public void runAutoTradingJob() {
        if (!simulatedAutoTradingEnabled) {
            logger.debug("Simulated auto trading is disabled via configuration");
            return;
        }

        LocalTime now = LocalTime.now(IST);
        if (now.isBefore(MARKET_OPEN) || now.isAfter(AUTO_TRADE_CUTOFF)) {
            return;
        }

        try {
            // autoPlaceTrade handles both monitoring open trades and placing new trades
            var result = simulatedTradingService.autoPlaceTrade();

            if (Boolean.TRUE.equals(result.get("tradePlaced"))) {
                logger.info("Auto trade placed: {}", result.get("message"));
            }

            if (Boolean.TRUE.equals(result.get("tradeExited"))) {
                logger.info("Trade exited on reverse signal: {} P&L: {}",
                        result.get("exitedTradeId"), result.get("exitPnl"));
            }

        } catch (Exception e) {
            logger.error("Error in auto trading scheduler: {}", e.getMessage(), e);
        }
    }

    // ============= Liquidity Sweep Analysis Scheduler =============

    /**
     * Liquidity Sweep Analysis Job - Runs every 15 minutes during market hours.
     * Analyzes market structure (BSL/SSL), whale activity, and generates trade signals.
     *
     * Cron: At minute 5, 20, 35, 50 of every hour, Monday to Friday, 9:30 AM to 3:15 PM IST
     * Offset by 5 minutes from verificationJob ("0,15,30,45") to avoid single-thread scheduler contention.
     */
    @Scheduled(cron = "0 5,20,35,50 9-15 * * MON-FRI", zone = "Asia/Kolkata")
    public void runLiquiditySweepAnalysis() {
        if (!liquiditySweepAnalysisEnabled) {
            logger.debug("Liquidity Sweep analysis is disabled via configuration");
            return;
        }

        LocalTime now = LocalTime.now(IST);
        // Only run after market has been open for at least 15 minutes
        if (now.isBefore(MARKET_OPEN.plusMinutes(15)) || now.isAfter(MARKET_CLOSE.minusMinutes(15))) {
            return;
        }

        try {
            logger.info("🐋 Running Liquidity Sweep analysis at {}", now);

            // Run analysis for NIFTY (appJobConfigNum 1)
            var analysis = liquiditySweepService.analyzeLiquiditySweep(1);

            if (analysis != null) {
                logger.info("🐋 Liquidity Sweep Analysis Complete:");
                logger.info("   Signal: {} ({})", analysis.getSignalType(), analysis.getSignalStrength());
                logger.info("   Confidence: {}%", analysis.getSignalConfidence());
                logger.info("   Whale Activity: {} ({})", analysis.getHasWhaleActivity(), analysis.getWhaleType());
                logger.info("   Sweep Type: {}", analysis.getSweepType());

                if (Boolean.TRUE.equals(analysis.getIsValidSetup())) {
                    logger.info("   ✅ VALID SETUP - Entry: {}, SL: {}, TP: {}",
                            analysis.getEntryPrice(),
                            analysis.getStopLossPrice(),
                            analysis.getTakeProfit2());
                }
            }

        } catch (Exception e) {
            logger.error("Error in Liquidity Sweep analysis scheduler: {}", e.getMessage(), e);
        }
    }
}
