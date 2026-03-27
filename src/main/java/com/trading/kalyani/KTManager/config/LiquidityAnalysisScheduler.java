package com.trading.kalyani.KTManager.config;

import com.trading.kalyani.KTManager.service.LiquidityZoneService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalTime;
import java.util.List;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.NIFTY_INSTRUMENT_TOKEN;

/**
 * Scheduler for automated liquidity zone analysis.
 *
 * Analyzes liquidity grabs and stop loss clusters across multiple timeframes
 * for NIFTY during market hours.
 *
 * Runs every 5 minutes during market hours (9:20 AM to 3:30 PM IST)
 * Also runs once after market close for end-of-day analysis
 */
@Component
public class LiquidityAnalysisScheduler {

    private static final Logger logger = LoggerFactory.getLogger(LiquidityAnalysisScheduler.class);

    private static final LocalTime MARKET_OPEN = LocalTime.of(9, 15);
    private static final LocalTime MARKET_CLOSE = LocalTime.of(15, 30);

    @Autowired
    private LiquidityZoneService liquidityZoneService;

    @Value("${liquidity.analysis.enabled:true}")
    private boolean liquidityAnalysisEnabled;

    private static final List<Long> INSTRUMENT_TOKENS = List.of(
            NIFTY_INSTRUMENT_TOKEN
    );

    private static final List<String> TIMEFRAMES = List.of("5min", "15min", "1hour");

    /**
     * Liquidity Analysis Job - Runs every 5 minutes during market hours
     * Analyzes liquidity zones for NIFTY across all timeframes
     *
     * Cron: Every 5 minutes (0, 5, 10, ..., 55), Monday to Friday, 9:20 AM to 3:30 PM IST
     */
    @Scheduled(cron = "0 20,25,30,35,40,45,50,55 9 * * MON-FRI", zone = "Asia/Kolkata")
    public void runLiquidityAnalysisFirstHour() {
        runLiquidityAnalysis();
    }

    @Scheduled(cron = "0 0,5,10,15,20,25,30,35,40,45,50,55 10-14 * * MON-FRI", zone = "Asia/Kolkata")
    public void runLiquidityAnalysisMiddleHours() {
        runLiquidityAnalysis();
    }

    @Scheduled(cron = "0 0,5,10,15,20,25,30 15 * * MON-FRI", zone = "Asia/Kolkata")
    public void runLiquidityAnalysisLastHour() {
        runLiquidityAnalysis();
    }

    /**
     * End of Day Analysis - Runs after market close
     * Performs comprehensive analysis with all data from the day
     *
     * Cron: 3:35 PM IST, Monday to Friday
     */
    @Scheduled(cron = "0 35 15 * * MON-FRI", zone = "Asia/Kolkata")
    public void runEndOfDayLiquidityAnalysis() {
        if (!liquidityAnalysisEnabled) {
            logger.info("Liquidity analysis is disabled. Skipping end-of-day analysis.");
            return;
        }

        logger.info("Running end-of-day liquidity analysis...");
        try {
            liquidityZoneService.analyzeAllIndices();
            logger.info("End-of-day liquidity analysis completed successfully");
        } catch (Exception e) {
            logger.error("Error during end-of-day liquidity analysis: {}", e.getMessage(), e);
        }
    }

    /**
     * Core liquidity analysis execution
     */
    private void runLiquidityAnalysis() {
        if (!liquidityAnalysisEnabled) {
            logger.debug("Liquidity analysis is disabled. Skipping scheduled analysis.");
            return;
        }

        LocalTime now = LocalTime.now();
        if (now.isBefore(MARKET_OPEN) || now.isAfter(MARKET_CLOSE)) {
            logger.debug("Outside market hours. Skipping liquidity analysis.");
            return;
        }

        logger.info("Running scheduled liquidity analysis for NIFTY...");

        try {
            for (Long instrumentToken : INSTRUMENT_TOKENS) {
                for (String timeframe : TIMEFRAMES) {
                    try {
                        liquidityZoneService.analyzeLiquidityZones(instrumentToken, timeframe);
                        logger.debug("Analyzed liquidity zones for token: {}, timeframe: {}",
                                instrumentToken, timeframe);
                    } catch (Exception e) {
                        logger.error("Error analyzing liquidity zones for token: {}, timeframe: {}: {}",
                                instrumentToken, timeframe, e.getMessage(), e);
                    }
                }
            }
            logger.info("Completed scheduled liquidity analysis");
        } catch (Exception e) {
            logger.error("Error during scheduled liquidity analysis: {}", e.getMessage(), e);
        }
    }

    /**
     * On-demand analysis - can be triggered manually
     * Useful for testing or during non-market hours
     */
    public void runOnDemandAnalysis() {
        logger.info("Running on-demand liquidity analysis...");
        try {
            liquidityZoneService.analyzeAllIndices();
            logger.info("On-demand liquidity analysis completed successfully");
        } catch (Exception e) {
            logger.error("Error during on-demand liquidity analysis: {}", e.getMessage(), e);
        }
    }
}

