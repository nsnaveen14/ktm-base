package com.trading.kalyani.KTManager.service;

import com.trading.kalyani.KTManager.entity.LiquiditySweepAnalysis;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Service interface for Liquidity Sweep Analysis.
 *
 * Implements the "Liquidity Sweep Pro [Whale Edition]" trading strategy:
 * - Market Structure: Identifies BSL/SSL liquidity pools
 * - Quant Engine: Detects institutional whale activity
 * - Smart Entry: Generates filtered trade signals
 */
public interface LiquiditySweepService {

    /**
     * Perform full liquidity sweep analysis for a job configuration.
     * This is the main entry point that runs all analysis layers.
     *
     * @param appJobConfigNum Job configuration number
     * @return LiquiditySweepAnalysis with complete analysis
     */
    LiquiditySweepAnalysis analyzeLiquiditySweep(Integer appJobConfigNum);

    /**
     * Get the latest liquidity sweep analysis for a job config
     */
    Optional<LiquiditySweepAnalysis> getLatestAnalysis(Integer appJobConfigNum);

    /**
     * Get the latest valid trade setup from liquidity sweep
     */
    Optional<LiquiditySweepAnalysis> getLatestValidSetup(Integer appJobConfigNum);

    /**
     * Check if there's an active liquidity sweep signal
     */
    Map<String, Object> checkLiquiditySweepSignal(Integer appJobConfigNum);

    /**
     * Get trade recommendation based on liquidity sweep analysis
     */
    Map<String, Object> getTradeRecommendation(Integer appJobConfigNum);

    /**
     * Validate if liquidity sweep supports a trade direction
     */
    boolean doesLiquiditySweepSupportTrade(String tradeDirection, Integer appJobConfigNum);

    /**
     * Get liquidity levels (BSL/SSL) for charting
     */
    Map<String, Object> getLiquidityLevels(Integer appJobConfigNum);

    /**
     * Get whale activity indicators
     */
    Map<String, Object> getWhaleActivityIndicators(Integer appJobConfigNum);

    /**
     * Get today's analysis history
     */
    List<LiquiditySweepAnalysis> getTodaysAnalyses(Integer appJobConfigNum);

    /**
     * Configure the whale detection threshold (default 2.5 sigma)
     */
    void setWhaleThreshold(double sigma);

    /**
     * Configure the lookback period for swing detection
     */
    void setLookbackPeriod(int periods);

    /**
     * Get current configuration
     */
    Map<String, Object> getConfiguration();
}

