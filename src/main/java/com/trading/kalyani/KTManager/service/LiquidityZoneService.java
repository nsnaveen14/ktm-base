package com.trading.kalyani.KTManager.service;

import com.trading.kalyani.KTManager.entity.LiquidityZoneAnalysis;

import java.util.List;
import java.util.Map;

/**
 * Service for liquidity zone analysis focusing on stop loss clusters
 * and liquidity grab detection for trading setups.
 */
public interface LiquidityZoneService {

    /**
     * Analyze liquidity zones for given instrument and timeframe
     */
    LiquidityZoneAnalysis analyzeLiquidityZones(Long instrumentToken, String timeframe);

    /**
     * Get latest analysis for instrument and timeframe
     */
    LiquidityZoneAnalysis getLatestAnalysis(Long instrumentToken, String timeframe);

    /**
     * Get all analyses for today (all timeframes)
     */
    List<LiquidityZoneAnalysis> getTodaysAnalyses(Long instrumentToken);

    /**
     * Get analyses for specific timeframe today
     */
    List<LiquidityZoneAnalysis> getTodaysAnalysesByTimeframe(Long instrumentToken, String timeframe);

    /**
     * Get comprehensive dashboard data for multiple instruments and timeframes
     */
    Map<String, Object> getDashboardData(List<Long> instrumentTokens, List<String> timeframes);

    /**
     * Get multi-timeframe analysis for single instrument
     */
    Map<String, Object> getMultiTimeframeAnalysis(Long instrumentToken);

    /**
     * Check for active liquidity grab setups
     */
    Map<String, Object> getActiveSetups(List<Long> instrumentTokens);

    /**
     * Get chart data for visualization
     */
    Map<String, Object> getChartData(Long instrumentToken, String timeframe);

    /**
     * Analyze liquidity zones for NIFTY across all timeframes
     */
    Map<String, Object> analyzeAllIndices();
}

