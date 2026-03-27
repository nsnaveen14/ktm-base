package com.trading.kalyani.KTManager.service;

import com.trading.kalyani.KTManager.entity.InternalOrderBlock;

import java.util.List;
import java.util.Map;

/**
 * Service for Internal Order Block (IOB) detection and trading.
 * IOBs represent institutional order flow footprints that indicate
 * potential reversal or continuation zones.
 */
public interface InternalOrderBlockService {

    /**
     * Scan for Internal Order Blocks in the given instrument using 5-minute candles
     */
    List<InternalOrderBlock> scanForIOBs(Long instrumentToken);

    /**
     * Scan for IOBs with specific timeframe
     */
    List<InternalOrderBlock> scanForIOBs(Long instrumentToken, String timeframe);

    /**
     * Get all fresh (unmitigated) IOBs for an instrument
     */
    List<InternalOrderBlock> getFreshIOBs(Long instrumentToken);

    /**
     * Get fresh bullish IOBs
     */
    List<InternalOrderBlock> getBullishIOBs(Long instrumentToken);

    /**
     * Get fresh bearish IOBs
     */
    List<InternalOrderBlock> getBearishIOBs(Long instrumentToken);

    /**
     * Get today's detected IOBs
     */
    List<InternalOrderBlock> getTodaysIOBs(Long instrumentToken);

    /**
     * Get IOBs that are valid for trading
     */
    List<InternalOrderBlock> getValidTradableIOBs(Long instrumentToken);

    /**
     * Check if price is at any IOB zone (for mitigation)
     */
    List<InternalOrderBlock> checkMitigation(Long instrumentToken, Double currentPrice);

    /**
     * Check if any targets have been hit for mitigated IOBs and send alerts.
     * This should be called periodically with the current price.
     */
    void checkTargetHits(Long instrumentToken, Double currentPrice);

    /**
     * Mark IOB as mitigated
     */
    void markAsMitigated(Long iobId);

    /**
     * Mark all fresh IOBs as mitigated for a given instrument
     * @param instrumentToken the instrument token to filter IOBs
     * @return the count of mitigated IOBs
     */
    int mitigateAllFresh(Long instrumentToken);

    /**
     * Mark multiple IOBs as completed manually
     * @param iobIds list of IOB IDs to mark as completed
     * @return the count of completed IOBs
     */
    int markAsCompleted(List<Long> iobIds);

    /**
     * Mark IOB as traded
     */
    void markAsTraded(Long iobId, String tradeId);

    /**
     * Mark IOB as traded and persist option premium targets at each index level.
     */
    void markAsTraded(Long iobId, String tradeId, Double premiumT1, Double premiumT2, Double premiumT3);

    /**
     * Get dashboard data for IOB analysis
     */
    Map<String, Object> getDashboardData(List<Long> instrumentTokens);

    /**
     * Get detailed analysis for a single instrument
     */
    Map<String, Object> getDetailedAnalysis(Long instrumentToken);

    /**
     * Analyze all configured indices (NIFTY)
     */
    Map<String, Object> analyzeAllIndices();

    /**
     * Generate trade setup from IOB
     */
    Map<String, Object> generateTradeSetup(Long iobId);

    /**
     * Execute trade based on IOB setup
     */
    Map<String, Object> executeTrade(Long iobId);

    /**
     * Expire old IOBs that are no longer relevant
     */
    void expireOldIOBs();

    /**
     * Get IOB statistics
     */
    Map<String, Object> getStatistics(Long instrumentToken);

    // ==================== Multi-Timeframe Analysis ====================

    /**
     * Scan for IOBs across multiple timeframes
     */
    Map<String, List<InternalOrderBlock>> scanMultipleTimeframes(Long instrumentToken, List<String> timeframes);

    /**
     * Get Multi-Timeframe confluence analysis
     */
    Map<String, Object> getMTFAnalysis(Long instrumentToken);

    /**
     * Calculate MTF confluence score for an IOB
     */
    Double calculateMTFConfluenceScore(InternalOrderBlock iob, Long instrumentToken);

    /**
     * Get IOBs that are aligned with higher timeframe direction
     */
    List<InternalOrderBlock> getHTFAlignedIOBs(Long instrumentToken);

    /**
     * Check if higher timeframe supports the trade direction
     */
    boolean isHTFAligned(Long instrumentToken, String tradeDirection);

}
