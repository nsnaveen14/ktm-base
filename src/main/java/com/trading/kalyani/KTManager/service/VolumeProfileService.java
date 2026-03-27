package com.trading.kalyani.KTManager.service;

import com.trading.kalyani.KTManager.entity.VolumeProfile;
import com.trading.kalyani.KTManager.entity.InternalOrderBlock;
import com.trading.kalyani.KTManager.model.HistoricalDataResponse.HistoricalCandle;

import java.util.List;
import java.util.Map;

/**
 * Service for Volume Profile Analysis.
 *
 * Provides:
 * - Volume at IOB zone formation analysis
 * - Displacement volume confirmation
 * - Point of Control (POC) calculation
 * - Volume delta analysis
 */
public interface VolumeProfileService {

    // ==================== Core Analysis ====================

    /**
     * Analyze volume profile for an instrument
     */
    VolumeProfile analyzeVolumeProfile(Long instrumentToken, String timeframe);

    /**
     * Analyze volume profile using provided candles
     */
    VolumeProfile analyzeVolumeProfile(Long instrumentToken, String timeframe, List<HistoricalCandle> candles);

    /**
     * Analyze volume profile for a specific IOB
     */
    VolumeProfile analyzeVolumeForIOB(InternalOrderBlock iob, List<HistoricalCandle> candles);

    /**
     * Get latest volume profile analysis
     */
    VolumeProfile getLatestAnalysis(Long instrumentToken, String timeframe);

    /**
     * Analyze all configured indices
     */
    Map<String, VolumeProfile> analyzeAllIndices();

    // ==================== Volume Profile Levels ====================

    /**
     * Calculate Point of Control (POC)
     */
    Double calculatePOC(List<HistoricalCandle> candles);

    /**
     * Calculate Value Area (70% of volume)
     */
    Map<String, Double> calculateValueArea(List<HistoricalCandle> candles);

    /**
     * Get High Volume Nodes
     */
    List<Double> getHighVolumeNodes(List<HistoricalCandle> candles, int count);

    /**
     * Get Low Volume Nodes (potential breakout areas)
     */
    List<Double> getLowVolumeNodes(List<HistoricalCandle> candles, int count);

    // ==================== IOB Volume Analysis ====================

    /**
     * Analyze volume at IOB candle formation
     */
    Map<String, Object> analyzeIOBCandleVolume(InternalOrderBlock iob, List<HistoricalCandle> candles);

    /**
     * Check if IOB has institutional volume (>1.5x average)
     */
    boolean hasInstitutionalVolume(InternalOrderBlock iob, List<HistoricalCandle> candles);

    /**
     * Get IOB volume type (INSTITUTIONAL, RETAIL, NORMAL)
     */
    String getIOBVolumeType(InternalOrderBlock iob, List<HistoricalCandle> candles);

    /**
     * Calculate IOB volume ratio (relative to average)
     */
    Double calculateIOBVolumeRatio(InternalOrderBlock iob, List<HistoricalCandle> candles);

    // ==================== Displacement Analysis ====================

    /**
     * Analyze displacement candle volume
     */
    Map<String, Object> analyzeDisplacementVolume(InternalOrderBlock iob, List<HistoricalCandle> candles);

    /**
     * Check if displacement volume confirms the move
     */
    boolean isDisplacementConfirmed(InternalOrderBlock iob, List<HistoricalCandle> candles);

    /**
     * Get displacement volume ratio
     */
    Double getDisplacementVolumeRatio(InternalOrderBlock iob, List<HistoricalCandle> candles);

    // ==================== Volume Delta Analysis ====================

    /**
     * Calculate volume delta (buying - selling volume)
     */
    Map<String, Object> calculateVolumeDelta(List<HistoricalCandle> candles);

    /**
     * Get cumulative volume delta
     */
    Long getCumulativeVolumeDelta(List<HistoricalCandle> candles);

    /**
     * Get delta direction (BULLISH, BEARISH, NEUTRAL)
     */
    String getDeltaDirection(List<HistoricalCandle> candles);

    /**
     * Get delta strength (STRONG, MODERATE, WEAK)
     */
    String getDeltaStrength(List<HistoricalCandle> candles);

    // ==================== POC Alignment ====================

    /**
     * Check if POC aligns with IOB zone
     */
    boolean isPOCAlignedWithIOB(InternalOrderBlock iob, List<HistoricalCandle> candles);

    /**
     * Get POC distance from IOB zone
     */
    Double getPOCDistanceFromIOB(InternalOrderBlock iob, List<HistoricalCandle> candles);

    // ==================== Volume Confluence ====================

    /**
     * Calculate overall volume confluence score for IOB
     */
    Double calculateVolumeConfluenceScore(InternalOrderBlock iob, List<HistoricalCandle> candles);

    /**
     * Get volume confluence breakdown
     */
    Map<String, Object> getVolumeConfluenceDetails(InternalOrderBlock iob, List<HistoricalCandle> candles);

    /**
     * Get volume-based trade bias
     */
    String getVolumeBias(List<HistoricalCandle> candles);

    // ==================== Volume Statistics ====================

    /**
     * Get volume statistics for period
     */
    Map<String, Object> getVolumeStatistics(List<HistoricalCandle> candles);

    /**
     * Calculate average volume
     */
    Long calculateAverageVolume(List<HistoricalCandle> candles);

    /**
     * Calculate volume standard deviation
     */
    Double calculateVolumeStdDev(List<HistoricalCandle> candles);

    /**
     * Identify volume spikes (>2x average)
     */
    List<Map<String, Object>> identifyVolumeSpikes(List<HistoricalCandle> candles);

    // ==================== Dashboard ====================

    /**
     * Get comprehensive volume profile dashboard
     */
    Map<String, Object> getDashboard(Long instrumentToken);

    /**
     * Get volume profile summary for multiple instruments
     */
    Map<String, Map<String, Object>> getMultiInstrumentSummary(List<Long> instrumentTokens);
}
