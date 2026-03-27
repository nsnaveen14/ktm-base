package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

/**
 * Entity representing Volume Profile Analysis for institutional order flow confirmation.
 *
 * This entity tracks:
 * - Volume at IOB zone formation (institutional volume = stronger IOB)
 * - Volume during displacement (high volume confirms move)
 * - Point of Control (POC) alignment with IOB zones
 * - Volume delta analysis for order flow confirmation
 */
@Entity
@Table(name = "volume_profile")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class VolumeProfile {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "instrument_token", nullable = false)
    private Long instrumentToken;

    @Column(name = "instrument_name")
    private String instrumentName;

    @Column(name = "timeframe", nullable = false)
    private String timeframe;

    @Column(name = "analysis_timestamp", nullable = false)
    private LocalDateTime analysisTimestamp;

    // ==================== Volume Profile Levels ====================

    /**
     * Point of Control (POC) - Price level with highest volume
     */
    @Column(name = "poc_level")
    private Double pocLevel;

    /**
     * Volume at POC level
     */
    @Column(name = "poc_volume")
    private Long pocVolume;

    /**
     * Value Area High - Upper boundary of 70% volume area
     */
    @Column(name = "value_area_high")
    private Double valueAreaHigh;

    /**
     * Value Area Low - Lower boundary of 70% volume area
     */
    @Column(name = "value_area_low")
    private Double valueAreaLow;

    /**
     * High Volume Node 1 - Secondary high volume level
     */
    @Column(name = "hvn_1")
    private Double hvn1;

    /**
     * High Volume Node 2 - Tertiary high volume level
     */
    @Column(name = "hvn_2")
    private Double hvn2;

    /**
     * Low Volume Node 1 - Primary low volume gap (potential breakout area)
     */
    @Column(name = "lvn_1")
    private Double lvn1;

    /**
     * Low Volume Node 2 - Secondary low volume gap
     */
    @Column(name = "lvn_2")
    private Double lvn2;

    // ==================== Volume Statistics ====================

    /**
     * Total volume in analysis period
     */
    @Column(name = "total_volume")
    private Long totalVolume;

    /**
     * Average volume per candle
     */
    @Column(name = "average_volume")
    private Long averageVolume;

    /**
     * Maximum volume in a single candle
     */
    @Column(name = "max_volume")
    private Long maxVolume;

    /**
     * Minimum volume in a single candle
     */
    @Column(name = "min_volume")
    private Long minVolume;

    /**
     * Volume standard deviation
     */
    @Column(name = "volume_std_dev")
    private Double volumeStdDev;

    // ==================== Volume at IOB Zone ====================

    /**
     * Associated IOB ID (if analyzing specific IOB)
     */
    @Column(name = "iob_id")
    private Long iobId;

    /**
     * Volume during IOB candle formation
     */
    @Column(name = "iob_candle_volume")
    private Long iobCandleVolume;

    /**
     * Volume relative to average (ratio): >1.5 = institutional, <0.5 = retail
     */
    @Column(name = "iob_volume_ratio")
    private Double iobVolumeRatio;

    /**
     * IOB volume classification: INSTITUTIONAL, RETAIL, NORMAL
     */
    @Column(name = "iob_volume_type")
    private String iobVolumeType;

    // ==================== Displacement Volume ====================

    /**
     * Volume during displacement candle(s)
     */
    @Column(name = "displacement_volume")
    private Long displacementVolume;

    /**
     * Displacement volume relative to average
     */
    @Column(name = "displacement_volume_ratio")
    private Double displacementVolumeRatio;

    /**
     * Whether displacement volume confirms the move (>1.2x average)
     */
    @Column(name = "displacement_confirmed")
    private Boolean displacementConfirmed;

    // ==================== Volume Delta Analysis ====================

    /**
     * Buying volume (up candles)
     */
    @Column(name = "buying_volume")
    private Long buyingVolume;

    /**
     * Selling volume (down candles)
     */
    @Column(name = "selling_volume")
    private Long sellingVolume;

    /**
     * Volume delta (buying - selling)
     */
    @Column(name = "volume_delta")
    private Long volumeDelta;

    /**
     * Cumulative volume delta
     */
    @Column(name = "cumulative_delta")
    private Long cumulativeDelta;

    /**
     * Delta direction: BULLISH (positive), BEARISH (negative), NEUTRAL
     */
    @Column(name = "delta_direction")
    private String deltaDirection;

    /**
     * Delta strength: STRONG, MODERATE, WEAK
     */
    @Column(name = "delta_strength")
    private String deltaStrength;

    // ==================== Volume Confluence Score ====================

    /**
     * POC alignment with IOB zone (within 0.5%)
     */
    @Column(name = "poc_iob_aligned")
    private Boolean pocIobAligned;

    /**
     * Overall volume confluence score (0-100)
     * Higher = stronger institutional footprint
     */
    @Column(name = "volume_confluence_score")
    private Double volumeConfluenceScore;

    /**
     * Volume profile bias: BULLISH, BEARISH, NEUTRAL
     */
    @Column(name = "volume_bias")
    private String volumeBias;

    // ==================== Analysis Range ====================

    /**
     * Start of analysis period
     */
    @Column(name = "period_start")
    private LocalDateTime periodStart;

    /**
     * End of analysis period
     */
    @Column(name = "period_end")
    private LocalDateTime periodEnd;

    /**
     * Number of candles analyzed
     */
    @Column(name = "candles_analyzed")
    private Integer candlesAnalyzed;

    /**
     * Price high during analysis period
     */
    @Column(name = "period_high")
    private Double periodHigh;

    /**
     * Price low during analysis period
     */
    @Column(name = "period_low")
    private Double periodLow;

    @Column(name = "current_price")
    private Double currentPrice;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
        if (analysisTimestamp == null) {
            analysisTimestamp = LocalDateTime.now();
        }
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
