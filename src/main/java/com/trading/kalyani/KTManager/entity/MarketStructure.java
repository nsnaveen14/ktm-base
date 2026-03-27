package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

/**
 * Entity representing Market Structure Analysis for Smart Money Concepts (SMC) trading.
 *
 * This entity tracks:
 * - Trend identification (HH/HL for uptrend, LH/LL for downtrend)
 * - Change of Character (CHoCH) detection for trend reversals
 * - Premium/Discount zones based on price range
 * - Market phase identification (accumulation, distribution, trending)
 */
@Entity
@Table(name = "market_structure")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MarketStructure {

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

    // ==================== Trend Identification ====================

    /**
     * Current market trend: UPTREND, DOWNTREND, SIDEWAYS
     */
    @Column(name = "trend_direction")
    private String trendDirection;

    /**
     * Trend strength: STRONG, MODERATE, WEAK
     */
    @Column(name = "trend_strength")
    private String trendStrength;

    /**
     * Number of consecutive higher highs (for uptrend)
     */
    @Column(name = "consecutive_hh_count")
    private Integer consecutiveHHCount;

    /**
     * Number of consecutive higher lows (for uptrend)
     */
    @Column(name = "consecutive_hl_count")
    private Integer consecutiveHLCount;

    /**
     * Number of consecutive lower highs (for downtrend)
     */
    @Column(name = "consecutive_lh_count")
    private Integer consecutiveLHCount;

    /**
     * Number of consecutive lower lows (for downtrend)
     */
    @Column(name = "consecutive_ll_count")
    private Integer consecutiveLLCount;

    // ==================== Swing Point Levels ====================

    /**
     * Most recent swing high price
     */
    @Column(name = "last_swing_high")
    private Double lastSwingHigh;

    @Column(name = "last_swing_high_time")
    private LocalDateTime lastSwingHighTime;

    /**
     * Most recent swing low price
     */
    @Column(name = "last_swing_low")
    private Double lastSwingLow;

    @Column(name = "last_swing_low_time")
    private LocalDateTime lastSwingLowTime;

    /**
     * Previous swing high (for HH/LH comparison)
     */
    @Column(name = "prev_swing_high")
    private Double prevSwingHigh;

    /**
     * Previous swing low (for HL/LL comparison)
     */
    @Column(name = "prev_swing_low")
    private Double prevSwingLow;

    // ==================== Change of Character (CHoCH) ====================

    /**
     * Whether a CHoCH (trend reversal signal) was detected
     */
    @Column(name = "choch_detected")
    private Boolean chochDetected;

    /**
     * Type of CHoCH: BULLISH_CHOCH (bearish to bullish), BEARISH_CHOCH (bullish to bearish)
     */
    @Column(name = "choch_type")
    private String chochType;

    /**
     * Price level where CHoCH occurred
     */
    @Column(name = "choch_level")
    private Double chochLevel;

    /**
     * Timestamp when CHoCH was detected
     */
    @Column(name = "choch_timestamp")
    private LocalDateTime chochTimestamp;

    // ==================== Premium/Discount Zones ====================

    /**
     * Session/Range high for premium/discount calculation
     */
    @Column(name = "range_high")
    private Double rangeHigh;

    /**
     * Session/Range low for premium/discount calculation
     */
    @Column(name = "range_low")
    private Double rangeLow;

    /**
     * Equilibrium level (50% of range)
     */
    @Column(name = "equilibrium_level")
    private Double equilibriumLevel;

    /**
     * Current price zone: PREMIUM (above 50%), DISCOUNT (below 50%), EQUILIBRIUM
     */
    @Column(name = "price_zone")
    private String priceZone;

    /**
     * Current price position in range (0-100%)
     */
    @Column(name = "price_position_percent")
    private Double pricePositionPercent;

    // ==================== Market Phase ====================

    /**
     * Current market phase: ACCUMULATION, DISTRIBUTION, MARKUP, MARKDOWN, RANGING
     */
    @Column(name = "market_phase")
    private String marketPhase;

    /**
     * Phase confidence score (0-100)
     */
    @Column(name = "phase_confidence")
    private Double phaseConfidence;

    /**
     * Volume behavior during phase: INCREASING, DECREASING, NEUTRAL
     */
    @Column(name = "volume_behavior")
    private String volumeBehavior;

    // ==================== Order Flow Direction ====================

    /**
     * Institutional order flow direction: BULLISH, BEARISH, NEUTRAL
     */
    @Column(name = "order_flow_direction")
    private String orderFlowDirection;

    /**
     * Order flow strength score (0-100)
     */
    @Column(name = "order_flow_strength")
    private Double orderFlowStrength;

    // ==================== Analysis Metadata ====================

    /**
     * Current price at time of analysis
     */
    @Column(name = "current_price")
    private Double currentPrice;

    /**
     * Overall bias for trade direction: BULLISH, BEARISH, NEUTRAL
     */
    @Column(name = "overall_bias")
    private String overallBias;

    /**
     * Confidence score for the overall analysis (0-100)
     */
    @Column(name = "analysis_confidence")
    private Double analysisConfidence;

    /**
     * Number of candles analyzed
     */
    @Column(name = "candles_analyzed")
    private Integer candlesAnalyzed;

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
