package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

/**
 * Entity representing an Internal Order Block (IOB) detection.
 * IOBs are key institutional footprint patterns that indicate potential
 * reversal or continuation zones based on order flow analysis.
 *
 * Bullish IOB: Last bearish candle before a significant bullish move
 * Bearish IOB: Last bullish candle before a significant bearish move
 */
@Entity
@Table(name = "internal_order_blocks")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class InternalOrderBlock {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "instrument_token", nullable = false)
    private Long instrumentToken;

    @Column(name = "instrument_name")
    private String instrumentName; // NIFTY

    @Column(name = "timeframe", nullable = false)
    private String timeframe; // 5min, 15min, etc.

    @Column(name = "detection_timestamp", nullable = false)
    private LocalDateTime detectionTimestamp;

    // Order Block Candle Details
    @Column(name = "ob_candle_time")
    private LocalDateTime obCandleTime;

    @Column(name = "ob_type")
    private String obType; // BULLISH_IOB, BEARISH_IOB

    @Column(name = "ob_high")
    private Double obHigh;

    @Column(name = "ob_low")
    private Double obLow;

    @Column(name = "ob_open")
    private Double obOpen;

    @Column(name = "ob_close")
    private Double obClose;

    // Zone levels (the actual tradeable zone)
    @Column(name = "zone_high")
    private Double zoneHigh;

    @Column(name = "zone_low")
    private Double zoneLow;

    @Column(name = "zone_midpoint")
    private Double zoneMidpoint;

    // Displacement candle details (the candle that broke structure)
    @Column(name = "displacement_high")
    private Double displacementHigh;

    @Column(name = "displacement_low")
    private Double displacementLow;

    @Column(name = "displacement_body_percent")
    private Double displacementBodyPercent; // Body to range ratio

    // Market context
    @Column(name = "current_price")
    private Double currentPrice;

    @Column(name = "distance_to_zone")
    private Double distanceToZone; // Points away from zone

    @Column(name = "distance_percent")
    private Double distancePercent;

    // Break of Structure (BOS) details
    @Column(name = "bos_level")
    private Double bosLevel; // The swing high/low that was broken

    @Column(name = "bos_type")
    private String bosType; // BULLISH_BOS, BEARISH_BOS

    // Fair Value Gap (FVG) presence
    @Column(name = "has_fvg")
    private Boolean hasFvg;

    @Column(name = "fvg_high")
    private Double fvgHigh;

    @Column(name = "fvg_low")
    private Double fvgLow;

    // ==================== FVG Validation (6-Factor) ====================

    /**
     * Whether the FVG passed overall validation (score >= 50)
     */
    @Column(name = "fvg_valid")
    private Boolean fvgValid;

    /**
     * Composite FVG validation score (0-100) based on 6 factors
     */
    @Column(name = "fvg_validation_score")
    private Double fvgValidationScore;

    /**
     * Human-readable breakdown of FVG validation factors
     */
    @Column(name = "fvg_validation_details", length = 1000)
    private String fvgValidationDetails;

    /**
     * FVG priority rank among same-type FVGs (1 = highest priority)
     * Bullish: lowest zone = highest priority; Bearish: highest zone = highest priority
     */
    @Column(name = "fvg_priority")
    private Integer fvgPriority;

    /**
     * Factor 1: FVG zone has not been tested/filled/mitigated by price since creation
     */
    @Column(name = "fvg_unmitigated")
    private Boolean fvgUnmitigated;

    /**
     * Factor 2: Reaction candle closes inside FVG or in direction of gap
     */
    @Column(name = "fvg_candle_reaction_valid")
    private Boolean fvgCandleReactionValid;

    /**
     * Factor 3: FVG overlaps with prior support (bullish) or resistance (bearish) levels
     */
    @Column(name = "fvg_sr_confluence")
    private Boolean fvgSrConfluence;

    /**
     * Factor 5: FVG is in valid Gann Box position (bullish in 0-0.5, bearish in 0.5-1)
     */
    @Column(name = "fvg_gann_box_valid")
    private Boolean fvgGannBoxValid;

    /**
     * Factor 6: FVG formed after a Break of Structure
     */
    @Column(name = "fvg_bos_confirmed")
    private Boolean fvgBosConfirmed;

    // Trade setup
    @Column(name = "trade_direction")
    private String tradeDirection; // LONG, SHORT

    @Column(name = "entry_price")
    private Double entryPrice;

    @Column(name = "stop_loss")
    private Double stopLoss;

    @Column(name = "target_1")
    private Double target1;

    @Column(name = "target_2")
    private Double target2;

    @Column(name = "target_3")
    private Double target3;

    @Column(name = "risk_reward_ratio")
    private Double riskRewardRatio;

    // Option premium equivalents at each index target (populated when trade is placed)
    @Column(name = "premium_t1")
    private Double premiumT1;

    @Column(name = "premium_t2")
    private Double premiumT2;

    @Column(name = "premium_t3")
    private Double premiumT3;

    // Status tracking
    @Column(name = "status")
    private String status; // FRESH, MITIGATED, EXPIRED, TRADED

    @Column(name = "is_valid")
    private Boolean isValid;

    @Column(name = "validation_notes")
    private String validationNotes;

    @Column(name = "signal_confidence")
    private Double signalConfidence;

    // ==================== Market Structure Context ====================

    /**
     * Market trend at time of IOB detection: UPTREND, DOWNTREND, SIDEWAYS
     */
    @Column(name = "market_trend")
    private String marketTrend;

    /**
     * Whether the trade direction aligns with higher timeframe trend
     */
    @Column(name = "trend_aligned")
    private Boolean trendAligned;

    /**
     * Price zone: PREMIUM, DISCOUNT, EQUILIBRIUM
     */
    @Column(name = "price_zone")
    private String priceZone;

    /**
     * Market phase: ACCUMULATION, DISTRIBUTION, MARKUP, MARKDOWN
     */
    @Column(name = "market_phase")
    private String marketPhase;

    /**
     * Whether CHoCH was detected near the IOB
     */
    @Column(name = "choch_confluence")
    private Boolean chochConfluence;

    /**
     * Market structure confluence score (0-100)
     */
    @Column(name = "structure_confluence_score")
    private Double structureConfluenceScore;

    // ==================== Volume Profile Context ====================

    /**
     * Volume at IOB candle formation
     */
    @Column(name = "iob_volume")
    private Long iobVolume;

    /**
     * IOB volume relative to average: >1.5 = INSTITUTIONAL, <0.5 = RETAIL
     */
    @Column(name = "iob_volume_ratio")
    private Double iobVolumeRatio;

    /**
     * Volume type: INSTITUTIONAL, RETAIL, NORMAL
     */
    @Column(name = "volume_type")
    private String volumeType;

    /**
     * Whether displacement volume confirms the move
     */
    @Column(name = "displacement_volume_confirmed")
    private Boolean displacementVolumeConfirmed;

    /**
     * Whether POC aligns with IOB zone
     */
    @Column(name = "poc_aligned")
    private Boolean pocAligned;

    /**
     * Volume delta direction: BULLISH, BEARISH, NEUTRAL
     */
    @Column(name = "volume_delta_direction")
    private String volumeDeltaDirection;

    /**
     * Volume confluence score (0-100)
     */
    @Column(name = "volume_confluence_score")
    private Double volumeConfluenceScore;

    // ==================== Risk Management Context ====================

    /**
     * Calculated position size based on risk parameters
     */
    @Column(name = "position_size")
    private Integer positionSize;

    /**
     * Number of lots
     */
    @Column(name = "lot_count")
    private Integer lotCount;

    /**
     * Risk amount for this trade
     */
    @Column(name = "risk_amount")
    private Double riskAmount;

    /**
     * ATR value at detection time
     */
    @Column(name = "atr_value")
    private Double atrValue;

    /**
     * Dynamic stop loss based on ATR
     */
    @Column(name = "dynamic_stop_loss")
    private Double dynamicStopLoss;

    /**
     * Whether trade passed risk validation
     */
    @Column(name = "risk_validated")
    private Boolean riskValidated;

    /**
     * Risk validation notes
     */
    @Column(name = "risk_notes")
    private String riskNotes;

    /**
     * Overall enhanced confidence score combining all factors
     */
    @Column(name = "enhanced_confidence")
    private Double enhancedConfidence;

    // Trade execution tracking
    @Column(name = "trade_taken")
    private Boolean tradeTaken;

    @Column(name = "trade_id")
    private String tradeId;

    @Column(name = "mitigation_time")
    private LocalDateTime mitigationTime;

    /**
     * Flag to track if mitigation alert has been sent (to prevent duplicate alerts)
     */
    @Column(name = "mitigation_alert_sent")
    private Boolean mitigationAlertSent;

    /**
     * Flag to track if detection alert has been sent (to prevent duplicate alerts)
     */
    @Column(name = "detection_alert_sent")
    private Boolean detectionAlertSent;

    /**
     * Flag to track if target hit alerts have been sent
     */
    @Column(name = "target1_alert_sent")
    private Boolean target1AlertSent;

    @Column(name = "target2_alert_sent")
    private Boolean target2AlertSent;

    @Column(name = "target3_alert_sent")
    private Boolean target3AlertSent;

    // ==================== Trade Timeline Tracking ====================

    /**
     * Time when price entered the zone (entry triggered)
     */
    @Column(name = "entry_triggered_time")
    private LocalDateTime entryTriggeredTime;

    /**
     * Actual entry price when zone was touched
     */
    @Column(name = "actual_entry_price")
    private Double actualEntryPrice;

    /**
     * Time when stop loss was hit
     */
    @Column(name = "stop_loss_hit_time")
    private LocalDateTime stopLossHitTime;

    /**
     * Price at which stop loss was hit
     */
    @Column(name = "stop_loss_hit_price")
    private Double stopLossHitPrice;

    /**
     * Time when Target 1 was hit
     */
    @Column(name = "target1_hit_time")
    private LocalDateTime target1HitTime;

    /**
     * Price at which Target 1 was hit
     */
    @Column(name = "target1_hit_price")
    private Double target1HitPrice;

    /**
     * Time when Target 2 was hit
     */
    @Column(name = "target2_hit_time")
    private LocalDateTime target2HitTime;

    /**
     * Price at which Target 2 was hit
     */
    @Column(name = "target2_hit_price")
    private Double target2HitPrice;

    /**
     * Time when Target 3 was hit
     */
    @Column(name = "target3_hit_time")
    private LocalDateTime target3HitTime;

    /**
     * Price at which Target 3 was hit
     */
    @Column(name = "target3_hit_price")
    private Double target3HitPrice;

    /**
     * Maximum favorable excursion (highest profit point reached)
     */
    @Column(name = "max_favorable_excursion")
    private Double maxFavorableExcursion;

    /**
     * Maximum adverse excursion (biggest drawdown)
     */
    @Column(name = "max_adverse_excursion")
    private Double maxAdverseExcursion;

    /**
     * Final outcome: WIN, LOSS, BREAKEVEN, ACTIVE
     */
    @Column(name = "trade_outcome")
    private String tradeOutcome;

    /**
     * Points captured from entry to exit
     */
    @Column(name = "points_captured")
    private Double pointsCaptured;

    /**
     * Unique signature for this IOB based on candle time, type and zone (for deduplication)
     */
    @Column(name = "iob_signature", unique = true)
    private String iobSignature;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
