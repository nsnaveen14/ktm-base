package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

/**
 * Entity for storing liquidity zone analysis data.
 * Focuses on identifying liquidity grab points where stop losses are clustered.
 */
@Entity
@Table(name = "liquidity_zone_analysis")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class LiquidityZoneAnalysis {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "instrument_token", nullable = false)
    private Long instrumentToken;

    @Column(name = "instrument_name")
    private String instrumentName; // NIFTY

    @Column(name = "timeframe", nullable = false)
    private String timeframe; // 5min, 15min, 1hour

    @Column(name = "analysis_timestamp", nullable = false)
    private LocalDateTime analysisTimestamp;

    // Current price data
    @Column(name = "current_price")
    private Double currentPrice;

    @Column(name = "current_high")
    private Double currentHigh;

    @Column(name = "current_low")
    private Double currentLow;

    // Previous day levels
    @Column(name = "previous_day_high")
    private Double previousDayHigh;

    @Column(name = "previous_day_low")
    private Double previousDayLow;

    // Day before yesterday levels
    @Column(name = "day_before_yesterday_high")
    private Double dayBeforeYesterdayHigh;

    @Column(name = "day_before_yesterday_low")
    private Double dayBeforeYesterdayLow;

    // Current day levels
    @Column(name = "current_day_high")
    private Double currentDayHigh;

    @Column(name = "current_day_low")
    private Double currentDayLow;

    // Timeframe specific highs/lows
    @Column(name = "timeframe_high_1") // Most recent swing high
    private Double timeframeHigh1;

    @Column(name = "timeframe_high_2")
    private Double timeframeHigh2;

    @Column(name = "timeframe_high_3")
    private Double timeframeHigh3;

    @Column(name = "timeframe_low_1") // Most recent swing low
    private Double timeframeLow1;

    @Column(name = "timeframe_low_2")
    private Double timeframeLow2;

    @Column(name = "timeframe_low_3")
    private Double timeframeLow3;

    // Liquidity zones (stop loss clusters)
    @Column(name = "buy_side_liquidity_1") // Stop losses above (long traders)
    private Double buySideLiquidity1;

    @Column(name = "buy_side_liquidity_2")
    private Double buySideLiquidity2;

    @Column(name = "buy_side_liquidity_3")
    private Double buySideLiquidity3;

    @Column(name = "sell_side_liquidity_1") // Stop losses below (short traders)
    private Double sellSideLiquidity1;

    @Column(name = "sell_side_liquidity_2")
    private Double sellSideLiquidity2;

    @Column(name = "sell_side_liquidity_3")
    private Double sellSideLiquidity3;

    // Liquidity grab detection
    @Column(name = "buy_side_grabbed")
    private Boolean buySideGrabbed; // Price swept above high

    @Column(name = "sell_side_grabbed")
    private Boolean sellSideGrabbed; // Price swept below low

    @Column(name = "grabbed_level")
    private Double grabbedLevel;

    @Column(name = "grab_type")
    private String grabType; // BUY_SIDE_GRAB, SELL_SIDE_GRAB

    // Market structure
    @Column(name = "market_structure")
    private String marketStructure; // BULLISH, BEARISH, RANGING

    @Column(name = "trend_strength")
    private Double trendStrength; // 0-100

    // Trade setup recommendation
    @Column(name = "trade_signal")
    private String tradeSignal; // LONG_UNWIND, SHORT_COVER, NO_SIGNAL

    @Column(name = "signal_confidence")
    private Double signalConfidence; // 0-100

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

    @Column(name = "position_type")
    private String positionType; // LONG, SHORT, NEUTRAL

    @Column(name = "strategy_type")
    private String strategyType; // LIQUIDITY_GRAB_REVERSAL, SWEEP_AND_RETEST

    // Analysis details
    @Column(name = "volume_at_grab")
    private Long volumeAtGrab;

    @Column(name = "is_valid_setup")
    private Boolean isValidSetup;

    @Column(name = "rejection_reason", length = 500)
    private String rejectionReason;

    @Column(name = "notes", length = 1000)
    private String notes;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        if (analysisTimestamp == null) {
            analysisTimestamp = LocalDateTime.now();
        }
    }
}

