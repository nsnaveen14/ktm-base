package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

/**
 * Entity representing the result of an IOB-based trade.
 * Tracks entry, exit, P&L, and performance metrics for backtesting and analysis.
 */
@Entity
@Table(name = "trade_results")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TradeResult {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // Link to the originating IOB
    @Column(name = "iob_id")
    private Long iobId;

    @Column(name = "trade_id")
    private String tradeId;

    @Column(name = "instrument_token", nullable = false)
    private Long instrumentToken;

    @Column(name = "instrument_name")
    private String instrumentName;

    @Column(name = "timeframe")
    private String timeframe;

    // Trade Direction
    @Column(name = "trade_direction")
    private String tradeDirection; // LONG, SHORT

    @Column(name = "trade_type")
    private String tradeType; // SIMULATED, LIVE, BACKTEST

    // Entry Details
    @Column(name = "entry_price")
    private Double entryPrice;

    @Column(name = "entry_time")
    private LocalDateTime entryTime;

    @Column(name = "entry_reason")
    private String entryReason; // ZONE_TOUCH, CONFIRMATION_CANDLE, MANUAL

    // Exit Details
    @Column(name = "exit_price")
    private Double exitPrice;

    @Column(name = "exit_time")
    private LocalDateTime exitTime;

    @Column(name = "exit_reason")
    private String exitReason; // TARGET_1, TARGET_2, TARGET_3, STOP_LOSS, MANUAL, TRAILING_SL

    // Stop Loss & Targets
    @Column(name = "stop_loss")
    private Double stopLoss;

    @Column(name = "target_1")
    private Double target1;

    @Column(name = "target_2")
    private Double target2;

    @Column(name = "target_3")
    private Double target3;

    @Column(name = "trailing_stop")
    private Double trailingStop;

    // Position Details
    @Column(name = "quantity")
    private Integer quantity;

    @Column(name = "lot_size")
    private Integer lotSize;

    @Column(name = "position_value")
    private Double positionValue;

    // P&L Metrics
    @Column(name = "gross_pnl")
    private Double grossPnl;

    @Column(name = "net_pnl")
    private Double netPnl;

    @Column(name = "pnl_percent")
    private Double pnlPercent;

    @Column(name = "pnl_points")
    private Double pnlPoints;

    @Column(name = "brokerage")
    private Double brokerage;

    @Column(name = "taxes")
    private Double taxes;

    // Risk Metrics
    @Column(name = "risk_amount")
    private Double riskAmount;

    @Column(name = "reward_amount")
    private Double rewardAmount;

    @Column(name = "planned_rr_ratio")
    private Double plannedRRRatio;

    @Column(name = "achieved_rr_ratio")
    private Double achievedRRRatio;

    @Column(name = "max_favorable_excursion")
    private Double maxFavorableExcursion; // MFE - max profit before exit

    @Column(name = "max_adverse_excursion")
    private Double maxAdverseExcursion; // MAE - max loss before exit

    // Outcome
    @Column(name = "outcome")
    private String outcome; // WIN, LOSS, BREAKEVEN

    @Column(name = "target_hit")
    private String targetHit; // NONE, TARGET_1, TARGET_2, TARGET_3

    @Column(name = "stop_loss_hit")
    private Boolean stopLossHit;

    // IOB Context (copied from IOB for historical analysis)
    @Column(name = "iob_type")
    private String iobType;

    @Column(name = "zone_high")
    private Double zoneHigh;

    @Column(name = "zone_low")
    private Double zoneLow;

    @Column(name = "signal_confidence")
    private Double signalConfidence;

    @Column(name = "enhanced_confidence")
    private Double enhancedConfidence;

    @Column(name = "had_fvg")
    private Boolean hadFvg;

    @Column(name = "was_trend_aligned")
    private Boolean wasTrendAligned;

    @Column(name = "volume_type")
    private String volumeType;

    // Trade Duration
    @Column(name = "duration_minutes")
    private Long durationMinutes;

    @Column(name = "holding_period")
    private String holdingPeriod; // INTRADAY, OVERNIGHT, MULTI_DAY

    // Status
    @Column(name = "status")
    private String status; // OPEN, CLOSED, CANCELLED

    @Column(name = "notes")
    private String notes;

    // Timestamps
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

    /**
     * Calculate P&L metrics after trade closure
     */
    public void calculateMetrics() {
        if (entryPrice == null || exitPrice == null) return;

        // Calculate P&L points
        if ("LONG".equals(tradeDirection)) {
            pnlPoints = exitPrice - entryPrice;
        } else {
            pnlPoints = entryPrice - exitPrice;
        }

        // Calculate gross P&L
        if (quantity != null && quantity > 0) {
            grossPnl = pnlPoints * quantity;
        } else {
            grossPnl = pnlPoints;
        }

        // Calculate net P&L (after charges)
        double charges = (brokerage != null ? brokerage : 0) + (taxes != null ? taxes : 0);
        netPnl = grossPnl - charges;

        // Calculate percentage
        if (positionValue != null && positionValue > 0) {
            pnlPercent = (netPnl / positionValue) * 100;
        }

        // Calculate achieved RR
        if (riskAmount != null && riskAmount > 0) {
            achievedRRRatio = netPnl / riskAmount;
        }

        // Determine outcome
        if (netPnl > 0) {
            outcome = "WIN";
        } else if (netPnl < 0) {
            outcome = "LOSS";
        } else {
            outcome = "BREAKEVEN";
        }

        // Calculate duration
        if (entryTime != null && exitTime != null) {
            durationMinutes = java.time.Duration.between(entryTime, exitTime).toMinutes();

            // Determine holding period
            if (durationMinutes < 375) { // Less than market hours
                holdingPeriod = "INTRADAY";
            } else if (durationMinutes < 1500) { // Less than 2 days
                holdingPeriod = "OVERNIGHT";
            } else {
                holdingPeriod = "MULTI_DAY";
            }
        }
    }
}
