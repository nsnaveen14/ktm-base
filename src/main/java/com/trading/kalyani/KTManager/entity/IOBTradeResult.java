package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

/**
 * Entity representing the outcome of an IOB-based trade.
 * Used for historical performance tracking and backtesting.
 */
@Entity
@Table(name = "iob_trade_results")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class IOBTradeResult {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // Link to the IOB that generated this trade
    @Column(name = "iob_id", nullable = false)
    private Long iobId;

    @Column(name = "trade_id", unique = true)
    private String tradeId;

    // Instrument details
    @Column(name = "instrument_token")
    private Long instrumentToken;

    @Column(name = "instrument_name")
    private String instrumentName;

    @Column(name = "timeframe")
    private String timeframe;

    // IOB details at trade time
    @Column(name = "iob_type")
    private String iobType; // BULLISH_IOB, BEARISH_IOB

    @Column(name = "trade_direction")
    private String tradeDirection; // LONG, SHORT

    @Column(name = "signal_confidence")
    private Double signalConfidence;

    @Column(name = "has_fvg")
    private Boolean hasFvg;

    // Entry details
    @Column(name = "zone_high")
    private Double zoneHigh;

    @Column(name = "zone_low")
    private Double zoneLow;

    @Column(name = "planned_entry")
    private Double plannedEntry;

    @Column(name = "actual_entry")
    private Double actualEntry;

    @Column(name = "entry_time")
    private LocalDateTime entryTime;

    @Column(name = "entry_trigger")
    private String entryTrigger; // ZONE_TOUCH, ZONE_MIDPOINT, ZONE_BREAK, MANUAL

    // Stop loss details
    @Column(name = "planned_stop_loss")
    private Double plannedStopLoss;

    @Column(name = "actual_stop_loss")
    private Double actualStopLoss;

    // Target details
    @Column(name = "target_1")
    private Double target1;

    @Column(name = "target_2")
    private Double target2;

    @Column(name = "target_3")
    private Double target3;

    @Column(name = "planned_rr")
    private Double plannedRR;

    // Exit details
    @Column(name = "exit_price")
    private Double exitPrice;

    @Column(name = "exit_time")
    private LocalDateTime exitTime;

    @Column(name = "exit_reason")
    private String exitReason; // STOP_LOSS, TARGET_1, TARGET_2, TARGET_3, TRAILING_SL, MANUAL, TIME_EXIT

    // Trade outcome
    @Column(name = "points_captured")
    private Double pointsCaptured;

    @Column(name = "risk_points")
    private Double riskPoints;

    @Column(name = "achieved_rr")
    private Double achievedRR; // Actual risk-reward achieved

    @Column(name = "is_winner")
    private Boolean isWinner;

    @Column(name = "target_hit")
    private Integer targetHit; // 0 = SL, 1 = T1, 2 = T2, 3 = T3

    // P&L (for actual trades)
    @Column(name = "quantity")
    private Integer quantity;

    @Column(name = "gross_pnl")
    private Double grossPnl;

    @Column(name = "net_pnl")
    private Double netPnl;

    // Trade type
    @Column(name = "trade_mode")
    private String tradeMode; // SIMULATED, LIVE, BACKTEST

    // Market context
    @Column(name = "market_trend")
    private String marketTrend; // BULLISH, BEARISH, SIDEWAYS

    @Column(name = "volatility")
    private Double volatility; // VIX or ATR at entry

    // Multi-timeframe context
    @Column(name = "htf_aligned")
    private Boolean htfAligned; // Was higher timeframe aligned?

    @Column(name = "mtf_confluence_score")
    private Double mtfConfluenceScore;

    // Peak tracking
    @Column(name = "peak_favorable")
    private Double peakFavorable; // Maximum favorable excursion

    @Column(name = "peak_adverse")
    private Double peakAdverse; // Maximum adverse excursion (drawdown)

    // Status
    @Column(name = "status")
    private String status; // PENDING, OPEN, CLOSED, CANCELLED

    // Notes
    @Column(name = "notes", length = 1000)
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
        if (status == null) {
            status = "PENDING";
        }
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }

    /**
     * Calculate P&L and trade metrics after exit
     */
    public void calculateMetrics() {
        if (actualEntry != null && exitPrice != null) {
            // Calculate points captured
            if ("LONG".equals(tradeDirection)) {
                pointsCaptured = exitPrice - actualEntry;
            } else {
                pointsCaptured = actualEntry - exitPrice;
            }

            // Calculate risk points
            if (actualStopLoss != null) {
                riskPoints = Math.abs(actualEntry - actualStopLoss);
            } else if (plannedStopLoss != null) {
                riskPoints = Math.abs(actualEntry - plannedStopLoss);
            }

            // Calculate achieved RR
            if (riskPoints != null && riskPoints > 0) {
                achievedRR = pointsCaptured / riskPoints;
            }

            // Determine if winner
            isWinner = pointsCaptured > 0;

            // Determine target hit
            if (pointsCaptured <= 0) {
                targetHit = 0; // Stop loss
            } else if (target1 != null && exitPrice != null) {
                double t1Points = Math.abs(target1 - actualEntry);
                double t2Points = target2 != null ? Math.abs(target2 - actualEntry) : Double.MAX_VALUE;
                double t3Points = target3 != null ? Math.abs(target3 - actualEntry) : Double.MAX_VALUE;

                if (pointsCaptured >= t3Points * 0.95) {
                    targetHit = 3;
                } else if (pointsCaptured >= t2Points * 0.95) {
                    targetHit = 2;
                } else if (pointsCaptured >= t1Points * 0.95) {
                    targetHit = 1;
                } else {
                    targetHit = 0;
                }
            }

            // Calculate P&L if quantity is set
            if (quantity != null && quantity > 0) {
                grossPnl = pointsCaptured * quantity;
                // Estimate brokerage (approx)
                netPnl = grossPnl - (quantity * 0.1); // Simplified
            }
        }
    }
}
