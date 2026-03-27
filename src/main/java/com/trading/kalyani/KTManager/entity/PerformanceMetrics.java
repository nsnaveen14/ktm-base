package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Entity representing aggregated performance metrics for IOB trading.
 * Stores daily, weekly, and overall statistics for performance tracking.
 */
@Entity
@Table(name = "performance_metrics")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PerformanceMetrics {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "metric_date", nullable = false)
    private LocalDate metricDate;

    @Column(name = "period_type")
    private String periodType; // DAILY, WEEKLY, MONTHLY, ALL_TIME

    @Column(name = "instrument_token")
    private Long instrumentToken; // null for ALL instruments

    @Column(name = "instrument_name")
    private String instrumentName;

    @Column(name = "strategy_type")
    private String strategyType; // IOB, IOB_WITH_FVG, etc.

    // Trade Counts
    @Column(name = "total_trades")
    private Integer totalTrades;

    @Column(name = "winning_trades")
    private Integer winningTrades;

    @Column(name = "losing_trades")
    private Integer losingTrades;

    @Column(name = "breakeven_trades")
    private Integer breakevenTrades;

    // Win/Loss Metrics
    @Column(name = "win_rate")
    private Double winRate; // Percentage

    @Column(name = "loss_rate")
    private Double lossRate;

    @Column(name = "avg_win_amount")
    private Double avgWinAmount;

    @Column(name = "avg_loss_amount")
    private Double avgLossAmount;

    @Column(name = "largest_win")
    private Double largestWin;

    @Column(name = "largest_loss")
    private Double largestLoss;

    @Column(name = "win_loss_ratio")
    private Double winLossRatio; // Avg Win / Avg Loss

    // Risk-Reward Metrics
    @Column(name = "avg_rr_planned")
    private Double avgRRPlanned;

    @Column(name = "avg_rr_achieved")
    private Double avgRRAchieved;

    @Column(name = "expectancy")
    private Double expectancy; // (Win% * AvgWin) - (Loss% * AvgLoss)

    @Column(name = "profit_factor")
    private Double profitFactor; // Total Wins / Total Losses

    // P&L Metrics
    @Column(name = "total_gross_pnl")
    private Double totalGrossPnl;

    @Column(name = "total_net_pnl")
    private Double totalNetPnl;

    @Column(name = "total_brokerage")
    private Double totalBrokerage;

    @Column(name = "total_taxes")
    private Double totalTaxes;

    @Column(name = "avg_pnl_per_trade")
    private Double avgPnlPerTrade;

    @Column(name = "avg_points_per_trade")
    private Double avgPointsPerTrade;

    // Drawdown Metrics
    @Column(name = "max_drawdown")
    private Double maxDrawdown;

    @Column(name = "max_drawdown_percent")
    private Double maxDrawdownPercent;

    @Column(name = "max_drawdown_duration_days")
    private Integer maxDrawdownDurationDays;

    @Column(name = "current_drawdown")
    private Double currentDrawdown;

    @Column(name = "peak_equity")
    private Double peakEquity;

    // Streak Analysis
    @Column(name = "current_win_streak")
    private Integer currentWinStreak;

    @Column(name = "max_win_streak")
    private Integer maxWinStreak;

    @Column(name = "current_loss_streak")
    private Integer currentLossStreak;

    @Column(name = "max_loss_streak")
    private Integer maxLossStreak;

    // Trade Duration
    @Column(name = "avg_trade_duration_minutes")
    private Double avgTradeDurationMinutes;

    @Column(name = "avg_winner_duration_minutes")
    private Double avgWinnerDurationMinutes;

    @Column(name = "avg_loser_duration_minutes")
    private Double avgLoserDurationMinutes;

    // Target Analysis
    @Column(name = "target_1_hit_count")
    private Integer target1HitCount;

    @Column(name = "target_2_hit_count")
    private Integer target2HitCount;

    @Column(name = "target_3_hit_count")
    private Integer target3HitCount;

    @Column(name = "stop_loss_hit_count")
    private Integer stopLossHitCount;

    @Column(name = "target_1_hit_rate")
    private Double target1HitRate;

    @Column(name = "target_2_hit_rate")
    private Double target2HitRate;

    @Column(name = "target_3_hit_rate")
    private Double target3HitRate;

    // IOB-Specific Metrics
    @Column(name = "bullish_iob_trades")
    private Integer bullishIOBTrades;

    @Column(name = "bearish_iob_trades")
    private Integer bearishIOBTrades;

    @Column(name = "bullish_win_rate")
    private Double bullishWinRate;

    @Column(name = "bearish_win_rate")
    private Double bearishWinRate;

    @Column(name = "fvg_confluence_trades")
    private Integer fvgConfluenceTrades;

    @Column(name = "fvg_confluence_win_rate")
    private Double fvgConfluenceWinRate;

    @Column(name = "trend_aligned_trades")
    private Integer trendAlignedTrades;

    @Column(name = "trend_aligned_win_rate")
    private Double trendAlignedWinRate;

    // Confidence Analysis
    @Column(name = "avg_confidence_winners")
    private Double avgConfidenceWinners;

    @Column(name = "avg_confidence_losers")
    private Double avgConfidenceLosers;

    @Column(name = "high_confidence_trades")
    private Integer highConfidenceTrades; // > 75%

    @Column(name = "high_confidence_win_rate")
    private Double highConfidenceWinRate;

    // Time-Based Analysis
    @Column(name = "morning_session_trades")
    private Integer morningSessionTrades; // 9:15-12:00

    @Column(name = "afternoon_session_trades")
    private Integer afternoonSessionTrades; // 12:00-3:30

    @Column(name = "morning_session_win_rate")
    private Double morningSessionWinRate;

    @Column(name = "afternoon_session_win_rate")
    private Double afternoonSessionWinRate;

    // Timestamps
    @Column(name = "calculation_timestamp")
    private LocalDateTime calculationTimestamp;

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
