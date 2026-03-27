package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Entity representing Risk Management configuration and tracking.
 *
 * This entity tracks:
 * - Position sizing based on account risk percentage
 * - Dynamic SL placement based on ATR
 * - Maximum daily loss limits
 * - Portfolio heat tracking (total risk across open positions)
 * - Correlation-based exposure limits
 */
@Entity
@Table(name = "risk_management")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RiskManagement {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "instrument_token")
    private Long instrumentToken;

    @Column(name = "instrument_name")
    private String instrumentName;

    @Column(name = "analysis_date", nullable = false)
    private LocalDate analysisDate;

    @Column(name = "analysis_timestamp")
    private LocalDateTime analysisTimestamp;

    // ==================== Account Configuration ====================

    /**
     * Total account capital
     */
    @Column(name = "account_capital")
    private Double accountCapital;

    /**
     * Risk per trade as percentage (e.g., 1.0 = 1% of account)
     */
    @Column(name = "risk_per_trade_percent")
    private Double riskPerTradePercent;

    /**
     * Maximum risk amount per trade in currency
     */
    @Column(name = "max_risk_per_trade")
    private Double maxRiskPerTrade;

    // ==================== Daily Loss Limits ====================

    /**
     * Maximum daily loss allowed as percentage of account
     */
    @Column(name = "max_daily_loss_percent")
    private Double maxDailyLossPercent;

    /**
     * Maximum daily loss in currency
     */
    @Column(name = "max_daily_loss_amount")
    private Double maxDailyLossAmount;

    /**
     * Current day's realized P&L
     */
    @Column(name = "daily_realized_pnl")
    private Double dailyRealizedPnl;

    /**
     * Current day's unrealized P&L from open positions
     */
    @Column(name = "daily_unrealized_pnl")
    private Double dailyUnrealizedPnl;

    /**
     * Total daily P&L (realized + unrealized)
     */
    @Column(name = "daily_total_pnl")
    private Double dailyTotalPnl;

    /**
     * Whether daily loss limit has been reached
     */
    @Column(name = "daily_limit_reached")
    private Boolean dailyLimitReached;

    /**
     * Number of trades taken today
     */
    @Column(name = "daily_trade_count")
    private Integer dailyTradeCount;

    /**
     * Maximum trades allowed per day
     */
    @Column(name = "max_daily_trades")
    private Integer maxDailyTrades;

    // ==================== ATR-Based Risk ====================

    /**
     * Average True Range (ATR) value
     */
    @Column(name = "atr_value")
    private Double atrValue;

    /**
     * ATR period used for calculation (default: 14)
     */
    @Column(name = "atr_period")
    private Integer atrPeriod;

    /**
     * ATR multiplier for stop loss (e.g., 1.5 = 1.5 * ATR)
     */
    @Column(name = "atr_sl_multiplier")
    private Double atrSlMultiplier;

    /**
     * Dynamic stop loss distance based on ATR
     */
    @Column(name = "dynamic_sl_distance")
    private Double dynamicSlDistance;

    /**
     * ATR as percentage of price (volatility measure)
     */
    @Column(name = "atr_percent")
    private Double atrPercent;

    // ==================== Position Sizing ====================

    /**
     * Calculated position size (quantity) for current risk parameters
     */
    @Column(name = "calculated_position_size")
    private Integer calculatedPositionSize;

    /**
     * Lot size for the instrument (for F&O)
     */
    @Column(name = "lot_size")
    private Integer lotSize;

    /**
     * Number of lots for the calculated position
     */
    @Column(name = "calculated_lots")
    private Integer calculatedLots;

    /**
     * Total position value
     */
    @Column(name = "position_value")
    private Double positionValue;

    /**
     * Risk amount for the calculated position
     */
    @Column(name = "position_risk_amount")
    private Double positionRiskAmount;

    // ==================== Portfolio Heat Tracking ====================

    /**
     * Number of currently open positions
     */
    @Column(name = "open_positions_count")
    private Integer openPositionsCount;

    /**
     * Total risk amount across all open positions
     */
    @Column(name = "total_open_risk")
    private Double totalOpenRisk;

    /**
     * Portfolio heat as percentage of account (total_open_risk / account_capital)
     */
    @Column(name = "portfolio_heat_percent")
    private Double portfolioHeatPercent;

    /**
     * Maximum portfolio heat allowed (default: 6%)
     */
    @Column(name = "max_portfolio_heat_percent")
    private Double maxPortfolioHeatPercent;

    /**
     * Whether portfolio heat limit has been reached
     */
    @Column(name = "portfolio_heat_exceeded")
    private Boolean portfolioHeatExceeded;

    /**
     * Remaining risk capacity in currency
     */
    @Column(name = "remaining_risk_capacity")
    private Double remainingRiskCapacity;

    // ==================== Exposure Limits ====================

    /**
     * Maximum exposure per instrument as percentage of account
     */
    @Column(name = "max_instrument_exposure_percent")
    private Double maxInstrumentExposurePercent;

    /**
     * Current exposure to this instrument
     */
    @Column(name = "current_instrument_exposure")
    private Double currentInstrumentExposure;

    /**
     * Maximum correlated exposure allowed
     */
    @Column(name = "max_correlated_exposure_percent")
    private Double maxCorrelatedExposurePercent;

    /**
     * Total correlated exposure (e.g., all index positions combined)
     */
    @Column(name = "correlated_exposure")
    private Double correlatedExposure;

    /**
     * Correlation group: INDEX, BANK, IT, PHARMA, etc.
     */
    @Column(name = "correlation_group")
    private String correlationGroup;

    // ==================== Risk Metrics ====================

    /**
     * Win rate from recent trades (percentage)
     */
    @Column(name = "win_rate")
    private Double winRate;

    /**
     * Average risk-reward achieved
     */
    @Column(name = "avg_rr_achieved")
    private Double avgRRachieved;

    /**
     * Maximum drawdown percentage
     */
    @Column(name = "max_drawdown_percent")
    private Double maxDrawdownPercent;

    /**
     * Current drawdown from peak
     */
    @Column(name = "current_drawdown_percent")
    private Double currentDrawdownPercent;

    /**
     * Peak account value
     */
    @Column(name = "peak_account_value")
    private Double peakAccountValue;

    /**
     * Profit factor (gross profit / gross loss)
     */
    @Column(name = "profit_factor")
    private Double profitFactor;

    /**
     * Sharpe ratio
     */
    @Column(name = "sharpe_ratio")
    private Double sharpeRatio;

    // ==================== Trade Approval Status ====================

    /**
     * Whether trading is allowed based on risk parameters
     */
    @Column(name = "trading_allowed")
    private Boolean tradingAllowed;

    /**
     * Reason if trading is not allowed
     */
    @Column(name = "trading_blocked_reason")
    private String tradingBlockedReason;

    /**
     * Risk assessment score (0-100, higher = safer to trade)
     */
    @Column(name = "risk_assessment_score")
    private Double riskAssessmentScore;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
        if (analysisDate == null) {
            analysisDate = LocalDate.now();
        }
        if (analysisTimestamp == null) {
            analysisTimestamp = LocalDateTime.now();
        }
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
