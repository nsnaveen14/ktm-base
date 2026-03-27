package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entity representing a simulated options trade.
 * Tracks trade lifecycle from entry to exit with P&L calculation.
 */
@Entity
@Table(name = "simulated_trades")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SimulatedTrade {

    private static final Logger logger = LoggerFactory.getLogger(SimulatedTrade.class);

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // Trade identification
    @Column(name = "trade_id", unique = true)
    private String tradeId;

    @Column(name = "trade_date")
    private LocalDateTime tradeDate;

    // Signal source
    @Column(name = "signal_source")
    private String signalSource; // TRADE_SETUP, EMA_CROSSOVER

    @Column(name = "signal_type")
    private String signalType; // BUY, SELL

    @Column(name = "signal_strength")
    private String signalStrength; // STRONG, MODERATE, WEAK

    // Option details
    @Column(name = "option_type")
    private String optionType; // CE, PE

    @Column(name = "strike_price")
    private Double strikePrice;

    @Column(name = "option_token")
    private Long optionToken; // Instrument token for the exact option taken

    @Column(name = "option_symbol")
    private String optionSymbol; // Tradingsymbol for the option at entry

    @Column(name = "underlying_price_at_entry")
    private Double underlyingPriceAtEntry;

    @Column(name = "underlying_stop_loss")
    private Double underlyingStopLoss; // index-level SL (IOB trades only)

    @Column(name = "iob_id")
    private Long iobId; // IOB that triggered this trade (IOB_SIGNAL trades only)

    // Position details
    @Column(name = "quantity")
    private Integer quantity; // Number of units (e.g., 300)

    @Column(name = "lot_size")
    private Integer lotSize; // NIFTY lot size (75)

    @Column(name = "num_lots")
    private Integer numLots; // Number of lots (e.g., 4)

    // Entry details
    @Column(name = "entry_price")
    private Double entryPrice; // Option premium at entry

    @Column(name = "entry_time")
    private LocalDateTime entryTime;

    @Column(name = "entry_reason")
    private String entryReason;

    // Exit details
    @Column(name = "exit_price")
    private Double exitPrice; // Option premium at exit

    @Column(name = "exit_time")
    private LocalDateTime exitTime;

    @Column(name = "exit_reason")
    private String exitReason; // TARGET_HIT, STOPLOSS_HIT, TRAILING_SL, MANUAL, TIME_EXIT

    // Risk management
    @Column(name = "target_price")
    private Double targetPrice;

    @Column(name = "stop_loss_price")
    private Double stopLossPrice;

    // IOB premium targets at each index level (IOB_SIGNAL trades only)
    @Column(name = "premium_t1")
    private Double premiumT1;

    @Column(name = "premium_t2")
    private Double premiumT2;

    @Column(name = "premium_t3")
    private Double premiumT3;

    @Column(name = "trailing_stop_loss")
    private Double trailingStopLoss;

    @Column(name = "risk_reward_ratio")
    private Double riskRewardRatio;

    // P&L
    @Column(name = "gross_pnl")
    private Double grossPnl;

    @Column(name = "brokerage")
    private Double brokerage;

    @Column(name = "net_pnl")
    private Double netPnl;

    @Column(name = "pnl_percentage")
    private Double pnlPercentage;

    // Status
    @Column(name = "status")
    private String status; // PENDING, OPEN, CLOSED, CANCELLED

    @Column(name = "is_profitable")
    private Boolean isProfitable;

    // Market context
    @Column(name = "vix_at_entry")
    private Double vixAtEntry;

    @Column(name = "market_trend")
    private String marketTrend; // BULLISH, BEARISH, SIDEWAYS

    // Peak tracking for trailing SL
    @Column(name = "peak_price")
    private Double peakPrice;

    @Column(name = "peak_pnl")
    private Double peakPnl;

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
     * Calculate P&L for this trade
     */
    public void calculatePnL() {
        if (entryPrice != null && exitPrice != null && quantity != null) {
            // For options: P&L = (Exit Price - Entry Price) * Quantity
            // For CE (BUY signal): We BUY the option, so profit if price goes up
            // For PE (SELL signal): We BUY PE, so profit if underlying goes down (PE price goes up)
            grossPnl = (exitPrice - entryPrice) * quantity;

            // Estimate brokerage (flat fee per lot + taxes)
            brokerage = numLots * 40.0; // Approx Rs 40 per lot round trip

            netPnl = grossPnl - brokerage;

            if (entryPrice > 0) {
                pnlPercentage = ((exitPrice - entryPrice) / entryPrice) * 100;
            }

            isProfitable = netPnl > 0;
        }
    }

    /**
     * Check if target is hit
     */
    public boolean isTargetHit(double currentPrice) {
        return targetPrice != null && currentPrice >= targetPrice;
    }

    /**
     * Check if stop loss is hit
     */
    public boolean isStopLossHit(double currentPrice) {
        if (trailingStopLoss != null && stopLossPrice != null && trailingStopLoss > stopLossPrice) {
            return currentPrice <= trailingStopLoss;
        }
        return stopLossPrice != null && currentPrice <= stopLossPrice;
    }

    /**
     * Update trailing stop loss if price has moved favorably
     *
     * @param currentPrice current option price
     * @param activationThresholdPercent percent (0-100) of distance to target required to activate trailing SL
     * @param trailPercentOfProfit percent (0-100) of profit to place trailing SL (e.g., 50 means trail at entry + 50% of profit)
     */
    public void updateTrailingStopLoss(double currentPrice, double activationThresholdPercent, double trailPercentOfProfit) {
        // Defensive checks
        if (entryPrice == null || targetPrice == null || quantity == null) return;

        // Update peak price
        if (peakPrice == null || currentPrice > peakPrice) {
            peakPrice = currentPrice;
        }

        // Calculate profit per unit and current P&L
        double profitPerUnit = currentPrice - entryPrice;
        double currentPnl = profitPerUnit * quantity;
        if (currentPnl > 0 && (peakPnl == null || currentPnl > peakPnl)) {
            peakPnl = currentPnl;
        }

        // Compute threshold price based on activationThresholdPercent
        double thresholdPrice = entryPrice + (activationThresholdPercent / 100.0) * (targetPrice - entryPrice);

        // If the trade is profitable and has crossed threshold, set/advance trailing SL
        if (profitPerUnit > 0 && currentPrice >= thresholdPrice) {
            double newTrailingSL = entryPrice + (profitPerUnit * (trailPercentOfProfit / 100.0));
            if (trailingStopLoss == null || newTrailingSL > trailingStopLoss) {
                Double previous = trailingStopLoss;
                trailingStopLoss = newTrailingSL;
                if (previous == null) {
                    logger.info("Trailing SL SET for trade {}: {} (entry={}, current={}, threshold={})", tradeId, trailingStopLoss, entryPrice, currentPrice, thresholdPrice);
                } else {
                    logger.debug("Trailing SL ADVANCED for trade {}: {} -> {} (entry={}, current={}, threshold={})", tradeId, previous, trailingStopLoss, entryPrice, currentPrice, thresholdPrice);
                }
            }
        }
    }
}
