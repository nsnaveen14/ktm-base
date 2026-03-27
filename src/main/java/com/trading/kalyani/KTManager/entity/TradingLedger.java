package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Entity representing daily trading summary/ledger.
 * Aggregates all trades for a given day.
 */
@Entity
@Table(name = "trading_ledger")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TradingLedger {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "trade_date", unique = true)
    private LocalDate tradeDate;

    // Trade counts
    @Column(name = "total_trades")
    private Integer totalTrades;

    @Column(name = "winning_trades")
    private Integer winningTrades;

    @Column(name = "losing_trades")
    private Integer losingTrades;

    @Column(name = "breakeven_trades")
    private Integer breakevenTrades;

    // Win rate
    @Column(name = "win_rate")
    private Double winRate;

    // P&L summary
    @Column(name = "gross_pnl")
    private Double grossPnl;

    @Column(name = "total_brokerage")
    private Double totalBrokerage;

    @Column(name = "net_pnl")
    private Double netPnl;

    // Trade breakdown by signal source
    @Column(name = "trade_setup_trades")
    private Integer tradeSetupTrades;

    @Column(name = "ema_crossover_trades")
    private Integer emaCrossoverTrades;

    @Column(name = "trade_setup_pnl")
    private Double tradeSetupPnl;

    @Column(name = "ema_crossover_pnl")
    private Double emaCrossoverPnl;

    // GEX trade tracking
    @Column(name = "gex_analysis_trades")
    private Integer gexAnalysisTrades;

    @Column(name = "gex_analysis_pnl")
    private Double gexAnalysisPnl;

    // Liquidity Sweep trade tracking
    @Column(name = "liquidity_sweep_trades")
    private Integer liquiditySweepTrades;

    @Column(name = "liquidity_sweep_pnl")
    private Double liquiditySweepPnl;

    // Trade breakdown by direction
    @Column(name = "buy_trades")
    private Integer buyTrades;

    @Column(name = "sell_trades")
    private Integer sellTrades;

    @Column(name = "buy_pnl")
    private Double buyPnl;

    @Column(name = "sell_pnl")
    private Double sellPnl;

    // Option type breakdown
    @Column(name = "ce_trades")
    private Integer ceTrades;

    @Column(name = "pe_trades")
    private Integer peTrades;

    @Column(name = "ce_pnl")
    private Double cePnl;

    @Column(name = "pe_pnl")
    private Double pePnl;

    // Exit reason breakdown
    @Column(name = "target_hit_count")
    private Integer targetHitCount;

    @Column(name = "stoploss_hit_count")
    private Integer stoplossHitCount;

    @Column(name = "trailing_sl_count")
    private Integer trailingSlCount;

    @Column(name = "time_exit_count")
    private Integer timeExitCount;

    @Column(name = "reverse_signal_count")
    private Integer reverseSignalCount;

    // Performance metrics
    @Column(name = "avg_profit_per_trade")
    private Double avgProfitPerTrade;

    @Column(name = "avg_loss_per_trade")
    private Double avgLossPerTrade;

    @Column(name = "profit_factor")
    private Double profitFactor; // Total Profits / Total Losses

    @Column(name = "max_drawdown")
    private Double maxDrawdown;

    @Column(name = "peak_pnl")
    private Double peakPnl;

    // Capital tracking
    @Column(name = "starting_capital")
    private Double startingCapital;

    @Column(name = "ending_capital")
    private Double endingCapital;

    @Column(name = "capital_return_percent")
    private Double capitalReturnPercent;

    // Market context
    @Column(name = "nifty_open")
    private Double niftyOpen;

    @Column(name = "nifty_close")
    private Double niftyClose;

    @Column(name = "nifty_change_percent")
    private Double niftyChangePercent;

    @Column(name = "avg_vix")
    private Double avgVix;

    // Timestamps
    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
        initializeDefaults();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }

    private void initializeDefaults() {
        if (totalTrades == null) totalTrades = 0;
        if (winningTrades == null) winningTrades = 0;
        if (losingTrades == null) losingTrades = 0;
        if (breakevenTrades == null) breakevenTrades = 0;
        if (grossPnl == null) grossPnl = 0.0;
        if (totalBrokerage == null) totalBrokerage = 0.0;
        if (netPnl == null) netPnl = 0.0;
        if (tradeSetupTrades == null) tradeSetupTrades = 0;
        if (emaCrossoverTrades == null) emaCrossoverTrades = 0;
        if (tradeSetupPnl == null) tradeSetupPnl = 0.0;
        if (emaCrossoverPnl == null) emaCrossoverPnl = 0.0;
        if (buyTrades == null) buyTrades = 0;
        if (sellTrades == null) sellTrades = 0;
        if (buyPnl == null) buyPnl = 0.0;
        if (sellPnl == null) sellPnl = 0.0;
        if (ceTrades == null) ceTrades = 0;
        if (peTrades == null) peTrades = 0;
        if (cePnl == null) cePnl = 0.0;
        if (pePnl == null) pePnl = 0.0;
        if (targetHitCount == null) targetHitCount = 0;
        if (stoplossHitCount == null) stoplossHitCount = 0;
        if (trailingSlCount == null) trailingSlCount = 0;
        if (timeExitCount == null) timeExitCount = 0;
        if (reverseSignalCount == null) reverseSignalCount = 0;
        if (gexAnalysisTrades == null) gexAnalysisTrades = 0;
        if (gexAnalysisPnl == null) gexAnalysisPnl = 0.0;
        if (liquiditySweepTrades == null) liquiditySweepTrades = 0;
        if (liquiditySweepPnl == null) liquiditySweepPnl = 0.0;
    }

    /**
     * Calculate derived metrics
     */
    public void calculateMetrics() {
        // Win rate
        if (totalTrades != null && totalTrades > 0) {
            winRate = (double) winningTrades / totalTrades * 100;
        }

        // Net P&L
        netPnl = grossPnl - totalBrokerage;

        // Capital return
        if (startingCapital != null && startingCapital > 0) {
            endingCapital = startingCapital + netPnl;
            capitalReturnPercent = (netPnl / startingCapital) * 100;
        }
    }
}

