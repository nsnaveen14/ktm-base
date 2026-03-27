package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;

/**
 * Entity representing a Brahmastra backtest result.
 */
@Entity
@Table(name = "brahmastra_backtest_results", indexes = {
    @Index(name = "idx_brahm_bt_symbol", columnList = "symbol"),
    @Index(name = "idx_brahm_bt_date", columnList = "run_timestamp")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BrahmastraBacktestResult {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // ==================== Backtest Parameters ====================

    @Column(name = "symbol")
    private String symbol;

    @Column(name = "timeframe")
    private String timeframe;

    @Column(name = "from_date")
    private LocalDateTime fromDate;

    @Column(name = "to_date")
    private LocalDateTime toDate;

    @Column(name = "run_timestamp")
    private LocalDateTime runTimestamp;

    @Column(name = "initial_capital")
    private Double initialCapital;

    @Column(name = "risk_per_trade")
    private Double riskPerTrade;

    @Column(name = "use_pcr")
    private Boolean usePCR;

    // ==================== Results Summary ====================

    @Column(name = "final_capital")
    private Double finalCapital;

    @Column(name = "net_pnl")
    private Double netPnL;

    @Column(name = "net_pnl_percent")
    private Double netPnLPercent;

    @Column(name = "total_trades")
    private Integer totalTrades;

    @Column(name = "winning_trades")
    private Integer winningTrades;

    @Column(name = "losing_trades")
    private Integer losingTrades;

    @Column(name = "win_rate")
    private Double winRate;

    @Column(name = "average_win")
    private Double averageWin;

    @Column(name = "average_loss")
    private Double averageLoss;

    @Column(name = "profit_factor")
    private Double profitFactor;

    @Column(name = "average_rr")
    private Double averageRR;

    @Column(name = "expectancy")
    private Double expectancy;

    // ==================== Risk Metrics ====================

    @Column(name = "max_drawdown")
    private Double maxDrawdown;

    @Column(name = "max_drawdown_percent")
    private Double maxDrawdownPercent;

    @Column(name = "sharpe_ratio")
    private Double sharpeRatio;

    @Column(name = "sortino_ratio")
    private Double sortinoRatio;

    @Column(name = "calmar_ratio")
    private Double calmarRatio;

    @Column(name = "volatility")
    private Double volatility;

    // ==================== Trade Details (JSON stored) ====================

    @Column(name = "trade_log_json", columnDefinition = "TEXT")
    private String tradeLogJson;

    @Column(name = "equity_curve_json", columnDefinition = "TEXT")
    private String equityCurveJson;

    // ==================== Metadata ====================

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        runTimestamp = LocalDateTime.now();
    }
}

