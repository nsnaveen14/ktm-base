package com.trading.kalyani.KTManager.dto.brahmastra;

import lombok.*;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Response DTO for backtest results.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BacktestResult {

    private String symbol;
    private String timeframe;
    private LocalDateTime backtestStart;
    private LocalDateTime backtestEnd;
    private LocalDateTime runTimestamp;

    // Capital Details
    private Double initialCapital;
    private Double finalCapital;
    private Double netPnL;
    private Double netPnLPercent;
    private Double riskPerTrade;

    // Trade Statistics
    private Integer totalTrades;
    private Integer winningTrades;
    private Integer losingTrades;
    private Double winRate; // Percentage
    private Integer maxConsecutiveWins;
    private Integer maxConsecutiveLosses;

    // Risk-Reward Metrics
    private Double averageWin;
    private Double averageLoss;
    private Double averageRiskReward;
    private Double profitFactor; // Gross Profit / Gross Loss
    private Double expectancy; // (Win% * Avg Win) - (Loss% * Avg Loss)

    // Drawdown Analysis
    private Double maxDrawdown;
    private Double maxDrawdownPercent;
    private LocalDateTime maxDrawdownDate;
    private Double currentDrawdown;

    // Risk Metrics
    private Double sharpeRatio;
    private Double sortinoRatio;
    private Double calmarRatio;
    private Double volatility;

    // Time Analysis
    private Double averageHoldingPeriodMinutes;
    private Double longestTrade;
    private Double shortestTrade;
    private Integer tradesPerDay;

    // Signal Analysis
    private Integer buySignals;
    private Integer sellSignals;
    private Double buyWinRate;
    private Double sellWinRate;

    // PCR Filter Impact (if enabled)
    private Integer signalsFilteredByPCR;
    private Double pcrFilterImprovement;

    // Trade Log
    private List<TradeLog> tradeLog;

    // Equity Curve Data Points
    private List<EquityPoint> equityCurve;
    private List<DrawdownPoint> drawdownCurve;
}

