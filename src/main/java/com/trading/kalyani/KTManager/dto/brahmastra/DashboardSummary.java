package com.trading.kalyani.KTManager.dto.brahmastra;

import lombok.*;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Dashboard summary aggregating performance across all assets.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DashboardSummary {

    private LocalDateTime generatedAt;

    // Overall Performance
    private Double totalPnL;
    private Double totalPnLPercent;
    private Integer totalTrades;
    private Double overallWinRate;
    private Double overallProfitFactor;

    // Today's Performance
    private Integer todaysTrades;
    private Double todaysPnL;
    private Integer todaysWinningTrades;
    private Integer todaysLosingTrades;
    private Integer activeSignals;

    // Per-Symbol Summary
    private List<SymbolSummary> symbolSummaries;

    // Market Bias (from PCR)
    private String currentMarketBias; // BULLISH, BEARISH, NEUTRAL
    private Double niftyPCR;

    // Live Signals
    private List<SignalDTO> liveSignals;

    // Recent Trades
    private List<TradeLog> recentTrades;

    // Strategy Health
    private Double last7DaysWinRate;
    private Double last30DaysWinRate;
    private Double currentDrawdown;
    private String strategyStatus; // HEALTHY, CAUTION, PAUSE

    // Option Chain Integration
    private List<OptionChainMetrics> optionChainMetrics;
    private String optionChainOverallBias; // Combined bias from all symbols
    private Double maxPainNifty;
}

