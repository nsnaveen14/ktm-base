package com.trading.kalyani.KTManager.dto.brahmastra;

import lombok.*;

/**
 * Per-symbol summary for dashboard.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SymbolSummary {

    private String symbol;
    private Long instrumentToken;
    private Integer totalTrades;
    private Integer winningTrades;
    private Integer losingTrades;
    private Double winRate;
    private Double totalPnL;
    private Double totalPnLPercent;
    private Double averageRR;
    private Double maxDrawdown;
    private Double sharpeRatio;
    private Integer activeSignals;
    private String lastSignalType;
    private Double lastSignalPrice;
    private String currentTrend; // BULLISH, BEARISH, NEUTRAL
}

