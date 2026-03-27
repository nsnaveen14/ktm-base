package com.trading.kalyani.KTManager.dto.brahmastra;

import lombok.*;
import java.time.LocalDateTime;

/**
 * Individual trade log entry for backtest results.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TradeLog {

    private Integer tradeNumber;
    private String signalType; // BUY, SELL
    private LocalDateTime entryTime;
    private LocalDateTime exitTime;
    private Double entryPrice;
    private Double exitPrice;
    private Double stopLoss;
    private Double target;
    private Double positionSize; // Number of units/lots
    private Double pnl;
    private Double pnlPercent;
    private Double cumulativePnl;
    private Double cumulativePnlPercent;
    private String exitReason; // STOP_LOSS, TARGET, SIGNAL_REVERSAL, EOD
    private Double riskReward;
    private Double drawdownAtExit;

    // Indicator values at entry
    private Double supertrendAtEntry;
    private Double macdAtEntry;
    private Double vwapAtEntry;
    private Double pcrAtEntry;
}

