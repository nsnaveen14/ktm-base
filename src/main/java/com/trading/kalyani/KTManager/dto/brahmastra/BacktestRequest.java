package com.trading.kalyani.KTManager.dto.brahmastra;

import lombok.*;
import java.time.LocalDate;

/**
 * Request DTO for backtesting.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BacktestRequest {

    private String symbol;
    private String timeframe;
    private LocalDate fromDate;
    private LocalDate toDate;
    private Boolean usePCR;

    // Capital & Risk Management
    private Double initialCapital; // e.g., 100000
    private Double riskPerTrade; // Percentage, e.g., 1.0 for 1%
    private Double maxPositionSize; // Optional: Maximum position size limit

    // Indicator Parameters
    private Integer supertrendPeriod = 20;
    private Double supertrendMultiplier = 2.0;
    private Integer macdFastPeriod = 12;
    private Integer macdSlowPeriod = 26;
    private Integer macdSignalPeriod = 9;
    private Double vwapTolerance = 0.002;

    // Exit Strategy
    private Boolean useTrailingStop = false;
    private Double trailingStopPercent = 0.5;
    private Boolean usePartialProfits = true;
    private Double partialProfitPercent = 50.0; // Close 50% at 1:1 RR
}

