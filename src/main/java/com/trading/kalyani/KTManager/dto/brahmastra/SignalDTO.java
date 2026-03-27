package com.trading.kalyani.KTManager.dto.brahmastra;

import lombok.*;
import java.time.LocalDateTime;

/**
 * Response DTO for trading signals.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SignalDTO {

    private Long id;
    private String symbol;
    private String timeframe;

    // Signal Details
    private String signalType; // BUY, SELL
    private LocalDateTime signalTime;
    private Double entryPrice;
    private Double stopLoss;
    private Double target1; // 1:1 RR
    private Double target2; // 1:2 RR
    private Double target3; // 1:3 RR
    private Double riskRewardRatio;
    private Double confidenceScore; // 0-100%

    // Indicator Values at Signal
    private Double supertrendValue;
    private String supertrendTrend; // BULLISH, BEARISH
    private Double macdLine;
    private Double macdSignalLine;
    private Double macdHistogram;
    private Double vwapValue;
    private Double priceToVwapPercent; // How far price is from VWAP

    // PCR Data (if enabled)
    private Double pcrValue;
    private String pcrBias; // BULLISH, BEARISH, NEUTRAL

    // Trade Management
    private String status; // ACTIVE, CLOSED, STOPPED_OUT, TARGET_HIT
    private LocalDateTime exitTime;
    private Double exitPrice;
    private Double pnl;
    private Double pnlPercent;

    // Candle Data
    private Double open;
    private Double high;
    private Double low;
    private Double close;
    private Long volume;
}

