package com.trading.kalyani.KTManager.dto.brahmastra;

import lombok.*;
import java.time.LocalDateTime;

/**
 * Live scan result DTO for WebSocket push.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class LiveScanResult {

    private String symbol;
    private Long instrumentToken;
    private LocalDateTime scanTime;
    private String signalType; // BUY, SELL, NO_SIGNAL
    private Double currentPrice;
    private Double entryPrice;
    private Double stopLoss;
    private Double target1;
    private Double target2;
    private Double riskReward;
    private Double confidenceScore;

    // Indicator Status
    private String supertrendStatus; // BULLISH, BEARISH
    private String macdStatus; // BULLISH, BEARISH
    private String vwapStatus; // ABOVE, BELOW, NEAR
    private String pcrBias; // BULLISH, BEARISH, NEUTRAL

    // Market Context
    private Double vwap;
    private Double supertrend;
    private Double macdLine;
    private Double macdSignal;
    private Double pcr;

    // Alert Details
    private Boolean isNewSignal;
    private String message;
    private String alertLevel; // HIGH, MEDIUM, LOW
}

