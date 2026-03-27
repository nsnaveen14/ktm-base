package com.trading.kalyani.KTManager.dto.brahmastra;

import lombok.*;
import java.time.LocalDate;

/**
 * Request DTO for signal generation.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SignalRequest {

    private String symbol; // NIFTY
    private String timeframe; // 1m, 3m, 5m, 15m, 30m, 1h, 1D
    private LocalDate fromDate;
    private LocalDate toDate;
    private Boolean usePCR; // Enable PCR-based market bias filtering

    // Optional parameters with defaults
    private Integer supertrendPeriod = 20;
    private Double supertrendMultiplier = 2.0;
    private Integer macdFastPeriod = 12;
    private Integer macdSlowPeriod = 26;
    private Integer macdSignalPeriod = 9;
    private Double vwapTolerance = 0.002; // ±0.2% tolerance from VWAP
}

