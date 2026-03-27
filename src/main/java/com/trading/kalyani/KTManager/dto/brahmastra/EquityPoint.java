package com.trading.kalyani.KTManager.dto.brahmastra;

import lombok.*;
import java.time.LocalDateTime;

/**
 * Equity curve data point for charting.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class EquityPoint {

    private LocalDateTime timestamp;
    private Double equity;
    private Double dailyReturn;
    private Double cumulativeReturn;
    private Integer tradeNumber;
}

