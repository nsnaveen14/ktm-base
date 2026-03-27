package com.trading.kalyani.KTManager.dto.brahmastra;

import lombok.*;
import java.time.LocalDateTime;

/**
 * Drawdown curve data point for charting.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DrawdownPoint {

    private LocalDateTime timestamp;
    private Double drawdown;
    private Double drawdownPercent;
    private Double peakEquity;
    private Double currentEquity;
}

