package com.trading.kalyani.KTManager.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO for updating target and stoploss on a simulated trade.
 * Allows absolute price overrides or percent-based adjustments relative to entryPrice.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UpdateTradeRiskRequest {
    private Double targetPrice;      // Absolute target price override
    private Double stopLossPrice;    // Absolute stoploss price override
    private Double targetPercent;    // Percent relative to entryPrice (e.g., 20 means +20%)
    private Double stoplossPercent;  // Percent relative to entryPrice (e.g., 10 means -10%)
    private Boolean force;           // Bypass some safety checks
    private String reason;           // Optional reason for audit/logging
}

