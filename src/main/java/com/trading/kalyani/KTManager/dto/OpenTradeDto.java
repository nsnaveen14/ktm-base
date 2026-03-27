package com.trading.kalyani.KTManager.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OpenTradeDto {
    private String tradeId;
    private String optionType;
    private Double strikePrice;
    private Double entryPrice;
    private String entryTime;
    private Double targetPrice;
    private Double stopLossPrice;
    private Double trailingStopLoss;
    private Double peakPrice;
    private Integer quantity;
    private Integer numLots;
    private String instrumentSymbol;
    private Long instrumentToken;
    private String status;
}

