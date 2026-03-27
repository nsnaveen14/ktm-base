package com.trading.kalyani.KTManager.model;

import lombok.*;

import java.util.List;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
@Builder
public class HistoricalDataResponse {

    private boolean success;
    private String message;
    private String instrumentToken;
    private String interval;
    private int candleCount;
    private List<HistoricalCandle> candles;

    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    @Builder
    public static class HistoricalCandle {
        private String timestamp;
        private double open;
        private double high;
        private double low;
        private double close;
        private long volume;
        private long oi; // Open Interest (if requested)
    }
}

