package com.trading.kalyani.KTManager.dto.brahmastra;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

/**
 * DTO for Brahmastra Indicator Metrics.
 * Contains real-time values and historical data for Supertrend, MACD, and VWAP indicators.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IndicatorMetrics {

    private String symbol;
    private String timeframe;
    private LocalDateTime timestamp;
    private Double currentPrice;

    // Supertrend Metrics (ATR-based, period 20, multiplier 2)
    private SupertrendData supertrend;

    // MACD Metrics (12, 26, 9)
    private MACDData macd;

    // VWAP Metrics
    private VWAPData vwap;

    // Triple Confirmation Status
    private String overallSignal; // BUY, SELL, NEUTRAL
    private Double confidenceScore;
    private String recommendation;

    // Historical data for charts
    private List<IndicatorHistoryPoint> history;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SupertrendData {
        private Double value;
        private String trend; // BULLISH, BEARISH
        private Double atrValue;
        private Double upperBand;
        private Double lowerBand;
        private Integer period;
        private Double multiplier;
        private Double priceDistance; // Distance from current price to supertrend line
        private Double priceDistancePercent;
        private Integer consecutiveBars; // Number of bars in current trend
        private Boolean isTrendChange;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MACDData {
        private Double macdLine;
        private Double signalLine;
        private Double histogram;
        private String signal; // BULLISH, BEARISH, NEUTRAL
        private Boolean crossover; // True if crossover just happened
        private String crossoverType; // BULLISH_CROSSOVER, BEARISH_CROSSOVER, NONE
        private Integer fastPeriod;
        private Integer slowPeriod;
        private Integer signalPeriod;
        private Double divergence; // MACD divergence from signal
        private Boolean isConverging; // Is MACD converging towards signal line
        private Integer barsSinceCrossover;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VWAPData {
        private Double value;
        private String position; // ABOVE, BELOW, AT_VWAP
        private Double priceToVwapPercent; // (Price - VWAP) / VWAP * 100
        private Double upperBand1SD; // VWAP + 1 Standard Deviation
        private Double lowerBand1SD; // VWAP - 1 Standard Deviation
        private Double upperBand2SD; // VWAP + 2 Standard Deviations
        private Double lowerBand2SD; // VWAP - 2 Standard Deviations
        private Double cumulativeVolume;
        private Double cumulativeTpv; // Typical Price * Volume
        private String tradingZone; // PREMIUM (above +1SD), DISCOUNT (below -1SD), FAIR_VALUE
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class IndicatorHistoryPoint {
        private LocalDateTime timestamp;
        private Double price;
        private Double open;
        private Double high;
        private Double low;
        private Double close;
        private Long volume;
        private Double supertrend;
        private String supertrendTrend;
        private Double macdLine;
        private Double macdSignal;
        private Double macdHistogram;
        private Double vwap;
        private Double vwapUpperBand;
        private Double vwapLowerBand;
    }
}

