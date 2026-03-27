package com.trading.kalyani.KTManager.dto;

import lombok.*;

/**
 * DTO for Telegram notification settings with fine-grained control.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TelegramSettings {

    // Global settings
    private boolean enabled;
    private String botToken;
    private String defaultChatId;
    private String parseMode;
    private boolean disableWebPagePreview;
    private boolean disableNotification;
    private int connectionTimeout;
    private int readTimeout;
    private int maxRetries;
    private long retryDelayMs;
    private long rateLimitMs;

    // Category settings with sub-types
    private TradeAlertSettings tradeAlerts;
    private PredictionAlertSettings predictionAlerts;
    private DeviationAlertSettings deviationAlerts;
    private SystemAlertSettings systemAlerts;
    private PatternAlertSettings patternAlerts;
    private MarketAlertSettings marketAlerts;

    /**
     * Trade Alert Settings
     */
    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class TradeAlertSettings {
        private boolean enabled;
        private boolean iobAlerts;           // Internal Order Block alerts
        private boolean iobMitigationAlerts; // IOB Mitigation alerts (when IOB zones are hit)
        private boolean tradeSetupAlerts;    // Trade setup from prediction service
        private boolean tradeDecisionAlerts; // Trade decision alerts (BUY/SELL signals from Nifty current week)
        private boolean liquidityZoneAlerts; // Liquidity zone alerts
        private boolean brahmastraAlerts;    // Brahmastra strategy trade signals
        private double minConfidenceThreshold; // Minimum confidence to trigger alert (0-100)
    }

    /**
     * Prediction Alert Settings
     */
    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class PredictionAlertSettings {
        private boolean enabled;
        private boolean candlePredictionAlerts;  // Candle prediction notifications
        private boolean trendChangeAlerts;       // Trend change notifications
        private boolean targetHitAlerts;         // When predicted targets are hit
    }

    /**
     * Deviation Alert Settings
     */
    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class DeviationAlertSettings {
        private boolean enabled;
        private double deviationThresholdPoints; // Points threshold (e.g., 10, 15, 20)
        private boolean largeDeviationOnly;      // Only alert for large deviations (>20 pts)
    }

    /**
     * System Alert Settings
     */
    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class SystemAlertSettings {
        private boolean enabled;
        private boolean tickerConnectionAlerts;  // Ticker connect/disconnect
        private boolean jobStatusAlerts;         // Job start/stop notifications
        private boolean errorAlerts;             // Error notifications
    }

    /**
     * Pattern Alert Settings
     */
    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class PatternAlertSettings {
        private boolean enabled;
        private boolean bullishPatternAlerts;    // Bullish reversal patterns
        private boolean bearishPatternAlerts;    // Bearish reversal patterns
        private double minConfidenceThreshold;   // Minimum confidence (0-100)
    }

    /**
     * Market Alert Settings
     */
    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class MarketAlertSettings {
        private boolean enabled;
        private boolean marketOpenCloseAlerts;   // Market open/close notifications
        private boolean significantMoveAlerts;   // Large market moves
        private double significantMoveThresholdPercent; // % move to trigger alert
    }

    /**
     * Create default settings
     */
    public static TelegramSettings defaults() {
        return TelegramSettings.builder()
            .enabled(false)
            .parseMode("HTML")
            .disableWebPagePreview(true)
            .disableNotification(false)
            .connectionTimeout(10000)
            .readTimeout(10000)
            .maxRetries(3)
            .retryDelayMs(1000)
            .rateLimitMs(1000)
            .tradeAlerts(TradeAlertSettings.builder()
                .enabled(true)
                .iobAlerts(true)
                .iobMitigationAlerts(true)
                .tradeSetupAlerts(true)
                .tradeDecisionAlerts(true)
                .liquidityZoneAlerts(true)
                .minConfidenceThreshold(51.0)
                .build())
            .predictionAlerts(PredictionAlertSettings.builder()
                .enabled(false)
                .candlePredictionAlerts(true)
                .trendChangeAlerts(true)
                .targetHitAlerts(false)
                .build())
            .deviationAlerts(DeviationAlertSettings.builder()
                .enabled(false)
                .deviationThresholdPoints(10.0)
                .largeDeviationOnly(false)
                .build())
            .systemAlerts(SystemAlertSettings.builder()
                .enabled(false)
                .tickerConnectionAlerts(true)
                .jobStatusAlerts(false)
                .errorAlerts(true)
                .build())
            .patternAlerts(PatternAlertSettings.builder()
                .enabled(false)
                .bullishPatternAlerts(true)
                .bearishPatternAlerts(true)
                .minConfidenceThreshold(60.0)
                .build())
            .marketAlerts(MarketAlertSettings.builder()
                .enabled(false)
                .marketOpenCloseAlerts(true)
                .significantMoveAlerts(true)
                .significantMoveThresholdPercent(1.0)
                .build())
            .build();
    }
}
