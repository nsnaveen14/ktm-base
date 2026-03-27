package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

/**
 * Entity to store Telegram notification settings.
 * Uses a single-row pattern (only one row with id=1).
 */
@Entity
@Table(name = "telegram_settings")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TelegramSettingsEntity {

    @Id
    private Long id = 1L; // Single row pattern

    // Global settings (bot token and chat ID are stored in application.yaml for security)
    @Column(name = "enabled")
    private Boolean enabled;

    @Column(name = "parse_mode")
    private String parseMode;

    @Column(name = "disable_web_page_preview")
    private Boolean disableWebPagePreview;

    @Column(name = "disable_notification")
    private Boolean disableNotification;

    @Column(name = "rate_limit_ms")
    private Long rateLimitMs;

    // Trade Alerts
    @Column(name = "trade_alerts_enabled")
    private Boolean tradeAlertsEnabled;

    @Column(name = "iob_alerts_enabled")
    private Boolean iobAlertsEnabled;

    @Column(name = "iob_mitigation_alerts_enabled")
    private Boolean iobMitigationAlertsEnabled;

    @Column(name = "trade_setup_alerts_enabled")
    private Boolean tradeSetupAlertsEnabled;

    @Column(name = "trade_decision_alerts_enabled")
    private Boolean tradeDecisionAlertsEnabled;

    @Column(name = "liquidity_zone_alerts_enabled")
    private Boolean liquidityZoneAlertsEnabled;

    @Column(name = "brahmastra_alerts_enabled")
    private Boolean brahmastraAlertsEnabled;

    @Column(name = "trade_min_confidence")
    private Double tradeMinConfidence;

    // Prediction Alerts
    @Column(name = "prediction_alerts_enabled")
    private Boolean predictionAlertsEnabled;

    @Column(name = "candle_prediction_alerts_enabled")
    private Boolean candlePredictionAlertsEnabled;

    @Column(name = "trend_change_alerts_enabled")
    private Boolean trendChangeAlertsEnabled;

    @Column(name = "target_hit_alerts_enabled")
    private Boolean targetHitAlertsEnabled;

    // Deviation Alerts
    @Column(name = "deviation_alerts_enabled")
    private Boolean deviationAlertsEnabled;

    @Column(name = "deviation_threshold_points")
    private Double deviationThresholdPoints;

    @Column(name = "large_deviation_only")
    private Boolean largeDeviationOnly;

    // System Alerts
    @Column(name = "system_alerts_enabled")
    private Boolean systemAlertsEnabled;

    @Column(name = "ticker_connection_alerts_enabled")
    private Boolean tickerConnectionAlertsEnabled;

    @Column(name = "job_status_alerts_enabled")
    private Boolean jobStatusAlertsEnabled;

    @Column(name = "error_alerts_enabled")
    private Boolean errorAlertsEnabled;

    // Pattern Alerts
    @Column(name = "pattern_alerts_enabled")
    private Boolean patternAlertsEnabled;

    @Column(name = "bullish_pattern_alerts_enabled")
    private Boolean bullishPatternAlertsEnabled;

    @Column(name = "bearish_pattern_alerts_enabled")
    private Boolean bearishPatternAlertsEnabled;

    @Column(name = "pattern_min_confidence")
    private Double patternMinConfidence;

    // Market Alerts
    @Column(name = "market_alerts_enabled")
    private Boolean marketAlertsEnabled;

    @Column(name = "market_open_close_alerts_enabled")
    private Boolean marketOpenCloseAlertsEnabled;

    @Column(name = "significant_move_alerts_enabled")
    private Boolean significantMoveAlertsEnabled;

    @Column(name = "significant_move_threshold_percent")
    private Double significantMoveThresholdPercent;

    // Timestamps
    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }

    /**
     * Create default settings entity
     */
    public static TelegramSettingsEntity defaults() {
        return TelegramSettingsEntity.builder()
            .id(1L)
            .enabled(true)
            .parseMode("HTML")
            .disableWebPagePreview(true)
            .disableNotification(false)
            .rateLimitMs(1000L)
            // Trade Alerts
            .tradeAlertsEnabled(true)
            .iobAlertsEnabled(true)
            .tradeSetupAlertsEnabled(true)
            .tradeDecisionAlertsEnabled(true)
            .liquidityZoneAlertsEnabled(true)
            .brahmastraAlertsEnabled(true)
            .tradeMinConfidence(51.0)
            // Prediction Alerts
            .predictionAlertsEnabled(false)
            .candlePredictionAlertsEnabled(true)
            .trendChangeAlertsEnabled(true)
            .targetHitAlertsEnabled(false)
            // Deviation Alerts
            .deviationAlertsEnabled(false)
            .deviationThresholdPoints(10.0)
            .largeDeviationOnly(false)
            // System Alerts
            .systemAlertsEnabled(false)
            .tickerConnectionAlertsEnabled(true)
            .jobStatusAlertsEnabled(false)
            .errorAlertsEnabled(true)
            // Pattern Alerts
            .patternAlertsEnabled(false)
            .bullishPatternAlertsEnabled(true)
            .bearishPatternAlertsEnabled(true)
            .patternMinConfidence(60.0)
            // Market Alerts
            .marketAlertsEnabled(false)
            .marketOpenCloseAlertsEnabled(true)
            .significantMoveAlertsEnabled(true)
            .significantMoveThresholdPercent(1.0)
            .build();
    }
}
