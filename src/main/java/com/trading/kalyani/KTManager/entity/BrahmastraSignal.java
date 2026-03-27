package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;

/**
 * Entity representing a Brahmastra (Triple Confirmation) trading signal.
 *
 * Triple Confirmation requires:
 * 1. Supertrend - Trend direction confirmation
 * 2. MACD - Momentum confirmation
 * 3. VWAP - Value area confirmation
 *
 * Optional PCR filter for market bias.
 */
@Entity
@Table(name = "brahmastra_signals", indexes = {
    @Index(name = "idx_brahm_instrument_date", columnList = "instrument_token, signal_time"),
    @Index(name = "idx_brahm_status", columnList = "status"),
    @Index(name = "idx_brahm_signal_type", columnList = "signal_type")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BrahmastraSignal {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // ==================== Instrument Details ====================

    @Column(name = "instrument_token", nullable = false)
    private Long instrumentToken;

    @Column(name = "symbol")
    private String symbol; // NIFTY

    @Column(name = "timeframe")
    private String timeframe; // 5m, 15m, etc.

    // ==================== Signal Details ====================

    @Column(name = "signal_type", nullable = false)
    private String signalType; // BUY, SELL

    @Column(name = "signal_time", nullable = false)
    private LocalDateTime signalTime;

    @Column(name = "entry_price")
    private Double entryPrice;

    @Column(name = "stop_loss")
    private Double stopLoss;

    @Column(name = "target1")
    private Double target1;

    @Column(name = "target2")
    private Double target2;

    @Column(name = "target3")
    private Double target3;

    @Column(name = "risk_reward_ratio")
    private Double riskRewardRatio;

    @Column(name = "confidence_score")
    private Double confidenceScore;

    // ==================== Indicator Values at Signal ====================

    @Column(name = "supertrend_value")
    private Double supertrendValue;

    @Column(name = "supertrend_trend")
    private String supertrendTrend; // BULLISH, BEARISH

    @Column(name = "macd_line")
    private Double macdLine;

    @Column(name = "macd_signal_line")
    private Double macdSignalLine;

    @Column(name = "macd_histogram")
    private Double macdHistogram;

    @Column(name = "vwap_value")
    private Double vwapValue;

    @Column(name = "price_to_vwap_percent")
    private Double priceToVwapPercent;

    // ==================== PCR Data ====================

    @Column(name = "pcr_value")
    private Double pcrValue;

    @Column(name = "pcr_bias")
    private String pcrBias; // BULLISH, BEARISH, NEUTRAL

    @Column(name = "pcr_filter_applied")
    private Boolean pcrFilterApplied;

    // ==================== Candle Data ====================

    @Column(name = "candle_open")
    private Double candleOpen;

    @Column(name = "candle_high")
    private Double candleHigh;

    @Column(name = "candle_low")
    private Double candleLow;

    @Column(name = "candle_close")
    private Double candleClose;

    @Column(name = "candle_volume")
    private Long candleVolume;

    // ==================== Trade Management ====================

    @Column(name = "status")
    private String status; // ACTIVE, CLOSED, STOPPED_OUT, TARGET_HIT, EXPIRED

    @Column(name = "exit_time")
    private LocalDateTime exitTime;

    @Column(name = "exit_price")
    private Double exitPrice;

    @Column(name = "exit_reason")
    private String exitReason; // STOP_LOSS, TARGET1, TARGET2, TARGET3, SIGNAL_REVERSAL, EOD

    @Column(name = "pnl")
    private Double pnl;

    @Column(name = "pnl_percent")
    private Double pnlPercent;

    // ==================== Metadata ====================

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @Column(name = "app_job_config_num")
    private Integer appJobConfigNum;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}

