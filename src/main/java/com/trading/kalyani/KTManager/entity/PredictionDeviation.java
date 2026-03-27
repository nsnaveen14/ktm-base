package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Entity to store prediction deviation statistics for analysis and correction.
 * Tracks the differences between predicted and actual values to improve prediction accuracy.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Entity
@Table(name = "prediction_deviation")
public class PredictionDeviation {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "prediction_deviation_seq")
    @SequenceGenerator(name = "prediction_deviation_seq", sequenceName = "prediction_deviation_seq", allocationSize = 1)
    private Long id;

    @Column(name = "instrument_token")
    private Long instrumentToken;

    @Column(name = "verification_time")
    private LocalDateTime verificationTime;

    @Column(name = "trading_date")
    private LocalDate tradingDate;

    // Batch info - verification happens every 15 mins
    @Column(name = "batch_id")
    private String batchId;  // Format: yyyyMMdd_HHmm

    // Number of predictions in this verification batch
    @Column(name = "predictions_verified")
    private Integer predictionsVerified;

    // Deviation metrics - Close Price
    @Column(name = "avg_close_deviation")
    private Double avgCloseDeviation;  // Average absolute deviation in points

    @Column(name = "avg_close_deviation_percent")
    private Double avgCloseDeviationPercent;  // Average deviation as percentage

    @Column(name = "max_close_deviation")
    private Double maxCloseDeviation;  // Maximum deviation in this batch

    @Column(name = "min_close_deviation")
    private Double minCloseDeviation;  // Minimum deviation in this batch

    @Column(name = "close_deviation_std")
    private Double closeDeviationStd;  // Standard deviation of close price deviations

    // Deviation metrics - High Price
    @Column(name = "avg_high_deviation")
    private Double avgHighDeviation;

    @Column(name = "avg_high_deviation_percent")
    private Double avgHighDeviationPercent;

    // Deviation metrics - Low Price
    @Column(name = "avg_low_deviation")
    private Double avgLowDeviation;

    @Column(name = "avg_low_deviation_percent")
    private Double avgLowDeviationPercent;

    // Directional accuracy
    @Column(name = "direction_accuracy_percent")
    private Double directionAccuracyPercent;  // % of times trend direction was correct

    @Column(name = "bullish_predictions")
    private Integer bullishPredictions;

    @Column(name = "bearish_predictions")
    private Integer bearishPredictions;

    @Column(name = "neutral_predictions")
    private Integer neutralPredictions;

    @Column(name = "correct_bullish")
    private Integer correctBullish;

    @Column(name = "correct_bearish")
    private Integer correctBearish;

    // Bias tracking - for correction
    @Column(name = "systematic_bias")
    private Double systematicBias;  // Positive = consistently predicting too high, Negative = too low

    @Column(name = "bias_direction")
    private String biasDirection;  // OVER_PREDICT, UNDER_PREDICT, NEUTRAL

    // Volatility comparison
    @Column(name = "predicted_avg_volatility")
    private Double predictedAvgVolatility;

    @Column(name = "actual_avg_volatility")
    private Double actualAvgVolatility;

    @Column(name = "volatility_underestimate_ratio")
    private Double volatilityUnderestimateRatio;  // > 1 means underestimating volatility

    // Market context during this batch
    @Column(name = "avg_pcr")
    private Double avgPcr;

    @Column(name = "avg_vix")
    private Double avgVix;

    @Column(name = "market_trend")
    private String marketTrend;

    // Time context
    @Column(name = "market_hour")
    private Integer marketHour;  // Which hour of trading (1-7)

    @Column(name = "is_opening_hour")
    private Boolean isOpeningHour;

    @Column(name = "is_closing_hour")
    private Boolean isClosingHour;

    @Column(name = "day_of_week")
    private Integer dayOfWeek;  // 1-5 for Mon-Fri

    @Column(name = "is_expiry_day")
    private Boolean isExpiryDay;

    // Correction factors suggested
    @Column(name = "suggested_close_correction")
    private Double suggestedCloseCorrection;  // Factor to apply to future predictions

    @Column(name = "suggested_volatility_correction")
    private Double suggestedVolatilityCorrection;

    // Running averages for correction (updated each verification)
    @Column(name = "cumulative_sessions")
    private Integer cumulativeSessions;

    @Column(name = "cumulative_avg_deviation")
    private Double cumulativeAvgDeviation;

    @Column(name = "cumulative_accuracy")
    private Double cumulativeAccuracy;

    // Prediction sequence specific deviations (1-5 candles)
    @Column(name = "seq1_avg_deviation")
    private Double seq1AvgDeviation;  // 1st minute prediction deviation

    @Column(name = "seq2_avg_deviation")
    private Double seq2AvgDeviation;  // 2nd minute

    @Column(name = "seq3_avg_deviation")
    private Double seq3AvgDeviation;  // 3rd minute

    @Column(name = "seq4_avg_deviation")
    private Double seq4AvgDeviation;  // 4th minute

    @Column(name = "seq5_avg_deviation")
    private Double seq5AvgDeviation;  // 5th minute

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    /**
     * Pre-persist hook to set creation timestamp
     */
    @PrePersist
    protected void onCreate() {
        this.createdAt = LocalDateTime.now();
    }
}

