package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Entity
@Table(name="PredictedCandleStick")
public class PredictedCandleStick {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "predicted_candle_seq")
    @SequenceGenerator(name = "predicted_candle_seq", sequenceName = "predicted_candle_stick_seq", allocationSize = 1)
    private Long id;

    Long instrumentToken;

    Double openPrice;
    Double highPrice;
    Double lowPrice;
    Double closePrice;

    LocalDateTime candleStartTime;
    LocalDateTime candleEndTime;

    // Prediction metadata
    LocalDateTime predictionGeneratedAt;

    Integer predictionSequence; // 1-5 for the five predicted candles

    Double confidenceScore; // 0-100 confidence in the prediction

    String trendDirection; // BULLISH, BEARISH, NEUTRAL

    Double predictedVolatility; // Expected volatility for the candle

    Double supportLevel;
    Double resistanceLevel;

    Integer maxPainStrike; // Max pain strike at prediction time

    Double pcrAtPrediction; // Put-Call Ratio at the time of prediction

    String predictionBasis; // Technical, OptionChain, Combined

    Boolean verified; // Flag to track if prediction has been verified against actual

    Double actualClosePrice; // To store actual close price for accuracy tracking

    Double predictionAccuracy; // Percentage accuracy when verified

}
