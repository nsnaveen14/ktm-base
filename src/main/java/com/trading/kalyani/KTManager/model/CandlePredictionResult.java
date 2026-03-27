package com.trading.kalyani.KTManager.model;

import com.trading.kalyani.KTManager.entity.PredictedCandleStick;
import lombok.*;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Model class for comprehensive candle prediction results.
 * Provides additional context and summary for intraday trading decisions.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class CandlePredictionResult {

    // Prediction metadata
    private LocalDateTime generatedAt;
    private String overallTrend;
    private Double averageConfidence;
    private String tradeRecommendation;

    // Market context
    private Double currentNiftyPrice;
    private Double supportLevel;
    private Double resistanceLevel;
    private Integer maxPainStrike;
    private Double pcr;

    // Technical indicators
    private Double ema9;
    private Double ema21;
    private Double rsi;
    private Double atr;
    private String emaCrossoverStatus;

    // Predicted price levels
    private Double predictedHighest;
    private Double predictedLowest;
    private Double expectedCloseIn5Min;

    // The actual predictions
    private List<PredictedCandleStick> predictedCandles;

    // Summary statistics
    private Integer bullishCandleCount;
    private Integer bearishCandleCount;
    private Double totalExpectedMove;

    // Trading edge indicators
    private String optionChainBias;
    private String technicalBias;
    private String combinedSignalStrength; // STRONG, MODERATE, WEAK

    /**
     * Calculate summary fields from the predicted candles
     */
    public void calculateSummary() {
        if (predictedCandles == null || predictedCandles.isEmpty()) {
            return;
        }

        // Count bullish/bearish candles
        bullishCandleCount = 0;
        bearishCandleCount = 0;
        for (PredictedCandleStick candle : predictedCandles) {
            if (candle.getClosePrice() > candle.getOpenPrice()) {
                bullishCandleCount++;
            } else if (candle.getClosePrice() < candle.getOpenPrice()) {
                bearishCandleCount++;
            }
        }

        // Calculate average confidence
        averageConfidence = predictedCandles.stream()
                .filter(c -> c.getConfidenceScore() != null)
                .mapToDouble(PredictedCandleStick::getConfidenceScore)
                .average()
                .orElse(50.0);

        // Find highest and lowest predicted prices
        predictedHighest = predictedCandles.stream()
                .filter(c -> c.getHighPrice() != null)
                .mapToDouble(PredictedCandleStick::getHighPrice)
                .max()
                .orElse(0.0);

        predictedLowest = predictedCandles.stream()
                .filter(c -> c.getLowPrice() != null)
                .mapToDouble(PredictedCandleStick::getLowPrice)
                .min()
                .orElse(0.0);

        // Get expected close after 5 minutes
        PredictedCandleStick lastCandle = predictedCandles.getLast();
        expectedCloseIn5Min = lastCandle.getClosePrice();

        // Calculate total expected move
        PredictedCandleStick firstCandle = predictedCandles.getFirst();
        if (firstCandle.getOpenPrice() != null && lastCandle.getClosePrice() != null) {
            totalExpectedMove = lastCandle.getClosePrice() - firstCandle.getOpenPrice();
        }

        // Determine signal strength
        determineCombinedSignalStrength();
    }

    /**
     * Determine the combined signal strength based on alignment of indicators
     */
    private void determineCombinedSignalStrength() {
        int alignedSignals = 0;

        // Check if trends align
        if (optionChainBias != null && technicalBias != null) {
            if (optionChainBias.equals(technicalBias)) {
                alignedSignals++;
            }
        }

        // Check confidence level
        if (averageConfidence != null && averageConfidence > 70) {
            alignedSignals++;
        }

        // Check candle direction consensus
        if (bullishCandleCount != null && bearishCandleCount != null) {
            if (bullishCandleCount >= 4 || bearishCandleCount >= 4) {
                alignedSignals++;
            }
        }

        // Set signal strength
        if (alignedSignals >= 3) {
            combinedSignalStrength = "STRONG";
        } else if (alignedSignals >= 2) {
            combinedSignalStrength = "MODERATE";
        } else {
            combinedSignalStrength = "WEAK";
        }
    }
}

