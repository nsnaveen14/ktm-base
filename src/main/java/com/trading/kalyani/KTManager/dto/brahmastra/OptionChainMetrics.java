package com.trading.kalyani.KTManager.dto.brahmastra;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

/**
 * DTO for Option Chain Metrics integrated with Brahmastra strategy.
 * Provides Max Pain and OI Analysis data for signal confirmation.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OptionChainMetrics {

    private String symbol;
    private LocalDateTime timestamp;
    private Double spotPrice;
    private Double atmStrike;

    // Max Pain Analysis
    private MaxPainData maxPain;

    // Open Interest Analysis
    private OIAnalysisData oiAnalysis;

    // Combined Signal
    private String optionChainSignal;
    private Double optionChainConfidence;
    private String optionChainBias;
    private String recommendedAction;

    /**
     * Max Pain Analysis - Strike where option sellers have minimum loss
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MaxPainData {
        private Double maxPainStrike;
        private Double maxPainSecondStrike;
        private Double distanceFromSpot;
        private Double distanceFromSpotPercent;
        private String priceRelation;
        private Boolean confirmsBullish;
        private Boolean confirmsBearish;
        private Double pullStrength;
        private Boolean actsAsSupport;
        private Boolean actsAsResistance;
    }

    /**
     * Open Interest Analysis - OI changes for trend confirmation
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OIAnalysisData {
        private Double pcr;
        private Double pcrChange;
        private String pcrTrend;
        private String pcrSignal;
        private Double totalCallOI;
        private Double totalPutOI;
        private Double callOIChange;
        private Double putOIChange;
        private String oiBuildUpType;
        private Boolean confirmsUptrend;
        private Boolean confirmsDowntrend;
        private Double highestCallOIStrike;
        private Double highestPutOIStrike;
        private Double callOIConcentration;
        private Double putOIConcentration;
        private List<StrikeOIChange> significantOIChanges;
    }

    /**
     * Strike-wise OI change data
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StrikeOIChange {
        private Double strikePrice;
        private Double callOIChange;
        private Double putOIChange;
        private Double callOIChangePercent;
        private Double putOIChangePercent;
        private String interpretation;
    }

}
