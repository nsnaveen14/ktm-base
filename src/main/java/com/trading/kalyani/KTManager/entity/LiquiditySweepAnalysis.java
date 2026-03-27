package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Entity to store Liquidity Sweep Analysis data.
 *
 * Implements the "Liquidity Sweep Pro [Whale Edition]" logic:
 * - Market Structure: Identifies BSL (Buy Side Liquidity) and SSL (Sell Side Liquidity) levels
 * - Quant Engine: Detects whale activity using Log-Normal Z-Score and Kaufman Efficiency Ratio
 * - Smart Entry: Generates signals when liquidity sweep + institutional activity is confirmed
 */
@Entity
@Getter
@Setter
@Builder
@Table(name = "liquidity_sweep_analysis")
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class LiquiditySweepAnalysis {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Integer appJobConfigNum;

    private LocalDateTime analysisTimestamp;

    // Current Market Data
    private Double spotPrice;
    private Double open;
    private Double high;
    private Double low;
    private Double close;
    private Long volume;

    // ============= MARKET STRUCTURE (Liquidity Pools) =============

    // Buy Side Liquidity (BSL) - Above swing highs where retail stop losses cluster
    private Double bslLevel1;           // Nearest BSL level
    private Double bslLevel2;           // Second BSL level
    private Double bslLevel3;           // Third BSL level

    // Sell Side Liquidity (SSL) - Below swing lows where retail stop losses cluster
    private Double sslLevel1;           // Nearest SSL level
    private Double sslLevel2;           // Second SSL level
    private Double sslLevel3;           // Third SSL level

    // Swing Points used to derive liquidity levels
    private Double swingHigh1;
    private Double swingHigh2;
    private Double swingHigh3;
    private Double swingLow1;
    private Double swingLow2;
    private Double swingLow3;

    // ============= QUANT ENGINE (Whale Detection) =============

    // Volume Analysis
    private Long averageVolume;         // Moving average of volume
    private Double volumeStdDev;        // Standard deviation of volume
    private Double logVolumeZScore;     // Log-normal Z-Score for volume anomaly detection
    private Boolean isVolumeAnomaly;    // True if Z-Score > whale threshold (default 2.5)
    private Double whaleThreshold;      // Configurable whale detection threshold (sigma)

    // Kaufman Efficiency Ratio (KER) - Quality of price movement
    private Double kaufmanEfficiencyRatio;  // KER value (0-1)
    private Double priceChange;             // Net price change over period
    private Double priceVolatility;         // Sum of absolute price changes

    // Whale Classification
    private String whaleType;           // ABSORPTION (Iceberg), PROPULSION (Drive), or NONE
    private Boolean isAbsorption;       // High Volume + Low Price Movement (reversal signal)
    private Boolean isPropulsion;       // High Volume + High Price Efficiency (breakout signal)
    private Boolean hasWhaleActivity;   // True if any whale activity detected

    // ============= TREND & MOMENTUM FILTERS =============

    private Double ema200;              // 200-period EMA for trend
    private Boolean isAboveEma200;      // True if price above EMA 200 (bullish trend)
    private Double rsiValue;            // RSI value (14-period default)
    private Boolean isRsiOversold;      // RSI < 30
    private Boolean isRsiOverbought;    // RSI > 70
    private String trendDirection;      // BULLISH, BEARISH, NEUTRAL

    // ============= LIQUIDITY SWEEP DETECTION =============

    // Sweep Events
    private Boolean bslSwept;           // True if price wicked above BSL level
    private Boolean sslSwept;           // True if price wicked below SSL level
    private Double sweptLevel;          // The actual level that was swept
    private String sweepType;           // BSL_SWEEP, SSL_SWEEP, or NONE

    // Sweep Validation
    private Boolean priceClosedBack;    // True if price closed back within range after sweep
    private Boolean hasInstitutionalConfirmation;  // True if sweep + whale activity
    private Boolean isTrendAligned;     // True if sweep aligns with EMA trend
    private Boolean isMomentumAligned;  // True if sweep aligns with RSI momentum

    // ============= TRADE SIGNAL =============

    private String signalType;          // BUY, SELL, or NONE
    private String signalStrength;      // STRONG, MODERATE, WEAK
    private Double signalConfidence;    // Confidence score 0-100
    private Boolean isValidSetup;       // All conditions met for trade

    // Entry/Exit Levels (ATR-based)
    private Double atrValue;            // Average True Range
    private Double entryPrice;          // Suggested entry price
    private Double stopLossPrice;       // Stop loss based on ATR
    private Double takeProfit1;         // TP1 (1:1 R:R)
    private Double takeProfit2;         // TP2 (1:2 R:R)
    private Double takeProfit3;         // TP3 (1:3 R:R)
    private Double riskRewardRatio;     // Actual R:R ratio
    private Double riskPoints;          // Points at risk (entry to SL)

    // Options Trading Suggestion
    private String suggestedOptionType; // CE or PE
    private Double suggestedStrike;     // ATM or OTM strike
    private String optionStrategy;      // BUY_CE, BUY_PE, etc.

    // ============= METADATA =============

    private String timeframe;           // 1m, 5m, 15m, 1h, 4h
    private Integer lookbackPeriod;     // Candles analyzed for swing detection
    private Integer volumePeriod;       // Period for volume analysis
    private String analysisNotes;       // Additional notes/reasoning

    @ManyToOne
    @JoinColumn(name = "jobIterationId", referencedColumnName = "id")
    private JobIterationDetails jobIterationDetails;

    // ============= TRANSIENT FIELDS =============

    @Transient
    private String signalColor;         // GREEN for BUY, RED for SELL

    @Transient
    private List<Double> recentHighs;   // Recent swing highs for analysis

    @Transient
    private List<Double> recentLows;    // Recent swing lows for analysis

    @Transient
    private List<Long> recentVolumes;   // Recent volumes for Z-score calculation

    /**
     * Check if this is a valid long (BUY) setup
     */
    public boolean isValidLongSetup() {
        return Boolean.TRUE.equals(sslSwept) &&
               Boolean.TRUE.equals(priceClosedBack) &&
               Boolean.TRUE.equals(hasInstitutionalConfirmation) &&
               Boolean.TRUE.equals(isAboveEma200) &&
               !Boolean.TRUE.equals(isRsiOverbought);
    }

    /**
     * Check if this is a valid short (SELL) setup
     */
    public boolean isValidShortSetup() {
        return Boolean.TRUE.equals(bslSwept) &&
               Boolean.TRUE.equals(priceClosedBack) &&
               Boolean.TRUE.equals(hasInstitutionalConfirmation) &&
               !Boolean.TRUE.equals(isAboveEma200) &&
               !Boolean.TRUE.equals(isRsiOversold);
    }
}

