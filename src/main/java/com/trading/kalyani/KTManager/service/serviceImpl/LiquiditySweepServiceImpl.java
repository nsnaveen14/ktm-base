package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.entity.LiquiditySweepAnalysis;
import com.trading.kalyani.KTManager.model.HistoricalDataRequest;
import com.trading.kalyani.KTManager.model.HistoricalDataResponse;
import com.trading.kalyani.KTManager.repository.LiquiditySweepRepository;
import com.trading.kalyani.KTManager.service.InstrumentService;
import com.trading.kalyani.KTManager.service.LiquiditySweepService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.LocalDate;
import java.util.*;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;

/**
 * Implementation of Liquidity Sweep Analysis Service.
 *
 * Implements the "Liquidity Sweep Pro [Whale Edition]" strategy with three synchronized layers:
 *
 * 1. MARKET STRUCTURE (Liquidity Pools)
 *    - Identifies BSL (Buy Side Liquidity) above swing highs
 *    - Identifies SSL (Sell Side Liquidity) below swing lows
 *
 * 2. QUANT ENGINE (Whale Detection)
 *    - Log-Normal Z-Score for volume anomaly detection
 *    - Kaufman Efficiency Ratio for price movement quality
 *    - Classifies whale type: Absorption (Iceberg) or Propulsion (Drive)
 *
 * 3. SMART ENTRY (Trade Signal)
 *    - Price sweeps liquidity level
 *    - Price closes back within range
 *    - Institutional activity confirmed
 *    - Trend and momentum filters aligned
 */
@Service
public class LiquiditySweepServiceImpl implements LiquiditySweepService {

    private static final Logger logger = LoggerFactory.getLogger(LiquiditySweepServiceImpl.class);

    // Configuration defaults
    private static final double DEFAULT_WHALE_THRESHOLD = 2.5;  // 2.5 sigma for whale detection
    private static final int DEFAULT_LOOKBACK_PERIOD = 20;      // Candles for swing detection
    private static final int DEFAULT_VOLUME_PERIOD = 20;        // Period for volume analysis
    private static final int DEFAULT_EMA_PERIOD = 200;          // EMA period for trend
    private static final int DEFAULT_RSI_PERIOD = 14;           // RSI period
    private static final double DEFAULT_ATR_MULTIPLIER = 1.5;   // ATR multiplier for SL
    private static final double DEFAULT_RISK_REWARD = 2.0;      // Default R:R ratio

    // Kaufman Efficiency thresholds
    private static final double KER_ABSORPTION_THRESHOLD = 0.3; // Low efficiency = absorption
    private static final double KER_PROPULSION_THRESHOLD = 0.7; // High efficiency = propulsion

    // RSI thresholds
    private static final double RSI_OVERSOLD = 30.0;
    private static final double RSI_OVERBOUGHT = 70.0;

    // Configuration (can be adjusted at runtime)
    private volatile double whaleThreshold = DEFAULT_WHALE_THRESHOLD;
    private volatile int lookbackPeriod = DEFAULT_LOOKBACK_PERIOD;
    private volatile int volumePeriod = DEFAULT_VOLUME_PERIOD;

    @Autowired
    private LiquiditySweepRepository repository;

    @Autowired
    private InstrumentService instrumentService;


    // ============= MAIN ANALYSIS METHOD =============

    @Override
    public LiquiditySweepAnalysis analyzeLiquiditySweep(Integer appJobConfigNum) {
        if (appJobConfigNum != null) {
            MDC.put("appJobConfigNum", String.valueOf(appJobConfigNum));
        }

        try {
            logger.info("Starting Liquidity Sweep analysis for appJobConfigNum: {}", appJobConfigNum);

            // Fetch historical candle data
            List<HistoricalDataResponse.HistoricalCandle> candles = fetchHistoricalData(appJobConfigNum);

            if (candles == null || candles.size() < lookbackPeriod) {
                logger.warn("Insufficient candle data for analysis. Need at least {} candles, got {}",
                        lookbackPeriod, candles != null ? candles.size() : 0);
                return null;
            }

            // Get current candle (latest)
            HistoricalDataResponse.HistoricalCandle currentCandle = candles.get(candles.size() - 1);

            // Build analysis
            LiquiditySweepAnalysis analysis = LiquiditySweepAnalysis.builder()
                    .appJobConfigNum(appJobConfigNum)
                    .analysisTimestamp(LocalDateTime.now())
                    .spotPrice(currentCandle.getClose())
                    .open(currentCandle.getOpen())
                    .high(currentCandle.getHigh())
                    .low(currentCandle.getLow())
                    .close(currentCandle.getClose())
                    .volume(currentCandle.getVolume())
                    .whaleThreshold(whaleThreshold)
                    .lookbackPeriod(lookbackPeriod)
                    .volumePeriod(volumePeriod)
                    .timeframe(getTimeframe(appJobConfigNum))
                    .build();

            // Layer 1: Market Structure (Liquidity Pools)
            identifyLiquidityPools(analysis, candles);

            // Layer 2: Quant Engine (Whale Detection)
            detectWhaleActivity(analysis, candles);

            // Layer 3: Trend & Momentum Filters
            calculateTrendAndMomentum(analysis, candles);

            // Layer 4: Liquidity Sweep Detection
            detectLiquiditySweep(analysis, currentCandle);

            // Layer 5: Generate Trade Signal
            generateTradeSignal(analysis);

            // Layer 6: Calculate Entry/Exit Levels
            calculateEntryExitLevels(analysis, candles);

            // Save analysis
            analysis = repository.save(analysis);

            logger.info("Liquidity Sweep analysis complete: Signal={}, Strength={}, Confidence={}%, WhaleType={}",
                    analysis.getSignalType(), analysis.getSignalStrength(),
                    analysis.getSignalConfidence(), analysis.getWhaleType());

            return analysis;

        } catch (Exception e) {
            logger.error("Error in liquidity sweep analysis: {}", e.getMessage(), e);
            return null;
        } finally {
            MDC.remove("appJobConfigNum");
        }
    }

    // ============= LAYER 1: MARKET STRUCTURE (Liquidity Pools) =============

    /**
     * Identify key pivot points where retail stop losses are likely clustered.
     * BSL (Buy Side Liquidity): Areas above swing highs
     * SSL (Sell Side Liquidity): Areas below swing lows
     */
    private void identifyLiquidityPools(LiquiditySweepAnalysis analysis,
                                         List<HistoricalDataResponse.HistoricalCandle> candles) {

        List<Double> swingHighs = new ArrayList<>();
        List<Double> swingLows = new ArrayList<>();

        // Find swing highs and lows using lookback period
        for (int i = 2; i < candles.size() - 2; i++) {
            HistoricalDataResponse.HistoricalCandle current = candles.get(i);
            HistoricalDataResponse.HistoricalCandle prev1 = candles.get(i - 1);
            HistoricalDataResponse.HistoricalCandle prev2 = candles.get(i - 2);
            HistoricalDataResponse.HistoricalCandle next1 = candles.get(i + 1);
            HistoricalDataResponse.HistoricalCandle next2 = candles.get(i + 2);

            // Swing High: Higher than 2 candles before and after
            if (current.getHigh() > prev1.getHigh() && current.getHigh() > prev2.getHigh() &&
                current.getHigh() > next1.getHigh() && current.getHigh() > next2.getHigh()) {
                swingHighs.add(current.getHigh());
            }

            // Swing Low: Lower than 2 candles before and after
            if (current.getLow() < prev1.getLow() && current.getLow() < prev2.getLow() &&
                current.getLow() < next1.getLow() && current.getLow() < next2.getLow()) {
                swingLows.add(current.getLow());
            }
        }

        // Sort and get the most recent/significant levels
        swingHighs.sort(Collections.reverseOrder()); // Highest first
        swingLows.sort(Comparator.naturalOrder());   // Lowest first

        // Set BSL levels (above swing highs - where buy stops are)
        if (swingHighs.size() >= 1) analysis.setBslLevel1(swingHighs.get(0));
        if (swingHighs.size() >= 2) analysis.setBslLevel2(swingHighs.get(1));
        if (swingHighs.size() >= 3) analysis.setBslLevel3(swingHighs.get(2));

        // Set SSL levels (below swing lows - where sell stops are)
        if (swingLows.size() >= 1) analysis.setSslLevel1(swingLows.get(0));
        if (swingLows.size() >= 2) analysis.setSslLevel2(swingLows.get(1));
        if (swingLows.size() >= 3) analysis.setSslLevel3(swingLows.get(2));

        // Store swing points
        if (swingHighs.size() >= 1) analysis.setSwingHigh1(swingHighs.get(0));
        if (swingHighs.size() >= 2) analysis.setSwingHigh2(swingHighs.get(1));
        if (swingHighs.size() >= 3) analysis.setSwingHigh3(swingHighs.get(2));
        if (swingLows.size() >= 1) analysis.setSwingLow1(swingLows.get(0));
        if (swingLows.size() >= 2) analysis.setSwingLow2(swingLows.get(1));
        if (swingLows.size() >= 3) analysis.setSwingLow3(swingLows.get(2));

        logger.debug("Identified {} swing highs and {} swing lows", swingHighs.size(), swingLows.size());
        logger.debug("BSL Levels: {}, {}, {}", analysis.getBslLevel1(), analysis.getBslLevel2(), analysis.getBslLevel3());
        logger.debug("SSL Levels: {}, {}, {}", analysis.getSslLevel1(), analysis.getSslLevel2(), analysis.getSslLevel3());
    }

    // ============= LAYER 2: QUANT ENGINE (Whale Detection) =============

    /**
     * Detect institutional whale activity using:
     * - Log-Normal Z-Score for volume anomaly detection
     * - Kaufman Efficiency Ratio for price movement quality
     */
    private void detectWhaleActivity(LiquiditySweepAnalysis analysis,
                                      List<HistoricalDataResponse.HistoricalCandle> candles) {

        // Extract volume data
        List<Long> volumes = candles.stream()
                .map(HistoricalDataResponse.HistoricalCandle::getVolume)
                .filter(Objects::nonNull)
                .toList();

        if (volumes.isEmpty()) {
            analysis.setHasWhaleActivity(false);
            analysis.setWhaleType("NONE");
            return;
        }

        // Calculate Log-Normal Z-Score for current volume
        double logVolumeZScore = calculateLogNormalZScore(volumes, analysis.getVolume());
        analysis.setLogVolumeZScore(logVolumeZScore);
        analysis.setIsVolumeAnomaly(logVolumeZScore > whaleThreshold);

        // Calculate average volume
        double avgVolume = volumes.stream().mapToLong(Long::longValue).average().orElse(0);
        analysis.setAverageVolume((long) avgVolume);

        // Calculate volume standard deviation
        double volumeStdDev = calculateStdDev(volumes.stream().map(Long::doubleValue).toList());
        analysis.setVolumeStdDev(volumeStdDev);

        // Calculate Kaufman Efficiency Ratio (KER)
        double ker = calculateKaufmanEfficiencyRatio(candles, lookbackPeriod);
        analysis.setKaufmanEfficiencyRatio(ker);

        // Calculate price change and volatility for KER context
        if (candles.size() >= lookbackPeriod) {
            double startPrice = candles.get(candles.size() - lookbackPeriod).getClose();
            double endPrice = candles.get(candles.size() - 1).getClose();
            analysis.setPriceChange(endPrice - startPrice);

            double volatility = 0;
            for (int i = candles.size() - lookbackPeriod + 1; i < candles.size(); i++) {
                volatility += Math.abs(candles.get(i).getClose() - candles.get(i - 1).getClose());
            }
            analysis.setPriceVolatility(volatility);
        }

        // Classify whale type based on Z-Score and KER
        boolean hasVolumeAnomaly = logVolumeZScore > whaleThreshold;

        if (hasVolumeAnomaly) {
            if (ker < KER_ABSORPTION_THRESHOLD) {
                // High Volume + Low Price Efficiency = Absorption (Iceberg)
                // Signals potential reversal
                analysis.setWhaleType("ABSORPTION");
                analysis.setIsAbsorption(true);
                analysis.setIsPropulsion(false);
                logger.info("🐋 Whale Activity: ABSORPTION (Iceberg) - Z-Score: {}, KER: {}",
                        String.format("%.2f", logVolumeZScore), String.format("%.2f", ker));
            } else if (ker > KER_PROPULSION_THRESHOLD) {
                // High Volume + High Price Efficiency = Propulsion (Drive)
                // Signals aggressive breakout
                analysis.setWhaleType("PROPULSION");
                analysis.setIsAbsorption(false);
                analysis.setIsPropulsion(true);
                logger.info("🚀 Whale Activity: PROPULSION (Drive) - Z-Score: {}, KER: {}",
                        String.format("%.2f", logVolumeZScore), String.format("%.2f", ker));
            } else {
                // High volume but neutral efficiency
                analysis.setWhaleType("ACCUMULATION");
                analysis.setIsAbsorption(false);
                analysis.setIsPropulsion(false);
                logger.info("📊 Whale Activity: ACCUMULATION - Z-Score: {}, KER: {}",
                        String.format("%.2f", logVolumeZScore), String.format("%.2f", ker));
            }
            analysis.setHasWhaleActivity(true);
        } else {
            analysis.setWhaleType("NONE");
            analysis.setIsAbsorption(false);
            analysis.setIsPropulsion(false);
            analysis.setHasWhaleActivity(false);
        }
    }

    /**
     * Calculate Log-Normal Z-Score for volume anomaly detection.
     * This normalizes volume data to detect statistically significant outliers.
     */
    private double calculateLogNormalZScore(List<Long> volumes, Long currentVolume) {
        if (volumes.isEmpty() || currentVolume == null || currentVolume <= 0) {
            return 0.0;
        }

        // Convert to log values
        List<Double> logVolumes = volumes.stream()
                .filter(v -> v > 0)
                .map(v -> Math.log(v.doubleValue()))
                .toList();

        if (logVolumes.isEmpty()) {
            return 0.0;
        }

        // Calculate mean of log volumes
        double mean = logVolumes.stream().mapToDouble(Double::doubleValue).average().orElse(0);

        // Calculate standard deviation of log volumes
        double variance = logVolumes.stream()
                .mapToDouble(v -> Math.pow(v - mean, 2))
                .average()
                .orElse(0);
        double stdDev = Math.sqrt(variance);

        if (stdDev == 0) {
            return 0.0;
        }

        // Calculate Z-Score for current log volume
        double logCurrentVolume = Math.log(currentVolume.doubleValue());
        return (logCurrentVolume - mean) / stdDev;
    }

    /**
     * Calculate Kaufman Efficiency Ratio (KER).
     * KER = Direction / Volatility
     * - High KER (close to 1): Price moved efficiently in one direction
     * - Low KER (close to 0): Price moved erratically with high volatility
     */
    private double calculateKaufmanEfficiencyRatio(List<HistoricalDataResponse.HistoricalCandle> candles, int period) {
        if (candles.size() < period) {
            return 0.5; // Neutral if insufficient data
        }

        int startIndex = candles.size() - period;

        // Direction: Net price change over the period
        double direction = Math.abs(candles.get(candles.size() - 1).getClose() -
                                    candles.get(startIndex).getClose());

        // Volatility: Sum of absolute price changes
        double volatility = 0;
        for (int i = startIndex + 1; i < candles.size(); i++) {
            volatility += Math.abs(candles.get(i).getClose() - candles.get(i - 1).getClose());
        }

        if (volatility == 0) {
            return 1.0; // Perfect efficiency if no volatility
        }

        return direction / volatility;
    }

    // ============= LAYER 3: TREND & MOMENTUM FILTERS =============

    /**
     * Calculate trend (EMA 200) and momentum (RSI) indicators.
     */
    private void calculateTrendAndMomentum(LiquiditySweepAnalysis analysis,
                                            List<HistoricalDataResponse.HistoricalCandle> candles) {

        // Calculate EMA 200
        double ema200 = calculateEMA(candles, DEFAULT_EMA_PERIOD);
        analysis.setEma200(ema200);
        analysis.setIsAboveEma200(analysis.getClose() > ema200);

        // Determine trend direction
        if (analysis.getClose() > ema200 * 1.002) { // 0.2% buffer
            analysis.setTrendDirection("BULLISH");
        } else if (analysis.getClose() < ema200 * 0.998) {
            analysis.setTrendDirection("BEARISH");
        } else {
            analysis.setTrendDirection("NEUTRAL");
        }

        // Calculate RSI
        double rsi = calculateRSI(candles, DEFAULT_RSI_PERIOD);
        analysis.setRsiValue(rsi);
        analysis.setIsRsiOversold(rsi < RSI_OVERSOLD);
        analysis.setIsRsiOverbought(rsi > RSI_OVERBOUGHT);

        logger.debug("Trend: {} | EMA200: {} | RSI: {}",
                analysis.getTrendDirection(), String.format("%.2f", ema200), String.format("%.2f", rsi));
    }

    /**
     * Calculate Exponential Moving Average
     */
    private double calculateEMA(List<HistoricalDataResponse.HistoricalCandle> candles, int period) {
        if (candles.size() < period) {
            // If not enough data, use simple average
            return candles.stream()
                    .mapToDouble(HistoricalDataResponse.HistoricalCandle::getClose)
                    .average()
                    .orElse(0);
        }

        double multiplier = 2.0 / (period + 1);
        double ema = candles.subList(0, period).stream()
                .mapToDouble(HistoricalDataResponse.HistoricalCandle::getClose)
                .average()
                .orElse(0);

        for (int i = period; i < candles.size(); i++) {
            double close = candles.get(i).getClose();
            ema = (close - ema) * multiplier + ema;
        }

        return ema;
    }

    /**
     * Calculate Relative Strength Index (RSI)
     */
    private double calculateRSI(List<HistoricalDataResponse.HistoricalCandle> candles, int period) {
        if (candles.size() < period + 1) {
            return 50.0; // Neutral RSI if insufficient data
        }

        double gainSum = 0;
        double lossSum = 0;

        // Calculate initial average gain and loss
        for (int i = candles.size() - period; i < candles.size(); i++) {
            double change = candles.get(i).getClose() - candles.get(i - 1).getClose();
            if (change > 0) {
                gainSum += change;
            } else {
                lossSum += Math.abs(change);
            }
        }

        double avgGain = gainSum / period;
        double avgLoss = lossSum / period;

        if (avgLoss == 0) {
            return 100.0;
        }

        double rs = avgGain / avgLoss;
        return 100 - (100 / (1 + rs));
    }

    // ============= LAYER 4: LIQUIDITY SWEEP DETECTION =============

    /**
     * Detect if price has swept a liquidity level.
     * A sweep occurs when:
     * - Price wicks above/below a level
     * - Price closes back within the range
     */
    private void detectLiquiditySweep(LiquiditySweepAnalysis analysis,
                                       HistoricalDataResponse.HistoricalCandle currentCandle) {

        boolean bslSwept = false;
        boolean sslSwept = false;
        Double sweptLevel = null;

        // Check BSL sweep (price wicked above BSL but closed below)
        if (analysis.getBslLevel1() != null) {
            if (currentCandle.getHigh() > analysis.getBslLevel1() &&
                currentCandle.getClose() < analysis.getBslLevel1()) {
                bslSwept = true;
                sweptLevel = analysis.getBslLevel1();
                logger.info("🔴 BSL SWEEP detected at level {}", sweptLevel);
            }
        }

        // Check SSL sweep (price wicked below SSL but closed above)
        if (analysis.getSslLevel1() != null) {
            if (currentCandle.getLow() < analysis.getSslLevel1() &&
                currentCandle.getClose() > analysis.getSslLevel1()) {
                sslSwept = true;
                sweptLevel = analysis.getSslLevel1();
                logger.info("🟢 SSL SWEEP detected at level {}", sweptLevel);
            }
        }

        analysis.setBslSwept(bslSwept);
        analysis.setSslSwept(sslSwept);
        analysis.setSweptLevel(sweptLevel);

        if (bslSwept) {
            analysis.setSweepType("BSL_SWEEP");
        } else if (sslSwept) {
            analysis.setSweepType("SSL_SWEEP");
        } else {
            analysis.setSweepType("NONE");
        }

        // Check if price closed back within range
        boolean closedBack = (bslSwept && currentCandle.getClose() < analysis.getBslLevel1()) ||
                             (sslSwept && currentCandle.getClose() > analysis.getSslLevel1());
        analysis.setPriceClosedBack(closedBack);

        // Check institutional confirmation
        boolean hasInstitutionalConfirmation = (bslSwept || sslSwept) &&
                                                Boolean.TRUE.equals(analysis.getHasWhaleActivity());
        analysis.setHasInstitutionalConfirmation(hasInstitutionalConfirmation);

        // Check trend alignment
        boolean trendAligned = (sslSwept && "BULLISH".equals(analysis.getTrendDirection())) ||
                               (bslSwept && "BEARISH".equals(analysis.getTrendDirection()));
        analysis.setIsTrendAligned(trendAligned);

        // Check momentum alignment
        boolean momentumAligned = (sslSwept && Boolean.TRUE.equals(analysis.getIsRsiOversold())) ||
                                  (bslSwept && Boolean.TRUE.equals(analysis.getIsRsiOverbought()));
        analysis.setIsMomentumAligned(momentumAligned);
    }

    // ============= LAYER 5: TRADE SIGNAL GENERATION =============

    /**
     * Generate trade signal based on all analysis layers.
     * A valid signal requires:
     * - Price swept a liquidity level
     * - Price closed back within range
     * - Institutional activity confirmed
     * - Trend and momentum filters aligned (optional for strength)
     */
    private void generateTradeSignal(LiquiditySweepAnalysis analysis) {
        String signalType = "NONE";
        String signalStrength = "WEAK";
        double confidence = 0;
        boolean isValidSetup = false;

        // Check for valid LONG setup (SSL sweep)
        if (Boolean.TRUE.equals(analysis.getSslSwept()) &&
            Boolean.TRUE.equals(analysis.getPriceClosedBack())) {

            signalType = "BUY";
            confidence = 40; // Base confidence for sweep + close back

            // Add confidence for institutional confirmation
            if (Boolean.TRUE.equals(analysis.getHasInstitutionalConfirmation())) {
                confidence += 25;
            }

            // Add confidence for trend alignment
            if (Boolean.TRUE.equals(analysis.getIsTrendAligned())) {
                confidence += 15;
            }

            // Add confidence for momentum alignment
            if (Boolean.TRUE.equals(analysis.getIsMomentumAligned())) {
                confidence += 10;
            }

            // Add confidence for absorption whale type (reversal signal)
            if ("ABSORPTION".equals(analysis.getWhaleType())) {
                confidence += 10;
            }

            // Determine signal strength
            if (confidence >= 80) {
                signalStrength = "STRONG";
            } else if (confidence >= 60) {
                signalStrength = "MODERATE";
            } else {
                signalStrength = "WEAK";
            }

            // Valid setup requires institutional confirmation at minimum
            isValidSetup = Boolean.TRUE.equals(analysis.getHasInstitutionalConfirmation());
        }

        // Check for valid SHORT setup (BSL sweep)
        else if (Boolean.TRUE.equals(analysis.getBslSwept()) &&
            Boolean.TRUE.equals(analysis.getPriceClosedBack())) {

            signalType = "SELL";
            confidence = 40;

            if (Boolean.TRUE.equals(analysis.getHasInstitutionalConfirmation())) {
                confidence += 25;
            }

            if (Boolean.TRUE.equals(analysis.getIsTrendAligned())) {
                confidence += 15;
            }

            if (Boolean.TRUE.equals(analysis.getIsMomentumAligned())) {
                confidence += 10;
            }

            if ("ABSORPTION".equals(analysis.getWhaleType())) {
                confidence += 10;
            }

            if (confidence >= 80) {
                signalStrength = "STRONG";
            } else if (confidence >= 60) {
                signalStrength = "MODERATE";
            } else {
                signalStrength = "WEAK";
            }

            isValidSetup = Boolean.TRUE.equals(analysis.getHasInstitutionalConfirmation());
        }

        analysis.setSignalType(signalType);
        analysis.setSignalStrength(signalStrength);
        analysis.setSignalConfidence(confidence);
        analysis.setIsValidSetup(isValidSetup);

        if (isValidSetup) {
            logger.info("✅ VALID {} SETUP - Strength: {}, Confidence: {}%",
                    signalType, signalStrength, confidence);
        }
    }

    // ============= LAYER 6: ENTRY/EXIT CALCULATION =============

    /**
     * Calculate entry, stop loss, and take profit levels using ATR.
     */
    private void calculateEntryExitLevels(LiquiditySweepAnalysis analysis,
                                           List<HistoricalDataResponse.HistoricalCandle> candles) {

        // Calculate ATR
        double atr = calculateATR(candles, 14);
        analysis.setAtrValue(atr);

        double entryPrice = analysis.getClose();
        analysis.setEntryPrice(entryPrice);

        if ("BUY".equals(analysis.getSignalType())) {
            // For BUY: SL below sweep level, TP above
            double stopLoss = analysis.getSweptLevel() != null ?
                    analysis.getSweptLevel() - (atr * 0.5) :
                    entryPrice - (atr * DEFAULT_ATR_MULTIPLIER);

            double riskPoints = entryPrice - stopLoss;

            analysis.setStopLossPrice(stopLoss);
            analysis.setRiskPoints(riskPoints);
            analysis.setTakeProfit1(entryPrice + riskPoints);      // 1:1 R:R
            analysis.setTakeProfit2(entryPrice + (riskPoints * 2)); // 1:2 R:R
            analysis.setTakeProfit3(entryPrice + (riskPoints * 3)); // 1:3 R:R
            analysis.setRiskRewardRatio(DEFAULT_RISK_REWARD);

            analysis.setSuggestedOptionType("CE");
            analysis.setSuggestedStrike(Math.round(entryPrice / 50) * 50.0); // Round to nearest 50
            analysis.setOptionStrategy("BUY_CE");

        } else if ("SELL".equals(analysis.getSignalType())) {
            // For SELL: SL above sweep level, TP below
            double stopLoss = analysis.getSweptLevel() != null ?
                    analysis.getSweptLevel() + (atr * 0.5) :
                    entryPrice + (atr * DEFAULT_ATR_MULTIPLIER);

            double riskPoints = stopLoss - entryPrice;

            analysis.setStopLossPrice(stopLoss);
            analysis.setRiskPoints(riskPoints);
            analysis.setTakeProfit1(entryPrice - riskPoints);
            analysis.setTakeProfit2(entryPrice - (riskPoints * 2));
            analysis.setTakeProfit3(entryPrice - (riskPoints * 3));
            analysis.setRiskRewardRatio(DEFAULT_RISK_REWARD);

            analysis.setSuggestedOptionType("PE");
            analysis.setSuggestedStrike(Math.round(entryPrice / 50) * 50.0);
            analysis.setOptionStrategy("BUY_PE");
        }
    }

    /**
     * Calculate Average True Range (ATR)
     */
    private double calculateATR(List<HistoricalDataResponse.HistoricalCandle> candles, int period) {
        if (candles.size() < period + 1) {
            return 0;
        }

        List<Double> trueRanges = new ArrayList<>();

        for (int i = 1; i < candles.size(); i++) {
            HistoricalDataResponse.HistoricalCandle current = candles.get(i);
            HistoricalDataResponse.HistoricalCandle previous = candles.get(i - 1);

            double highLow = current.getHigh() - current.getLow();
            double highClose = Math.abs(current.getHigh() - previous.getClose());
            double lowClose = Math.abs(current.getLow() - previous.getClose());

            double trueRange = Math.max(highLow, Math.max(highClose, lowClose));
            trueRanges.add(trueRange);
        }

        // Calculate ATR as average of last 'period' true ranges
        int startIdx = Math.max(0, trueRanges.size() - period);
        return trueRanges.subList(startIdx, trueRanges.size()).stream()
                .mapToDouble(Double::doubleValue)
                .average()
                .orElse(0);
    }

    // ============= HELPER METHODS =============

    private List<HistoricalDataResponse.HistoricalCandle> fetchHistoricalData(Integer appJobConfigNum) {
        try {
            String instrumentToken = getInstrumentToken(appJobConfigNum);
            String interval = "15minute"; // Default to 15-minute candles for liquidity sweep

            HistoricalDataRequest request = new HistoricalDataRequest();
            request.setInstrumentToken(instrumentToken);
            request.setInterval(interval);
            request.setFromDate(LocalDateTime.now().minusDays(5)); // Last 5 days of data
            request.setToDate(LocalDateTime.now());
            request.setContinuous(false);
            request.setOi(false);

            HistoricalDataResponse response = instrumentService.getHistoricalData(request);

            if (response != null && response.isSuccess() && response.getCandles() != null) {
                logger.debug("Fetched {} candles for liquidity sweep analysis", response.getCandles().size());
                return response.getCandles();
            }

            return new ArrayList<>();

        } catch (Exception e) {
            logger.error("Error fetching historical data: {}", e.getMessage());
            return new ArrayList<>();
        }
    }

    private String getInstrumentToken(Integer appJobConfigNum) {
        if (appJobConfigNum != null) {
            var instruments = instrumentService.getInstrumentsFromAppJobConfigNum(appJobConfigNum);
            if (!instruments.isEmpty()) {
                return String.valueOf(instruments.get(0).getInstrument().getInstrument_token());
            }
            logger.warn("No instruments found for appJobConfigNum: {}, defaulting to NIFTY", appJobConfigNum);
        }
        return String.valueOf(NIFTY_INSTRUMENT_TOKEN);
    }

    private String getTimeframe(Integer appJobConfigNum) {
        // TODO: derive timeframe from appJobConfigNum job type config
        logger.debug("getTimeframe: appJobConfigNum={} not yet mapped, defaulting to 15m", appJobConfigNum);
        return "15m";
    }

    private double calculateStdDev(List<Double> values) {
        if (values.isEmpty()) return 0;
        double mean = values.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        double variance = values.stream()
                .mapToDouble(v -> Math.pow(v - mean, 2))
                .average()
                .orElse(0);
        return Math.sqrt(variance);
    }

    // ============= SERVICE INTERFACE IMPLEMENTATIONS =============

    @Override
    public Optional<LiquiditySweepAnalysis> getLatestAnalysis(Integer appJobConfigNum) {
        return repository.findLatestByAppJobConfigNum(appJobConfigNum);
    }

    @Override
    public Optional<LiquiditySweepAnalysis> getLatestValidSetup(Integer appJobConfigNum) {
        return repository.findLatestValidSetupByAppJobConfigNum(appJobConfigNum);
    }

    @Override
    public Map<String, Object> checkLiquiditySweepSignal(Integer appJobConfigNum) {
        Map<String, Object> result = new HashMap<>();
        result.put("hasSignal", false);
        result.put("signalSource", "LIQUIDITY_SWEEP");

        try {
            // Run fresh analysis
            LiquiditySweepAnalysis analysis = analyzeLiquiditySweep(appJobConfigNum);

            if (analysis != null && Boolean.TRUE.equals(analysis.getIsValidSetup())) {
                result.put("hasSignal", true);
                result.put("signalType", analysis.getSignalType());
                result.put("signalStrength", analysis.getSignalStrength());
                result.put("confidence", analysis.getSignalConfidence());
                result.put("whaleType", analysis.getWhaleType());
                result.put("sweepType", analysis.getSweepType());
                result.put("sweptLevel", analysis.getSweptLevel());
                result.put("entryPrice", analysis.getEntryPrice());
                result.put("stopLoss", analysis.getStopLossPrice());
                result.put("takeProfit", analysis.getTakeProfit2());
                result.put("suggestedOption", analysis.getSuggestedOptionType());
                result.put("analysisId", analysis.getId());
            } else if (analysis != null) {
                result.put("reason", "No valid liquidity sweep setup detected");
                result.put("whaleActivity", analysis.getHasWhaleActivity());
                result.put("sweepType", analysis.getSweepType());
            }

        } catch (Exception e) {
            logger.error("Error checking liquidity sweep signal: {}", e.getMessage());
            result.put("error", e.getMessage());
        }

        return result;
    }

    @Override
    public Map<String, Object> getTradeRecommendation(Integer appJobConfigNum) {
        Map<String, Object> recommendation = new HashMap<>();

        Optional<LiquiditySweepAnalysis> latestSetup = getLatestValidSetup(appJobConfigNum);

        if (latestSetup.isPresent()) {
            LiquiditySweepAnalysis analysis = latestSetup.get();
            recommendation.put("hasRecommendation", true);
            recommendation.put("signal", analysis.getSignalType());
            recommendation.put("strength", analysis.getSignalStrength());
            recommendation.put("confidence", analysis.getSignalConfidence());
            recommendation.put("whaleType", analysis.getWhaleType());
            recommendation.put("entry", analysis.getEntryPrice());
            recommendation.put("stopLoss", analysis.getStopLossPrice());
            recommendation.put("takeProfit1", analysis.getTakeProfit1());
            recommendation.put("takeProfit2", analysis.getTakeProfit2());
            recommendation.put("takeProfit3", analysis.getTakeProfit3());
            recommendation.put("riskReward", analysis.getRiskRewardRatio());
            recommendation.put("optionType", analysis.getSuggestedOptionType());
            recommendation.put("optionStrategy", analysis.getOptionStrategy());
            recommendation.put("analysisTime", analysis.getAnalysisTimestamp());
        } else {
            recommendation.put("hasRecommendation", false);
            recommendation.put("message", "No valid liquidity sweep setup available");
        }

        return recommendation;
    }

    @Override
    public boolean doesLiquiditySweepSupportTrade(String tradeDirection, Integer appJobConfigNum) {
        Optional<LiquiditySweepAnalysis> latest = getLatestAnalysis(appJobConfigNum);

        if (latest.isEmpty()) {
            return true; // Allow if no analysis available
        }

        LiquiditySweepAnalysis analysis = latest.get();

        // Check if the trade direction aligns with liquidity sweep signal
        if ("BUY".equals(tradeDirection)) {
            // BUY is supported if we have SSL sweep or no BSL sweep
            return !Boolean.TRUE.equals(analysis.getBslSwept()) ||
                   Boolean.TRUE.equals(analysis.getSslSwept());
        } else if ("SELL".equals(tradeDirection)) {
            // SELL is supported if we have BSL sweep or no SSL sweep
            return !Boolean.TRUE.equals(analysis.getSslSwept()) ||
                   Boolean.TRUE.equals(analysis.getBslSwept());
        }

        return true;
    }

    @Override
    public Map<String, Object> getLiquidityLevels(Integer appJobConfigNum) {
        Map<String, Object> levels = new HashMap<>();

        Optional<LiquiditySweepAnalysis> latest = getLatestAnalysis(appJobConfigNum);

        if (latest.isPresent()) {
            LiquiditySweepAnalysis analysis = latest.get();

            Map<String, Double> bslLevels = new LinkedHashMap<>();
            if (analysis.getBslLevel1() != null) bslLevels.put("BSL1", analysis.getBslLevel1());
            if (analysis.getBslLevel2() != null) bslLevels.put("BSL2", analysis.getBslLevel2());
            if (analysis.getBslLevel3() != null) bslLevels.put("BSL3", analysis.getBslLevel3());

            Map<String, Double> sslLevels = new LinkedHashMap<>();
            if (analysis.getSslLevel1() != null) sslLevels.put("SSL1", analysis.getSslLevel1());
            if (analysis.getSslLevel2() != null) sslLevels.put("SSL2", analysis.getSslLevel2());
            if (analysis.getSslLevel3() != null) sslLevels.put("SSL3", analysis.getSslLevel3());

            levels.put("buySideLiquidity", bslLevels);
            levels.put("sellSideLiquidity", sslLevels);
            levels.put("currentPrice", analysis.getSpotPrice());
            levels.put("analysisTime", analysis.getAnalysisTimestamp());
        }

        return levels;
    }

    @Override
    public Map<String, Object> getWhaleActivityIndicators(Integer appJobConfigNum) {
        Map<String, Object> indicators = new HashMap<>();

        Optional<LiquiditySweepAnalysis> latest = getLatestAnalysis(appJobConfigNum);

        if (latest.isPresent()) {
            LiquiditySweepAnalysis analysis = latest.get();
            indicators.put("hasWhaleActivity", analysis.getHasWhaleActivity());
            indicators.put("whaleType", analysis.getWhaleType());
            indicators.put("volumeZScore", analysis.getLogVolumeZScore());
            indicators.put("kaufmanEfficiency", analysis.getKaufmanEfficiencyRatio());
            indicators.put("isAbsorption", analysis.getIsAbsorption());
            indicators.put("isPropulsion", analysis.getIsPropulsion());
            indicators.put("averageVolume", analysis.getAverageVolume());
            indicators.put("currentVolume", analysis.getVolume());
            indicators.put("whaleThreshold", analysis.getWhaleThreshold());
        }

        return indicators;
    }

    @Override
    public List<LiquiditySweepAnalysis> getTodaysAnalyses(Integer appJobConfigNum) {
        LocalDateTime startOfDay = LocalDate.now().atStartOfDay();
        return repository.findTodaysAnalyses(appJobConfigNum, startOfDay);
    }

    @Override
    public void setWhaleThreshold(double sigma) {
        this.whaleThreshold = sigma;
        logger.info("Whale threshold updated to {} sigma", sigma);
    }

    @Override
    public void setLookbackPeriod(int periods) {
        this.lookbackPeriod = periods;
        logger.info("Lookback period updated to {} periods", periods);
    }

    @Override
    public Map<String, Object> getConfiguration() {
        Map<String, Object> config = new HashMap<>();
        config.put("whaleThreshold", whaleThreshold);
        config.put("lookbackPeriod", lookbackPeriod);
        config.put("volumePeriod", volumePeriod);
        config.put("emaPeriod", DEFAULT_EMA_PERIOD);
        config.put("rsiPeriod", DEFAULT_RSI_PERIOD);
        config.put("atrMultiplier", DEFAULT_ATR_MULTIPLIER);
        config.put("defaultRiskReward", DEFAULT_RISK_REWARD);
        config.put("kerAbsorptionThreshold", KER_ABSORPTION_THRESHOLD);
        config.put("kerPropulsionThreshold", KER_PROPULSION_THRESHOLD);
        return config;
    }
}

