package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.entity.MarketStructure;
import com.trading.kalyani.KTManager.model.HistoricalDataRequest;
import com.trading.kalyani.KTManager.model.HistoricalDataResponse;
import com.trading.kalyani.KTManager.model.HistoricalDataResponse.HistoricalCandle;
import com.trading.kalyani.KTManager.repository.MarketStructureRepository;
import com.trading.kalyani.KTManager.service.InstrumentService;
import com.trading.kalyani.KTManager.service.MarketStructureService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;

/**
 * Implementation of MarketStructureService for SMC-based market structure analysis.
 */
@Service
public class MarketStructureServiceImpl implements MarketStructureService {

    private static final Logger logger = LoggerFactory.getLogger(MarketStructureServiceImpl.class);

    @Autowired
    private MarketStructureRepository marketStructureRepository;

    @Autowired
    private InstrumentService instrumentService;

    private static final int SWING_LOOKBACK = 3;
    private static final int DEFAULT_LOOKBACK_CANDLES = 100;

    // ==================== Core Analysis ====================

    @Override
    public MarketStructure analyzeMarketStructure(Long instrumentToken, String timeframe) {
        logger.info("Analyzing market structure for token: {}, timeframe: {}", instrumentToken, timeframe);

        try {
            List<HistoricalCandle> candles = fetchHistoricalCandles(instrumentToken, timeframe);
            if (candles == null || candles.size() < 20) {
                logger.warn("Insufficient candles for market structure analysis");
                return null;
            }
            return analyzeMarketStructure(instrumentToken, timeframe, candles);
        } catch (Exception e) {
            logger.error("Error analyzing market structure: {}", e.getMessage(), e);
            return null;
        }
    }

    @Override
    public MarketStructure analyzeMarketStructure(Long instrumentToken, String timeframe, List<HistoricalCandle> candles) {
        if (candles == null || candles.size() < 20) {
            return null;
        }

        MarketStructure structure = new MarketStructure();
        structure.setInstrumentToken(instrumentToken);
        structure.setInstrumentName(getInstrumentName(instrumentToken));
        structure.setTimeframe(timeframe);
        structure.setAnalysisTimestamp(LocalDateTime.now());
        structure.setCandlesAnalyzed(candles.size());

        Double currentPrice = candles.get(candles.size() - 1).getClose();
        structure.setCurrentPrice(currentPrice);

        // Identify swing points
        List<SwingPoint> swingHighs = identifySwingHighs(candles);
        List<SwingPoint> swingLows = identifySwingLows(candles);

        // Analyze trend
        analyzeTrend(structure, swingHighs, swingLows);

        // Detect CHoCH
        detectChangeOfCharacter(structure, candles, swingHighs, swingLows);

        // Calculate premium/discount zones
        calculatePremiumDiscountZones(structure, candles, currentPrice);

        // Determine market phase
        determineMarketPhase(structure, candles, swingHighs, swingLows);

        // Calculate overall bias
        calculateOverallBias(structure);

        // Save to database
        marketStructureRepository.save(structure);
        logger.info("Saved market structure analysis for {}: Trend={}, Phase={}, Bias={}",
                structure.getInstrumentName(), structure.getTrendDirection(),
                structure.getMarketPhase(), structure.getOverallBias());

        return structure;
    }

    @Override
    public MarketStructure getLatestAnalysis(Long instrumentToken, String timeframe) {
        return marketStructureRepository
                .findTopByInstrumentTokenAndTimeframeOrderByAnalysisTimestampDesc(instrumentToken, timeframe)
                .orElse(null);
    }

    @Override
    public Map<String, MarketStructure> analyzeAllIndices() {
        Map<String, MarketStructure> results = new HashMap<>();

        MarketStructure nifty = analyzeMarketStructure(NIFTY_INSTRUMENT_TOKEN, "5min");
        if (nifty != null) results.put("NIFTY", nifty);

        return results;
    }

    // ==================== Trend Analysis ====================

    private void analyzeTrend(MarketStructure structure, List<SwingPoint> swingHighs, List<SwingPoint> swingLows) {
        if (swingHighs.size() < 2 || swingLows.size() < 2) {
            structure.setTrendDirection("SIDEWAYS");
            structure.setTrendStrength("WEAK");
            return;
        }

        // Get last few swing points
        int hhCount = 0, hlCount = 0, lhCount = 0, llCount = 0;

        // Analyze swing highs (most recent first)
        for (int i = swingHighs.size() - 1; i > 0 && i > swingHighs.size() - 5; i--) {
            if (swingHighs.get(i).price > swingHighs.get(i - 1).price) {
                hhCount++;
            } else if (swingHighs.get(i).price < swingHighs.get(i - 1).price) {
                lhCount++;
            }
        }

        // Analyze swing lows (most recent first)
        for (int i = swingLows.size() - 1; i > 0 && i > swingLows.size() - 5; i--) {
            if (swingLows.get(i).price > swingLows.get(i - 1).price) {
                hlCount++;
            } else if (swingLows.get(i).price < swingLows.get(i - 1).price) {
                llCount++;
            }
        }

        structure.setConsecutiveHHCount(hhCount);
        structure.setConsecutiveHLCount(hlCount);
        structure.setConsecutiveLHCount(lhCount);
        structure.setConsecutiveLLCount(llCount);

        // Set swing point levels
        SwingPoint lastHigh = swingHighs.get(swingHighs.size() - 1);
        SwingPoint lastLow = swingLows.get(swingLows.size() - 1);
        structure.setLastSwingHigh(lastHigh.price);
        structure.setLastSwingHighTime(lastHigh.timestamp);
        structure.setLastSwingLow(lastLow.price);
        structure.setLastSwingLowTime(lastLow.timestamp);

        if (swingHighs.size() >= 2) {
            structure.setPrevSwingHigh(swingHighs.get(swingHighs.size() - 2).price);
        }
        if (swingLows.size() >= 2) {
            structure.setPrevSwingLow(swingLows.get(swingLows.size() - 2).price);
        }

        // Determine trend direction
        int bullishScore = hhCount + hlCount;
        int bearishScore = lhCount + llCount;

        if (bullishScore >= 3 && bullishScore > bearishScore * 1.5) {
            structure.setTrendDirection("UPTREND");
            structure.setTrendStrength(bullishScore >= 4 ? "STRONG" : "MODERATE");
        } else if (bearishScore >= 3 && bearishScore > bullishScore * 1.5) {
            structure.setTrendDirection("DOWNTREND");
            structure.setTrendStrength(bearishScore >= 4 ? "STRONG" : "MODERATE");
        } else {
            structure.setTrendDirection("SIDEWAYS");
            structure.setTrendStrength("WEAK");
        }
    }

    @Override
    public String getTrendDirection(Long instrumentToken, String timeframe) {
        MarketStructure structure = getLatestAnalysis(instrumentToken, timeframe);
        return structure != null ? structure.getTrendDirection() : "UNKNOWN";
    }

    @Override
    public String getTrendStrength(Long instrumentToken, String timeframe) {
        MarketStructure structure = getLatestAnalysis(instrumentToken, timeframe);
        return structure != null ? structure.getTrendStrength() : "UNKNOWN";
    }

    @Override
    public boolean isUptrend(Long instrumentToken, String timeframe) {
        return "UPTREND".equals(getTrendDirection(instrumentToken, timeframe));
    }

    @Override
    public boolean isDowntrend(Long instrumentToken, String timeframe) {
        return "DOWNTREND".equals(getTrendDirection(instrumentToken, timeframe));
    }

    @Override
    public Map<String, String> getMultiTimeframeTrend(Long instrumentToken, List<String> timeframes) {
        Map<String, String> trends = new LinkedHashMap<>();
        for (String tf : timeframes) {
            MarketStructure structure = analyzeMarketStructure(instrumentToken, tf);
            trends.put(tf, structure != null ? structure.getTrendDirection() : "UNKNOWN");
        }
        return trends;
    }

    // ==================== CHoCH Detection ====================

    private void detectChangeOfCharacter(MarketStructure structure, List<HistoricalCandle> candles,
                                         List<SwingPoint> swingHighs, List<SwingPoint> swingLows) {
        structure.setChochDetected(false);

        if (swingHighs.size() < 3 || swingLows.size() < 3) {
            return;
        }

        // Look for CHoCH in recent candles
        int lookbackStart = Math.max(0, candles.size() - 30);

        // Bullish CHoCH: After a series of LH/LL, price breaks above a swing high
        // Bearish CHoCH: After a series of HH/HL, price breaks below a swing low

        for (int i = candles.size() - 1; i >= lookbackStart; i--) {
            HistoricalCandle candle = candles.get(i);

            // Check for bullish CHoCH
            if (structure.getConsecutiveLHCount() >= 2 || structure.getConsecutiveLLCount() >= 2) {
                for (SwingPoint sh : swingHighs) {
                    if (sh.index < i && candle.getClose() > sh.price && candle.getOpen() < sh.price) {
                        structure.setChochDetected(true);
                        structure.setChochType("BULLISH_CHOCH");
                        structure.setChochLevel(sh.price);
                        structure.setChochTimestamp(parseTimestamp(candle.getTimestamp()));
                        return;
                    }
                }
            }

            // Check for bearish CHoCH
            if (structure.getConsecutiveHHCount() >= 2 || structure.getConsecutiveHLCount() >= 2) {
                for (SwingPoint sl : swingLows) {
                    if (sl.index < i && candle.getClose() < sl.price && candle.getOpen() > sl.price) {
                        structure.setChochDetected(true);
                        structure.setChochType("BEARISH_CHOCH");
                        structure.setChochLevel(sl.price);
                        structure.setChochTimestamp(parseTimestamp(candle.getTimestamp()));
                        return;
                    }
                }
            }
        }
    }

    @Override
    public boolean hasChochOccurred(Long instrumentToken, String timeframe) {
        MarketStructure structure = getLatestAnalysis(instrumentToken, timeframe);
        return structure != null && Boolean.TRUE.equals(structure.getChochDetected());
    }

    @Override
    public Map<String, Object> getLatestChoch(Long instrumentToken, String timeframe) {
        MarketStructure structure = getLatestAnalysis(instrumentToken, timeframe);
        Map<String, Object> result = new HashMap<>();

        if (structure != null && Boolean.TRUE.equals(structure.getChochDetected())) {
            result.put("detected", true);
            result.put("type", structure.getChochType());
            result.put("level", structure.getChochLevel());
            result.put("timestamp", structure.getChochTimestamp());
        } else {
            result.put("detected", false);
        }

        return result;
    }

    @Override
    public List<Map<String, Object>> getChochHistory(Long instrumentToken, String timeframe, int limit) {
        return marketStructureRepository
                .findByInstrumentTokenAndTimeframeAndChochDetectedTrueOrderByChochTimestampDesc(
                        instrumentToken, timeframe)
                .stream()
                .limit(limit)
                .map(s -> {
                    Map<String, Object> choch = new HashMap<>();
                    choch.put("type", s.getChochType());
                    choch.put("level", s.getChochLevel());
                    choch.put("timestamp", s.getChochTimestamp());
                    return choch;
                })
                .toList();
    }

    // ==================== Premium/Discount Zones ====================

    private void calculatePremiumDiscountZones(MarketStructure structure, List<HistoricalCandle> candles, Double currentPrice) {
        // Use recent range (last 50 candles or available)
        int rangeStart = Math.max(0, candles.size() - 50);

        double rangeHigh = Double.MIN_VALUE;
        double rangeLow = Double.MAX_VALUE;

        for (int i = rangeStart; i < candles.size(); i++) {
            rangeHigh = Math.max(rangeHigh, candles.get(i).getHigh());
            rangeLow = Math.min(rangeLow, candles.get(i).getLow());
        }

        structure.setRangeHigh(rangeHigh);
        structure.setRangeLow(rangeLow);

        double equilibrium = (rangeHigh + rangeLow) / 2;
        structure.setEquilibriumLevel(equilibrium);

        // Calculate position within range (0-100%)
        double range = rangeHigh - rangeLow;
        double positionPercent = range > 0 ? ((currentPrice - rangeLow) / range) * 100 : 50;
        structure.setPricePositionPercent(positionPercent);

        // Determine zone
        if (positionPercent >= 70) {
            structure.setPriceZone("PREMIUM");
        } else if (positionPercent <= 30) {
            structure.setPriceZone("DISCOUNT");
        } else {
            structure.setPriceZone("EQUILIBRIUM");
        }
    }

    @Override
    public String getPriceZone(Long instrumentToken, String timeframe) {
        MarketStructure structure = getLatestAnalysis(instrumentToken, timeframe);
        return structure != null ? structure.getPriceZone() : "UNKNOWN";
    }

    @Override
    public Map<String, Double> getZoneLevels(Long instrumentToken, String timeframe) {
        MarketStructure structure = getLatestAnalysis(instrumentToken, timeframe);
        Map<String, Double> levels = new HashMap<>();

        if (structure != null) {
            levels.put("rangeHigh", structure.getRangeHigh());
            levels.put("rangeLow", structure.getRangeLow());
            levels.put("equilibrium", structure.getEquilibriumLevel());
            levels.put("premiumStart", structure.getEquilibriumLevel() +
                    (structure.getRangeHigh() - structure.getEquilibriumLevel()) * 0.4);
            levels.put("discountEnd", structure.getEquilibriumLevel() -
                    (structure.getEquilibriumLevel() - structure.getRangeLow()) * 0.4);
        }

        return levels;
    }

    @Override
    public boolean isInDiscountZone(Long instrumentToken, String timeframe) {
        return "DISCOUNT".equals(getPriceZone(instrumentToken, timeframe));
    }

    @Override
    public boolean isInPremiumZone(Long instrumentToken, String timeframe) {
        return "PREMIUM".equals(getPriceZone(instrumentToken, timeframe));
    }

    // ==================== Market Phase ====================

    private void determineMarketPhase(MarketStructure structure, List<HistoricalCandle> candles,
                                      List<SwingPoint> swingHighs, List<SwingPoint> swingLows) {
        String trend = structure.getTrendDirection();
        String priceZone = structure.getPriceZone();

        // Calculate volume behavior
        long recentAvgVolume = calculateRecentAverageVolume(candles, 20);
        long olderAvgVolume = calculateOlderAverageVolume(candles, 20, 40);

        String volumeBehavior = "NEUTRAL";
        if (recentAvgVolume > olderAvgVolume * 1.2) {
            volumeBehavior = "INCREASING";
        } else if (recentAvgVolume < olderAvgVolume * 0.8) {
            volumeBehavior = "DECREASING";
        }
        structure.setVolumeBehavior(volumeBehavior);

        // Determine phase
        double confidence = 60.0;

        if ("UPTREND".equals(trend)) {
            if ("INCREASING".equals(volumeBehavior)) {
                structure.setMarketPhase("MARKUP");
                confidence = 75.0;
            } else if ("DECREASING".equals(volumeBehavior)) {
                structure.setMarketPhase("DISTRIBUTION");
                confidence = 65.0;
            } else {
                structure.setMarketPhase("MARKUP");
                confidence = 60.0;
            }
        } else if ("DOWNTREND".equals(trend)) {
            if ("INCREASING".equals(volumeBehavior)) {
                structure.setMarketPhase("MARKDOWN");
                confidence = 75.0;
            } else if ("DECREASING".equals(volumeBehavior)) {
                structure.setMarketPhase("ACCUMULATION");
                confidence = 65.0;
            } else {
                structure.setMarketPhase("MARKDOWN");
                confidence = 60.0;
            }
        } else {
            // Sideways
            if ("DISCOUNT".equals(priceZone) && "DECREASING".equals(volumeBehavior)) {
                structure.setMarketPhase("ACCUMULATION");
                confidence = 70.0;
            } else if ("PREMIUM".equals(priceZone) && "DECREASING".equals(volumeBehavior)) {
                structure.setMarketPhase("DISTRIBUTION");
                confidence = 70.0;
            } else {
                structure.setMarketPhase("RANGING");
                confidence = 55.0;
            }
        }

        structure.setPhaseConfidence(confidence);
    }

    @Override
    public String getMarketPhase(Long instrumentToken, String timeframe) {
        MarketStructure structure = getLatestAnalysis(instrumentToken, timeframe);
        return structure != null ? structure.getMarketPhase() : "UNKNOWN";
    }

    @Override
    public Map<String, Object> getMarketPhaseDetails(Long instrumentToken, String timeframe) {
        MarketStructure structure = getLatestAnalysis(instrumentToken, timeframe);
        Map<String, Object> details = new HashMap<>();

        if (structure != null) {
            details.put("phase", structure.getMarketPhase());
            details.put("confidence", structure.getPhaseConfidence());
            details.put("volumeBehavior", structure.getVolumeBehavior());
            details.put("trend", structure.getTrendDirection());
            details.put("priceZone", structure.getPriceZone());
        }

        return details;
    }

    @Override
    public boolean isAccumulationPhase(Long instrumentToken, String timeframe) {
        return "ACCUMULATION".equals(getMarketPhase(instrumentToken, timeframe));
    }

    @Override
    public boolean isDistributionPhase(Long instrumentToken, String timeframe) {
        return "DISTRIBUTION".equals(getMarketPhase(instrumentToken, timeframe));
    }

    // ==================== Trade Bias ====================

    private void calculateOverallBias(MarketStructure structure) {
        int bullishScore = 0;
        int bearishScore = 0;

        // Trend contribution
        if ("UPTREND".equals(structure.getTrendDirection())) {
            bullishScore += "STRONG".equals(structure.getTrendStrength()) ? 30 : 20;
        } else if ("DOWNTREND".equals(structure.getTrendDirection())) {
            bearishScore += "STRONG".equals(structure.getTrendStrength()) ? 30 : 20;
        }

        // Price zone contribution
        if ("DISCOUNT".equals(structure.getPriceZone())) {
            bullishScore += 15;
        } else if ("PREMIUM".equals(structure.getPriceZone())) {
            bearishScore += 15;
        }

        // Market phase contribution
        if ("ACCUMULATION".equals(structure.getMarketPhase()) || "MARKUP".equals(structure.getMarketPhase())) {
            bullishScore += 20;
        } else if ("DISTRIBUTION".equals(structure.getMarketPhase()) || "MARKDOWN".equals(structure.getMarketPhase())) {
            bearishScore += 20;
        }

        // CHoCH contribution
        if (Boolean.TRUE.equals(structure.getChochDetected())) {
            if ("BULLISH_CHOCH".equals(structure.getChochType())) {
                bullishScore += 25;
            } else if ("BEARISH_CHOCH".equals(structure.getChochType())) {
                bearishScore += 25;
            }
        }

        // Determine overall bias
        if (bullishScore > bearishScore * 1.3) {
            structure.setOverallBias("BULLISH");
            structure.setOrderFlowDirection("BULLISH");
        } else if (bearishScore > bullishScore * 1.3) {
            structure.setOverallBias("BEARISH");
            structure.setOrderFlowDirection("BEARISH");
        } else {
            structure.setOverallBias("NEUTRAL");
            structure.setOrderFlowDirection("NEUTRAL");
        }

        // Calculate confidence
        int totalScore = bullishScore + bearishScore;
        double confidence = totalScore > 0 ?
                (Math.abs(bullishScore - bearishScore) / (double) totalScore) * 100 : 50;
        structure.setAnalysisConfidence(Math.min(confidence + 40, 95));
        structure.setOrderFlowStrength(confidence);
    }

    @Override
    public String getOverallBias(Long instrumentToken, String timeframe) {
        MarketStructure structure = getLatestAnalysis(instrumentToken, timeframe);
        return structure != null ? structure.getOverallBias() : "UNKNOWN";
    }

    @Override
    public Map<String, Object> getBiasWithConfidence(Long instrumentToken, String timeframe) {
        MarketStructure structure = getLatestAnalysis(instrumentToken, timeframe);
        Map<String, Object> result = new HashMap<>();

        if (structure != null) {
            result.put("bias", structure.getOverallBias());
            result.put("confidence", structure.getAnalysisConfidence());
            result.put("orderFlowDirection", structure.getOrderFlowDirection());
            result.put("orderFlowStrength", structure.getOrderFlowStrength());
        }

        return result;
    }

    @Override
    public boolean isTradeAlignedWithStructure(Long instrumentToken, String timeframe, String tradeDirection) {
        String bias = getOverallBias(instrumentToken, timeframe);

        if ("LONG".equalsIgnoreCase(tradeDirection)) {
            return "BULLISH".equals(bias) || "NEUTRAL".equals(bias);
        } else if ("SHORT".equalsIgnoreCase(tradeDirection)) {
            return "BEARISH".equals(bias) || "NEUTRAL".equals(bias);
        }

        return false;
    }

    // ==================== Swing Points ====================

    @Override
    public List<Map<String, Object>> getSwingHighs(Long instrumentToken, String timeframe, int limit) {
        MarketStructure structure = getLatestAnalysis(instrumentToken, timeframe);
        List<Map<String, Object>> result = new ArrayList<>();

        if (structure != null) {
            if (structure.getLastSwingHigh() != null) {
                Map<String, Object> sh = new HashMap<>();
                sh.put("price", structure.getLastSwingHigh());
                sh.put("timestamp", structure.getLastSwingHighTime());
                sh.put("type", "SWING_HIGH");
                result.add(sh);
            }
            if (structure.getPrevSwingHigh() != null) {
                Map<String, Object> sh = new HashMap<>();
                sh.put("price", structure.getPrevSwingHigh());
                sh.put("type", "PREV_SWING_HIGH");
                result.add(sh);
            }
        }

        return result;
    }

    @Override
    public List<Map<String, Object>> getSwingLows(Long instrumentToken, String timeframe, int limit) {
        MarketStructure structure = getLatestAnalysis(instrumentToken, timeframe);
        List<Map<String, Object>> result = new ArrayList<>();

        if (structure != null) {
            if (structure.getLastSwingLow() != null) {
                Map<String, Object> sl = new HashMap<>();
                sl.put("price", structure.getLastSwingLow());
                sl.put("timestamp", structure.getLastSwingLowTime());
                sl.put("type", "SWING_LOW");
                result.add(sl);
            }
            if (structure.getPrevSwingLow() != null) {
                Map<String, Object> sl = new HashMap<>();
                sl.put("price", structure.getPrevSwingLow());
                sl.put("type", "PREV_SWING_LOW");
                result.add(sl);
            }
        }

        return result;
    }

    @Override
    public Map<String, List<Double>> getKeyLevels(Long instrumentToken, String timeframe) {
        MarketStructure structure = getLatestAnalysis(instrumentToken, timeframe);
        Map<String, List<Double>> levels = new HashMap<>();

        List<Double> resistances = new ArrayList<>();
        List<Double> supports = new ArrayList<>();

        if (structure != null) {
            if (structure.getLastSwingHigh() != null) resistances.add(structure.getLastSwingHigh());
            if (structure.getPrevSwingHigh() != null) resistances.add(structure.getPrevSwingHigh());
            if (structure.getRangeHigh() != null) resistances.add(structure.getRangeHigh());

            if (structure.getLastSwingLow() != null) supports.add(structure.getLastSwingLow());
            if (structure.getPrevSwingLow() != null) supports.add(structure.getPrevSwingLow());
            if (structure.getRangeLow() != null) supports.add(structure.getRangeLow());
        }

        levels.put("resistances", resistances);
        levels.put("supports", supports);
        return levels;
    }

    // ==================== Dashboard ====================

    @Override
    public Map<String, Object> getDashboard(Long instrumentToken) {
        Map<String, Object> dashboard = new HashMap<>();

        MarketStructure structure5m = analyzeMarketStructure(instrumentToken, "5min");
        MarketStructure structure15m = analyzeMarketStructure(instrumentToken, "15min");

        dashboard.put("instrumentToken", instrumentToken);
        dashboard.put("instrumentName", getInstrumentName(instrumentToken));
        dashboard.put("analysisTime", LocalDateTime.now());

        if (structure5m != null) {
            dashboard.put("5min", structureToMap(structure5m));
        }
        if (structure15m != null) {
            dashboard.put("15min", structureToMap(structure15m));
        }

        // Overall summary
        Map<String, Object> summary = new HashMap<>();
        if (structure5m != null) {
            summary.put("trend", structure5m.getTrendDirection());
            summary.put("bias", structure5m.getOverallBias());
            summary.put("phase", structure5m.getMarketPhase());
            summary.put("priceZone", structure5m.getPriceZone());
            summary.put("chochDetected", structure5m.getChochDetected());
        }
        dashboard.put("summary", summary);

        return dashboard;
    }

    @Override
    public Map<String, Map<String, Object>> getMultiInstrumentSummary(List<Long> instrumentTokens) {
        Map<String, Map<String, Object>> summaries = new HashMap<>();

        for (Long token : instrumentTokens) {
            summaries.put(getInstrumentName(token), getDashboard(token));
        }

        return summaries;
    }

    // ==================== Helper Methods ====================

    private List<SwingPoint> identifySwingHighs(List<HistoricalCandle> candles) {
        List<SwingPoint> swingHighs = new ArrayList<>();

        for (int i = SWING_LOOKBACK; i < candles.size() - SWING_LOOKBACK; i++) {
            double currentHigh = candles.get(i).getHigh();
            boolean isSwingHigh = true;

            for (int j = i - SWING_LOOKBACK; j <= i + SWING_LOOKBACK; j++) {
                if (j != i && candles.get(j).getHigh() >= currentHigh) {
                    isSwingHigh = false;
                    break;
                }
            }

            if (isSwingHigh) {
                swingHighs.add(new SwingPoint(i, currentHigh, parseTimestamp(candles.get(i).getTimestamp())));
            }
        }

        return swingHighs;
    }

    private List<SwingPoint> identifySwingLows(List<HistoricalCandle> candles) {
        List<SwingPoint> swingLows = new ArrayList<>();

        for (int i = SWING_LOOKBACK; i < candles.size() - SWING_LOOKBACK; i++) {
            double currentLow = candles.get(i).getLow();
            boolean isSwingLow = true;

            for (int j = i - SWING_LOOKBACK; j <= i + SWING_LOOKBACK; j++) {
                if (j != i && candles.get(j).getLow() <= currentLow) {
                    isSwingLow = false;
                    break;
                }
            }

            if (isSwingLow) {
                swingLows.add(new SwingPoint(i, currentLow, parseTimestamp(candles.get(i).getTimestamp())));
            }
        }

        return swingLows;
    }

    private long calculateRecentAverageVolume(List<HistoricalCandle> candles, int period) {
        int start = Math.max(0, candles.size() - period);
        long totalVolume = 0;
        int count = 0;

        for (int i = start; i < candles.size(); i++) {
            Long vol = candles.get(i).getVolume();
            if (vol != null) {
                totalVolume += vol;
                count++;
            }
        }

        return count > 0 ? totalVolume / count : 0;
    }

    private long calculateOlderAverageVolume(List<HistoricalCandle> candles, int recentPeriod, int totalPeriod) {
        int end = Math.max(0, candles.size() - recentPeriod);
        int start = Math.max(0, end - (totalPeriod - recentPeriod));
        long totalVolume = 0;
        int count = 0;

        for (int i = start; i < end; i++) {
            Long vol = candles.get(i).getVolume();
            if (vol != null) {
                totalVolume += vol;
                count++;
            }
        }

        return count > 0 ? totalVolume / count : 0;
    }

    private List<HistoricalCandle> fetchHistoricalCandles(Long instrumentToken, String timeframe) {
        try {
            LocalDate today = LocalDate.now();
            LocalDateTime fromDate = today.minusDays(5).atStartOfDay();
            LocalDateTime toDate = today.atTime(23, 59, 59);

            HistoricalDataRequest request = new HistoricalDataRequest();
            request.setInstrumentToken(String.valueOf(instrumentToken));
            request.setInterval(mapTimeframeToKiteInterval(timeframe));
            request.setFromDate(fromDate);
            request.setToDate(toDate);

            HistoricalDataResponse response = instrumentService.getHistoricalData(request);
            return response != null && response.isSuccess() ? response.getCandles() : Collections.emptyList();
        } catch (Exception e) {
            logger.error("Error fetching candles: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    private String mapTimeframeToKiteInterval(String timeframe) {
        return switch (timeframe) {
            case "5min"  -> "5minute";
            case "15min" -> "15minute";
            case "1hour" -> "60minute";
            default -> {
                logger.warn("Unknown timeframe '{}', defaulting to 5minute", timeframe);
                yield "5minute";
            }
        };
    }

    private String getInstrumentName(Long token) {
        if (NIFTY_INSTRUMENT_TOKEN.equals(token)) return "NIFTY";
        return "UNKNOWN";
    }

    private static final DateTimeFormatter KITE_TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ");

    private LocalDateTime parseTimestamp(String timestamp) {
        if (timestamp == null || timestamp.isBlank()) return null;
        // Try Kite format first: 2026-03-20T14:45:00+0530 (offset without colon)
        try {
            return java.time.ZonedDateTime.parse(timestamp, KITE_TIMESTAMP_FORMATTER).toLocalDateTime();
        } catch (Exception e) {
            // Fallback: ISO with colon offset (+05:30) or UTC (Z)
            try {
                return java.time.ZonedDateTime.parse(timestamp).toLocalDateTime();
            } catch (Exception e2) {
                try {
                    return LocalDateTime.parse(timestamp.replace("Z", ""), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                } catch (Exception e3) {
                    logger.warn("Failed to parse timestamp '{}': {}", timestamp, e3.getMessage());
                    return null;
                }
            }
        }
    }

    private Map<String, Object> structureToMap(MarketStructure s) {
        Map<String, Object> map = new HashMap<>();
        map.put("trend", s.getTrendDirection());
        map.put("trendStrength", s.getTrendStrength());
        map.put("bias", s.getOverallBias());
        map.put("confidence", s.getAnalysisConfidence());
        map.put("phase", s.getMarketPhase());
        map.put("phaseConfidence", s.getPhaseConfidence());
        map.put("priceZone", s.getPriceZone());
        map.put("pricePositionPercent", s.getPricePositionPercent());
        map.put("chochDetected", s.getChochDetected());
        map.put("chochType", s.getChochType());
        map.put("lastSwingHigh", s.getLastSwingHigh());
        map.put("lastSwingLow", s.getLastSwingLow());
        map.put("rangeHigh", s.getRangeHigh());
        map.put("rangeLow", s.getRangeLow());
        map.put("equilibrium", s.getEquilibriumLevel());
        return map;
    }

    /**
     * Inner class to represent swing points
     */
    private static class SwingPoint {
        int index;
        double price;
        LocalDateTime timestamp;

        SwingPoint(int index, double price, LocalDateTime timestamp) {
            this.index = index;
            this.price = price;
            this.timestamp = timestamp;
        }
    }
}
