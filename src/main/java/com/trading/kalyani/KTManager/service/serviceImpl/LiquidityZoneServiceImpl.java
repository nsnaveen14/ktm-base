package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.entity.LiquidityZoneAnalysis;
import com.trading.kalyani.KTManager.model.HistoricalDataRequest;
import com.trading.kalyani.KTManager.model.HistoricalDataResponse;
import com.trading.kalyani.KTManager.model.HistoricalDataResponse.HistoricalCandle;
import com.trading.kalyani.KTManager.model.PreviousDayHighLowResponse;
import com.trading.kalyani.KTManager.repository.LiquidityZoneAnalysisRepository;
import com.trading.kalyani.KTManager.service.InstrumentService;
import com.trading.kalyani.KTManager.service.LiquidityZoneService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;

/**
 * Implementation of LiquidityZoneService for analyzing liquidity zones
 * and detecting stop loss clusters for trading opportunities.
 */
@Service
public class LiquidityZoneServiceImpl implements LiquidityZoneService {

    private static final Logger logger = LoggerFactory.getLogger(LiquidityZoneServiceImpl.class);

    private static final DateTimeFormatter ZONE_DATETIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ");

    @Autowired
    private LiquidityZoneAnalysisRepository liquidityZoneRepository;

    @Autowired
    private InstrumentService instrumentService;


    // Timeframe intervals in minutes
    private static final Map<String, Integer> TIMEFRAME_MINUTES = Map.of(
            "5min", 5,
            "15min", 15,
            "1hour", 60
    );

    private static final int LOOKBACK_CANDLES = 50; // Number of candles to analyze

    @Override
    public LiquidityZoneAnalysis analyzeLiquidityZones(Long instrumentToken, String timeframe) {
        logger.info("Analyzing liquidity zones for token: {}, timeframe: {}", instrumentToken, timeframe);

        try {
            LiquidityZoneAnalysis analysis = LiquidityZoneAnalysis.builder()
                    .instrumentToken(instrumentToken)
                    .instrumentName(getInstrumentName(instrumentToken))
                    .timeframe(timeframe)
                    .analysisTimestamp(LocalDateTime.now())
                    .build();

            // ALWAYS fetch previous day high/low first (works even when market is closed)
            PreviousDayHighLowResponse prevDay = instrumentService.getPreviousDayHighLow(String.valueOf(instrumentToken));
            if (prevDay != null && prevDay.isSuccess()) {
                analysis.setPreviousDayHigh(prevDay.getHigh());
                analysis.setPreviousDayLow(prevDay.getLow());
                logger.info("Previous day data fetched for token {}: High={}, Low={}",
                        instrumentToken, prevDay.getHigh(), prevDay.getLow());
            } else {
                logger.warn("Could not fetch previous day data for token: {}", instrumentToken);
            }

            // ALWAYS fetch day before yesterday high/low (works even when market is closed)
            setDayBeforeYesterdayHighLow(analysis, instrumentToken);

            // Get historical candles from Kite API for intraday analysis
            List<HistoricalCandle> kiteCandles = getHistoricalCandlesFromKite(instrumentToken, timeframe);
            if (kiteCandles == null || kiteCandles.isEmpty()) {
                logger.warn("No intraday candles available from Kite API for token: {}, timeframe: {}", instrumentToken, timeframe);
                analysis.setIsValidSetup(false);
                analysis.setRejectionReason("No intraday historical data available from Kite API");
                // Still save the analysis with previous day data
                return liquidityZoneRepository.save(analysis);
            }

            // Get current price from last candle
            Double currentPrice = kiteCandles.get(kiteCandles.size() - 1).getClose();
            logger.info("Using last candle close price as current price: {} for token: {}", currentPrice, instrumentToken);
            analysis.setCurrentPrice(currentPrice);


            // Get current day high/low from today's candles
            setCurrentDayHighLow(analysis, kiteCandles);

            // Identify swing highs and lows
            identifySwingPoints(analysis, kiteCandles);

            // Identify liquidity zones (stop loss clusters)
            identifyLiquidityZones(analysis, kiteCandles, currentPrice);

            // Detect liquidity grabs
            detectLiquidityGrabs(analysis, kiteCandles, currentPrice);

            // Determine market structure
            analyzeMarketStructure(analysis, kiteCandles, currentPrice);

            // Generate trade setup
            generateTradeSetup(analysis, currentPrice);

            // Save analysis
            return liquidityZoneRepository.save(analysis);

        } catch (Exception e) {
            logger.error("Error analyzing liquidity zones: {}", e.getMessage(), e);
            return null;
        }
    }

    @Override
    public LiquidityZoneAnalysis getLatestAnalysis(Long instrumentToken, String timeframe) {
        return liquidityZoneRepository
                .findFirstByInstrumentTokenAndTimeframeOrderByAnalysisTimestampDesc(instrumentToken, timeframe)
                .orElse(null);
    }

    @Override
    public List<LiquidityZoneAnalysis> getTodaysAnalyses(Long instrumentToken) {
        LocalDateTime startOfDay = LocalDate.now().atStartOfDay();
        return liquidityZoneRepository.findTodaysAnalyses(instrumentToken, startOfDay);
    }

    @Override
    public List<LiquidityZoneAnalysis> getTodaysAnalysesByTimeframe(Long instrumentToken, String timeframe) {
        LocalDateTime startOfDay = LocalDate.now().atStartOfDay();
        return liquidityZoneRepository.findTodaysAnalysesByTimeframe(instrumentToken, timeframe, startOfDay);
    }

    @Override
    public Map<String, Object> getDashboardData(List<Long> instrumentTokens, List<String> timeframes) {
        Map<String, Object> dashboard = new HashMap<>();
        LocalDateTime startOfDay = LocalDate.now().atStartOfDay();

        List<Map<String, Object>> allAnalyses = new ArrayList<>();

        for (Long token : instrumentTokens) {
            for (String timeframe : timeframes) {
                LiquidityZoneAnalysis analysis = getLatestAnalysis(token, timeframe);
                if (analysis != null) {
                    Map<String, Object> analysisMap = convertToMap(analysis);
                    allAnalyses.add(analysisMap);
                }
            }
        }

        dashboard.put("analyses", allAnalyses);
        dashboard.put("totalAnalyses", allAnalyses.size());
        dashboard.put("validSetups", allAnalyses.stream()
                .filter(a -> Boolean.TRUE.equals(a.get("isValidSetup"))).count());
        dashboard.put("liquidityGrabs", allAnalyses.stream()
                .filter(a -> Boolean.TRUE.equals(a.get("buySideGrabbed")) ||
                             Boolean.TRUE.equals(a.get("sellSideGrabbed"))).count());

        return dashboard;
    }

    @Override
    public Map<String, Object> getMultiTimeframeAnalysis(Long instrumentToken) {
        Map<String, Object> result = new HashMap<>();
        result.put("instrumentToken", instrumentToken);
        result.put("instrumentName", getInstrumentName(instrumentToken));

        List<String> timeframes = List.of("5min", "15min", "1hour");
        Map<String, Object> timeframeData = new HashMap<>();

        for (String timeframe : timeframes) {
            LiquidityZoneAnalysis analysis = getLatestAnalysis(instrumentToken, timeframe);

            // If no analysis exists, run fresh analysis
            if (analysis == null) {
                logger.info("No existing analysis found for token: {}, timeframe: {}. Running fresh analysis.",
                        instrumentToken, timeframe);
                analysis = analyzeLiquidityZones(instrumentToken, timeframe);
            }

            if (analysis != null) {
                timeframeData.put(timeframe, convertToMap(analysis));
            }
        }

        result.put("timeframes", timeframeData);
        result.put("timestamp", LocalDateTime.now());

        return result;
    }

    @Override
    public Map<String, Object> getActiveSetups(List<Long> instrumentTokens) {
        Map<String, Object> result = new HashMap<>();
        LocalDateTime startOfDay = LocalDate.now().atStartOfDay();

        List<Map<String, Object>> activeSetups = new ArrayList<>();

        for (Long token : instrumentTokens) {
            List<LiquidityZoneAnalysis> validSetups = liquidityZoneRepository
                    .findValidSetupsToday(token, startOfDay);

            for (LiquidityZoneAnalysis setup : validSetups) {
                activeSetups.add(convertToMap(setup));
            }
        }

        result.put("activeSetups", activeSetups);
        result.put("count", activeSetups.size());
        result.put("timestamp", LocalDateTime.now());

        return result;
    }

    @Override
    public Map<String, Object> getChartData(Long instrumentToken, String timeframe) {
        Map<String, Object> chartData = new HashMap<>();

        LiquidityZoneAnalysis analysis = getLatestAnalysis(instrumentToken, timeframe);
        if (analysis == null) {
            chartData.put("error", "No analysis available");
            return chartData;
        }

        // Liquidity zones for charting
        List<Map<String, Object>> buySideZones = new ArrayList<>();
        List<Map<String, Object>> sellSideZones = new ArrayList<>();

        if (analysis.getBuySideLiquidity1() != null) {
            buySideZones.add(Map.of("level", analysis.getBuySideLiquidity1(), "strength", 3));
        }
        if (analysis.getBuySideLiquidity2() != null) {
            buySideZones.add(Map.of("level", analysis.getBuySideLiquidity2(), "strength", 2));
        }
        if (analysis.getBuySideLiquidity3() != null) {
            buySideZones.add(Map.of("level", analysis.getBuySideLiquidity3(), "strength", 1));
        }

        if (analysis.getSellSideLiquidity1() != null) {
            sellSideZones.add(Map.of("level", analysis.getSellSideLiquidity1(), "strength", 3));
        }
        if (analysis.getSellSideLiquidity2() != null) {
            sellSideZones.add(Map.of("level", analysis.getSellSideLiquidity2(), "strength", 2));
        }
        if (analysis.getSellSideLiquidity3() != null) {
            sellSideZones.add(Map.of("level", analysis.getSellSideLiquidity3(), "strength", 1));
        }

        chartData.put("buySideZones", buySideZones);
        chartData.put("sellSideZones", sellSideZones);
        chartData.put("currentPrice", analysis.getCurrentPrice());
        chartData.put("previousDayHigh", analysis.getPreviousDayHigh());
        chartData.put("previousDayLow", analysis.getPreviousDayLow());
        chartData.put("currentDayHigh", analysis.getCurrentDayHigh());
        chartData.put("currentDayLow", analysis.getCurrentDayLow());

        return chartData;
    }

    @Override
    public Map<String, Object> analyzeAllIndices() {
        Map<String, Object> result = new HashMap<>();

        List<Long> tokens = List.of(NIFTY_INSTRUMENT_TOKEN);
        List<String> timeframes = List.of("5min", "15min", "1hour");

        Map<String, Object> analyses = new HashMap<>();

        for (Long token : tokens) {
            String instrumentName = getInstrumentName(token);
            Map<String, Object> instrumentAnalyses = new HashMap<>();

            for (String timeframe : timeframes) {
                LiquidityZoneAnalysis analysis = analyzeLiquidityZones(token, timeframe);
                if (analysis != null) {
                    instrumentAnalyses.put(timeframe, convertToMap(analysis));
                }
            }

            analyses.put(instrumentName, instrumentAnalyses);
        }

        result.put("analyses", analyses);
        result.put("timestamp", LocalDateTime.now());

        return result;
    }

    // Helper methods


    private String getInstrumentName(Long instrumentToken) {
        if (instrumentToken.equals(NIFTY_INSTRUMENT_TOKEN)) {
            return "NIFTY";
        }
        return "UNKNOWN";
    }

    /**
     * Fetch historical candles from Kite API for the given timeframe
     */
    private List<HistoricalCandle> getHistoricalCandlesFromKite(Long instrumentToken, String timeframe) {
        try {
            // Map timeframe to Kite API interval
            String kiteInterval = mapTimeframeToKiteInterval(timeframe);

            // Calculate from date based on timeframe
            LocalDateTime toDate = LocalDateTime.now();
            LocalDateTime fromDate;

            switch (timeframe) {
                case "5min":
                    fromDate = toDate.minusDays(5); // 5 days for 5-minute candles
                    break;
                case "15min":
                    fromDate = toDate.minusDays(7); // 7 days for 15-minute candles
                    break;
                case "1hour":
                    fromDate = toDate.minusDays(15); // 15 days for 1-hour candles
                    break;
                default:
                    fromDate = toDate.minusDays(5);
            }

            logger.info("Fetching historical data from Kite API for token: {}, interval: {}, from: {}, to: {}",
                    instrumentToken, kiteInterval, fromDate, toDate);

            HistoricalDataRequest request = HistoricalDataRequest.builder()
                    .instrumentToken(String.valueOf(instrumentToken))
                    .fromDate(fromDate)
                    .toDate(toDate)
                    .interval(kiteInterval)
                    .continuous(false)
                    .oi(false)
                    .build();

            HistoricalDataResponse response = instrumentService.getHistoricalData(request);

            if (response != null && response.isSuccess() && response.getCandles() != null) {
                logger.info("Retrieved {} candles from Kite API for token: {}",
                        response.getCandles().size(), instrumentToken);

                // Limit to most recent LOOKBACK_CANDLES
                List<HistoricalCandle> candles = response.getCandles();
                if (candles.size() > LOOKBACK_CANDLES) {
                    candles = candles.subList(candles.size() - LOOKBACK_CANDLES, candles.size());
                }

                return candles;
            } else {
                logger.warn("Failed to fetch historical data from Kite API: {}",
                        response != null ? response.getMessage() : "null response");
                return new ArrayList<>();
            }
        } catch (Exception e) {
            logger.error("Error fetching historical candles from Kite API: {}", e.getMessage(), e);
            return new ArrayList<>();
        }
    }

    /**
     * Map internal timeframe to Kite API interval
     */
    private String mapTimeframeToKiteInterval(String timeframe) {
        switch (timeframe) {
            case "5min":
                return "5minute";
            case "15min":
                return "15minute";
            case "1hour":
                return "60minute";
            default:
                return "5minute";
        }
    }


    private void setCurrentDayHighLow(LiquidityZoneAnalysis analysis, List<HistoricalCandle> candles) {
        LocalDateTime startOfDay = LocalDate.now().atTime(9, 15);

        // Filter candles for today only
        List<HistoricalCandle> todaysCandles = candles.stream()
                .filter(candle -> {
                    try {
                        String timestamp = candle.getTimestamp();
                        LocalDateTime candleTime;

                        // Try parsing with timezone format first (e.g., 2026-01-16T09:15:00+0530)
                        try {
                            ZonedDateTime zdt = ZonedDateTime.parse(timestamp, ZONE_DATETIME_FORMATTER);
                            candleTime = zdt.toLocalDateTime();
                        } catch (Exception e1) {
                            // Fallback to simple ISO format
                            candleTime = LocalDateTime.parse(timestamp.replace("Z", "")
                                    .replace("T", " ").replace(" ", "T"));
                        }

                        return candleTime.toLocalDate().equals(LocalDate.now()) &&
                               (candleTime.isAfter(startOfDay) || candleTime.equals(startOfDay));
                    } catch (Exception e) {
                        logger.debug("Failed to parse timestamp: {} - {}", candle.getTimestamp(), e.getMessage());
                        return false;
                    }
                })
                .toList();

        if (!todaysCandles.isEmpty()) {
            double dayHigh = todaysCandles.stream()
                    .mapToDouble(HistoricalCandle::getHigh)
                    .max()
                    .orElse(0.0);
            double dayLow = todaysCandles.stream()
                    .mapToDouble(HistoricalCandle::getLow)
                    .min()
                    .orElse(0.0);

            analysis.setCurrentDayHigh(dayHigh);
            analysis.setCurrentDayLow(dayLow);

            logger.debug("Current day high: {}, low: {} for token: {}", dayHigh, dayLow, analysis.getInstrumentToken());
        } else {
            logger.debug("No candles found for current day for token: {}", analysis.getInstrumentToken());
        }
    }

    /**
     * Fetch day before yesterday's high and low using historical daily candle data.
     * This method handles market holidays by fetching data for last 15 days and selecting
     * the second to last trading day.
     */
    private void setDayBeforeYesterdayHighLow(LiquidityZoneAnalysis analysis, Long instrumentToken) {
        try {
            // Get daily candles for the past 15 days to ensure we capture enough trading days
            // even with extended holidays
            LocalDateTime toDate = LocalDateTime.now().minusDays(1).withHour(23).withMinute(59);
            LocalDateTime fromDate = toDate.minusDays(15);

            HistoricalDataRequest request = HistoricalDataRequest.builder()
                    .instrumentToken(String.valueOf(instrumentToken))
                    .fromDate(fromDate)
                    .toDate(toDate)
                    .interval("day")
                    .continuous(false)
                    .oi(false)
                    .build();

            HistoricalDataResponse response = instrumentService.getHistoricalData(request);

            if (response != null && response.isSuccess() && response.getCandles() != null && response.getCandles().size() >= 2) {
                List<HistoricalCandle> dailyCandles = response.getCandles();
                // Day before yesterday is the second to last candle (most recent trading day - 1)
                int dayBeforeYesterdayIndex = dailyCandles.size() - 2;
                HistoricalCandle dayBeforeYesterdayCandle = dailyCandles.get(dayBeforeYesterdayIndex);

                analysis.setDayBeforeYesterdayHigh(dayBeforeYesterdayCandle.getHigh());
                analysis.setDayBeforeYesterdayLow(dayBeforeYesterdayCandle.getLow());

                logger.debug("Day before yesterday high: {}, low: {} for token: {}",
                        dayBeforeYesterdayCandle.getHigh(), dayBeforeYesterdayCandle.getLow(), instrumentToken);
            } else {
                logger.warn("Could not fetch day before yesterday data for token: {}", instrumentToken);
            }
        } catch (Exception e) {
            logger.error("Error fetching day before yesterday data for token {}: {}", instrumentToken, e.getMessage());
        }
    }

    private void identifySwingPoints(LiquidityZoneAnalysis analysis, List<HistoricalCandle> candles) {
        if (candles.size() < 5) return;

        List<Double> highs = new ArrayList<>();
        List<Double> lows = new ArrayList<>();

        // Find swing highs and lows using pivots
        for (int i = 2; i < candles.size() - 2; i++) {
            HistoricalCandle current = candles.get(i);
            HistoricalCandle prev1 = candles.get(i - 1);
            HistoricalCandle prev2 = candles.get(i - 2);
            HistoricalCandle next1 = candles.get(i + 1);
            HistoricalCandle next2 = candles.get(i + 2);

            // Swing high
            if (current.getHigh() > prev1.getHigh() &&
                current.getHigh() > prev2.getHigh() &&
                current.getHigh() > next1.getHigh() &&
                current.getHigh() > next2.getHigh()) {
                highs.add(current.getHigh());
            }

            // Swing low
            if (current.getLow() < prev1.getLow() &&
                current.getLow() < prev2.getLow() &&
                current.getLow() < next1.getLow() &&
                current.getLow() < next2.getLow()) {
                lows.add(current.getLow());
            }
        }

        // Set most recent swing points
        if (!highs.isEmpty()) {
            Collections.reverse(highs);
            analysis.setTimeframeHigh1(highs.get(0));
            analysis.setTimeframeHigh2(highs.size() > 1 ? highs.get(1) : null);
            analysis.setTimeframeHigh3(highs.size() > 2 ? highs.get(2) : null);
        }

        if (!lows.isEmpty()) {
            Collections.reverse(lows);
            analysis.setTimeframeLow1(lows.get(0));
            analysis.setTimeframeLow2(lows.size() > 1 ? lows.get(1) : null);
            analysis.setTimeframeLow3(lows.size() > 2 ? lows.get(2) : null);
        }

        logger.debug("Identified {} swing highs and {} swing lows", highs.size(), lows.size());
    }

    private void identifyLiquidityZones(LiquidityZoneAnalysis analysis, List<HistoricalCandle> candles, Double currentPrice) {
        // Buy-side liquidity (above highs - where long stop losses are)
        List<Double> buyLiquidityLevels = new ArrayList<>();

        if (analysis.getTimeframeHigh1() != null) buyLiquidityLevels.add(analysis.getTimeframeHigh1());
        if (analysis.getTimeframeHigh2() != null) buyLiquidityLevels.add(analysis.getTimeframeHigh2());
        if (analysis.getTimeframeHigh3() != null) buyLiquidityLevels.add(analysis.getTimeframeHigh3());
        if (analysis.getCurrentDayHigh() != null) buyLiquidityLevels.add(analysis.getCurrentDayHigh());
        if (analysis.getPreviousDayHigh() != null) buyLiquidityLevels.add(analysis.getPreviousDayHigh());

        // Sort and filter levels above current price
        List<Double> buyLiquidityAbove = buyLiquidityLevels.stream()
                .filter(level -> level > currentPrice)
                .distinct()
                .sorted()
                .toList();

        if (!buyLiquidityAbove.isEmpty()) {
            analysis.setBuySideLiquidity1(buyLiquidityAbove.get(0));
            analysis.setBuySideLiquidity2(buyLiquidityAbove.size() > 1 ? buyLiquidityAbove.get(1) : null);
            analysis.setBuySideLiquidity3(buyLiquidityAbove.size() > 2 ? buyLiquidityAbove.get(2) : null);
        }

        // Sell-side liquidity (below lows - where short stop losses are)
        List<Double> sellLiquidityLevels = new ArrayList<>();

        if (analysis.getTimeframeLow1() != null) sellLiquidityLevels.add(analysis.getTimeframeLow1());
        if (analysis.getTimeframeLow2() != null) sellLiquidityLevels.add(analysis.getTimeframeLow2());
        if (analysis.getTimeframeLow3() != null) sellLiquidityLevels.add(analysis.getTimeframeLow3());
        if (analysis.getCurrentDayLow() != null) sellLiquidityLevels.add(analysis.getCurrentDayLow());
        if (analysis.getPreviousDayLow() != null) sellLiquidityLevels.add(analysis.getPreviousDayLow());

        // Sort and filter levels below current price
        List<Double> sellLiquidityBelow = sellLiquidityLevels.stream()
                .filter(level -> level < currentPrice)
                .distinct()
                .sorted(Comparator.reverseOrder())
                .toList();

        if (!sellLiquidityBelow.isEmpty()) {
            analysis.setSellSideLiquidity1(sellLiquidityBelow.get(0));
            analysis.setSellSideLiquidity2(sellLiquidityBelow.size() > 1 ? sellLiquidityBelow.get(1) : null);
            analysis.setSellSideLiquidity3(sellLiquidityBelow.size() > 2 ? sellLiquidityBelow.get(2) : null);
        }
    }

    private void detectLiquidityGrabs(LiquidityZoneAnalysis analysis, List<HistoricalCandle> candles, Double currentPrice) {
        if (candles.isEmpty()) return;

        HistoricalCandle latestCandle = candles.get(candles.size() - 1);
        double recentHigh = latestCandle.getHigh();
        double recentLow = latestCandle.getLow();

        analysis.setBuySideGrabbed(false);
        analysis.setSellSideGrabbed(false);

        // Check if price swept above buy-side liquidity
        Double buySide1 = analysis.getBuySideLiquidity1();
        if (buySide1 != null && recentHigh > buySide1 && currentPrice < buySide1) {
            // Price swept above high but closed back below - buy-side liquidity grabbed
            analysis.setBuySideGrabbed(true);
            analysis.setGrabType("BUY_SIDE_GRAB");
            analysis.setGrabbedLevel(buySide1);
            logger.info("Buy-side liquidity grabbed at level: {}", buySide1);
        }

        // Check if price swept below sell-side liquidity
        Double sellSide1 = analysis.getSellSideLiquidity1();
        if (sellSide1 != null && recentLow < sellSide1 && currentPrice > sellSide1) {
            // Price swept below low but closed back above - sell-side liquidity grabbed
            analysis.setSellSideGrabbed(true);
            analysis.setGrabType("SELL_SIDE_GRAB");
            analysis.setGrabbedLevel(sellSide1);
            logger.info("Sell-side liquidity grabbed at level: {}", sellSide1);
        }

        // If both sides grabbed, determine which is more recent/significant
        if (Boolean.TRUE.equals(analysis.getBuySideGrabbed()) && Boolean.TRUE.equals(analysis.getSellSideGrabbed())) {
            double distanceToHigh = Math.abs(currentPrice - buySide1);
            double distanceToLow = Math.abs(currentPrice - sellSide1);

            if (distanceToHigh < distanceToLow) {
                analysis.setGrabType("BUY_SIDE_GRAB");
                analysis.setGrabbedLevel(buySide1);
            } else {
                analysis.setGrabType("SELL_SIDE_GRAB");
                analysis.setGrabbedLevel(sellSide1);
            }
        }
    }

    private void analyzeMarketStructure(LiquidityZoneAnalysis analysis, List<HistoricalCandle> candles, Double currentPrice) {
        if (candles.size() < 10) {
            analysis.setMarketStructure("RANGING");
            analysis.setTrendStrength(50.0);
            return;
        }

        boolean higherHighs = false;
        boolean lowerLows = false;

        if (analysis.getTimeframeHigh1() != null && analysis.getTimeframeHigh2() != null) {
            higherHighs = analysis.getTimeframeHigh1() > analysis.getTimeframeHigh2();
        }

        if (analysis.getTimeframeLow1() != null && analysis.getTimeframeLow2() != null) {
            lowerLows = analysis.getTimeframeLow1() < analysis.getTimeframeLow2();
        }

        if (higherHighs && !lowerLows) {
            analysis.setMarketStructure("BULLISH");
            analysis.setTrendStrength(70.0);
        } else if (lowerLows && !higherHighs) {
            analysis.setMarketStructure("BEARISH");
            analysis.setTrendStrength(70.0);
        } else {
            analysis.setMarketStructure("RANGING");
            analysis.setTrendStrength(40.0);
        }
    }

    private void generateTradeSetup(LiquidityZoneAnalysis analysis, Double currentPrice) {
        analysis.setIsValidSetup(false);
        analysis.setTradeSignal("NO_SIGNAL");

        // Long Unwind Setup: Buy-side liquidity grabbed (stops above highs hit)
        // Expect: Short covering rally as shorts panic
        if (Boolean.TRUE.equals(analysis.getBuySideGrabbed())) {
            analysis.setTradeSignal("SHORT_COVER");
            analysis.setPositionType("LONG");
            analysis.setStrategyType("LIQUIDITY_GRAB_REVERSAL");
            analysis.setEntryPrice(currentPrice);

            // Stop loss below recent low
            Double stopLoss = analysis.getSellSideLiquidity1();
            if (stopLoss != null) {
                analysis.setStopLoss(stopLoss);

                double riskPoints = currentPrice - stopLoss;
                analysis.setTarget1(currentPrice + riskPoints * 1.5);
                analysis.setTarget2(currentPrice + riskPoints * 2.5);
                analysis.setTarget3(currentPrice + riskPoints * 3.5);
                analysis.setRiskRewardRatio(2.5);

                analysis.setSignalConfidence(75.0);
                analysis.setIsValidSetup(true);
                analysis.setNotes("Buy-side liquidity grabbed. Expect short covering rally.");

                logger.info("Valid SHORT_COVER setup identified at {}", currentPrice);
            }
        }

        // Short Unwind Setup: Sell-side liquidity grabbed (stops below lows hit)
        // Expect: Long unwinding as longs panic
        else if (Boolean.TRUE.equals(analysis.getSellSideGrabbed())) {
            analysis.setTradeSignal("LONG_UNWIND");
            analysis.setPositionType("SHORT");
            analysis.setStrategyType("LIQUIDITY_GRAB_REVERSAL");
            analysis.setEntryPrice(currentPrice);

            // Stop loss above recent high
            Double stopLoss = analysis.getBuySideLiquidity1();
            if (stopLoss != null) {
                analysis.setStopLoss(stopLoss);

                double riskPoints = stopLoss - currentPrice;
                analysis.setTarget1(currentPrice - riskPoints * 1.5);
                analysis.setTarget2(currentPrice - riskPoints * 2.5);
                analysis.setTarget3(currentPrice - riskPoints * 3.5);
                analysis.setRiskRewardRatio(2.5);

                analysis.setSignalConfidence(75.0);
                analysis.setIsValidSetup(true);
                analysis.setNotes("Sell-side liquidity grabbed. Expect long unwinding.");

                logger.info("Valid LONG_UNWIND setup identified at {}", currentPrice);
            }
        }

        // Validate setup
        if (analysis.getStopLoss() == null || analysis.getTarget1() == null) {
            analysis.setIsValidSetup(false);
            analysis.setRejectionReason("Unable to calculate stop loss or targets");
        }
    }

    private Map<String, Object> convertToMap(LiquidityZoneAnalysis analysis) {
        Map<String, Object> map = new HashMap<>();

        map.put("id", analysis.getId());
        map.put("instrumentToken", analysis.getInstrumentToken());
        map.put("instrumentName", analysis.getInstrumentName());
        map.put("timeframe", analysis.getTimeframe());
        map.put("analysisTimestamp", analysis.getAnalysisTimestamp());
        map.put("currentPrice", analysis.getCurrentPrice());

        map.put("previousDayHigh", analysis.getPreviousDayHigh());
        map.put("previousDayLow", analysis.getPreviousDayLow());
        map.put("dayBeforeYesterdayHigh", analysis.getDayBeforeYesterdayHigh());
        map.put("dayBeforeYesterdayLow", analysis.getDayBeforeYesterdayLow());
        map.put("currentDayHigh", analysis.getCurrentDayHigh());
        map.put("currentDayLow", analysis.getCurrentDayLow());

        map.put("timeframeHigh1", analysis.getTimeframeHigh1());
        map.put("timeframeHigh2", analysis.getTimeframeHigh2());
        map.put("timeframeHigh3", analysis.getTimeframeHigh3());
        map.put("timeframeLow1", analysis.getTimeframeLow1());
        map.put("timeframeLow2", analysis.getTimeframeLow2());
        map.put("timeframeLow3", analysis.getTimeframeLow3());

        map.put("buySideLiquidity1", analysis.getBuySideLiquidity1());
        map.put("buySideLiquidity2", analysis.getBuySideLiquidity2());
        map.put("buySideLiquidity3", analysis.getBuySideLiquidity3());
        map.put("sellSideLiquidity1", analysis.getSellSideLiquidity1());
        map.put("sellSideLiquidity2", analysis.getSellSideLiquidity2());
        map.put("sellSideLiquidity3", analysis.getSellSideLiquidity3());

        map.put("buySideGrabbed", analysis.getBuySideGrabbed());
        map.put("sellSideGrabbed", analysis.getSellSideGrabbed());
        map.put("grabbedLevel", analysis.getGrabbedLevel());
        map.put("grabType", analysis.getGrabType());

        map.put("marketStructure", analysis.getMarketStructure());
        map.put("trendStrength", analysis.getTrendStrength());

        map.put("tradeSignal", analysis.getTradeSignal());
        map.put("signalConfidence", analysis.getSignalConfidence());
        map.put("entryPrice", analysis.getEntryPrice());
        map.put("stopLoss", analysis.getStopLoss());
        map.put("target1", analysis.getTarget1());
        map.put("target2", analysis.getTarget2());
        map.put("target3", analysis.getTarget3());
        map.put("riskRewardRatio", analysis.getRiskRewardRatio());

        map.put("positionType", analysis.getPositionType());
        map.put("strategyType", analysis.getStrategyType());
        map.put("isValidSetup", analysis.getIsValidSetup());
        map.put("rejectionReason", analysis.getRejectionReason());
        map.put("notes", analysis.getNotes());

        return map;
    }
}

