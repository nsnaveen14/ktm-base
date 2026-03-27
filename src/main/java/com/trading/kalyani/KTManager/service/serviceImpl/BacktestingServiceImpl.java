package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.entity.InternalOrderBlock;
import com.trading.kalyani.KTManager.entity.PerformanceMetrics;
import com.trading.kalyani.KTManager.entity.TradeResult;
import com.trading.kalyani.KTManager.model.HistoricalDataRequest;
import com.trading.kalyani.KTManager.model.HistoricalDataResponse;
import com.trading.kalyani.KTManager.model.HistoricalDataResponse.HistoricalCandle;
import com.trading.kalyani.KTManager.repository.TradeResultRepository;
import com.trading.kalyani.KTManager.service.BacktestingService;
import com.trading.kalyani.KTManager.service.InstrumentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;

/**
 * Implementation of BacktestingService for historical IOB strategy validation.
 */
@Service
public class BacktestingServiceImpl implements BacktestingService {

    private static final Logger logger = LoggerFactory.getLogger(BacktestingServiceImpl.class);

    @Autowired
    private InstrumentService instrumentService;

    @Autowired
    private TradeResultRepository tradeResultRepository;

    // Store backtest sessions
    private final Map<String, Map<String, Object>> backtestSessions = new ConcurrentHashMap<>();

    // Default parameters
    private static final int DEFAULT_SWING_LOOKBACK = 3;
    private static final double DEFAULT_MIN_DISPLACEMENT_BODY_PERCENT = 0.6;
    private static final double DEFAULT_MIN_CONFIDENCE = 50.0;
    private static final double DEFAULT_RR_TARGET = 2.5;

    @Override
    public Map<String, Object> runBacktest(Long instrumentToken, String timeframe,
                                            LocalDateTime startDate, LocalDateTime endDate,
                                            Map<String, Object> parameters) {
        String backtestId = "BT_" + instrumentToken + "_" + System.currentTimeMillis();
        logger.info("Starting backtest {} for token {} from {} to {}",
                backtestId, instrumentToken, startDate, endDate);

        Map<String, Object> result = new HashMap<>();
        result.put("backtestId", backtestId);
        result.put("instrumentToken", instrumentToken);
        result.put("timeframe", timeframe);
        result.put("startDate", startDate);
        result.put("endDate", endDate);
        result.put("parameters", parameters);
        result.put("status", "RUNNING");
        result.put("startTime", LocalDateTime.now());

        try {
            // Extract parameters with defaults
            int swingLookback = parameters != null && parameters.containsKey("swingLookback") ?
                    ((Number) parameters.get("swingLookback")).intValue() : DEFAULT_SWING_LOOKBACK;
            double minDisplacement = parameters != null && parameters.containsKey("minDisplacementPercent") ?
                    ((Number) parameters.get("minDisplacementPercent")).doubleValue() : DEFAULT_MIN_DISPLACEMENT_BODY_PERCENT;
            double minConfidence = parameters != null && parameters.containsKey("minConfidence") ?
                    ((Number) parameters.get("minConfidence")).doubleValue() : DEFAULT_MIN_CONFIDENCE;
            double rrTarget = parameters != null && parameters.containsKey("rrTarget") ?
                    ((Number) parameters.get("rrTarget")).doubleValue() : DEFAULT_RR_TARGET;

            // Fetch historical data
            List<HistoricalCandle> candles = fetchHistoricalData(instrumentToken, timeframe, startDate, endDate);
            if (candles == null || candles.isEmpty()) {
                result.put("status", "FAILED");
                result.put("error", "No historical data available");
                return result;
            }

            result.put("totalCandles", candles.size());

            // Detect IOBs in historical data
            List<BacktestIOB> detectedIOBs = detectIOBsForBacktest(candles, swingLookback, minDisplacement);
            result.put("totalIOBsDetected", detectedIOBs.size());

            // Simulate trades for each IOB
            List<TradeResult> backtestTrades = simulateTrades(detectedIOBs, candles,
                    instrumentToken, minConfidence, rrTarget);
            result.put("totalTradesSimulated", backtestTrades.size());

            // Save backtest trades
            backtestTrades.forEach(trade -> {
                trade.setTradeType("BACKTEST");
                trade.setTradeId(backtestId + "_" + trade.getIobId());
            });
            tradeResultRepository.saveAll(backtestTrades);

            // Calculate performance metrics
            Map<String, Object> performance = calculateBacktestPerformance(backtestTrades);
            result.put("performance", performance);

            // Generate equity curve
            List<Map<String, Object>> equityCurve = generateEquityCurve(backtestTrades);
            result.put("equityCurve", equityCurve);

            // Trade distribution analysis
            Map<String, Object> distribution = analyzeTradeDistribution(backtestTrades);
            result.put("distribution", distribution);

            result.put("status", "COMPLETED");
            result.put("endTime", LocalDateTime.now());

            // Store session
            backtestSessions.put(backtestId, result);

            logger.info("Backtest {} completed. Trades: {}, Win Rate: {}%",
                    backtestId, backtestTrades.size(),
                    performance.get("winRate"));

        } catch (Exception e) {
            logger.error("Backtest {} failed: {}", backtestId, e.getMessage(), e);
            result.put("status", "FAILED");
            result.put("error", e.getMessage());
        }

        return result;
    }

    @Override
    public Map<String, Object> runBacktestAllIndices(LocalDateTime startDate, LocalDateTime endDate,
                                                      Map<String, Object> parameters) {
        Map<String, Object> result = new HashMap<>();

        // Run backtest for NIFTY
        Map<String, Object> niftyResult = runBacktest(NIFTY_INSTRUMENT_TOKEN, "5min",
                startDate, endDate, parameters);
        result.put("NIFTY", niftyResult);
        result.put("timestamp", LocalDateTime.now());

        return result;
    }

    @Override
    public Map<String, Object> getBacktestResults(String backtestId) {
        return backtestSessions.getOrDefault(backtestId, Map.of("error", "Backtest not found"));
    }

    @Override
    public List<Map<String, Object>> getBacktestHistory() {
        return backtestSessions.values().stream()
                .map(session -> {
                    Map<String, Object> summary = new HashMap<>();
                    summary.put("backtestId", session.get("backtestId"));
                    summary.put("instrumentToken", session.get("instrumentToken"));
                    summary.put("startDate", session.get("startDate"));
                    summary.put("endDate", session.get("endDate"));
                    summary.put("status", session.get("status"));
                    summary.put("totalTrades", session.get("totalTradesSimulated"));

                    @SuppressWarnings("unchecked")
                    Map<String, Object> perf = (Map<String, Object>) session.get("performance");
                    if (perf != null) {
                        summary.put("winRate", perf.get("winRate"));
                        summary.put("totalPnl", perf.get("totalNetPnl"));
                    }
                    return summary;
                })
                .toList();
    }

    @Override
    public Map<String, Object> compareBacktests(String backtestId1, String backtestId2) {
        Map<String, Object> bt1 = backtestSessions.get(backtestId1);
        Map<String, Object> bt2 = backtestSessions.get(backtestId2);

        if (bt1 == null || bt2 == null) {
            return Map.of("error", "One or both backtests not found");
        }

        Map<String, Object> comparison = new HashMap<>();
        comparison.put("backtest1", backtestId1);
        comparison.put("backtest2", backtestId2);

        @SuppressWarnings("unchecked")
        Map<String, Object> perf1 = (Map<String, Object>) bt1.get("performance");
        @SuppressWarnings("unchecked")
        Map<String, Object> perf2 = (Map<String, Object>) bt2.get("performance");

        if (perf1 != null && perf2 != null) {
            Map<String, Object> metrics = new HashMap<>();

            String[] metricsToCompare = {"winRate", "totalNetPnl", "profitFactor",
                    "maxDrawdown", "expectancy", "avgRR"};

            for (String metric : metricsToCompare) {
                Map<String, Object> metricComparison = new HashMap<>();
                metricComparison.put("bt1", perf1.get(metric));
                metricComparison.put("bt2", perf2.get(metric));

                if (perf1.get(metric) instanceof Number && perf2.get(metric) instanceof Number) {
                    double val1 = ((Number) perf1.get(metric)).doubleValue();
                    double val2 = ((Number) perf2.get(metric)).doubleValue();
                    metricComparison.put("difference", val2 - val1);
                    metricComparison.put("percentChange", val1 != 0 ? ((val2 - val1) / val1) * 100 : 0);
                }

                metrics.put(metric, metricComparison);
            }

            comparison.put("metrics", metrics);
        }

        return comparison;
    }

    @Override
    public Map<String, Object> optimizeParameters(Long instrumentToken,
                                                   LocalDateTime startDate, LocalDateTime endDate) {
        logger.info("Starting parameter optimization for token {}", instrumentToken);

        Map<String, Object> result = new HashMap<>();
        List<Map<String, Object>> optimizationRuns = new ArrayList<>();

        // Parameter ranges to test
        int[] swingLookbacks = {2, 3, 4, 5};
        double[] displacementPercents = {0.5, 0.6, 0.7, 0.8};
        double[] minConfidences = {50.0, 60.0, 70.0};
        double[] rrTargets = {1.5, 2.0, 2.5, 3.0};

        Map<String, Object> bestParams = null;
        double bestScore = Double.MIN_VALUE;

        // Test combinations
        for (int swing : swingLookbacks) {
            for (double disp : displacementPercents) {
                for (double conf : minConfidences) {
                    for (double rr : rrTargets) {
                        Map<String, Object> params = Map.of(
                                "swingLookback", swing,
                                "minDisplacementPercent", disp,
                                "minConfidence", conf,
                                "rrTarget", rr
                        );

                        Map<String, Object> btResult = runBacktest(instrumentToken, "5min",
                                startDate, endDate, params);

                        @SuppressWarnings("unchecked")
                        Map<String, Object> perf = (Map<String, Object>) btResult.get("performance");

                        if (perf != null) {
                            // Calculate optimization score
                            // Score = ProfitFactor * WinRate - MaxDrawdownPercent
                            double profitFactor = perf.get("profitFactor") != null ?
                                    ((Number) perf.get("profitFactor")).doubleValue() : 0;
                            double winRate = perf.get("winRate") != null ?
                                    ((Number) perf.get("winRate")).doubleValue() : 0;
                            double maxDDPercent = perf.get("maxDrawdownPercent") != null ?
                                    ((Number) perf.get("maxDrawdownPercent")).doubleValue() : 0;

                            double score = (profitFactor * winRate / 100) - (maxDDPercent / 100);

                            Map<String, Object> run = new HashMap<>();
                            run.put("params", params);
                            run.put("score", score);
                            run.put("winRate", winRate);
                            run.put("profitFactor", profitFactor);
                            run.put("maxDrawdownPercent", maxDDPercent);
                            run.put("totalTrades", btResult.get("totalTradesSimulated"));
                            optimizationRuns.add(run);

                            if (score > bestScore) {
                                bestScore = score;
                                bestParams = params;
                            }
                        }
                    }
                }
            }
        }

        // Sort by score
        optimizationRuns.sort((a, b) ->
                Double.compare(((Number) b.get("score")).doubleValue(),
                              ((Number) a.get("score")).doubleValue()));

        result.put("optimizationRuns", optimizationRuns.subList(0, Math.min(10, optimizationRuns.size())));
        result.put("bestParameters", bestParams);
        result.put("bestScore", bestScore);
        result.put("totalCombinationsTested", optimizationRuns.size());
        result.put("timestamp", LocalDateTime.now());

        logger.info("Parameter optimization completed. Best score: {}", bestScore);

        return result;
    }

    // ==================== Private Helper Methods ====================

    private List<HistoricalCandle> fetchHistoricalData(Long instrumentToken, String timeframe,
                                                        LocalDateTime startDate, LocalDateTime endDate) {
        try {
            String kiteInterval = "5minute";
            if ("15min".equals(timeframe)) kiteInterval = "15minute";
            if ("1hour".equals(timeframe)) kiteInterval = "60minute";

            HistoricalDataRequest request = HistoricalDataRequest.builder()
                    .instrumentToken(String.valueOf(instrumentToken))
                    .fromDate(startDate)
                    .toDate(endDate)
                    .interval(kiteInterval)
                    .continuous(false)
                    .oi(false)
                    .build();

            HistoricalDataResponse response = instrumentService.getHistoricalData(request);

            if (response != null && response.isSuccess() && response.getCandles() != null) {
                return response.getCandles();
            }
        } catch (Exception e) {
            logger.error("Error fetching historical data: {}", e.getMessage());
        }
        return new ArrayList<>();
    }

    private List<BacktestIOB> detectIOBsForBacktest(List<HistoricalCandle> candles,
                                                     int swingLookback, double minDisplacement) {
        List<BacktestIOB> iobs = new ArrayList<>();

        // Identify swing points
        List<SwingPoint> swingHighs = identifySwingHighs(candles, swingLookback);
        List<SwingPoint> swingLows = identifySwingLows(candles, swingLookback);

        // Detect bullish IOBs
        for (int i = swingLookback + 5; i < candles.size() - 1; i++) {
            HistoricalCandle current = candles.get(i);

            // Check for bullish BOS
            SwingPoint brokenHigh = findBrokenSwingHigh(swingHighs, current, i);
            if (brokenHigh != null) {
                int obIndex = findLastBearishCandle(candles, brokenHigh.index, i);
                if (obIndex != -1 && hasValidDisplacement(candles, obIndex, i, true, minDisplacement)) {
                    BacktestIOB iob = createBacktestIOB(candles, obIndex, i, "BULLISH_IOB", brokenHigh);
                    iobs.add(iob);
                }
            }

            // Check for bearish BOS
            SwingPoint brokenLow = findBrokenSwingLow(swingLows, current, i);
            if (brokenLow != null) {
                int obIndex = findLastBullishCandle(candles, brokenLow.index, i);
                if (obIndex != -1 && hasValidDisplacement(candles, obIndex, i, false, minDisplacement)) {
                    BacktestIOB iob = createBacktestIOB(candles, obIndex, i, "BEARISH_IOB", brokenLow);
                    iobs.add(iob);
                }
            }
        }

        return iobs;
    }

    private List<TradeResult> simulateTrades(List<BacktestIOB> iobs, List<HistoricalCandle> candles,
                                              Long instrumentToken, double minConfidence, double rrTarget) {
        List<TradeResult> trades = new ArrayList<>();

        for (BacktestIOB iob : iobs) {
            if (iob.confidence < minConfidence) continue;

            // Find entry - when price returns to zone
            int entryIndex = findZoneTouch(candles, iob);
            if (entryIndex == -1 || entryIndex >= candles.size() - 1) continue;

            HistoricalCandle entryCandle = candles.get(entryIndex);
            double entryPrice = iob.zoneMidpoint;

            // Calculate trade setup
            double stopLoss, target1, target2, target3;
            boolean isLong = "BULLISH_IOB".equals(iob.type);

            if (isLong) {
                stopLoss = iob.zoneLow - (iob.zoneLow * 0.001);
                double risk = entryPrice - stopLoss;
                target1 = entryPrice + risk * 1.5;
                target2 = entryPrice + risk * rrTarget;
                target3 = entryPrice + risk * 4.0;
            } else {
                stopLoss = iob.zoneHigh + (iob.zoneHigh * 0.001);
                double risk = stopLoss - entryPrice;
                target1 = entryPrice - risk * 1.5;
                target2 = entryPrice - risk * rrTarget;
                target3 = entryPrice - risk * 4.0;
            }

            // Simulate trade outcome
            TradeOutcome outcome = simulateTradeOutcome(candles, entryIndex, isLong,
                    stopLoss, target1, target2, target3);

            // Create trade result
            TradeResult trade = TradeResult.builder()
                    .iobId((long) iob.obIndex)
                    .instrumentToken(instrumentToken)
                    .instrumentName(getInstrumentName(instrumentToken))
                    .timeframe("5min")
                    .tradeDirection(isLong ? "LONG" : "SHORT")
                    .tradeType("BACKTEST")
                    .entryPrice(entryPrice)
                    .entryTime(parseTimestamp(entryCandle.getTimestamp()))
                    .entryReason("ZONE_TOUCH")
                    .stopLoss(stopLoss)
                    .target1(target1)
                    .target2(target2)
                    .target3(target3)
                    .exitPrice(outcome.exitPrice)
                    .exitTime(outcome.exitTime)
                    .exitReason(outcome.exitReason)
                    .quantity(1)
                    .iobType(iob.type)
                    .zoneHigh(iob.zoneHigh)
                    .zoneLow(iob.zoneLow)
                    .signalConfidence(iob.confidence)
                    .hadFvg(iob.hasFvg)
                    .status("CLOSED")
                    .build();

            // Calculate P&L
            double riskPerUnit = Math.abs(entryPrice - stopLoss);
            trade.setRiskAmount(riskPerUnit);
            trade.setPlannedRRRatio(rrTarget);
            trade.calculateMetrics();

            trades.add(trade);
        }

        return trades;
    }

    private TradeOutcome simulateTradeOutcome(List<HistoricalCandle> candles, int entryIndex,
                                               boolean isLong, double stopLoss,
                                               double target1, double target2, double target3) {
        TradeOutcome outcome = new TradeOutcome();

        for (int i = entryIndex + 1; i < candles.size(); i++) {
            HistoricalCandle candle = candles.get(i);

            // Check stop loss
            if (isLong && candle.getLow() <= stopLoss) {
                outcome.exitPrice = stopLoss;
                outcome.exitTime = parseTimestamp(candle.getTimestamp());
                outcome.exitReason = "STOP_LOSS";
                return outcome;
            }
            if (!isLong && candle.getHigh() >= stopLoss) {
                outcome.exitPrice = stopLoss;
                outcome.exitTime = parseTimestamp(candle.getTimestamp());
                outcome.exitReason = "STOP_LOSS";
                return outcome;
            }

            // Check Target 2 (default exit)
            if (isLong && candle.getHigh() >= target2) {
                outcome.exitPrice = target2;
                outcome.exitTime = parseTimestamp(candle.getTimestamp());
                outcome.exitReason = "TARGET_2";
                return outcome;
            }
            if (!isLong && candle.getLow() <= target2) {
                outcome.exitPrice = target2;
                outcome.exitTime = parseTimestamp(candle.getTimestamp());
                outcome.exitReason = "TARGET_2";
                return outcome;
            }

            // Exit at end of day or after max holding period (75 candles = ~6 hours)
            if (i - entryIndex > 75) {
                outcome.exitPrice = candle.getClose();
                outcome.exitTime = parseTimestamp(candle.getTimestamp());
                outcome.exitReason = "TIME_EXIT";
                return outcome;
            }
        }

        // Exit at last candle if still open
        HistoricalCandle lastCandle = candles.get(candles.size() - 1);
        outcome.exitPrice = lastCandle.getClose();
        outcome.exitTime = parseTimestamp(lastCandle.getTimestamp());
        outcome.exitReason = "END_OF_DATA";
        return outcome;
    }

    private int findZoneTouch(List<HistoricalCandle> candles, BacktestIOB iob) {
        boolean isLong = "BULLISH_IOB".equals(iob.type);

        for (int i = iob.bosIndex + 1; i < candles.size(); i++) {
            HistoricalCandle candle = candles.get(i);

            // For long, wait for price to come back down to zone
            if (isLong && candle.getLow() <= iob.zoneHigh && candle.getLow() >= iob.zoneLow) {
                return i;
            }
            // For short, wait for price to come back up to zone
            if (!isLong && candle.getHigh() >= iob.zoneLow && candle.getHigh() <= iob.zoneHigh) {
                return i;
            }

            // Skip if price moved too far without touching zone
            if (i - iob.bosIndex > 50) break;
        }
        return -1;
    }

    private Map<String, Object> calculateBacktestPerformance(List<TradeResult> trades) {
        Map<String, Object> perf = new HashMap<>();

        if (trades.isEmpty()) {
            perf.put("totalTrades", 0);
            perf.put("winRate", 0.0);
            return perf;
        }

        int wins = 0, losses = 0;
        double totalPnl = 0, totalWins = 0, totalLosses = 0;
        double maxDrawdown = 0, peak = 0, current = 0;

        for (TradeResult trade : trades) {
            if ("WIN".equals(trade.getOutcome())) {
                wins++;
                totalWins += trade.getNetPnl() != null ? trade.getNetPnl() : 0;
            } else if ("LOSS".equals(trade.getOutcome())) {
                losses++;
                totalLosses += Math.abs(trade.getNetPnl() != null ? trade.getNetPnl() : 0);
            }

            totalPnl += trade.getNetPnl() != null ? trade.getNetPnl() : 0;
            current = totalPnl;

            if (current > peak) peak = current;
            double dd = peak - current;
            if (dd > maxDrawdown) maxDrawdown = dd;
        }

        perf.put("totalTrades", trades.size());
        perf.put("wins", wins);
        perf.put("losses", losses);
        perf.put("winRate", trades.size() > 0 ? (double) wins / trades.size() * 100 : 0);
        perf.put("totalNetPnl", totalPnl);
        perf.put("avgPnlPerTrade", totalPnl / trades.size());
        perf.put("avgWin", wins > 0 ? totalWins / wins : 0);
        perf.put("avgLoss", losses > 0 ? totalLosses / losses : 0);
        perf.put("profitFactor", totalLosses > 0 ? totalWins / totalLosses : totalWins);
        perf.put("maxDrawdown", maxDrawdown);
        perf.put("maxDrawdownPercent", peak > 0 ? maxDrawdown / peak * 100 : 0);
        perf.put("avgRR", trades.stream()
                .filter(t -> t.getAchievedRRRatio() != null)
                .mapToDouble(TradeResult::getAchievedRRRatio)
                .average().orElse(0));

        // Expectancy
        double winProb = (double) wins / trades.size();
        double avgWin = wins > 0 ? totalWins / wins : 0;
        double avgLoss = losses > 0 ? totalLosses / losses : 0;
        perf.put("expectancy", (winProb * avgWin) - ((1 - winProb) * avgLoss));

        return perf;
    }

    private List<Map<String, Object>> generateEquityCurve(List<TradeResult> trades) {
        List<Map<String, Object>> curve = new ArrayList<>();
        double cumPnl = 0;

        trades.sort(Comparator.comparing(TradeResult::getExitTime));

        for (TradeResult trade : trades) {
            cumPnl += trade.getNetPnl() != null ? trade.getNetPnl() : 0;

            Map<String, Object> point = new HashMap<>();
            point.put("time", trade.getExitTime());
            point.put("pnl", trade.getNetPnl());
            point.put("cumulativePnl", cumPnl);
            point.put("outcome", trade.getOutcome());
            curve.add(point);
        }

        return curve;
    }

    private Map<String, Object> analyzeTradeDistribution(List<TradeResult> trades) {
        Map<String, Object> dist = new HashMap<>();

        // By IOB type
        long bullish = trades.stream().filter(t -> "BULLISH_IOB".equals(t.getIobType())).count();
        long bearish = trades.stream().filter(t -> "BEARISH_IOB".equals(t.getIobType())).count();
        dist.put("byType", Map.of("bullish", bullish, "bearish", bearish));

        // By exit reason
        Map<String, Long> byExit = trades.stream()
                .collect(Collectors.groupingBy(
                        t -> t.getExitReason() != null ? t.getExitReason() : "UNKNOWN",
                        Collectors.counting()));
        dist.put("byExitReason", byExit);

        // By holding duration
        long intraday = trades.stream()
                .filter(t -> "INTRADAY".equals(t.getHoldingPeriod())).count();
        long overnight = trades.stream()
                .filter(t -> "OVERNIGHT".equals(t.getHoldingPeriod())).count();
        dist.put("byHoldingPeriod", Map.of("intraday", intraday, "overnight", overnight));

        return dist;
    }

    // ==================== Swing Point Detection ====================

    private List<SwingPoint> identifySwingHighs(List<HistoricalCandle> candles, int lookback) {
        List<SwingPoint> swingHighs = new ArrayList<>();
        for (int i = lookback; i < candles.size() - lookback; i++) {
            HistoricalCandle current = candles.get(i);
            boolean isSwingHigh = true;
            for (int j = 1; j <= lookback; j++) {
                if (candles.get(i - j).getHigh() >= current.getHigh() ||
                    candles.get(i + j).getHigh() >= current.getHigh()) {
                    isSwingHigh = false;
                    break;
                }
            }
            if (isSwingHigh) {
                swingHighs.add(new SwingPoint(i, current.getHigh()));
            }
        }
        return swingHighs;
    }

    private List<SwingPoint> identifySwingLows(List<HistoricalCandle> candles, int lookback) {
        List<SwingPoint> swingLows = new ArrayList<>();
        for (int i = lookback; i < candles.size() - lookback; i++) {
            HistoricalCandle current = candles.get(i);
            boolean isSwingLow = true;
            for (int j = 1; j <= lookback; j++) {
                if (candles.get(i - j).getLow() <= current.getLow() ||
                    candles.get(i + j).getLow() <= current.getLow()) {
                    isSwingLow = false;
                    break;
                }
            }
            if (isSwingLow) {
                swingLows.add(new SwingPoint(i, current.getLow()));
            }
        }
        return swingLows;
    }

    private SwingPoint findBrokenSwingHigh(List<SwingPoint> swingHighs, HistoricalCandle candle, int idx) {
        for (SwingPoint sh : swingHighs) {
            if (sh.index < idx && sh.index > idx - 30) {
                if (candle.getClose() > sh.price && candle.getOpen() < sh.price) {
                    return sh;
                }
            }
        }
        return null;
    }

    private SwingPoint findBrokenSwingLow(List<SwingPoint> swingLows, HistoricalCandle candle, int idx) {
        for (SwingPoint sl : swingLows) {
            if (sl.index < idx && sl.index > idx - 30) {
                if (candle.getClose() < sl.price && candle.getOpen() > sl.price) {
                    return sl;
                }
            }
        }
        return null;
    }

    private int findLastBearishCandle(List<HistoricalCandle> candles, int start, int end) {
        for (int i = end - 1; i >= start; i--) {
            HistoricalCandle c = candles.get(i);
            if (c.getClose() < c.getOpen()) return i;
        }
        return -1;
    }

    private int findLastBullishCandle(List<HistoricalCandle> candles, int start, int end) {
        for (int i = end - 1; i >= start; i--) {
            HistoricalCandle c = candles.get(i);
            if (c.getClose() > c.getOpen()) return i;
        }
        return -1;
    }

    private boolean hasValidDisplacement(List<HistoricalCandle> candles, int obIdx, int bosIdx,
                                          boolean bullish, double minDisplacement) {
        if (bosIdx <= obIdx + 1) return false;
        HistoricalCandle disp = candles.get(obIdx + 1);
        double body = Math.abs(disp.getClose() - disp.getOpen());
        double range = disp.getHigh() - disp.getLow();
        if (range == 0) return false;
        boolean correctDir = bullish ? disp.getClose() > disp.getOpen() : disp.getClose() < disp.getOpen();
        return body / range >= minDisplacement && correctDir;
    }

    private BacktestIOB createBacktestIOB(List<HistoricalCandle> candles, int obIdx, int bosIdx,
                                           String type, SwingPoint bos) {
        HistoricalCandle ob = candles.get(obIdx);
        BacktestIOB iob = new BacktestIOB();
        iob.obIndex = obIdx;
        iob.bosIndex = bosIdx;
        iob.type = type;
        iob.zoneHigh = ob.getHigh();
        iob.zoneLow = ob.getLow();
        iob.zoneMidpoint = (ob.getHigh() + ob.getLow()) / 2;
        iob.bosLevel = bos.price;
        iob.timestamp = parseTimestamp(ob.getTimestamp());

        // Calculate confidence
        double confidence = 70.0;
        double zoneSize = (iob.zoneHigh - iob.zoneLow) / iob.zoneMidpoint * 100;
        if (zoneSize > 1.0) confidence -= 15;
        if (zoneSize < 0.05) confidence -= 10;

        // Check for FVG
        if (obIdx + 2 < candles.size()) {
            HistoricalCandle c1 = candles.get(obIdx);
            HistoricalCandle c3 = candles.get(obIdx + 2);
            if ("BULLISH_IOB".equals(type)) {
                iob.hasFvg = c3.getLow() > c1.getHigh();
            } else {
                iob.hasFvg = c3.getHigh() < c1.getLow();
            }
            if (iob.hasFvg) confidence += 15;
        }

        iob.confidence = Math.max(0, Math.min(100, confidence));
        return iob;
    }

    private LocalDateTime parseTimestamp(String timestamp) {
        try {
            ZonedDateTime zdt = ZonedDateTime.parse(timestamp,
                    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ"));
            return zdt.toLocalDateTime();
        } catch (Exception e) {
            logger.warn("Failed to parse timestamp '{}', defaulting to now: {}", timestamp, e.getMessage());
            return LocalDateTime.now();
        }
    }

    private String getInstrumentName(Long token) {
        if (token.equals(NIFTY_INSTRUMENT_TOKEN)) return "NIFTY";
        return "UNKNOWN";
    }

    // ==================== Inner Classes ====================

    private static class SwingPoint {
        int index;
        double price;

        SwingPoint(int index, double price) {
            this.index = index;
            this.price = price;
        }
    }

    private static class BacktestIOB {
        int obIndex;
        int bosIndex;
        String type;
        double zoneHigh;
        double zoneLow;
        double zoneMidpoint;
        double bosLevel;
        double confidence;
        boolean hasFvg;
        LocalDateTime timestamp;
    }

    private static class TradeOutcome {
        double exitPrice;
        LocalDateTime exitTime;
        String exitReason;
    }
}
