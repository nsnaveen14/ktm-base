package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.entity.IOBTradeResult;
import com.trading.kalyani.KTManager.entity.InternalOrderBlock;
import com.trading.kalyani.KTManager.model.HistoricalDataRequest;
import com.trading.kalyani.KTManager.model.HistoricalDataResponse;
import com.trading.kalyani.KTManager.repository.IOBTradeResultRepository;
import com.trading.kalyani.KTManager.repository.InternalOrderBlockRepository;
import com.trading.kalyani.KTManager.service.IOBAutoTradeService;
import com.trading.kalyani.KTManager.service.InstrumentService;
import com.trading.kalyani.KTManager.service.TelegramNotificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;

/**
 * Implementation of IOB Auto Trade Service.
 * Handles automated trading based on Internal Order Block signals.
 */
@Service
public class IOBAutoTradeServiceImpl implements IOBAutoTradeService {

    private static final Logger logger = LoggerFactory.getLogger(IOBAutoTradeServiceImpl.class);

    private static final DateTimeFormatter SIGNATURE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmm");
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    @Autowired
    private IOBTradeResultRepository tradeResultRepository;

    @Autowired
    private InternalOrderBlockRepository iobRepository;

    @Autowired
    private InstrumentService instrumentService;

    @Autowired(required = false)
    private TelegramNotificationService telegramNotificationService;

    @Autowired(required = false)
    private SimpMessagingTemplate messagingTemplate;

    @Lazy
    @Autowired(required = false)
    private com.trading.kalyani.KTManager.service.InternalOrderBlockService internalOrderBlockService;

    // Configuration
    private final AtomicBoolean autoTradingEnabled = new AtomicBoolean(false);
    private final Map<String, Object> autoTradeConfig = new ConcurrentHashMap<>();

    // Backtest sequence counter to ensure unique trade IDs
    private final AtomicLong backtestTradeCounter = new AtomicLong(0);

    // Default configuration values
    private static final double DEFAULT_MIN_CONFIDENCE = 85.0; // Minimum 85% confidence for auto-trade
    private static final double DEFAULT_MAX_ZONE_DISTANCE_PERCENT = 0.5;
    private static final int DEFAULT_MAX_OPEN_TRADES = 3;
    private static final double DEFAULT_DAILY_LOSS_LIMIT = 5000.0;
    private static final double DEFAULT_RISK_PER_TRADE = 1000.0;
    private static final double DEFAULT_TRAILING_SL_ACTIVATION = 1.0; // Activate after 1R profit
    private static final double DEFAULT_TRAILING_SL_DISTANCE = 0.5; // Trail at 0.5R

    // Track last processed prices to avoid duplicate processing
    private final Map<Long, Double> lastProcessedPrices = new ConcurrentHashMap<>();

    public IOBAutoTradeServiceImpl() {
        // Initialize default configuration
        autoTradeConfig.put("minConfidence", DEFAULT_MIN_CONFIDENCE);
        autoTradeConfig.put("maxZoneDistancePercent", DEFAULT_MAX_ZONE_DISTANCE_PERCENT);
        autoTradeConfig.put("maxOpenTrades", DEFAULT_MAX_OPEN_TRADES);
        autoTradeConfig.put("dailyLossLimit", DEFAULT_DAILY_LOSS_LIMIT);
        autoTradeConfig.put("riskPerTrade", DEFAULT_RISK_PER_TRADE);
        autoTradeConfig.put("trailingSLActivation", DEFAULT_TRAILING_SL_ACTIVATION);
        autoTradeConfig.put("trailingSLDistance", DEFAULT_TRAILING_SL_DISTANCE);
        autoTradeConfig.put("entryOnZoneTouch", true);
        autoTradeConfig.put("entryOnZoneMidpoint", false);
        autoTradeConfig.put("requireFVG", false);
        autoTradeConfig.put("requireHTFAlignment", false);
    }

    // ==================== Auto Trading Control ====================

    @Override
    public void setAutoTradingEnabled(boolean enabled) {
        this.autoTradingEnabled.set(enabled);
        logger.info("IOB Auto-trading {}", enabled ? "ENABLED" : "DISABLED");

        if (telegramNotificationService != null && telegramNotificationService.isConfigured()) {
            String message = enabled ? "🤖 IOB Auto-trading ENABLED" : "⏹️ IOB Auto-trading DISABLED";
            telegramNotificationService.sendSystemAlert("IOB Auto-Trade", message);
        }
    }

    @Override
    public boolean isAutoTradingEnabled() {
        return autoTradingEnabled.get();
    }

    @Override
    public Map<String, Object> getAutoTradeConfig() {
        Map<String, Object> config = new HashMap<>(autoTradeConfig);
        config.put("autoTradingEnabled", autoTradingEnabled.get());
        return config;
    }

    @Override
    public void updateAutoTradeConfig(Map<String, Object> config) {
        if (config != null) {
            autoTradeConfig.putAll(config);
            logger.info("IOB Auto-trade config updated: {}", config);
        }
    }

    // ==================== Trade Entry ====================

    @Override
    public boolean shouldEnterTrade(InternalOrderBlock iob, Double currentPrice) {
        if (!autoTradingEnabled.get()) return false;
        if (iob == null || currentPrice == null) return false;

        // Check confidence threshold - use enhancedConfidence if available, else signalConfidence
        double minConfidence = getConfigDouble("minConfidence", DEFAULT_MIN_CONFIDENCE);
        Double iobConfidence = iob.getEnhancedConfidence() != null ?
                iob.getEnhancedConfidence() : iob.getSignalConfidence();

        if (iobConfidence == null || iobConfidence < minConfidence) {
            logger.debug("IOB {} rejected: confidence {}% < minimum {}%",
                    iob.getId(), iobConfidence, minConfidence);
            return false;
        }

        // Check if already traded
        if (Boolean.TRUE.equals(iob.getTradeTaken())) {
            return false;
        }

        // Check max open trades
        if (isMaxOpenTradesReached()) {
            return false;
        }

        // Check daily loss limit
        if (isDailyLossLimitReached()) {
            return false;
        }

        // Check if price is near zone
        double maxDistance = getConfigDouble("maxZoneDistancePercent", DEFAULT_MAX_ZONE_DISTANCE_PERCENT);
        boolean priceNearZone = isPriceNearZone(iob, currentPrice, maxDistance);

        // Check entry conditions based on configuration
        boolean entryOnTouch = getConfigBoolean("entryOnZoneTouch", true);
        boolean entryOnMidpoint = getConfigBoolean("entryOnZoneMidpoint", false);

        if (entryOnTouch) {
            // Entry when price touches the zone
            if (isPriceInZone(iob, currentPrice)) {
                return true;
            }
        }

        if (entryOnMidpoint) {
            // Entry when price reaches zone midpoint
            if (isPriceAtMidpoint(iob, currentPrice)) {
                return true;
            }
        }

        // Check FVG requirement
        boolean requireFVG = getConfigBoolean("requireFVG", false);
        if (requireFVG && !Boolean.TRUE.equals(iob.getHasFvg())) {
            return false;
        }

        return priceNearZone;
    }

    @Override
    public IOBTradeResult enterTrade(InternalOrderBlock iob, Double currentPrice, String entryTrigger) {
        if (iob == null || currentPrice == null) return null;

        try {
            String tradeId = "IOB_" + iob.getId() + "_" + System.currentTimeMillis();

            // Use enhancedConfidence if available, otherwise use signalConfidence
            Double confidence = iob.getEnhancedConfidence() != null ?
                    iob.getEnhancedConfidence() : iob.getSignalConfidence();

            IOBTradeResult trade = IOBTradeResult.builder()
                    .iobId(iob.getId())
                    .tradeId(tradeId)
                    .instrumentToken(iob.getInstrumentToken())
                    .instrumentName(iob.getInstrumentName())
                    .timeframe(iob.getTimeframe())
                    .iobType(iob.getObType())
                    .tradeDirection(iob.getTradeDirection())
                    .signalConfidence(confidence)
                    .hasFvg(iob.getHasFvg())
                    .zoneHigh(iob.getZoneHigh())
                    .zoneLow(iob.getZoneLow())
                    .plannedEntry(iob.getEntryPrice())
                    .actualEntry(currentPrice)
                    .entryTime(LocalDateTime.now())
                    .entryTrigger(entryTrigger)
                    .plannedStopLoss(iob.getStopLoss())
                    .actualStopLoss(iob.getStopLoss())
                    .target1(iob.getTarget1())
                    .target2(iob.getTarget2())
                    .target3(iob.getTarget3())
                    .plannedRR(iob.getRiskRewardRatio())
                    .tradeMode("SIMULATED")
                    .status("OPEN")
                    .peakFavorable(0.0)
                    .peakAdverse(0.0)
                    .build();

            // Calculate risk points
            if (iob.getStopLoss() != null) {
                trade.setRiskPoints(Math.abs(currentPrice - iob.getStopLoss()));
            }

            // Calculate position size
            double riskAmount = getConfigDouble("riskPerTrade", DEFAULT_RISK_PER_TRADE);
            Integer positionSize = calculatePositionSize(currentPrice, iob.getStopLoss(), riskAmount);
            trade.setQuantity(positionSize);

            // Save trade
            tradeResultRepository.save(trade);

            // Mark IOB as traded
            iob.setTradeTaken(true);
            iob.setTradeId(tradeId);
            iob.setStatus("TRADED");
            iobRepository.save(iob);

            logger.info("IOB Trade entered: {} {} at {} (IOB: {})",
                    trade.getTradeDirection(), trade.getInstrumentName(), currentPrice, iob.getId());

            // Send notification
            sendTradeEntryNotification(trade, iob);

            // Push WebSocket update
            pushTradeUpdate(trade, "ENTRY");

            return trade;

        } catch (Exception e) {
            logger.error("Error entering IOB trade: {}", e.getMessage(), e);
            return null;
        }
    }

    @Override
    public IOBTradeResult manualEnterTrade(Long iobId) {
        Optional<InternalOrderBlock> iobOpt = iobRepository.findById(iobId);
        if (iobOpt.isEmpty()) {
            logger.warn("IOB not found for manual entry: {}", iobId);
            return null;
        }

        InternalOrderBlock iob = iobOpt.get();
        Double currentPrice = iob.getCurrentPrice();

        return enterTrade(iob, currentPrice, "MANUAL");
    }

    // ==================== Trade Management ====================

    @Override
    public List<IOBTradeResult> monitorOpenTrades() {
        List<IOBTradeResult> openTrades = tradeResultRepository.findOpenTrades();
        List<IOBTradeResult> processedTrades = new ArrayList<>();

        for (IOBTradeResult trade : openTrades) {
            // Get current price for the instrument
            Double currentPrice = getCurrentPrice(trade.getInstrumentToken());
            if (currentPrice == null) continue;

            // Check exit conditions
            IOBTradeResult processedTrade = checkExitConditions(trade, currentPrice);
            if (processedTrade != null) {
                processedTrades.add(processedTrade);
            }
        }

        return processedTrades;
    }

    @Override
    public void updateTrailingStopLosses(Double currentPrice) {
        List<IOBTradeResult> openTrades = tradeResultRepository.findOpenTrades();

        double activationR = getConfigDouble("trailingSLActivation", DEFAULT_TRAILING_SL_ACTIVATION);
        double trailDistance = getConfigDouble("trailingSLDistance", DEFAULT_TRAILING_SL_DISTANCE);

        List<IOBTradeResult> toSave = new ArrayList<>();

        for (IOBTradeResult trade : openTrades) {
            if (trade.getRiskPoints() == null || trade.getRiskPoints() <= 0) continue;

            boolean modified = false;
            double currentPnlR = calculateCurrentRR(trade, currentPrice);

            // Update peak favorable excursion
            if (currentPnlR > 0) {
                double peakR = trade.getPeakFavorable() != null ? trade.getPeakFavorable() / trade.getRiskPoints() : 0;
                if (currentPnlR > peakR) {
                    trade.setPeakFavorable(currentPnlR * trade.getRiskPoints());
                    modified = true;
                }
            } else {
                // Update peak adverse excursion
                double adversePoints = Math.abs(currentPnlR * trade.getRiskPoints());
                if (trade.getPeakAdverse() == null || adversePoints > trade.getPeakAdverse()) {
                    trade.setPeakAdverse(adversePoints);
                    modified = true;
                }
            }

            // Check if trailing SL should be activated
            if (currentPnlR >= activationR) {
                double newSL;
                if ("LONG".equals(trade.getTradeDirection())) {
                    // For long: trail SL up
                    newSL = currentPrice - (trailDistance * trade.getRiskPoints());
                    if (trade.getActualStopLoss() == null || newSL > trade.getActualStopLoss()) {
                        trade.setActualStopLoss(newSL);
                        logger.info("Trailing SL updated for trade {}: {}", trade.getTradeId(), newSL);
                        modified = true;
                    }
                } else {
                    // For short: trail SL down
                    newSL = currentPrice + (trailDistance * trade.getRiskPoints());
                    if (trade.getActualStopLoss() == null || newSL < trade.getActualStopLoss()) {
                        trade.setActualStopLoss(newSL);
                        logger.info("Trailing SL updated for trade {}: {}", trade.getTradeId(), newSL);
                        modified = true;
                    }
                }
            }

            if (modified) toSave.add(trade);
        }

        if (!toSave.isEmpty()) {
            tradeResultRepository.saveAll(toSave);
        }
    }

    @Override
    public List<IOBTradeResult> processExits(Long instrumentToken, Double currentPrice) {
        List<IOBTradeResult> openTrades = tradeResultRepository.findOpenTradesByInstrument(instrumentToken);
        List<IOBTradeResult> exitedTrades = new ArrayList<>();

        for (IOBTradeResult trade : openTrades) {
            IOBTradeResult exited = checkExitConditions(trade, currentPrice);
            if (exited != null && "CLOSED".equals(exited.getStatus())) {
                exitedTrades.add(exited);
            }
        }

        return exitedTrades;
    }

    private IOBTradeResult checkExitConditions(IOBTradeResult trade, Double currentPrice) {
        if (trade == null || currentPrice == null) return null;

        String exitReason = null;
        boolean isLong = "LONG".equals(trade.getTradeDirection());

        // Check stop loss
        if (trade.getActualStopLoss() != null) {
            if (isLong && currentPrice <= trade.getActualStopLoss()) {
                exitReason = "STOP_LOSS";
            } else if (!isLong && currentPrice >= trade.getActualStopLoss()) {
                exitReason = "STOP_LOSS";
            }
        }

        // Check targets
        if (exitReason == null && trade.getTarget3() != null) {
            if (isLong && currentPrice >= trade.getTarget3()) {
                exitReason = "TARGET_3";
            } else if (!isLong && currentPrice <= trade.getTarget3()) {
                exitReason = "TARGET_3";
            }
        }

        if (exitReason == null && trade.getTarget2() != null) {
            if (isLong && currentPrice >= trade.getTarget2()) {
                exitReason = "TARGET_2";
            } else if (!isLong && currentPrice <= trade.getTarget2()) {
                exitReason = "TARGET_2";
            }
        }

        if (exitReason == null && trade.getTarget1() != null) {
            if (isLong && currentPrice >= trade.getTarget1()) {
                exitReason = "TARGET_1";
            } else if (!isLong && currentPrice <= trade.getTarget1()) {
                exitReason = "TARGET_1";
            }
        }

        if (exitReason != null) {
            return exitTrade(trade.getId(), exitReason, currentPrice);
        }

        // Update trailing SL
        updateTrailingSLForTrade(trade, currentPrice);

        return trade;
    }

    private void updateTrailingSLForTrade(IOBTradeResult trade, Double currentPrice) {
        double activationR = getConfigDouble("trailingSLActivation", DEFAULT_TRAILING_SL_ACTIVATION);
        double trailDistance = getConfigDouble("trailingSLDistance", DEFAULT_TRAILING_SL_DISTANCE);

        if (trade.getRiskPoints() == null || trade.getRiskPoints() <= 0) return;

        double currentPnlR = calculateCurrentRR(trade, currentPrice);

        if (currentPnlR >= activationR) {
            double newSL;
            boolean isLong = "LONG".equals(trade.getTradeDirection());

            if (isLong) {
                newSL = currentPrice - (trailDistance * trade.getRiskPoints());
                if (trade.getActualStopLoss() == null || newSL > trade.getActualStopLoss()) {
                    trade.setActualStopLoss(newSL);
                    tradeResultRepository.save(trade);
                }
            } else {
                newSL = currentPrice + (trailDistance * trade.getRiskPoints());
                if (trade.getActualStopLoss() == null || newSL < trade.getActualStopLoss()) {
                    trade.setActualStopLoss(newSL);
                    tradeResultRepository.save(trade);
                }
            }
        }
    }

    @Override
    public IOBTradeResult exitTrade(Long tradeResultId, String exitReason, Double exitPrice) {
        Optional<IOBTradeResult> tradeOpt = tradeResultRepository.findById(tradeResultId);
        if (tradeOpt.isEmpty()) return null;

        IOBTradeResult trade = tradeOpt.get();
        if (!"OPEN".equals(trade.getStatus())) return trade;

        trade.setExitPrice(exitPrice);
        trade.setExitTime(LocalDateTime.now());
        trade.setExitReason(exitReason);
        trade.setStatus("CLOSED");

        // Calculate metrics
        trade.calculateMetrics();

        tradeResultRepository.save(trade);

        logger.info("IOB Trade exited: {} {} at {} (Reason: {}, P&L: {})",
                trade.getTradeDirection(), trade.getInstrumentName(), exitPrice,
                exitReason, trade.getPointsCaptured());

        // Send notification
        sendTradeExitNotification(trade);

        // Push WebSocket update
        pushTradeUpdate(trade, "EXIT");

        return trade;
    }

    @Override
    public List<IOBTradeResult> exitAllOpenTrades(String exitReason) {
        List<IOBTradeResult> openTrades = tradeResultRepository.findOpenTrades();
        List<IOBTradeResult> exitedTrades = new ArrayList<>();

        for (IOBTradeResult trade : openTrades) {
            Double currentPrice = getCurrentPrice(trade.getInstrumentToken());
            if (currentPrice != null) {
                IOBTradeResult exited = exitTrade(trade.getId(), exitReason, currentPrice);
                if (exited != null) {
                    exitedTrades.add(exited);
                }
            }
        }

        logger.info("Exited {} open IOB trades with reason: {}", exitedTrades.size(), exitReason);
        return exitedTrades;
    }

    // ==================== Real-Time Processing ====================

    @Override
    public void processPriceTick(Long instrumentToken, Double currentPrice, LocalDateTime tickTime) {
        if (!autoTradingEnabled.get()) return;
        if (instrumentToken == null || currentPrice == null) return;

        // Avoid processing same price twice
        Double lastPrice = lastProcessedPrices.get(instrumentToken);
        if (lastPrice != null && Math.abs(lastPrice - currentPrice) < 0.01) {
            return;
        }
        lastProcessedPrices.put(instrumentToken, currentPrice);

        try {
            // Process exits for open trades
            processExits(instrumentToken, currentPrice);

            // Check for new entries
            if (!isDailyLossLimitReached() && !isMaxOpenTradesReached()) {
                checkForEntries(instrumentToken, currentPrice);
            }

        } catch (Exception e) {
            logger.error("Error processing price tick: {}", e.getMessage());
        }
    }

    @Override
    public List<IOBTradeResult> checkForEntries(Long instrumentToken, Double currentPrice) {
        List<IOBTradeResult> newTrades = new ArrayList<>();

        // Get fresh IOBs for this instrument
        List<InternalOrderBlock> freshIOBs = iobRepository.findFreshIOBs(instrumentToken);

        for (InternalOrderBlock iob : freshIOBs) {
            if (Boolean.TRUE.equals(iob.getTradeTaken())) continue;

            if (shouldEnterTrade(iob, currentPrice)) {
                String trigger = isPriceInZone(iob, currentPrice) ? "ZONE_TOUCH" : "ZONE_APPROACH";
                IOBTradeResult trade = enterTrade(iob, currentPrice, trigger);
                if (trade != null) {
                    newTrades.add(trade);
                }
            }
        }

        return newTrades;
    }

    // ==================== Performance Tracking ====================

    @Override
    public Map<String, Object> getTodaysSummary() {
        LocalDateTime startOfDay = LocalDate.now().atStartOfDay();
        LocalDateTime endOfDay = startOfDay.plusDays(1);

        Map<String, Object> summary = new HashMap<>();

        List<IOBTradeResult> todaysTrades = tradeResultRepository.findTodaysTrades(startOfDay);
        List<IOBTradeResult> openTrades = tradeResultRepository.findOpenTrades();

        long closedCount = todaysTrades.stream().filter(t -> "CLOSED".equals(t.getStatus())).count();
        long winCount = todaysTrades.stream().filter(t -> Boolean.TRUE.equals(t.getIsWinner())).count();

        Double totalPnl = tradeResultRepository.getTotalPnl(startOfDay, endOfDay);
        Double avgRR = tradeResultRepository.getAverageRR(startOfDay, endOfDay);

        summary.put("totalTrades", todaysTrades.size());
        summary.put("openTrades", openTrades.size());
        summary.put("closedTrades", closedCount);
        summary.put("wins", winCount);
        summary.put("losses", closedCount - winCount);
        summary.put("winRate", closedCount > 0 ? (winCount * 100.0 / closedCount) : 0);
        summary.put("totalPnl", totalPnl != null ? totalPnl : 0.0);
        summary.put("averageRR", avgRR != null ? avgRR : 0.0);
        summary.put("autoTradingEnabled", autoTradingEnabled.get());
        summary.put("timestamp", LocalDateTime.now());

        return summary;
    }

    @Override
    public List<IOBTradeResult> getOpenTrades() {
        return tradeResultRepository.findOpenTrades();
    }

    @Override
    public List<IOBTradeResult> getTodaysTrades() {
        return tradeResultRepository.findTodaysTrades(LocalDate.now().atStartOfDay());
    }

    @Override
    public List<IOBTradeResult> getTradeHistory(LocalDate startDate, LocalDate endDate) {
        return tradeResultRepository.findClosedTradesInRange(
                startDate.atStartOfDay(),
                endDate.plusDays(1).atStartOfDay());
    }

    @Override
    public Map<String, Object> getPerformanceStatistics() {
        LocalDateTime startDate = LocalDate.now().minusDays(30).atStartOfDay();
        LocalDateTime endDate = LocalDateTime.now();
        return getPerformanceStatistics(startDate, endDate);
    }

    @Override
    public Map<String, Object> getPerformanceStatistics(LocalDateTime startDate, LocalDateTime endDate) {
        Map<String, Object> stats = new HashMap<>();

        Long totalTrades = tradeResultRepository.countClosedTrades(startDate, endDate);
        Long winningTrades = tradeResultRepository.countWinningTrades(startDate, endDate);
        Double avgRR = tradeResultRepository.getAverageRR(startDate, endDate);
        Double totalPnl = tradeResultRepository.getTotalPnl(startDate, endDate);

        stats.put("period", Map.of("start", startDate, "end", endDate));
        stats.put("totalTrades", totalTrades != null ? totalTrades : 0);
        stats.put("winningTrades", winningTrades != null ? winningTrades : 0);
        stats.put("losingTrades", totalTrades != null && winningTrades != null ? totalTrades - winningTrades : 0);
        stats.put("winRate", totalTrades != null && totalTrades > 0 && winningTrades != null ?
                (winningTrades * 100.0 / totalTrades) : 0);
        stats.put("averageRR", avgRR != null ? avgRR : 0.0);
        stats.put("totalPnl", totalPnl != null ? totalPnl : 0.0);

        // Performance by IOB type
        List<Object[]> byType = tradeResultRepository.getPerformanceByIOBType();
        stats.put("performanceByType", formatPerformanceData(byType));

        // Performance by timeframe
        List<Object[]> byTimeframe = tradeResultRepository.getPerformanceByTimeframe();
        stats.put("performanceByTimeframe", formatPerformanceData(byTimeframe));

        return stats;
    }

    private List<Map<String, Object>> formatPerformanceData(List<Object[]> data) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Object[] row : data) {
            Map<String, Object> item = new HashMap<>();
            item.put("category", row[0]);
            item.put("totalTrades", row[1]);
            item.put("wins", row[2]);
            item.put("avgRR", row[3]);
            result.add(item);
        }
        return result;
    }

    @Override
    public Double getWinRate(LocalDateTime startDate, LocalDateTime endDate) {
        Long total = tradeResultRepository.countClosedTrades(startDate, endDate);
        Long wins = tradeResultRepository.countWinningTrades(startDate, endDate);

        if (total == null || total == 0) return 0.0;
        return (wins != null ? wins : 0) * 100.0 / total;
    }

    @Override
    public Double getAverageRR(LocalDateTime startDate, LocalDateTime endDate) {
        Double avgRR = tradeResultRepository.getAverageRR(startDate, endDate);
        return avgRR != null ? avgRR : 0.0;
    }

    // ==================== Backtesting ====================

    @Override
    public Map<String, Object> runBacktest(Long instrumentToken, LocalDate startDate, LocalDate endDate, String timeframe) {
        return runBacktest(instrumentToken, startDate, endDate, timeframe, new HashMap<>());
    }

    @Override
    @Transactional
    public Map<String, Object> runBacktest(Long instrumentToken, LocalDate startDate, LocalDate endDate,
                                            String timeframe, Map<String, Object> params) {
        logger.info("Running backtest for {} from {} to {} on {}", instrumentToken, startDate, endDate, timeframe);

        Map<String, Object> results = new HashMap<>();
        List<IOBTradeResult> backtestTrades = new ArrayList<>();

        try {
            // Fetch historical data
            String kiteInterval = convertTimeframe(timeframe);
            HistoricalDataRequest request = HistoricalDataRequest.builder()
                    .instrumentToken(String.valueOf(instrumentToken))
                    .fromDate(startDate.atStartOfDay())
                    .toDate(endDate.atTime(15, 30))
                    .interval(kiteInterval)
                    .continuous(false)
                    .oi(false)
                    .build();

            HistoricalDataResponse response = instrumentService.getHistoricalData(request);
            if (response == null || !response.isSuccess() || response.getCandles() == null) {
                results.put("success", false);
                results.put("error", "Failed to fetch historical data");
                return results;
            }

            List<HistoricalDataResponse.HistoricalCandle> candles = response.getCandles();
            logger.info("Fetched {} candles for backtest", candles.size());

            // Clear previous backtest results using atomic delete
            clearBacktestResults(instrumentToken);

            // Reset backtest counter for this run
            long runId = System.currentTimeMillis();
            backtestTradeCounter.set(0);

            // Simulate IOB detection and trading on historical data
            int wins = 0;
            int losses = 0;
            double totalPnl = 0;
            double totalRR = 0;
            double totalPointsCaptured = 0;

            // Track processed IOB signatures to prevent duplicates
            Set<String> processedIOBSignatures = new HashSet<>();

            // Get minimum confidence from config (default 85%)
            double minConfidence = getConfigDouble("minConfidence", DEFAULT_MIN_CONFIDENCE);

            // Process candles in sequence
            for (int i = 50; i < candles.size() - 10; i++) {
                // Simulate IOB detection at this point
                List<HistoricalDataResponse.HistoricalCandle> lookbackCandles = candles.subList(Math.max(0, i - 50), i + 1);

                // Detect IOBs in the lookback window using proper detection logic
                List<InternalOrderBlock> detectedIOBs = detectIOBsInCandles(instrumentToken, timeframe, lookbackCandles);

                for (InternalOrderBlock iob : detectedIOBs) {
                    // Check confidence threshold
                    Double confidence = iob.getSignalConfidence();
                    if (confidence == null || confidence < minConfidence) {
                        continue; // Skip low confidence IOBs
                    }

                    // Generate unique signature for this IOB to prevent duplicates
                    String iobSignature = generateBacktestIOBSignature(iob);
                    if (processedIOBSignatures.contains(iobSignature)) {
                        continue; // Skip duplicate IOB
                    }

                    // Check if price enters zone in subsequent candles
                    for (int j = i + 1; j < Math.min(i + 10, candles.size()); j++) {
                        HistoricalDataResponse.HistoricalCandle testCandle = candles.get(j);
                        double testPrice = testCandle.getClose();

                        if (isPriceInZone(iob, testPrice)) {
                            // Mark IOB as processed
                            processedIOBSignatures.add(iobSignature);

                            // Generate unique trade ID
                            String tradeId = String.format("BT_%d_%d", runId, backtestTradeCounter.incrementAndGet());

                            // Simulate trade
                            IOBTradeResult trade = simulateBacktestTrade(iob, testCandle, candles, j, tradeId);
                            if (trade != null) {
                                trade.setTradeMode("BACKTEST");
                                tradeResultRepository.save(trade);
                                backtestTrades.add(trade);

                                if (Boolean.TRUE.equals(trade.getIsWinner())) {
                                    wins++;
                                } else {
                                    losses++;
                                }
                                if (trade.getNetPnl() != null) {
                                    totalPnl += trade.getNetPnl();
                                }
                                if (trade.getAchievedRR() != null) {
                                    totalRR += trade.getAchievedRR();
                                }
                                if (trade.getPointsCaptured() != null) {
                                    totalPointsCaptured += trade.getPointsCaptured();
                                }
                            }
                            break;
                        }
                    }
                }
            }

            int totalTrades = wins + losses;
            results.put("success", true);
            results.put("instrumentToken", instrumentToken);
            results.put("period", Map.of("start", startDate, "end", endDate));
            results.put("timeframe", timeframe);
            results.put("totalTrades", totalTrades);
            results.put("wins", wins);
            results.put("losses", losses);
            results.put("winRate", totalTrades > 0 ? (wins * 100.0 / totalTrades) : 0);
            results.put("totalPnl", totalPnl);
            results.put("averageRR", totalTrades > 0 ? (totalRR / totalTrades) : 0);
            results.put("totalPointsCaptured", totalPointsCaptured);
            results.put("trades", backtestTrades.stream().map(this::tradeToMap).toList());

        } catch (Exception e) {
            logger.error("Backtest error: {}", e.getMessage(), e);
            results.put("success", false);
            results.put("error", e.getMessage());
        }

        return results;
    }

    /**
     * Generate unique signature for backtest IOB to prevent duplicates
     */
    private String generateBacktestIOBSignature(InternalOrderBlock iob) {
        // Use more precise signature including candle time if available
        String candleTimeStr = iob.getObCandleTime() != null
            ? iob.getObCandleTime().format(SIGNATURE_FORMATTER)
            : String.format("%.0f_%.0f", iob.getZoneHigh(), iob.getZoneLow());

        return String.format("%s_%s_%s_%s",
                iob.getInstrumentToken(),
                iob.getObType(),
                candleTimeStr,
                iob.getTimeframe());
    }

    private IOBTradeResult simulateBacktestTrade(InternalOrderBlock iob,
                                                  HistoricalDataResponse.HistoricalCandle entryCandle,
                                                  List<HistoricalDataResponse.HistoricalCandle> candles,
                                                  int entryIndex,
                                                  String tradeId) {
        try {
            if (iob.getStopLoss() == null || iob.getTarget1() == null
                    || iob.getTarget2() == null || iob.getTarget3() == null) {
                logger.warn("Skipping backtest trade — missing SL/target values");
                return null;
            }
            double entryPrice = iob.getZoneMidpoint();
            double stopLoss = iob.getStopLoss();
            double target1 = iob.getTarget1();
            double target2 = iob.getTarget2();
            double target3 = iob.getTarget3();
            boolean isLong = "LONG".equals(iob.getTradeDirection());

            String exitReason = null;
            double exitPrice = 0;
            int exitIndex = entryIndex;

            // Simulate price movement after entry
            for (int i = entryIndex + 1; i < Math.min(entryIndex + 50, candles.size()); i++) {
                HistoricalDataResponse.HistoricalCandle candle = candles.get(i);

                if (isLong) {
                    // Check stop loss
                    if (candle.getLow() <= stopLoss) {
                        exitReason = "STOP_LOSS";
                        exitPrice = stopLoss;
                        exitIndex = i;
                        break;
                    }
                    // Check targets
                    if (candle.getHigh() >= target2) {
                        exitReason = "TARGET_2";
                        exitPrice = target2;
                        exitIndex = i;
                        break;
                    }
                    if (candle.getHigh() >= target1) {
                        exitReason = "TARGET_1";
                        exitPrice = target1;
                        exitIndex = i;
                        break;
                    }
                } else {
                    // Check stop loss
                    if (candle.getHigh() >= stopLoss) {
                        exitReason = "STOP_LOSS";
                        exitPrice = stopLoss;
                        exitIndex = i;
                        break;
                    }
                    // Check targets
                    if (candle.getLow() <= target2) {
                        exitReason = "TARGET_2";
                        exitPrice = target2;
                        exitIndex = i;
                        break;
                    }
                    if (candle.getLow() <= target1) {
                        exitReason = "TARGET_1";
                        exitPrice = target1;
                        exitIndex = i;
                        break;
                    }
                }
            }

            // If no exit, close at last candle
            if (exitReason == null && exitIndex < candles.size() - 1) {
                exitReason = "TIME_EXIT";
                exitPrice = candles.get(Math.min(entryIndex + 49, candles.size() - 1)).getClose();
            }

            if (exitReason == null) return null;

            IOBTradeResult trade = IOBTradeResult.builder()
                    .iobId(iob.getId() != null ? iob.getId() : 0L)
                    .tradeId(tradeId)
                    .instrumentToken(iob.getInstrumentToken())
                    .instrumentName(iob.getInstrumentName())
                    .timeframe(iob.getTimeframe())
                    .iobType(iob.getObType())
                    .tradeDirection(iob.getTradeDirection())
                    .signalConfidence(iob.getSignalConfidence())
                    .hasFvg(iob.getHasFvg())
                    .zoneHigh(iob.getZoneHigh())
                    .zoneLow(iob.getZoneLow())
                    .plannedEntry(iob.getEntryPrice())
                    .actualEntry(entryPrice)
                    .entryTime(parseTimestamp(entryCandle.getTimestamp()))
                    .entryTrigger("BACKTEST")
                    .plannedStopLoss(stopLoss)
                    .actualStopLoss(stopLoss)
                    .target1(target1)
                    .target2(target2)
                    .target3(target3)
                    .plannedRR(iob.getRiskRewardRatio())
                    .exitPrice(exitPrice)
                    .exitTime(parseTimestamp(candles.get(exitIndex).getTimestamp()))
                    .exitReason(exitReason)
                    .tradeMode("BACKTEST")
                    .status("CLOSED")
                    .quantity(75)
                    .build();

            trade.calculateMetrics();
            return trade;

        } catch (Exception e) {
            logger.error("Error simulating backtest trade: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Detect IOBs in candles using the same logic as InternalOrderBlockServiceImpl.
     * Implements proper swing point detection and displacement validation.
     */
    private List<InternalOrderBlock> detectIOBsInCandles(Long instrumentToken, String timeframe,
                                                          List<HistoricalDataResponse.HistoricalCandle> candles) {
        List<InternalOrderBlock> iobs = new ArrayList<>();

        if (candles.size() < 20) return iobs;

        // Configuration constants matching InternalOrderBlockServiceImpl
        final int SWING_LOOKBACK = 3;
        final double MIN_DISPLACEMENT_BODY_PERCENT = 0.6;

        // Identify swing highs
        List<int[]> swingHighs = new ArrayList<>(); // [index, price as int*100]
        for (int i = SWING_LOOKBACK; i < candles.size() - SWING_LOOKBACK; i++) {
            HistoricalDataResponse.HistoricalCandle current = candles.get(i);
            boolean isSwingHigh = true;
            for (int j = 1; j <= SWING_LOOKBACK; j++) {
                if (candles.get(i - j).getHigh() >= current.getHigh() ||
                    candles.get(i + j).getHigh() >= current.getHigh()) {
                    isSwingHigh = false;
                    break;
                }
            }
            if (isSwingHigh) {
                swingHighs.add(new int[]{i, (int)(current.getHigh() * 100)});
            }
        }

        // Identify swing lows
        List<int[]> swingLows = new ArrayList<>();
        for (int i = SWING_LOOKBACK; i < candles.size() - SWING_LOOKBACK; i++) {
            HistoricalDataResponse.HistoricalCandle current = candles.get(i);
            boolean isSwingLow = true;
            for (int j = 1; j <= SWING_LOOKBACK; j++) {
                if (candles.get(i - j).getLow() <= current.getLow() ||
                    candles.get(i + j).getLow() <= current.getLow()) {
                    isSwingLow = false;
                    break;
                }
            }
            if (isSwingLow) {
                swingLows.add(new int[]{i, (int)(current.getLow() * 100)});
            }
        }

        // Detect bullish IOBs (Break of Structure to the upside)
        for (int i = SWING_LOOKBACK + 5; i < candles.size() - 1; i++) {
            HistoricalDataResponse.HistoricalCandle currentCandle = candles.get(i);

            // Check if current candle broke above a recent swing high
            int[] brokenSwingHigh = null;
            for (int[] sh : swingHighs) {
                if (sh[0] < i && sh[0] > i - 30) {
                    double shPrice = sh[1] / 100.0;
                    if (currentCandle.getClose() > shPrice && currentCandle.getOpen() < shPrice) {
                        brokenSwingHigh = sh;
                        break;
                    }
                }
            }

            if (brokenSwingHigh != null) {
                // Find the last bearish candle before this bullish move
                int obIndex = -1;
                for (int j = i - 1; j >= brokenSwingHigh[0]; j--) {
                    HistoricalDataResponse.HistoricalCandle candle = candles.get(j);
                    if (candle.getClose() < candle.getOpen()) {
                        obIndex = j;
                        break;
                    }
                }

                if (obIndex != -1) {
                    // Verify displacement
                    if (hasValidDisplacement(candles, obIndex, i, true, MIN_DISPLACEMENT_BODY_PERCENT)) {
                        HistoricalDataResponse.HistoricalCandle obCandle = candles.get(obIndex);
                        InternalOrderBlock iob = createBacktestIOB(instrumentToken, timeframe, obCandle, obIndex,
                                candles, "BULLISH_IOB", currentCandle.getClose());
                        if (iob != null && Boolean.TRUE.equals(iob.getIsValid())) {
                            iobs.add(iob);
                        }
                    }
                }
            }

            // Check if current candle broke below a recent swing low
            int[] brokenSwingLow = null;
            for (int[] sl : swingLows) {
                if (sl[0] < i && sl[0] > i - 30) {
                    double slPrice = sl[1] / 100.0;
                    if (currentCandle.getClose() < slPrice && currentCandle.getOpen() > slPrice) {
                        brokenSwingLow = sl;
                        break;
                    }
                }
            }

            if (brokenSwingLow != null) {
                // Find the last bullish candle before this bearish move
                int obIndex = -1;
                for (int j = i - 1; j >= brokenSwingLow[0]; j--) {
                    HistoricalDataResponse.HistoricalCandle candle = candles.get(j);
                    if (candle.getClose() > candle.getOpen()) {
                        obIndex = j;
                        break;
                    }
                }

                if (obIndex != -1) {
                    // Verify displacement
                    if (hasValidDisplacement(candles, obIndex, i, false, MIN_DISPLACEMENT_BODY_PERCENT)) {
                        HistoricalDataResponse.HistoricalCandle obCandle = candles.get(obIndex);
                        InternalOrderBlock iob = createBacktestIOB(instrumentToken, timeframe, obCandle, obIndex,
                                candles, "BEARISH_IOB", currentCandle.getClose());
                        if (iob != null && Boolean.TRUE.equals(iob.getIsValid())) {
                            iobs.add(iob);
                        }
                    }
                }
            }
        }

        return iobs;
    }

    /**
     * Check if there's valid displacement after the OB candle (matching InternalOrderBlockServiceImpl logic)
     */
    private boolean hasValidDisplacement(List<HistoricalDataResponse.HistoricalCandle> candles,
                                         int obIndex, int bosIndex, boolean bullish, double minBodyPercent) {
        if (bosIndex <= obIndex + 1) return false;

        HistoricalDataResponse.HistoricalCandle displacementCandle = candles.get(obIndex + 1);
        double body = Math.abs(displacementCandle.getClose() - displacementCandle.getOpen());
        double range = displacementCandle.getHigh() - displacementCandle.getLow();

        if (range == 0) return false;

        double bodyPercent = body / range;

        // Check direction matches expected
        boolean correctDirection = bullish ?
                displacementCandle.getClose() > displacementCandle.getOpen() :
                displacementCandle.getClose() < displacementCandle.getOpen();

        return bodyPercent >= minBodyPercent && correctDirection;
    }

    /**
     * Check for Fair Value Gap near the IOB
     */
    private boolean checkForFVG(List<HistoricalDataResponse.HistoricalCandle> candles, int obIndex, boolean bullish) {
        if (obIndex + 2 >= candles.size()) {
            return false;
        }

        HistoricalDataResponse.HistoricalCandle candle1 = candles.get(obIndex);
        HistoricalDataResponse.HistoricalCandle candle3 = candles.get(obIndex + 2);

        if (bullish) {
            // Bullish FVG: candle3.low > candle1.high
            return candle3.getLow() > candle1.getHigh();
        } else {
            // Bearish FVG: candle3.high < candle1.low
            return candle3.getHigh() < candle1.getLow();
        }
    }

    private InternalOrderBlock createBacktestIOB(Long instrumentToken, String timeframe,
                                                  HistoricalDataResponse.HistoricalCandle obCandle,
                                                  int obIndex,
                                                  List<HistoricalDataResponse.HistoricalCandle> candles,
                                                  String obType, Double currentPrice) {
        InternalOrderBlock iob = new InternalOrderBlock();
        iob.setInstrumentToken(instrumentToken);
        iob.setInstrumentName(getInstrumentName(instrumentToken));
        iob.setTimeframe(timeframe);
        iob.setObType(obType);
        iob.setObHigh(obCandle.getHigh());
        iob.setObLow(obCandle.getLow());
        iob.setObOpen(obCandle.getOpen());
        iob.setObClose(obCandle.getClose());
        iob.setZoneHigh(obCandle.getHigh());
        iob.setZoneLow(obCandle.getLow());
        iob.setZoneMidpoint((obCandle.getHigh() + obCandle.getLow()) / 2);
        iob.setCurrentPrice(currentPrice);
        iob.setObCandleTime(parseTimestamp(obCandle.getTimestamp()));

        // Check for FVG
        boolean hasFvg = checkForFVG(candles, obIndex, "BULLISH_IOB".equals(obType));
        iob.setHasFvg(hasFvg);

        if ("BULLISH_IOB".equals(obType)) {
            iob.setTradeDirection("LONG");
            iob.setEntryPrice(iob.getZoneMidpoint());
            double sl = iob.getZoneLow() - (iob.getZoneLow() * 0.001);
            iob.setStopLoss(sl);
            double risk = iob.getEntryPrice() - sl;
            iob.setTarget1(iob.getEntryPrice() + risk * 1.5);
            iob.setTarget2(iob.getEntryPrice() + risk * 2.5);
            iob.setTarget3(iob.getEntryPrice() + risk * 4.0);

            // Calculate distance to zone
            double distanceToZone = iob.getZoneHigh() - currentPrice;
            iob.setDistanceToZone(distanceToZone);
            iob.setDistancePercent((distanceToZone / currentPrice) * 100);
        } else {
            iob.setTradeDirection("SHORT");
            iob.setEntryPrice(iob.getZoneMidpoint());
            double sl = iob.getZoneHigh() + (iob.getZoneHigh() * 0.001);
            iob.setStopLoss(sl);
            double risk = sl - iob.getEntryPrice();
            iob.setTarget1(iob.getEntryPrice() - risk * 1.5);
            iob.setTarget2(iob.getEntryPrice() - risk * 2.5);
            iob.setTarget3(iob.getEntryPrice() - risk * 4.0);

            // Calculate distance to zone
            double distanceToZone = currentPrice - iob.getZoneLow();
            iob.setDistanceToZone(distanceToZone);
            iob.setDistancePercent((distanceToZone / currentPrice) * 100);
        }

        // Validate and calculate confidence using same logic as InternalOrderBlockServiceImpl.validateIOB()
        validateIOBAndCalculateConfidence(iob, currentPrice);

        iob.setRiskRewardRatio(2.5);
        return iob;
    }

    /**
     * Get instrument name from token
     */
    private String getInstrumentName(Long instrumentToken) {
        if (instrumentToken.equals(NIFTY_INSTRUMENT_TOKEN)) return "NIFTY";
        return "UNKNOWN";
    }

    /**
     * Validate IOB and calculate confidence score.
     * Uses the same logic as InternalOrderBlockServiceImpl.validateIOB()
     * Sets both signalConfidence and isValid on the IOB.
     */
    private void validateIOBAndCalculateConfidence(InternalOrderBlock iob, Double currentPrice) {
        boolean isValid = true;
        StringBuilder notes = new StringBuilder();
        double confidence = 70.0;

        // Check if zone is reasonable size (not too wide)
        double zoneSize = iob.getZoneHigh() - iob.getZoneLow();
        double zoneSizePercent = (zoneSize / currentPrice) * 100;

        if (zoneSizePercent > 1.0) {
            notes.append("Zone too wide (").append(String.format("%.2f", zoneSizePercent)).append("%). ");
            confidence -= 15;
        }

        if (zoneSizePercent < 0.05) {
            notes.append("Zone too narrow. ");
            confidence -= 10;
        }

        // Boost confidence for ideal zone size
        if (zoneSizePercent >= 0.05 && zoneSizePercent <= 0.3) {
            confidence += 10; // Ideal zone size
        }

        // Check distance from current price to zone
        Double distancePercent = iob.getDistancePercent();
        if (distancePercent != null) {
            double absDistancePercent = Math.abs(distancePercent);
            if (absDistancePercent > 2.0) {
                notes.append("Price far from zone (").append(String.format("%.2f", absDistancePercent)).append("%). ");
                confidence -= 10;
            } else if (absDistancePercent < 0.5) {
                confidence += 10; // Price close to zone (fresh)
            }
        }

        // Boost confidence if FVG present
        if (Boolean.TRUE.equals(iob.getHasFvg())) {
            notes.append("FVG confluence present. ");
            confidence += 15;
        }

        // Check if price is already inside or past the zone (may be mitigated)
        if ("BULLISH_IOB".equals(iob.getObType())) {
            if (currentPrice <= iob.getZoneLow()) {
                notes.append("Price below zone - may be mitigated. ");
                confidence -= 20;
            }
        } else {
            if (currentPrice >= iob.getZoneHigh()) {
                notes.append("Price above zone - may be mitigated. ");
                confidence -= 20;
            }
        }

        // Ensure confidence stays within bounds [0, 100]
        confidence = Math.max(0, Math.min(100, confidence));

        // Set validity based on confidence threshold
        iob.setIsValid(confidence >= 50);
        iob.setSignalConfidence(confidence);
        iob.setValidationNotes(notes.toString());
    }

    @Override
    public List<IOBTradeResult> getBacktestResults(Long instrumentToken) {
        return tradeResultRepository.findBacktestTrades(instrumentToken);
    }

    @Override
    @Transactional
    public void clearBacktestResults(Long instrumentToken) {
        // Use atomic delete for better reliability and performance
        int deletedCount = tradeResultRepository.deleteBacktestTradesByInstrument(instrumentToken);
        logger.info("Cleared {} backtest trades for instrument {}", deletedCount, instrumentToken);
    }

    // ==================== Risk Management ====================

    @Override
    public boolean isDailyLossLimitReached() {
        LocalDateTime startOfDay = LocalDate.now().atStartOfDay();
        LocalDateTime endOfDay = startOfDay.plusDays(1);

        Double todayPnl = tradeResultRepository.getTotalPnl(startOfDay, endOfDay);
        double dailyLimit = getConfigDouble("dailyLossLimit", DEFAULT_DAILY_LOSS_LIMIT);

        return todayPnl != null && todayPnl < -dailyLimit;
    }

    @Override
    public boolean isMaxOpenTradesReached() {
        List<IOBTradeResult> openTrades = tradeResultRepository.findOpenTrades();
        int maxTrades = getConfigInt("maxOpenTrades", DEFAULT_MAX_OPEN_TRADES);
        return openTrades.size() >= maxTrades;
    }

    @Override
    public Double getPortfolioHeat() {
        List<IOBTradeResult> openTrades = tradeResultRepository.findOpenTrades();
        double totalRisk = 0;

        for (IOBTradeResult trade : openTrades) {
            if (trade.getRiskPoints() != null && trade.getQuantity() != null) {
                totalRisk += trade.getRiskPoints() * trade.getQuantity();
            }
        }

        return totalRisk;
    }

    @Override
    public Integer calculatePositionSize(Double entryPrice, Double stopLoss, Double riskAmount) {
        if (entryPrice == null || stopLoss == null || riskAmount == null) return 75;

        double riskPerUnit = Math.abs(entryPrice - stopLoss);
        if (riskPerUnit <= 0) return 75;

        int quantity = (int) (riskAmount / riskPerUnit);
        // Round to nearest lot size (75 for NIFTY)
        int lots = Math.max(1, quantity / 75);
        return lots * 75;
    }

    // ==================== Helper Methods ====================

    private boolean isPriceNearZone(InternalOrderBlock iob, Double currentPrice, double maxDistancePercent) {
        if (iob.getZoneHigh() == null || iob.getZoneLow() == null) return false;

        double zoneMid = (iob.getZoneHigh() + iob.getZoneLow()) / 2;
        double distancePercent = Math.abs(currentPrice - zoneMid) / zoneMid * 100;

        return distancePercent <= maxDistancePercent;
    }

    private boolean isPriceInZone(InternalOrderBlock iob, Double currentPrice) {
        if (iob.getZoneHigh() == null || iob.getZoneLow() == null) return false;
        return currentPrice >= iob.getZoneLow() && currentPrice <= iob.getZoneHigh();
    }

    private boolean isPriceAtMidpoint(InternalOrderBlock iob, Double currentPrice) {
        if (iob.getZoneMidpoint() == null) return false;
        double tolerance = (iob.getZoneHigh() - iob.getZoneLow()) * 0.1;
        return Math.abs(currentPrice - iob.getZoneMidpoint()) <= tolerance;
    }

    private double calculateCurrentRR(IOBTradeResult trade, Double currentPrice) {
        if (trade.getActualEntry() == null || trade.getRiskPoints() == null || trade.getRiskPoints() <= 0) {
            return 0;
        }

        double pnlPoints;
        if ("LONG".equals(trade.getTradeDirection())) {
            pnlPoints = currentPrice - trade.getActualEntry();
        } else {
            pnlPoints = trade.getActualEntry() - currentPrice;
        }

        return pnlPoints / trade.getRiskPoints();
    }

    private Double getCurrentPrice(Long instrumentToken) {
        // This would typically fetch from a price cache or make an API call
        // For now, return null - real implementation would integrate with price feed
        return lastProcessedPrices.get(instrumentToken);
    }

    private double getConfigDouble(String key, double defaultValue) {
        Object value = autoTradeConfig.get(key);
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return defaultValue;
    }

    private int getConfigInt(String key, int defaultValue) {
        Object value = autoTradeConfig.get(key);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return defaultValue;
    }

    private boolean getConfigBoolean(String key, boolean defaultValue) {
        Object value = autoTradeConfig.get(key);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return defaultValue;
    }

    private String convertTimeframe(String timeframe) {
        switch (timeframe) {
            case "5min": return "5minute";
            case "15min": return "15minute";
            case "1hour": return "60minute";
            case "daily": return "day";
            default: return "5minute";
        }
    }

    private LocalDateTime parseTimestamp(String timestamp) {
        if (timestamp == null || timestamp.isBlank()) {
            logger.warn("parseTimestamp called with null/blank value — returning null");
            return null;
        }
        try {
            return java.time.ZonedDateTime.parse(timestamp,
                    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ")).toLocalDateTime();
        } catch (Exception e) {
            try {
                return java.time.ZonedDateTime.parse(timestamp).toLocalDateTime();
            } catch (Exception e2) {
                logger.warn("Failed to parse timestamp '{}' — returning null", timestamp);
                return null;
            }
        }
    }

    /**
     * Generate a unique signature for an IOB to prevent duplicate trades.
     * The signature is based on the IOB type, zone levels, and timeframe.
     */
    private String generateIOBSignature(InternalOrderBlock iob) {
        return String.format("%s_%s_%.2f_%.2f_%s",
                iob.getInstrumentToken(),
                iob.getObType(),
                iob.getZoneHigh() != null ? iob.getZoneHigh() : 0.0,
                iob.getZoneLow() != null ? iob.getZoneLow() : 0.0,
                iob.getTimeframe());
    }

    private Map<String, Object> tradeToMap(IOBTradeResult trade) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", trade.getId());
        map.put("tradeId", trade.getTradeId());
        map.put("iobId", trade.getIobId());
        map.put("instrumentToken", trade.getInstrumentToken());
        map.put("instrumentName", trade.getInstrumentName());
        map.put("timeframe", trade.getTimeframe());
        map.put("iobType", trade.getIobType());
        map.put("tradeDirection", trade.getTradeDirection());
        map.put("signalConfidence", trade.getSignalConfidence());
        map.put("hasFvg", trade.getHasFvg());
        map.put("zoneHigh", trade.getZoneHigh());
        map.put("zoneLow", trade.getZoneLow());
        map.put("plannedEntry", trade.getPlannedEntry());
        map.put("actualEntry", trade.getActualEntry());
        map.put("entryTime", trade.getEntryTime());
        map.put("entryTrigger", trade.getEntryTrigger());
        map.put("plannedStopLoss", trade.getPlannedStopLoss());
        map.put("actualStopLoss", trade.getActualStopLoss());
        map.put("target1", trade.getTarget1());
        map.put("target2", trade.getTarget2());
        map.put("target3", trade.getTarget3());
        map.put("plannedRR", trade.getPlannedRR());
        map.put("exitPrice", trade.getExitPrice());
        map.put("exitTime", trade.getExitTime());
        map.put("exitReason", trade.getExitReason());
        map.put("pointsCaptured", trade.getPointsCaptured());
        map.put("riskPoints", trade.getRiskPoints());
        map.put("achievedRR", trade.getAchievedRR());
        map.put("isWinner", trade.getIsWinner());
        map.put("targetHit", trade.getTargetHit());
        map.put("quantity", trade.getQuantity());
        map.put("grossPnl", trade.getGrossPnl());
        map.put("netPnl", trade.getNetPnl());
        map.put("status", trade.getStatus());
        map.put("tradeMode", trade.getTradeMode());
        map.put("htfAligned", trade.getHtfAligned());
        map.put("mtfConfluenceScore", trade.getMtfConfluenceScore());
        return map;
    }

    private void sendTradeEntryNotification(IOBTradeResult trade, InternalOrderBlock iob) {
        if (telegramNotificationService == null || !telegramNotificationService.isConfigured()) return;

        try {
            String emoji = "LONG".equals(trade.getTradeDirection()) ? "🟢" : "🔴";
            String title = String.format("%s IOB Trade Entry - %s", emoji, trade.getInstrumentName());

            Map<String, Object> data = new HashMap<>();
            data.put("Trade ID", trade.getTradeId());
            data.put("Direction", trade.getTradeDirection());
            data.put("Entry", String.format("₹%.2f", trade.getActualEntry()));
            data.put("Stop Loss", String.format("₹%.2f", trade.getActualStopLoss()));
            data.put("Target 1", String.format("₹%.2f", trade.getTarget1()));
            data.put("Target 2", String.format("₹%.2f", trade.getTarget2()));
            data.put("Trigger", trade.getEntryTrigger());
            data.put("Confidence", String.format("%.1f%%", trade.getSignalConfidence()));

            String message = String.format("Entered %s trade on %s at ₹%.2f",
                    trade.getTradeDirection(), trade.getInstrumentName(), trade.getActualEntry());

            telegramNotificationService.sendTradeAlertAsync(title, message, data);
        } catch (Exception e) {
            logger.warn("Failed to send trade entry notification: {}", e.getMessage());
        }
    }

    private void sendTradeExitNotification(IOBTradeResult trade) {
        if (telegramNotificationService == null || !telegramNotificationService.isConfigured()) return;

        try {
            String emoji = Boolean.TRUE.equals(trade.getIsWinner()) ? "✅" : "❌";
            String title = String.format("%s IOB Trade Exit - %s", emoji, trade.getInstrumentName());

            Map<String, Object> data = new HashMap<>();
            data.put("Trade ID", trade.getTradeId());
            data.put("Direction", trade.getTradeDirection());
            data.put("Entry", String.format("₹%.2f", trade.getActualEntry()));
            data.put("Exit", String.format("₹%.2f", trade.getExitPrice()));
            data.put("Exit Reason", trade.getExitReason());
            data.put("Points", String.format("%.2f", trade.getPointsCaptured()));
            data.put("RR Achieved", String.format("%.2f", trade.getAchievedRR()));
            data.put("Result", Boolean.TRUE.equals(trade.getIsWinner()) ? "WIN ✓" : "LOSS ✗");

            String message = String.format("Exited %s trade on %s - %s (%.2f pts)",
                    trade.getTradeDirection(), trade.getInstrumentName(),
                    trade.getExitReason(), trade.getPointsCaptured());

            telegramNotificationService.sendTradeAlertAsync(title, message, data);
        } catch (Exception e) {
            logger.warn("Failed to send trade exit notification: {}", e.getMessage());
        }
    }

    private void pushTradeUpdate(IOBTradeResult trade, String eventType) {
        if (messagingTemplate == null) return;

        try {
            Map<String, Object> update = new HashMap<>();
            update.put("eventType", eventType);
            update.put("trade", tradeToMap(trade));
            update.put("timestamp", LocalDateTime.now());

            messagingTemplate.convertAndSend(SIMPLE_BROKER + IOB_SIGNAL_TOPIC, update);
        } catch (Exception e) {
            logger.warn("Failed to push trade update via WebSocket: {}", e.getMessage());
        }
    }
}
