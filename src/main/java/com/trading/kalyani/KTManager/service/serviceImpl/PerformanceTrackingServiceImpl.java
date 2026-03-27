package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.entity.InternalOrderBlock;
import com.trading.kalyani.KTManager.entity.PerformanceMetrics;
import com.trading.kalyani.KTManager.entity.TradeResult;
import com.trading.kalyani.KTManager.repository.PerformanceMetricsRepository;
import com.trading.kalyani.KTManager.repository.TradeResultRepository;
import com.trading.kalyani.KTManager.service.PerformanceTrackingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Implementation of PerformanceTrackingService for tracking IOB trade performance.
 */
@Service
public class PerformanceTrackingServiceImpl implements PerformanceTrackingService {

    private static final Logger logger = LoggerFactory.getLogger(PerformanceTrackingServiceImpl.class);

    @Autowired
    private TradeResultRepository tradeResultRepository;

    @Autowired
    private PerformanceMetricsRepository metricsRepository;

    // Default transaction costs
    private static final double DEFAULT_BROKERAGE_PER_ORDER = 20.0;
    private static final double STT_RATE = 0.00025; // 0.025% on sell side
    private static final double EXCHANGE_TXN_CHARGE = 0.0000345;
    private static final double SEBI_CHARGE = 0.000001;
    private static final double STAMP_DUTY = 0.00003;
    private static final double GST_RATE = 0.18;

    // ==================== Trade Result Management ====================

    @Override
    @Transactional
    public TradeResult createTradeFromIOB(InternalOrderBlock iob, Double entryPrice,
                                          Integer quantity, String tradeType, String entryReason) {
        logger.info("Creating trade result from IOB: {}", iob.getId());

        TradeResult trade = TradeResult.builder()
                .iobId(iob.getId())
                .tradeId("TR_" + iob.getId() + "_" + System.currentTimeMillis())
                .instrumentToken(iob.getInstrumentToken())
                .instrumentName(iob.getInstrumentName())
                .timeframe(iob.getTimeframe())
                .tradeDirection(iob.getTradeDirection())
                .tradeType(tradeType != null ? tradeType : "SIMULATED")
                .entryPrice(entryPrice)
                .entryTime(LocalDateTime.now())
                .entryReason(entryReason != null ? entryReason : "ZONE_TOUCH")
                .stopLoss(iob.getStopLoss())
                .target1(iob.getTarget1())
                .target2(iob.getTarget2())
                .target3(iob.getTarget3())
                .quantity(quantity)
                .lotSize(iob.getLotCount() != null ? quantity / iob.getLotCount() : quantity)
                .positionValue(entryPrice * quantity)
                .iobType(iob.getObType())
                .zoneHigh(iob.getZoneHigh())
                .zoneLow(iob.getZoneLow())
                .signalConfidence(iob.getSignalConfidence())
                .enhancedConfidence(iob.getEnhancedConfidence())
                .hadFvg(iob.getHasFvg())
                .wasTrendAligned(iob.getTrendAligned())
                .volumeType(iob.getVolumeType())
                .status("OPEN")
                .build();

        // Calculate risk amount
        if (iob.getStopLoss() != null) {
            double riskPerUnit = Math.abs(entryPrice - iob.getStopLoss());
            trade.setRiskAmount(riskPerUnit * quantity);
        }
        trade.setPlannedRRRatio(iob.getRiskRewardRatio());

        tradeResultRepository.save(trade);
        logger.info("Created trade result: {} for IOB {}", trade.getTradeId(), iob.getId());

        return trade;
    }

    @Override
    @Transactional
    public TradeResult recordTradeEntry(Long tradeResultId, Double entryPrice,
                                        LocalDateTime entryTime, Integer quantity) {
        TradeResult trade = tradeResultRepository.findById(tradeResultId)
                .orElseThrow(() -> new RuntimeException("Trade not found: " + tradeResultId));

        trade.setEntryPrice(entryPrice);
        trade.setEntryTime(entryTime);
        trade.setQuantity(quantity);
        trade.setPositionValue(entryPrice * quantity);

        // Recalculate risk
        if (trade.getStopLoss() != null) {
            double riskPerUnit = Math.abs(entryPrice - trade.getStopLoss());
            trade.setRiskAmount(riskPerUnit * quantity);
        }

        return tradeResultRepository.save(trade);
    }

    @Override
    @Transactional
    public TradeResult closeTradeResult(Long tradeResultId, Double exitPrice,
                                        LocalDateTime exitTime, String exitReason) {
        TradeResult trade = tradeResultRepository.findById(tradeResultId)
                .orElseThrow(() -> new RuntimeException("Trade not found: " + tradeResultId));

        trade.setExitPrice(exitPrice);
        trade.setExitTime(exitTime);
        trade.setExitReason(exitReason);
        trade.setStatus("CLOSED");

        // Determine which target was hit
        determineTargetHit(trade, exitPrice, exitReason);

        // Calculate transaction costs
        calculateTransactionCosts(trade);

        // Calculate P&L and metrics
        trade.calculateMetrics();

        tradeResultRepository.save(trade);
        logger.info("Closed trade {}: Outcome={}, P&L={}",
                trade.getTradeId(), trade.getOutcome(), trade.getNetPnl());

        return trade;
    }

    private void determineTargetHit(TradeResult trade, Double exitPrice, String exitReason) {
        if (exitReason != null && exitReason.startsWith("TARGET")) {
            trade.setTargetHit(exitReason);
            trade.setStopLossHit(false);
        } else if ("STOP_LOSS".equals(exitReason) || "TRAILING_SL".equals(exitReason)) {
            trade.setTargetHit("NONE");
            trade.setStopLossHit(true);
        } else {
            // Determine based on exit price
            boolean isLong = "LONG".equals(trade.getTradeDirection());

            if (trade.getTarget3() != null) {
                if ((isLong && exitPrice >= trade.getTarget3()) ||
                    (!isLong && exitPrice <= trade.getTarget3())) {
                    trade.setTargetHit("TARGET_3");
                    return;
                }
            }
            if (trade.getTarget2() != null) {
                if ((isLong && exitPrice >= trade.getTarget2()) ||
                    (!isLong && exitPrice <= trade.getTarget2())) {
                    trade.setTargetHit("TARGET_2");
                    return;
                }
            }
            if (trade.getTarget1() != null) {
                if ((isLong && exitPrice >= trade.getTarget1()) ||
                    (!isLong && exitPrice <= trade.getTarget1())) {
                    trade.setTargetHit("TARGET_1");
                    return;
                }
            }
            trade.setTargetHit("NONE");
            trade.setStopLossHit(trade.getStopLoss() != null &&
                ((isLong && exitPrice <= trade.getStopLoss()) ||
                 (!isLong && exitPrice >= trade.getStopLoss())));
        }
    }

    private void calculateTransactionCosts(TradeResult trade) {
        if (trade.getPositionValue() == null || trade.getQuantity() == null) return;

        double turnover = trade.getPositionValue() * 2; // Entry + Exit

        // Brokerage (flat per order for F&O)
        trade.setBrokerage(DEFAULT_BROKERAGE_PER_ORDER * 2);

        // Calculate taxes and charges
        double stt = trade.getPositionValue() * STT_RATE; // STT on sell side
        double exchangeCharge = turnover * EXCHANGE_TXN_CHARGE;
        double sebiCharge = turnover * SEBI_CHARGE;
        double stampDuty = trade.getPositionValue() * STAMP_DUTY;
        double gst = (trade.getBrokerage() + exchangeCharge) * GST_RATE;

        trade.setTaxes(stt + exchangeCharge + sebiCharge + stampDuty + gst);
    }

    @Override
    @Transactional
    public TradeResult updateTrailingStop(Long tradeResultId, Double trailingStop) {
        TradeResult trade = tradeResultRepository.findById(tradeResultId)
                .orElseThrow(() -> new RuntimeException("Trade not found: " + tradeResultId));

        trade.setTrailingStop(trailingStop);
        return tradeResultRepository.save(trade);
    }

    @Override
    public List<TradeResult> getOpenTrades() {
        return tradeResultRepository.findOpenTrades();
    }

    @Override
    public List<TradeResult> getOpenTradesByInstrument(Long instrumentToken) {
        return tradeResultRepository.findOpenTradesByInstrument(instrumentToken);
    }

    @Override
    public TradeResult getTradeByIOBId(Long iobId) {
        return tradeResultRepository.findByIobId(iobId).orElse(null);
    }

    @Override
    public TradeResult getTradeByTradeId(String tradeId) {
        return tradeResultRepository.findByTradeId(tradeId).orElse(null);
    }

    @Override
    public List<TradeResult> getRecentTrades(int limit) {
        return tradeResultRepository.findRecentTrades(PageRequest.of(0, limit));
    }

    @Override
    public List<TradeResult> getTradesInRange(LocalDateTime startDate, LocalDateTime endDate) {
        return tradeResultRepository.findClosedTradesInRange(startDate, endDate);
    }

    // ==================== Performance Metrics Calculation ====================

    @Override
    @Transactional
    public PerformanceMetrics calculateAllTimeMetrics() {
        logger.info("Calculating all-time performance metrics");

        List<TradeResult> allTrades = tradeResultRepository.findAllClosedTrades();
        if (allTrades.isEmpty()) {
            logger.info("No closed trades found for metrics calculation");
            return createEmptyMetrics("ALL_TIME", LocalDate.now());
        }

        PerformanceMetrics metrics = calculateMetricsFromTrades(allTrades, "ALL_TIME", LocalDate.now());
        metrics.setInstrumentToken(null); // All instruments

        metricsRepository.save(metrics);
        logger.info("Calculated all-time metrics: WinRate={}%, TotalPnl={}",
                metrics.getWinRate(), metrics.getTotalNetPnl());

        return metrics;
    }

    @Override
    @Transactional
    public PerformanceMetrics calculateDailyMetrics(LocalDate date) {
        LocalDateTime startOfDay = date.atStartOfDay();
        LocalDateTime endOfDay = date.atTime(23, 59, 59);

        List<TradeResult> dayTrades = tradeResultRepository.findClosedTradesInRange(startOfDay, endOfDay);

        PerformanceMetrics metrics = calculateMetricsFromTrades(dayTrades, "DAILY", date);
        metricsRepository.save(metrics);

        return metrics;
    }

    @Override
    @Transactional
    public PerformanceMetrics calculateWeeklyMetrics(LocalDate weekStartDate) {
        LocalDateTime startOfWeek = weekStartDate.atStartOfDay();
        LocalDateTime endOfWeek = weekStartDate.plusDays(6).atTime(23, 59, 59);

        List<TradeResult> weekTrades = tradeResultRepository.findClosedTradesInRange(startOfWeek, endOfWeek);

        PerformanceMetrics metrics = calculateMetricsFromTrades(weekTrades, "WEEKLY", weekStartDate);
        metricsRepository.save(metrics);

        return metrics;
    }

    @Override
    @Transactional
    public PerformanceMetrics calculateMonthlyMetrics(int year, int month) {
        LocalDate monthStart = LocalDate.of(year, month, 1);
        LocalDate monthEnd = monthStart.plusMonths(1).minusDays(1);

        LocalDateTime startOfMonth = monthStart.atStartOfDay();
        LocalDateTime endOfMonth = monthEnd.atTime(23, 59, 59);

        List<TradeResult> monthTrades = tradeResultRepository.findClosedTradesInRange(startOfMonth, endOfMonth);

        PerformanceMetrics metrics = calculateMetricsFromTrades(monthTrades, "MONTHLY", monthStart);
        metricsRepository.save(metrics);

        return metrics;
    }

    private PerformanceMetrics calculateMetricsFromTrades(List<TradeResult> trades,
                                                          String periodType, LocalDate metricDate) {
        PerformanceMetrics metrics = new PerformanceMetrics();
        metrics.setMetricDate(metricDate);
        metrics.setPeriodType(periodType);
        metrics.setCalculationTimestamp(LocalDateTime.now());
        metrics.setStrategyType("IOB");

        if (trades.isEmpty()) {
            return createEmptyMetrics(periodType, metricDate);
        }

        // Basic counts
        metrics.setTotalTrades(trades.size());

        Map<String, List<TradeResult>> byOutcome = trades.stream()
                .collect(Collectors.groupingBy(t -> t.getOutcome() != null ? t.getOutcome() : "UNKNOWN"));
        List<TradeResult> winners  = byOutcome.getOrDefault("WIN",       List.of());
        List<TradeResult> losers   = byOutcome.getOrDefault("LOSS",      List.of());
        List<TradeResult> breakeven = byOutcome.getOrDefault("BREAKEVEN", List.of());

        metrics.setWinningTrades(winners.size());
        metrics.setLosingTrades(losers.size());
        metrics.setBreakevenTrades(breakeven.size());

        // Win/Loss rates
        metrics.setWinRate((double) winners.size() / trades.size() * 100);
        metrics.setLossRate((double) losers.size() / trades.size() * 100);

        // Average amounts
        if (!winners.isEmpty()) {
            metrics.setAvgWinAmount(winners.stream()
                    .mapToDouble(t -> t.getNetPnl() != null ? t.getNetPnl() : 0)
                    .average().orElse(0));
            metrics.setLargestWin(winners.stream()
                    .mapToDouble(t -> t.getNetPnl() != null ? t.getNetPnl() : 0)
                    .max().orElse(0));
        }

        if (!losers.isEmpty()) {
            metrics.setAvgLossAmount(Math.abs(losers.stream()
                    .mapToDouble(t -> t.getNetPnl() != null ? t.getNetPnl() : 0)
                    .average().orElse(0)));
            metrics.setLargestLoss(Math.abs(losers.stream()
                    .mapToDouble(t -> t.getNetPnl() != null ? t.getNetPnl() : 0)
                    .min().orElse(0)));
        }

        // Win/Loss Ratio
        if (metrics.getAvgLossAmount() != null && metrics.getAvgLossAmount() > 0) {
            metrics.setWinLossRatio(metrics.getAvgWinAmount() / metrics.getAvgLossAmount());
        }

        // Risk-Reward metrics
        metrics.setAvgRRPlanned(trades.stream()
                .filter(t -> t.getPlannedRRRatio() != null)
                .mapToDouble(TradeResult::getPlannedRRRatio)
                .average().orElse(0));

        metrics.setAvgRRAchieved(trades.stream()
                .filter(t -> t.getAchievedRRRatio() != null)
                .mapToDouble(TradeResult::getAchievedRRRatio)
                .average().orElse(0));

        // Expectancy = (Win% * AvgWin) - (Loss% * AvgLoss)
        double winProb = metrics.getWinRate() / 100;
        double lossProb = metrics.getLossRate() / 100;
        double avgWin = metrics.getAvgWinAmount() != null ? metrics.getAvgWinAmount() : 0;
        double avgLoss = metrics.getAvgLossAmount() != null ? metrics.getAvgLossAmount() : 0;
        metrics.setExpectancy((winProb * avgWin) - (lossProb * avgLoss));

        // Profit Factor
        double totalWins = winners.stream()
                .mapToDouble(t -> t.getNetPnl() != null ? t.getNetPnl() : 0)
                .sum();
        double totalLosses = Math.abs(losers.stream()
                .mapToDouble(t -> t.getNetPnl() != null ? t.getNetPnl() : 0)
                .sum());
        if (totalLosses > 0) {
            metrics.setProfitFactor(totalWins / totalLosses);
        }

        // P&L Totals
        metrics.setTotalGrossPnl(trades.stream()
                .mapToDouble(t -> t.getGrossPnl() != null ? t.getGrossPnl() : 0)
                .sum());
        metrics.setTotalNetPnl(trades.stream()
                .mapToDouble(t -> t.getNetPnl() != null ? t.getNetPnl() : 0)
                .sum());
        metrics.setTotalBrokerage(trades.stream()
                .mapToDouble(t -> t.getBrokerage() != null ? t.getBrokerage() : 0)
                .sum());
        metrics.setTotalTaxes(trades.stream()
                .mapToDouble(t -> t.getTaxes() != null ? t.getTaxes() : 0)
                .sum());

        metrics.setAvgPnlPerTrade(metrics.getTotalNetPnl() / trades.size());
        metrics.setAvgPointsPerTrade(trades.stream()
                .filter(t -> t.getPnlPoints() != null)
                .mapToDouble(TradeResult::getPnlPoints)
                .average().orElse(0));

        // Drawdown calculation
        calculateDrawdown(metrics, trades);

        // Streak analysis
        calculateStreaks(metrics, trades);

        // Trade duration
        metrics.setAvgTradeDurationMinutes(trades.stream()
                .filter(t -> t.getDurationMinutes() != null)
                .mapToDouble(t -> t.getDurationMinutes().doubleValue())
                .average().orElse(0));

        metrics.setAvgWinnerDurationMinutes(winners.stream()
                .filter(t -> t.getDurationMinutes() != null)
                .mapToDouble(t -> t.getDurationMinutes().doubleValue())
                .average().orElse(0));

        metrics.setAvgLoserDurationMinutes(losers.stream()
                .filter(t -> t.getDurationMinutes() != null)
                .mapToDouble(t -> t.getDurationMinutes().doubleValue())
                .average().orElse(0));

        // Target analysis
        calculateTargetMetrics(metrics, trades);

        // IOB-specific metrics
        calculateIOBSpecificMetrics(metrics, trades, winners);

        // Confidence analysis
        calculateConfidenceMetrics(metrics, trades, winners);

        // Time-based analysis
        calculateTimeBasedMetrics(metrics, trades, winners);

        return metrics;
    }

    private void calculateDrawdown(PerformanceMetrics metrics, List<TradeResult> trades) {
        if (trades.isEmpty()) return;

        // Sort by exit time
        List<TradeResult> sortedTrades = trades.stream()
                .filter(t -> t.getExitTime() != null)
                .sorted(Comparator.comparing(TradeResult::getExitTime))
                .toList();

        double runningPnl = 0;
        double peakPnl = 0;
        double maxDrawdown = 0;

        for (TradeResult trade : sortedTrades) {
            runningPnl += (trade.getNetPnl() != null ? trade.getNetPnl() : 0);

            if (runningPnl > peakPnl) {
                peakPnl = runningPnl;
            }

            double drawdown = peakPnl - runningPnl;
            if (drawdown > maxDrawdown) {
                maxDrawdown = drawdown;
            }
        }

        metrics.setMaxDrawdown(maxDrawdown);
        metrics.setPeakEquity(peakPnl);
        metrics.setCurrentDrawdown(peakPnl - runningPnl);

        if (peakPnl > 0) {
            metrics.setMaxDrawdownPercent((maxDrawdown / peakPnl) * 100);
        }
    }

    private void calculateStreaks(PerformanceMetrics metrics, List<TradeResult> trades) {
        if (trades.isEmpty()) return;

        List<TradeResult> sortedTrades = trades.stream()
                .filter(t -> t.getExitTime() != null)
                .sorted(Comparator.comparing(TradeResult::getExitTime))
                .toList();

        int currentWinStreak = 0;
        int currentLossStreak = 0;
        int maxWinStreak = 0;
        int maxLossStreak = 0;

        for (TradeResult trade : sortedTrades) {
            if ("WIN".equals(trade.getOutcome())) {
                currentWinStreak++;
                currentLossStreak = 0;
                maxWinStreak = Math.max(maxWinStreak, currentWinStreak);
            } else if ("LOSS".equals(trade.getOutcome())) {
                currentLossStreak++;
                currentWinStreak = 0;
                maxLossStreak = Math.max(maxLossStreak, currentLossStreak);
            }
        }

        metrics.setCurrentWinStreak(currentWinStreak);
        metrics.setMaxWinStreak(maxWinStreak);
        metrics.setCurrentLossStreak(currentLossStreak);
        metrics.setMaxLossStreak(maxLossStreak);
    }

    private void calculateTargetMetrics(PerformanceMetrics metrics, List<TradeResult> trades) {
        int t1Count = 0, t2Count = 0, t3Count = 0, slCount = 0;

        for (TradeResult trade : trades) {
            if ("TARGET_1".equals(trade.getTargetHit())) t1Count++;
            else if ("TARGET_2".equals(trade.getTargetHit())) t2Count++;
            else if ("TARGET_3".equals(trade.getTargetHit())) t3Count++;

            if (Boolean.TRUE.equals(trade.getStopLossHit())) slCount++;
        }

        metrics.setTarget1HitCount(t1Count);
        metrics.setTarget2HitCount(t2Count);
        metrics.setTarget3HitCount(t3Count);
        metrics.setStopLossHitCount(slCount);

        int total = trades.size();
        if (total > 0) {
            metrics.setTarget1HitRate((double) t1Count / total * 100);
            metrics.setTarget2HitRate((double) t2Count / total * 100);
            metrics.setTarget3HitRate((double) t3Count / total * 100);
        }
    }

    private void calculateIOBSpecificMetrics(PerformanceMetrics metrics,
                                              List<TradeResult> trades,
                                              List<TradeResult> winners) {
        List<TradeResult> bullishTrades = trades.stream()
                .filter(t -> "BULLISH_IOB".equals(t.getIobType()))
                .toList();
        List<TradeResult> bearishTrades = trades.stream()
                .filter(t -> "BEARISH_IOB".equals(t.getIobType()))
                .toList();

        metrics.setBullishIOBTrades(bullishTrades.size());
        metrics.setBearishIOBTrades(bearishTrades.size());

        if (!bullishTrades.isEmpty()) {
            long bullishWins = bullishTrades.stream()
                    .filter(t -> "WIN".equals(t.getOutcome())).count();
            metrics.setBullishWinRate((double) bullishWins / bullishTrades.size() * 100);
        }

        if (!bearishTrades.isEmpty()) {
            long bearishWins = bearishTrades.stream()
                    .filter(t -> "WIN".equals(t.getOutcome())).count();
            metrics.setBearishWinRate((double) bearishWins / bearishTrades.size() * 100);
        }

        // FVG confluence trades
        List<TradeResult> fvgTrades = trades.stream()
                .filter(t -> Boolean.TRUE.equals(t.getHadFvg()))
                .toList();
        metrics.setFvgConfluenceTrades(fvgTrades.size());
        if (!fvgTrades.isEmpty()) {
            long fvgWins = fvgTrades.stream()
                    .filter(t -> "WIN".equals(t.getOutcome())).count();
            metrics.setFvgConfluenceWinRate((double) fvgWins / fvgTrades.size() * 100);
        }

        // Trend-aligned trades
        List<TradeResult> trendTrades = trades.stream()
                .filter(t -> Boolean.TRUE.equals(t.getWasTrendAligned()))
                .toList();
        metrics.setTrendAlignedTrades(trendTrades.size());
        if (!trendTrades.isEmpty()) {
            long trendWins = trendTrades.stream()
                    .filter(t -> "WIN".equals(t.getOutcome())).count();
            metrics.setTrendAlignedWinRate((double) trendWins / trendTrades.size() * 100);
        }
    }

    private void calculateConfidenceMetrics(PerformanceMetrics metrics,
                                             List<TradeResult> trades,
                                             List<TradeResult> winners) {
        List<TradeResult> losers = trades.stream()
                .filter(t -> "LOSS".equals(t.getOutcome()))
                .toList();

        metrics.setAvgConfidenceWinners(winners.stream()
                .filter(t -> t.getSignalConfidence() != null)
                .mapToDouble(TradeResult::getSignalConfidence)
                .average().orElse(0));

        metrics.setAvgConfidenceLosers(losers.stream()
                .filter(t -> t.getSignalConfidence() != null)
                .mapToDouble(TradeResult::getSignalConfidence)
                .average().orElse(0));

        // High confidence trades (> 75%)
        List<TradeResult> highConfTrades = trades.stream()
                .filter(t -> t.getSignalConfidence() != null && t.getSignalConfidence() > 75)
                .toList();
        metrics.setHighConfidenceTrades(highConfTrades.size());
        if (!highConfTrades.isEmpty()) {
            long highConfWins = highConfTrades.stream()
                    .filter(t -> "WIN".equals(t.getOutcome())).count();
            metrics.setHighConfidenceWinRate((double) highConfWins / highConfTrades.size() * 100);
        }
    }

    private void calculateTimeBasedMetrics(PerformanceMetrics metrics,
                                            List<TradeResult> trades,
                                            List<TradeResult> winners) {
        LocalTime noonTime = LocalTime.of(12, 0);

        List<TradeResult> morningTrades = trades.stream()
                .filter(t -> t.getEntryTime() != null &&
                        t.getEntryTime().toLocalTime().isBefore(noonTime))
                .toList();
        List<TradeResult> afternoonTrades = trades.stream()
                .filter(t -> t.getEntryTime() != null &&
                        !t.getEntryTime().toLocalTime().isBefore(noonTime))
                .toList();

        metrics.setMorningSessionTrades(morningTrades.size());
        metrics.setAfternoonSessionTrades(afternoonTrades.size());

        if (!morningTrades.isEmpty()) {
            long morningWins = morningTrades.stream()
                    .filter(t -> "WIN".equals(t.getOutcome())).count();
            metrics.setMorningSessionWinRate((double) morningWins / morningTrades.size() * 100);
        }

        if (!afternoonTrades.isEmpty()) {
            long afternoonWins = afternoonTrades.stream()
                    .filter(t -> "WIN".equals(t.getOutcome())).count();
            metrics.setAfternoonSessionWinRate((double) afternoonWins / afternoonTrades.size() * 100);
        }
    }

    private PerformanceMetrics createEmptyMetrics(String periodType, LocalDate metricDate) {
        PerformanceMetrics metrics = new PerformanceMetrics();
        metrics.setMetricDate(metricDate);
        metrics.setPeriodType(periodType);
        metrics.setCalculationTimestamp(LocalDateTime.now());
        metrics.setStrategyType("IOB");
        metrics.setTotalTrades(0);
        metrics.setWinningTrades(0);
        metrics.setLosingTrades(0);
        metrics.setBreakevenTrades(0);
        metrics.setWinRate(0.0);
        metrics.setTotalNetPnl(0.0);
        return metrics;
    }

    @Override
    public PerformanceMetrics getLatestAllTimeMetrics() {
        return metricsRepository.findLatestAllTimeMetrics().orElse(null);
    }

    @Override
    public List<PerformanceMetrics> getDailyMetrics(int days) {
        return metricsRepository.findRecentDailyMetrics(PageRequest.of(0, days));
    }

    @Override
    public PerformanceMetrics getInstrumentMetrics(Long instrumentToken) {
        List<PerformanceMetrics> metrics = metricsRepository
                .findByInstrumentAndPeriod(instrumentToken, "ALL_TIME");
        return metrics.isEmpty() ? null : metrics.get(0);
    }

    // ==================== Dashboard Data ====================

    @Override
    public Map<String, Object> getPerformanceDashboard() {
        Map<String, Object> dashboard = new HashMap<>();

        // All-time metrics
        PerformanceMetrics allTime = getLatestAllTimeMetrics();
        if (allTime == null) {
            allTime = calculateAllTimeMetrics();
        }
        dashboard.put("allTimeMetrics", convertMetricsToMap(allTime));

        // Recent daily metrics
        List<PerformanceMetrics> dailyMetrics = getDailyMetrics(30);
        dashboard.put("dailyMetrics", dailyMetrics.stream()
                .map(this::convertMetricsToMap)
                .toList());

        // Recent trades
        List<TradeResult> recentTrades = getRecentTrades(20);
        dashboard.put("recentTrades", recentTrades.stream()
                .map(this::convertTradeToMap)
                .toList());

        // Open trades
        List<TradeResult> openTrades = getOpenTrades();
        dashboard.put("openTrades", openTrades.stream()
                .map(this::convertTradeToMap)
                .toList());

        // Quick stats
        Map<String, Object> quickStats = new HashMap<>();
        if (allTime != null) {
            quickStats.put("totalTrades", allTime.getTotalTrades());
            quickStats.put("winRate", allTime.getWinRate());
            quickStats.put("totalPnl", allTime.getTotalNetPnl());
            quickStats.put("profitFactor", allTime.getProfitFactor());
            quickStats.put("expectancy", allTime.getExpectancy());
            quickStats.put("maxDrawdown", allTime.getMaxDrawdown());
            quickStats.put("avgRR", allTime.getAvgRRAchieved());
        }
        dashboard.put("quickStats", quickStats);

        dashboard.put("timestamp", LocalDateTime.now());
        return dashboard;
    }

    @Override
    public List<Map<String, Object>> getEquityCurve(LocalDateTime startDate, LocalDateTime endDate) {
        List<TradeResult> trades = tradeResultRepository.findClosedTradesInRange(startDate, endDate);

        List<Map<String, Object>> equityCurve = new ArrayList<>();
        double cumulativePnl = 0;

        List<TradeResult> sorted = trades.stream()
                .sorted(Comparator.comparing(TradeResult::getExitTime))
                .toList();

        for (TradeResult trade : sorted) {
            cumulativePnl += (trade.getNetPnl() != null ? trade.getNetPnl() : 0);

            Map<String, Object> point = new HashMap<>();
            point.put("date", trade.getExitTime());
            point.put("pnl", trade.getNetPnl());
            point.put("cumulativePnl", cumulativePnl);
            point.put("tradeId", trade.getTradeId());
            point.put("outcome", trade.getOutcome());
            equityCurve.add(point);
        }

        return equityCurve;
    }

    @Override
    public Map<String, Object> getWinLossDistribution() {
        List<Object[]> distribution = tradeResultRepository.countByOutcome();

        Map<String, Object> result = new HashMap<>();
        for (Object[] row : distribution) {
            result.put((String) row[0], row[1]);
        }
        return result;
    }

    @Override
    public Map<String, Object> getPerformanceByIOBType() {
        Map<String, Object> result = new HashMap<>();

        List<TradeResult> bullishTrades = tradeResultRepository.findByIOBType("BULLISH_IOB");
        List<TradeResult> bearishTrades = tradeResultRepository.findByIOBType("BEARISH_IOB");

        result.put("bullish", calculateQuickStats(bullishTrades));
        result.put("bearish", calculateQuickStats(bearishTrades));

        return result;
    }

    @Override
    public Map<String, Object> getPerformanceByConfidence() {
        Map<String, Object> result = new HashMap<>();

        List<TradeResult> highConf = tradeResultRepository.findHighConfidenceTrades();
        List<TradeResult> allTrades = tradeResultRepository.findAllClosedTrades();

        List<TradeResult> lowConf = allTrades.stream()
                .filter(t -> t.getSignalConfidence() == null || t.getSignalConfidence() <= 75)
                .toList();

        result.put("highConfidence", calculateQuickStats(highConf));
        result.put("lowConfidence", calculateQuickStats(lowConf));

        return result;
    }

    @Override
    public Map<String, Object> getPerformanceByTimeOfDay() {
        Map<String, Object> result = new HashMap<>();

        List<TradeResult> allTrades = tradeResultRepository.findAllClosedTrades();
        LocalTime noon = LocalTime.of(12, 0);

        List<TradeResult> morning = allTrades.stream()
                .filter(t -> t.getEntryTime() != null &&
                        t.getEntryTime().toLocalTime().isBefore(noon))
                .toList();

        List<TradeResult> afternoon = allTrades.stream()
                .filter(t -> t.getEntryTime() != null &&
                        !t.getEntryTime().toLocalTime().isBefore(noon))
                .toList();

        result.put("morning", calculateQuickStats(morning));
        result.put("afternoon", calculateQuickStats(afternoon));

        return result;
    }

    @Override
    public Map<String, Object> getDrawdownAnalysis() {
        PerformanceMetrics metrics = getLatestAllTimeMetrics();
        if (metrics == null) {
            metrics = calculateAllTimeMetrics();
        }

        Map<String, Object> result = new HashMap<>();
        result.put("maxDrawdown", metrics.getMaxDrawdown());
        result.put("maxDrawdownPercent", metrics.getMaxDrawdownPercent());
        result.put("currentDrawdown", metrics.getCurrentDrawdown());
        result.put("peakEquity", metrics.getPeakEquity());
        result.put("maxDrawdownDurationDays", metrics.getMaxDrawdownDurationDays());

        return result;
    }

    private Map<String, Object> calculateQuickStats(List<TradeResult> trades) {
        Map<String, Object> stats = new HashMap<>();

        if (trades.isEmpty()) {
            stats.put("count", 0);
            stats.put("winRate", 0.0);
            stats.put("avgPnl", 0.0);
            return stats;
        }

        long wins = trades.stream().filter(t -> "WIN".equals(t.getOutcome())).count();
        double totalPnl = trades.stream()
                .mapToDouble(t -> t.getNetPnl() != null ? t.getNetPnl() : 0)
                .sum();

        stats.put("count", trades.size());
        stats.put("wins", wins);
        stats.put("losses", trades.size() - wins);
        stats.put("winRate", (double) wins / trades.size() * 100);
        stats.put("totalPnl", totalPnl);
        stats.put("avgPnl", totalPnl / trades.size());

        return stats;
    }

    // ==================== Trade Outcome Checking ====================

    @Override
    public void checkTradeOutcomes(Map<Long, Double> currentPrices) {
        List<TradeResult> openTrades = getOpenTrades();

        for (TradeResult trade : openTrades) {
            Double currentPrice = currentPrices.get(trade.getInstrumentToken());
            if (currentPrice == null) continue;

            Map<String, Object> status = checkTradeStatus(trade.getId(), currentPrice);

            if (Boolean.TRUE.equals(status.get("shouldClose"))) {
                String exitReason = (String) status.get("exitReason");
                closeTradeResult(trade.getId(), currentPrice, LocalDateTime.now(), exitReason);
            } else if (status.get("newTrailingStop") != null) {
                updateTrailingStop(trade.getId(), (Double) status.get("newTrailingStop"));
            }
        }
    }

    @Override
    public Map<String, Object> checkTradeStatus(Long tradeResultId, Double currentPrice) {
        TradeResult trade = tradeResultRepository.findById(tradeResultId).orElse(null);
        if (trade == null) {
            return Map.of("error", "Trade not found");
        }

        Map<String, Object> status = new HashMap<>();
        boolean isLong = "LONG".equals(trade.getTradeDirection());

        // Check stop loss
        Double effectiveSL = trade.getTrailingStop() != null ?
                trade.getTrailingStop() : trade.getStopLoss();
        if (effectiveSL != null) {
            boolean slHit = isLong ? currentPrice <= effectiveSL : currentPrice >= effectiveSL;
            if (slHit) {
                status.put("shouldClose", true);
                status.put("exitReason", trade.getTrailingStop() != null ? "TRAILING_SL" : "STOP_LOSS");
                return status;
            }
        }

        // Check targets
        if (trade.getTarget3() != null) {
            boolean t3Hit = isLong ? currentPrice >= trade.getTarget3() :
                    currentPrice <= trade.getTarget3();
            if (t3Hit) {
                status.put("shouldClose", true);
                status.put("exitReason", "TARGET_3");
                return status;
            }
        }

        if (trade.getTarget2() != null) {
            boolean t2Hit = isLong ? currentPrice >= trade.getTarget2() :
                    currentPrice <= trade.getTarget2();
            if (t2Hit) {
                // Could trail stop to entry or Target 1
                Double newTrailingSL = trade.getTarget1() != null ? trade.getTarget1() :
                        trade.getEntryPrice();
                status.put("target2Hit", true);
                status.put("newTrailingStop", newTrailingSL);
            }
        }

        if (trade.getTarget1() != null) {
            boolean t1Hit = isLong ? currentPrice >= trade.getTarget1() :
                    currentPrice <= trade.getTarget1();
            if (t1Hit && trade.getTrailingStop() == null) {
                // Move SL to entry (breakeven)
                status.put("target1Hit", true);
                status.put("newTrailingStop", trade.getEntryPrice());
            }
        }

        status.put("shouldClose", false);
        status.put("currentPrice", currentPrice);
        status.put("pnlPoints", isLong ? currentPrice - trade.getEntryPrice() :
                trade.getEntryPrice() - currentPrice);

        return status;
    }

    // ==================== Scheduled Tasks ====================

    @Override
    @Scheduled(cron = "0 0 16 * * MON-FRI", zone = "Asia/Kolkata") // 4 PM daily
    public void runDailyMetricsCalculation() {
        logger.info("Running daily metrics calculation");
        try {
            // Calculate today's metrics
            calculateDailyMetrics(LocalDate.now());

            // Recalculate all-time metrics
            calculateAllTimeMetrics();

            logger.info("Daily metrics calculation completed");
        } catch (Exception e) {
            logger.error("Error in daily metrics calculation: {}", e.getMessage(), e);
        }
    }

    @Override
    @Transactional
    public void recalculateAllMetrics() {
        logger.info("Recalculating all performance metrics");

        // Calculate all-time metrics
        calculateAllTimeMetrics();

        // Calculate daily metrics for last 30 days
        for (int i = 0; i < 30; i++) {
            LocalDate date = LocalDate.now().minusDays(i);
            calculateDailyMetrics(date);
        }

        logger.info("All metrics recalculation completed");
    }

    // ==================== Helper Methods ====================

    private Map<String, Object> convertMetricsToMap(PerformanceMetrics metrics) {
        if (metrics == null) return new HashMap<>();

        Map<String, Object> map = new HashMap<>();
        map.put("id", metrics.getId());
        map.put("metricDate", metrics.getMetricDate());
        map.put("periodType", metrics.getPeriodType());
        map.put("totalTrades", metrics.getTotalTrades());
        map.put("winningTrades", metrics.getWinningTrades());
        map.put("losingTrades", metrics.getLosingTrades());
        map.put("winRate", metrics.getWinRate());
        map.put("avgWinAmount", metrics.getAvgWinAmount());
        map.put("avgLossAmount", metrics.getAvgLossAmount());
        map.put("winLossRatio", metrics.getWinLossRatio());
        map.put("expectancy", metrics.getExpectancy());
        map.put("profitFactor", metrics.getProfitFactor());
        map.put("totalNetPnl", metrics.getTotalNetPnl());
        map.put("avgPnlPerTrade", metrics.getAvgPnlPerTrade());
        map.put("maxDrawdown", metrics.getMaxDrawdown());
        map.put("maxDrawdownPercent", metrics.getMaxDrawdownPercent());
        map.put("maxWinStreak", metrics.getMaxWinStreak());
        map.put("maxLossStreak", metrics.getMaxLossStreak());
        map.put("avgRRPlanned", metrics.getAvgRRPlanned());
        map.put("avgRRAchieved", metrics.getAvgRRAchieved());
        map.put("target1HitRate", metrics.getTarget1HitRate());
        map.put("target2HitRate", metrics.getTarget2HitRate());
        map.put("target3HitRate", metrics.getTarget3HitRate());
        map.put("fvgConfluenceWinRate", metrics.getFvgConfluenceWinRate());
        map.put("trendAlignedWinRate", metrics.getTrendAlignedWinRate());
        map.put("highConfidenceWinRate", metrics.getHighConfidenceWinRate());
        map.put("calculationTimestamp", metrics.getCalculationTimestamp());

        return map;
    }

    private Map<String, Object> convertTradeToMap(TradeResult trade) {
        if (trade == null) return new HashMap<>();

        Map<String, Object> map = new HashMap<>();
        map.put("id", trade.getId());
        map.put("tradeId", trade.getTradeId());
        map.put("iobId", trade.getIobId());
        map.put("instrumentName", trade.getInstrumentName());
        map.put("tradeDirection", trade.getTradeDirection());
        map.put("tradeType", trade.getTradeType());
        map.put("entryPrice", trade.getEntryPrice());
        map.put("entryTime", trade.getEntryTime());
        map.put("exitPrice", trade.getExitPrice());
        map.put("exitTime", trade.getExitTime());
        map.put("exitReason", trade.getExitReason());
        map.put("stopLoss", trade.getStopLoss());
        map.put("target1", trade.getTarget1());
        map.put("target2", trade.getTarget2());
        map.put("target3", trade.getTarget3());
        map.put("quantity", trade.getQuantity());
        map.put("grossPnl", trade.getGrossPnl());
        map.put("netPnl", trade.getNetPnl());
        map.put("pnlPercent", trade.getPnlPercent());
        map.put("outcome", trade.getOutcome());
        map.put("targetHit", trade.getTargetHit());
        map.put("achievedRRRatio", trade.getAchievedRRRatio());
        map.put("signalConfidence", trade.getSignalConfidence());
        map.put("hadFvg", trade.getHadFvg());
        map.put("wasTrendAligned", trade.getWasTrendAligned());
        map.put("durationMinutes", trade.getDurationMinutes());
        map.put("status", trade.getStatus());

        return map;
    }
}
