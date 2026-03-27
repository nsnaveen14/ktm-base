package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.entity.InternalOrderBlock;
import com.trading.kalyani.KTManager.entity.RiskManagement;
import com.trading.kalyani.KTManager.entity.SimulatedTrade;
import com.trading.kalyani.KTManager.model.HistoricalDataRequest;
import com.trading.kalyani.KTManager.model.HistoricalDataResponse;
import com.trading.kalyani.KTManager.model.HistoricalDataResponse.HistoricalCandle;
import com.trading.kalyani.KTManager.repository.RiskManagementRepository;
import com.trading.kalyani.KTManager.service.InstrumentService;
import com.trading.kalyani.KTManager.service.RiskManagementService;
import com.trading.kalyani.KTManager.service.SimulatedTradingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;

/**
 * Implementation of RiskManagementService for position sizing and risk control.
 */
@Service
public class RiskManagementServiceImpl implements RiskManagementService {

    private static final Logger logger = LoggerFactory.getLogger(RiskManagementServiceImpl.class);

    @Autowired
    private RiskManagementRepository riskManagementRepository;

    @Autowired
    private InstrumentService instrumentService;

    @Autowired(required = false)
    private SimulatedTradingService simulatedTradingService;

    // Default configuration values
    @Value("${risk.default.account.capital:1000000}")
    private Double defaultAccountCapital;

    @Value("${risk.default.risk.per.trade.percent:1.0}")
    private Double defaultRiskPerTradePercent;

    @Value("${risk.default.max.daily.loss.percent:3.0}")
    private Double defaultMaxDailyLossPercent;

    @Value("${risk.default.max.portfolio.heat.percent:6.0}")
    private Double defaultMaxPortfolioHeatPercent;

    @Value("${risk.default.atr.period:14}")
    private Integer defaultAtrPeriod;

    @Value("${risk.default.atr.sl.multiplier:1.5}")
    private Double defaultAtrSlMultiplier;

    // In-memory tracking for daily metrics
    private final Map<Long, Map<String, Object>> openPositions = new ConcurrentHashMap<>();
    private Double dailyRealizedPnl = 0.0;
    private Double dailyUnrealizedPnl = 0.0;
    private Integer dailyTradeCount = 0;

    // ==================== Account Configuration ====================

    @Override
    public RiskManagement initializeAccount(Double accountCapital, Double riskPerTradePercent,
                                            Double maxDailyLossPercent, Double maxPortfolioHeatPercent) {
        RiskManagement config = getOrCreateTodayConfig(null);

        config.setAccountCapital(accountCapital != null ? accountCapital : defaultAccountCapital);
        config.setRiskPerTradePercent(riskPerTradePercent != null ? riskPerTradePercent : defaultRiskPerTradePercent);
        config.setMaxDailyLossPercent(maxDailyLossPercent != null ? maxDailyLossPercent : defaultMaxDailyLossPercent);
        config.setMaxPortfolioHeatPercent(maxPortfolioHeatPercent != null ? maxPortfolioHeatPercent : defaultMaxPortfolioHeatPercent);

        // Calculate derived values
        config.setMaxRiskPerTrade(config.getAccountCapital() * config.getRiskPerTradePercent() / 100);
        config.setMaxDailyLossAmount(config.getAccountCapital() * config.getMaxDailyLossPercent() / 100);

        // Initialize ATR settings
        config.setAtrPeriod(defaultAtrPeriod);
        config.setAtrSlMultiplier(defaultAtrSlMultiplier);

        // Initialize daily metrics
        config.setDailyRealizedPnl(0.0);
        config.setDailyUnrealizedPnl(0.0);
        config.setDailyTotalPnl(0.0);
        config.setDailyTradeCount(0);
        config.setMaxDailyTrades(20); // Default max trades
        config.setDailyLimitReached(false);

        // Initialize portfolio heat
        config.setOpenPositionsCount(0);
        config.setTotalOpenRisk(0.0);
        config.setPortfolioHeatPercent(0.0);
        config.setPortfolioHeatExceeded(false);
        config.setRemainingRiskCapacity(config.getAccountCapital() * config.getMaxPortfolioHeatPercent() / 100);

        // Set exposure limits
        config.setMaxInstrumentExposurePercent(20.0); // Max 20% per instrument
        config.setMaxCorrelatedExposurePercent(40.0); // Max 40% for correlated group

        // Trading allowed by default
        config.setTradingAllowed(true);
        config.setRiskAssessmentScore(100.0);

        riskManagementRepository.save(config);
        logger.info("Initialized risk management: Capital={}, Risk/Trade={}%, Max Daily Loss={}%",
                config.getAccountCapital(), config.getRiskPerTradePercent(), config.getMaxDailyLossPercent());

        return config;
    }

    @Override
    public RiskManagement getAccountConfig() {
        return riskManagementRepository.findTopByInstrumentTokenIsNullOrderByAnalysisTimestampDesc()
                .orElseGet(() -> initializeAccount(null, null, null, null));
    }

    @Override
    public void updateAccountCapital(Double newCapital) {
        RiskManagement config = getAccountConfig();
        config.setAccountCapital(newCapital);
        config.setMaxRiskPerTrade(newCapital * config.getRiskPerTradePercent() / 100);
        config.setMaxDailyLossAmount(newCapital * config.getMaxDailyLossPercent() / 100);
        riskManagementRepository.save(config);
    }

    @Override
    public void updateRiskParameters(Double riskPerTradePercent, Double maxDailyLossPercent,
                                     Double maxPortfolioHeatPercent) {
        RiskManagement config = getAccountConfig();

        if (riskPerTradePercent != null) {
            config.setRiskPerTradePercent(riskPerTradePercent);
            config.setMaxRiskPerTrade(config.getAccountCapital() * riskPerTradePercent / 100);
        }
        if (maxDailyLossPercent != null) {
            config.setMaxDailyLossPercent(maxDailyLossPercent);
            config.setMaxDailyLossAmount(config.getAccountCapital() * maxDailyLossPercent / 100);
        }
        if (maxPortfolioHeatPercent != null) {
            config.setMaxPortfolioHeatPercent(maxPortfolioHeatPercent);
        }

        riskManagementRepository.save(config);
    }

    // ==================== Position Sizing ====================

    @Override
    public Map<String, Object> calculatePositionSize(Long instrumentToken, Double entryPrice,
                                                     Double stopLoss, Integer lotSize) {
        Map<String, Object> result = new HashMap<>();

        RiskManagement config = getAccountConfig();
        Double maxRisk = config.getMaxRiskPerTrade();

        // Calculate risk per unit
        double riskPerUnit = Math.abs(entryPrice - stopLoss);
        if (riskPerUnit <= 0) {
            result.put("error", "Invalid stop loss - same as entry");
            result.put("calculatedQuantity", 0);
            return result;
        }

        // Calculate raw position size
        int rawQuantity = (int) (maxRisk / riskPerUnit);

        // Adjust for lot size
        int lots = lotSize != null && lotSize > 0 ? rawQuantity / lotSize : rawQuantity;
        int adjustedQuantity = lotSize != null && lotSize > 0 ? lots * lotSize : rawQuantity;

        // Calculate actual risk
        double actualRisk = adjustedQuantity * riskPerUnit;
        double positionValue = adjustedQuantity * entryPrice;

        result.put("entryPrice", entryPrice);
        result.put("stopLoss", stopLoss);
        result.put("riskPerUnit", riskPerUnit);
        result.put("maxRiskAllowed", maxRisk);
        result.put("rawQuantity", rawQuantity);
        result.put("lotSize", lotSize);
        result.put("calculatedLots", lots);
        result.put("calculatedQuantity", adjustedQuantity);
        result.put("positionValue", positionValue);
        result.put("actualRiskAmount", actualRisk);
        result.put("riskPercent", (actualRisk / config.getAccountCapital()) * 100);

        // Check against portfolio heat
        double newPortfolioHeat = config.getTotalOpenRisk() + actualRisk;
        double maxHeat = config.getAccountCapital() * config.getMaxPortfolioHeatPercent() / 100;
        result.put("portfolioHeatAfterTrade", (newPortfolioHeat / config.getAccountCapital()) * 100);
        result.put("wouldExceedPortfolioHeat", newPortfolioHeat > maxHeat);

        return result;
    }

    @Override
    public Map<String, Object> calculatePositionSizeForIOB(InternalOrderBlock iob) {
        if (iob == null || iob.getEntryPrice() == null || iob.getStopLoss() == null) {
            return Map.of("error", "Invalid IOB - missing entry or stop loss");
        }

        // Get lot size for instrument
        Integer lotSize = getLotSize(iob.getInstrumentToken());

        return calculatePositionSize(
                iob.getInstrumentToken(),
                iob.getEntryPrice(),
                iob.getStopLoss(),
                lotSize
        );
    }

    @Override
    public Integer getMaxPositionSize(Long instrumentToken, Double entryPrice, Double stopLoss) {
        Map<String, Object> sizing = calculatePositionSize(instrumentToken, entryPrice, stopLoss, null);
        return (Integer) sizing.getOrDefault("calculatedQuantity", 0);
    }

    @Override
    public Double calculateRiskAmount(Integer quantity, Double entryPrice, Double stopLoss) {
        if (quantity == null || entryPrice == null || stopLoss == null) return 0.0;
        return quantity * Math.abs(entryPrice - stopLoss);
    }

    // ==================== ATR-Based Risk ====================

    @Override
    public Double calculateATR(List<HistoricalCandle> candles, int period) {
        if (candles == null || candles.size() < period + 1) {
            return null;
        }

        List<Double> trueRanges = new ArrayList<>();

        for (int i = 1; i < candles.size(); i++) {
            HistoricalCandle current = candles.get(i);
            HistoricalCandle previous = candles.get(i - 1);

            double highLow = current.getHigh() - current.getLow();
            double highPrevClose = Math.abs(current.getHigh() - previous.getClose());
            double lowPrevClose = Math.abs(current.getLow() - previous.getClose());

            double tr = Math.max(highLow, Math.max(highPrevClose, lowPrevClose));
            trueRanges.add(tr);
        }

        // Calculate ATR as SMA of last 'period' true ranges
        int start = Math.max(0, trueRanges.size() - period);
        double sum = 0;
        int count = 0;
        for (int i = start; i < trueRanges.size(); i++) {
            sum += trueRanges.get(i);
            count++;
        }

        return count > 0 ? sum / count : null;
    }

    @Override
    public Double calculateATR(Long instrumentToken, String timeframe, int period) {
        List<HistoricalCandle> candles = fetchHistoricalCandles(instrumentToken, timeframe);
        return calculateATR(candles, period);
    }

    @Override
    public Double calculateDynamicStopLoss(Long instrumentToken, String tradeDirection,
                                           Double entryPrice, Double atrMultiplier) {
        Double atr = calculateATR(instrumentToken, "5min", defaultAtrPeriod);
        if (atr == null || entryPrice == null) return null;

        double slDistance = atr * (atrMultiplier != null ? atrMultiplier : defaultAtrSlMultiplier);

        if ("LONG".equalsIgnoreCase(tradeDirection)) {
            return entryPrice - slDistance;
        } else {
            return entryPrice + slDistance;
        }
    }

    @Override
    public Double getATRPercent(Long instrumentToken, String timeframe) {
        List<HistoricalCandle> candles = fetchHistoricalCandles(instrumentToken, timeframe);
        if (candles == null || candles.isEmpty()) return null;

        Double atr = calculateATR(candles, defaultAtrPeriod);
        Double currentPrice = candles.get(candles.size() - 1).getClose();

        if (atr != null && currentPrice > 0) {
            return (atr / currentPrice) * 100;
        }
        return null;
    }

    @Override
    public Integer adjustPositionSizeForVolatility(Integer basePositionSize, Double currentATRPercent,
                                                   Double normalATRPercent) {
        if (basePositionSize == null || currentATRPercent == null || normalATRPercent == null) {
            return basePositionSize;
        }

        // If current volatility is higher, reduce position size
        double volatilityRatio = normalATRPercent / currentATRPercent;

        // Cap the adjustment between 0.5x and 1.5x
        volatilityRatio = Math.max(0.5, Math.min(1.5, volatilityRatio));

        return (int) (basePositionSize * volatilityRatio);
    }

    // ==================== Daily Loss Limits ====================

    @Override
    public boolean isDailyLossLimitReached() {
        RiskManagement config = getAccountConfig();
        double totalLoss = Math.min(0, dailyRealizedPnl + dailyUnrealizedPnl);
        return Math.abs(totalLoss) >= config.getMaxDailyLossAmount();
    }

    @Override
    public Double getRemainingDailyLoss() {
        RiskManagement config = getAccountConfig();
        double totalLoss = Math.min(0, dailyRealizedPnl + dailyUnrealizedPnl);
        return config.getMaxDailyLossAmount() - Math.abs(totalLoss);
    }

    @Override
    public void recordTradePnl(Double pnl, boolean isRealized) {
        if (pnl == null) return;

        if (isRealized) {
            dailyRealizedPnl += pnl;
            dailyTradeCount++;
        } else {
            dailyUnrealizedPnl = pnl; // Replace unrealized P&L
        }

        updateDailyPnlInConfig();
    }

    @Override
    public void updateUnrealizedPnl(Double unrealizedPnl) {
        this.dailyUnrealizedPnl = unrealizedPnl != null ? unrealizedPnl : 0.0;
        updateDailyPnlInConfig();
    }

    @Override
    public Map<String, Object> getDailyPnlSummary() {
        RiskManagement config = getAccountConfig();
        Map<String, Object> summary = new HashMap<>();

        summary.put("realizedPnl", dailyRealizedPnl);
        summary.put("unrealizedPnl", dailyUnrealizedPnl);
        summary.put("totalPnl", dailyRealizedPnl + dailyUnrealizedPnl);
        summary.put("maxDailyLoss", config.getMaxDailyLossAmount());
        summary.put("remainingLossAllowance", getRemainingDailyLoss());
        summary.put("limitReached", isDailyLossLimitReached());
        summary.put("tradeCount", dailyTradeCount);
        summary.put("maxTrades", config.getMaxDailyTrades());

        return summary;
    }

    @Override
    public boolean isDailyTradeLimitReached() {
        RiskManagement config = getAccountConfig();
        return dailyTradeCount >= config.getMaxDailyTrades();
    }

    @Override
    public void incrementDailyTradeCount() {
        dailyTradeCount++;
        RiskManagement config = getAccountConfig();
        config.setDailyTradeCount(dailyTradeCount);
        riskManagementRepository.save(config);
    }

    // ==================== Portfolio Heat ====================

    @Override
    public Double calculatePortfolioHeat() {
        RiskManagement config = getAccountConfig();
        double totalRisk = openPositions.values().stream()
                .mapToDouble(p -> (Double) p.getOrDefault("riskAmount", 0.0))
                .sum();
        return (totalRisk / config.getAccountCapital()) * 100;
    }

    @Override
    public boolean isPortfolioHeatExceeded() {
        RiskManagement config = getAccountConfig();
        return calculatePortfolioHeat() >= config.getMaxPortfolioHeatPercent();
    }

    @Override
    public Double getRemainingRiskCapacity() {
        RiskManagement config = getAccountConfig();
        double maxRisk = config.getAccountCapital() * config.getMaxPortfolioHeatPercent() / 100;
        double currentRisk = openPositions.values().stream()
                .mapToDouble(p -> (Double) p.getOrDefault("riskAmount", 0.0))
                .sum();
        return maxRisk - currentRisk;
    }

    @Override
    public void addOpenPosition(Long instrumentToken, Double riskAmount, Integer quantity) {
        Map<String, Object> position = new HashMap<>();
        position.put("riskAmount", riskAmount);
        position.put("quantity", quantity);
        position.put("openTime", LocalDateTime.now());
        openPositions.put(instrumentToken, position);

        updatePortfolioHeatInConfig();
    }

    @Override
    public void removeOpenPosition(Long instrumentToken) {
        openPositions.remove(instrumentToken);
        updatePortfolioHeatInConfig();
    }

    @Override
    public Map<Long, Map<String, Object>> getOpenPositionsRisk() {
        return new HashMap<>(openPositions);
    }

    @Override
    public Map<String, Object> getPortfolioHeatSummary() {
        RiskManagement config = getAccountConfig();
        Map<String, Object> summary = new HashMap<>();

        summary.put("openPositionsCount", openPositions.size());
        summary.put("totalOpenRisk", openPositions.values().stream()
                .mapToDouble(p -> (Double) p.getOrDefault("riskAmount", 0.0))
                .sum());
        summary.put("portfolioHeatPercent", calculatePortfolioHeat());
        summary.put("maxHeatPercent", config.getMaxPortfolioHeatPercent());
        summary.put("remainingCapacity", getRemainingRiskCapacity());
        summary.put("exceeded", isPortfolioHeatExceeded());

        return summary;
    }

    // ==================== Exposure Limits ====================

    @Override
    public boolean isInstrumentExposureLimitReached(Long instrumentToken, Double additionalExposure) {
        RiskManagement config = getAccountConfig();
        double maxExposure = config.getAccountCapital() * config.getMaxInstrumentExposurePercent() / 100;

        Map<String, Object> existingPosition = openPositions.get(instrumentToken);
        double currentExposure = existingPosition != null ?
                (Double) existingPosition.getOrDefault("riskAmount", 0.0) : 0.0;

        return (currentExposure + additionalExposure) > maxExposure;
    }

    @Override
    public boolean isCorrelatedExposureLimitReached(String correlationGroup, Double additionalExposure) {
        RiskManagement config = getAccountConfig();
        double maxCorrelated = config.getAccountCapital() * config.getMaxCorrelatedExposurePercent() / 100;

        double currentCorrelated = 0;
        for (Map.Entry<Long, Map<String, Object>> entry : openPositions.entrySet()) {
            if (correlationGroup.equals(getCorrelationGroup(entry.getKey()))) {
                currentCorrelated += (Double) entry.getValue().getOrDefault("riskAmount", 0.0);
            }
        }

        return (currentCorrelated + additionalExposure) > maxCorrelated;
    }

    @Override
    public Map<String, Double> getExposureByCorrelationGroup() {
        Map<String, Double> exposure = new HashMap<>();

        for (Map.Entry<Long, Map<String, Object>> entry : openPositions.entrySet()) {
            String group = getCorrelationGroup(entry.getKey());
            double risk = (Double) entry.getValue().getOrDefault("riskAmount", 0.0);
            exposure.merge(group, risk, Double::sum);
        }

        return exposure;
    }

    @Override
    public String getCorrelationGroup(Long instrumentToken) {
        if (NIFTY_INSTRUMENT_TOKEN.equals(instrumentToken)) {
            return "INDEX";
        }
        return "OTHER";
    }

    // ==================== Risk Metrics ====================

    @Override
    public Double calculateWinRate(int tradeDays) {
        if (simulatedTradingService == null) return null;

        // Get trades from the service - use trade history
        LocalDate endDate = LocalDate.now();
        LocalDate startDate = endDate.minusDays(tradeDays);
        List<SimulatedTrade> trades = simulatedTradingService.getTradeHistory(startDate, endDate);
        if (trades == null || trades.isEmpty()) return 0.0;

        // Filter only closed trades
        List<SimulatedTrade> closedTrades = trades.stream()
                .filter(t -> t.getExitPrice() != null && t.getNetPnl() != null)
                .toList();
        if (closedTrades.isEmpty()) return 0.0;

        long winners = closedTrades.stream()
                .filter(t -> t.getNetPnl() != null && t.getNetPnl() > 0)
                .count();

        return (double) winners / closedTrades.size() * 100;
    }

    @Override
    public Double calculateAverageRRAchieved(int tradeDays) {
        if (simulatedTradingService == null) return null;

        LocalDate endDate = LocalDate.now();
        LocalDate startDate = endDate.minusDays(tradeDays);
        List<SimulatedTrade> trades = simulatedTradingService.getTradeHistory(startDate, endDate);
        if (trades == null || trades.isEmpty()) return 0.0;

        // Filter only closed trades
        List<SimulatedTrade> closedTrades = trades.stream()
                .filter(t -> t.getExitPrice() != null && t.getNetPnl() != null)
                .toList();
        if (closedTrades.isEmpty()) return 0.0;

        double totalRR = 0;
        int count = 0;

        for (SimulatedTrade trade : closedTrades) {
            if (trade.getNetPnl() != null && trade.getEntryPrice() != null && trade.getExitPrice() != null) {
                // Calculate rough RR
                double entryExit = Math.abs(trade.getExitPrice() - trade.getEntryPrice());
                if (entryExit > 0) {
                    totalRR += trade.getNetPnl() / (entryExit * 25); // Approximate
                    count++;
                }
            }
        }

        return count > 0 ? totalRR / count : 0.0;
    }

    @Override
    public Double calculateMaxDrawdown(int tradeDays) {
        if (simulatedTradingService == null) return null;

        LocalDate endDate = LocalDate.now();
        LocalDate startDate = endDate.minusDays(tradeDays);
        List<SimulatedTrade> trades = simulatedTradingService.getTradeHistory(startDate, endDate);
        if (trades == null || trades.isEmpty()) return 0.0;

        // Filter only closed trades
        List<SimulatedTrade> closedTrades = trades.stream()
                .filter(t -> t.getExitPrice() != null && t.getNetPnl() != null)
                .toList();
        if (closedTrades.isEmpty()) return 0.0;

        double peak = 0;
        double maxDrawdown = 0;
        double runningPnl = 0;

        for (SimulatedTrade trade : closedTrades) {
            if (trade.getNetPnl() != null) {
                runningPnl += trade.getNetPnl();
                peak = Math.max(peak, runningPnl);
                double drawdown = peak - runningPnl;
                maxDrawdown = Math.max(maxDrawdown, drawdown);
            }
        }

        RiskManagement config = getAccountConfig();
        return (maxDrawdown / config.getAccountCapital()) * 100;
    }

    @Override
    public Double calculateCurrentDrawdown() {
        RiskManagement config = getAccountConfig();
        Double peak = config.getPeakAccountValue();
        if (peak == null || peak <= 0) return 0.0;

        double currentValue = config.getAccountCapital() + dailyRealizedPnl + dailyUnrealizedPnl;
        return ((peak - currentValue) / peak) * 100;
    }

    @Override
    public Double calculateProfitFactor(int tradeDays) {
        if (simulatedTradingService == null) return null;

        LocalDate endDate = LocalDate.now();
        LocalDate startDate = endDate.minusDays(tradeDays);
        List<SimulatedTrade> trades = simulatedTradingService.getTradeHistory(startDate, endDate);
        if (trades == null || trades.isEmpty()) return 0.0;

        // Filter only closed trades
        List<SimulatedTrade> closedTrades = trades.stream()
                .filter(t -> t.getExitPrice() != null && t.getNetPnl() != null)
                .toList();
        if (closedTrades.isEmpty()) return 0.0;

        double grossProfit = closedTrades.stream()
                .filter(t -> t.getNetPnl() != null && t.getNetPnl() > 0)
                .mapToDouble(SimulatedTrade::getNetPnl)
                .sum();

        double grossLoss = Math.abs(closedTrades.stream()
                .filter(t -> t.getNetPnl() != null && t.getNetPnl() < 0)
                .mapToDouble(SimulatedTrade::getNetPnl)
                .sum());

        return grossLoss > 0 ? grossProfit / grossLoss : grossProfit > 0 ? Double.MAX_VALUE : 0.0;
    }

    @Override
    public Map<String, Object> getRiskMetrics(int tradeDays) {
        Map<String, Object> metrics = new HashMap<>();

        metrics.put("winRate", calculateWinRate(tradeDays));
        metrics.put("avgRRAchieved", calculateAverageRRAchieved(tradeDays));
        metrics.put("maxDrawdownPercent", calculateMaxDrawdown(tradeDays));
        metrics.put("currentDrawdownPercent", calculateCurrentDrawdown());
        metrics.put("profitFactor", calculateProfitFactor(tradeDays));
        metrics.put("tradeDays", tradeDays);

        return metrics;
    }

    // ==================== Trade Approval ====================

    @Override
    public boolean isTradingAllowed() {
        if (isDailyLossLimitReached()) return false;
        if (isDailyTradeLimitReached()) return false;
        if (isPortfolioHeatExceeded()) return false;
        return true;
    }

    @Override
    public Map<String, Object> getTradingStatus() {
        Map<String, Object> status = new HashMap<>();

        status.put("allowed", isTradingAllowed());

        List<String> reasons = new ArrayList<>();
        if (isDailyLossLimitReached()) reasons.add("Daily loss limit reached");
        if (isDailyTradeLimitReached()) reasons.add("Daily trade limit reached");
        if (isPortfolioHeatExceeded()) reasons.add("Portfolio heat exceeded");

        status.put("blockedReasons", reasons);
        status.put("riskAssessmentScore", getRiskAssessmentScore());

        return status;
    }

    @Override
    public Map<String, Object> validateTrade(Long instrumentToken, String tradeDirection,
                                              Double entryPrice, Double stopLoss, Integer quantity) {
        Map<String, Object> validation = new HashMap<>();
        List<String> warnings = new ArrayList<>();
        List<String> errors = new ArrayList<>();

        // Check if trading is allowed
        if (!isTradingAllowed()) {
            errors.add("Trading is currently blocked - check risk limits");
        }

        // Calculate risk for this trade
        double riskAmount = calculateRiskAmount(quantity, entryPrice, stopLoss);
        RiskManagement config = getAccountConfig();

        // Check risk per trade
        if (riskAmount > config.getMaxRiskPerTrade()) {
            errors.add(String.format("Risk (%.2f) exceeds max per trade (%.2f)",
                    riskAmount, config.getMaxRiskPerTrade()));
        }

        // Check remaining daily loss
        if (riskAmount > getRemainingDailyLoss()) {
            warnings.add("Risk exceeds remaining daily loss allowance");
        }

        // Check portfolio heat
        if (isPortfolioHeatExceeded()) {
            errors.add("Portfolio heat limit already exceeded");
        } else {
            double newHeat = calculatePortfolioHeat() + (riskAmount / config.getAccountCapital() * 100);
            if (newHeat > config.getMaxPortfolioHeatPercent()) {
                warnings.add("Trade would exceed portfolio heat limit");
            }
        }

        // Check instrument exposure
        if (isInstrumentExposureLimitReached(instrumentToken, riskAmount)) {
            warnings.add("Instrument exposure limit would be exceeded");
        }

        // Check correlated exposure
        String corrGroup = getCorrelationGroup(instrumentToken);
        if (isCorrelatedExposureLimitReached(corrGroup, riskAmount)) {
            warnings.add("Correlated exposure limit would be exceeded for " + corrGroup);
        }

        validation.put("valid", errors.isEmpty());
        validation.put("errors", errors);
        validation.put("warnings", warnings);
        validation.put("riskAmount", riskAmount);
        validation.put("riskPercent", (riskAmount / config.getAccountCapital()) * 100);

        return validation;
    }

    @Override
    public Double getRiskAssessmentScore() {
        double score = 100.0;

        RiskManagement config = getAccountConfig();

        // Daily loss proximity (max -30)
        double dailyLossUsed = Math.abs(Math.min(0, dailyRealizedPnl + dailyUnrealizedPnl));
        double dailyLossPercent = dailyLossUsed / config.getMaxDailyLossAmount() * 100;
        score -= Math.min(30, dailyLossPercent * 0.5);

        // Portfolio heat (max -30)
        double heatPercent = calculatePortfolioHeat();
        double heatRatio = heatPercent / config.getMaxPortfolioHeatPercent() * 100;
        score -= Math.min(30, heatRatio * 0.3);

        // Trade count (max -20)
        double tradeRatio = (double) dailyTradeCount / config.getMaxDailyTrades() * 100;
        score -= Math.min(20, tradeRatio * 0.2);

        // Current drawdown (max -20)
        double drawdown = calculateCurrentDrawdown();
        score -= Math.min(20, drawdown);

        return Math.max(0, score);
    }

    @Override
    public Map<String, Object> preTradeRiskCheck(InternalOrderBlock iob) {
        if (iob == null) {
            return Map.of("approved", false, "error", "Invalid IOB");
        }

        Map<String, Object> sizing = calculatePositionSizeForIOB(iob);
        Integer quantity = (Integer) sizing.get("calculatedQuantity");
        Double riskAmount = (Double) sizing.get("actualRiskAmount");

        if (quantity == null || quantity <= 0) {
            return Map.of("approved", false, "error", "Cannot calculate position size");
        }

        Map<String, Object> validation = validateTrade(
                iob.getInstrumentToken(),
                iob.getTradeDirection(),
                iob.getEntryPrice(),
                iob.getStopLoss(),
                quantity
        );

        validation.put("positionSizing", sizing);
        validation.put("approved", (Boolean) validation.get("valid"));

        return validation;
    }

    // ==================== Dashboard ====================

    @Override
    public Map<String, Object> getDashboard() {
        Map<String, Object> dashboard = new HashMap<>();

        RiskManagement config = getAccountConfig();
        dashboard.put("accountCapital", config.getAccountCapital());
        dashboard.put("riskPerTradePercent", config.getRiskPerTradePercent());
        dashboard.put("maxRiskPerTrade", config.getMaxRiskPerTrade());

        dashboard.put("dailyPnl", getDailyPnlSummary());
        dashboard.put("portfolioHeat", getPortfolioHeatSummary());
        dashboard.put("tradingStatus", getTradingStatus());
        dashboard.put("riskMetrics", getRiskMetrics(30));
        dashboard.put("alerts", getRiskAlerts());

        return dashboard;
    }

    @Override
    public Map<String, Object> getDailyRiskReport() {
        Map<String, Object> report = new HashMap<>();

        report.put("date", LocalDate.now());
        report.put("dailyPnl", getDailyPnlSummary());
        report.put("portfolioHeat", getPortfolioHeatSummary());
        report.put("exposureByGroup", getExposureByCorrelationGroup());
        report.put("riskMetrics", getRiskMetrics(30));
        report.put("tradingStatus", getTradingStatus());

        return report;
    }

    @Override
    public List<Map<String, Object>> getRiskAlerts() {
        List<Map<String, Object>> alerts = new ArrayList<>();

        RiskManagement config = getAccountConfig();

        // Daily loss warning (>70% used)
        double dailyLossUsed = Math.abs(Math.min(0, dailyRealizedPnl + dailyUnrealizedPnl));
        double dailyLossPercent = (dailyLossUsed / config.getMaxDailyLossAmount()) * 100;
        if (dailyLossPercent >= 70) {
            Map<String, Object> alert = new HashMap<>();
            alert.put("type", "DAILY_LOSS_WARNING");
            alert.put("severity", dailyLossPercent >= 90 ? "CRITICAL" : "WARNING");
            alert.put("message", String.format("Daily loss at %.1f%% of limit", dailyLossPercent));
            alert.put("value", dailyLossPercent);
            alerts.add(alert);
        }

        // Portfolio heat warning
        double heatPercent = calculatePortfolioHeat();
        if (heatPercent >= config.getMaxPortfolioHeatPercent() * 0.7) {
            Map<String, Object> alert = new HashMap<>();
            alert.put("type", "PORTFOLIO_HEAT_WARNING");
            alert.put("severity", heatPercent >= config.getMaxPortfolioHeatPercent() ? "CRITICAL" : "WARNING");
            alert.put("message", String.format("Portfolio heat at %.1f%%", heatPercent));
            alert.put("value", heatPercent);
            alerts.add(alert);
        }

        // Trade count warning
        double tradePercent = ((double) dailyTradeCount / config.getMaxDailyTrades()) * 100;
        if (tradePercent >= 80) {
            Map<String, Object> alert = new HashMap<>();
            alert.put("type", "TRADE_LIMIT_WARNING");
            alert.put("severity", tradePercent >= 100 ? "CRITICAL" : "WARNING");
            alert.put("message", String.format("Trade count at %d/%d", dailyTradeCount, config.getMaxDailyTrades()));
            alert.put("value", dailyTradeCount);
            alerts.add(alert);
        }

        return alerts;
    }

    // ==================== Lifecycle ====================

    @Override
    public void resetDailyMetrics() {
        dailyRealizedPnl = 0.0;
        dailyUnrealizedPnl = 0.0;
        dailyTradeCount = 0;
        openPositions.clear();

        RiskManagement config = getAccountConfig();
        config.setDailyRealizedPnl(0.0);
        config.setDailyUnrealizedPnl(0.0);
        config.setDailyTotalPnl(0.0);
        config.setDailyTradeCount(0);
        config.setDailyLimitReached(false);
        config.setOpenPositionsCount(0);
        config.setTotalOpenRisk(0.0);
        config.setPortfolioHeatPercent(0.0);
        config.setPortfolioHeatExceeded(false);
        config.setTradingAllowed(true);

        riskManagementRepository.save(config);
        logger.info("Daily risk metrics reset");
    }

    @Override
    public void endOfDayProcessing() {
        RiskManagement config = getAccountConfig();

        // Update peak account value if current is higher
        double currentValue = config.getAccountCapital() + dailyRealizedPnl;
        if (config.getPeakAccountValue() == null || currentValue > config.getPeakAccountValue()) {
            config.setPeakAccountValue(currentValue);
        }

        // Update risk metrics
        config.setWinRate(calculateWinRate(30));
        config.setAvgRRachieved(calculateAverageRRAchieved(30));
        config.setMaxDrawdownPercent(calculateMaxDrawdown(30));
        config.setProfitFactor(calculateProfitFactor(30));

        riskManagementRepository.save(config);
        logger.info("End of day risk processing completed. Daily P&L: {}", dailyRealizedPnl);
    }

    @Override
    public void saveRiskState() {
        updateDailyPnlInConfig();
        updatePortfolioHeatInConfig();
    }

    // ==================== Helper Methods ====================

    private RiskManagement getOrCreateTodayConfig(Long instrumentToken) {
        LocalDate today = LocalDate.now();

        Optional<RiskManagement> existing = instrumentToken != null ?
                riskManagementRepository.findByInstrumentTokenAndAnalysisDate(instrumentToken, today) :
                riskManagementRepository.findByInstrumentTokenIsNullAndAnalysisDate(today);

        if (existing.isPresent()) {
            return existing.get();
        }

        RiskManagement config = new RiskManagement();
        config.setInstrumentToken(instrumentToken);
        config.setAnalysisDate(today);
        config.setAnalysisTimestamp(LocalDateTime.now());
        return config;
    }

    private void updateDailyPnlInConfig() {
        RiskManagement config = getAccountConfig();
        config.setDailyRealizedPnl(dailyRealizedPnl);
        config.setDailyUnrealizedPnl(dailyUnrealizedPnl);
        config.setDailyTotalPnl(dailyRealizedPnl + dailyUnrealizedPnl);
        config.setDailyTradeCount(dailyTradeCount);
        config.setDailyLimitReached(isDailyLossLimitReached());

        updateTradingAllowed(config);
        riskManagementRepository.save(config);
    }

    private void updatePortfolioHeatInConfig() {
        RiskManagement config = getAccountConfig();

        double totalRisk = openPositions.values().stream()
                .mapToDouble(p -> (Double) p.getOrDefault("riskAmount", 0.0))
                .sum();

        config.setOpenPositionsCount(openPositions.size());
        config.setTotalOpenRisk(totalRisk);
        config.setPortfolioHeatPercent(calculatePortfolioHeat());
        config.setPortfolioHeatExceeded(isPortfolioHeatExceeded());
        config.setRemainingRiskCapacity(getRemainingRiskCapacity());

        updateTradingAllowed(config);
        riskManagementRepository.save(config);
    }

    private void updateTradingAllowed(RiskManagement config) {
        boolean allowed = isTradingAllowed();
        config.setTradingAllowed(allowed);
        config.setRiskAssessmentScore(getRiskAssessmentScore());

        if (!allowed) {
            List<String> reasons = new ArrayList<>();
            if (isDailyLossLimitReached()) reasons.add("Daily loss limit");
            if (isDailyTradeLimitReached()) reasons.add("Trade limit");
            if (isPortfolioHeatExceeded()) reasons.add("Portfolio heat");
            config.setTradingBlockedReason(String.join(", ", reasons));
        } else {
            config.setTradingBlockedReason(null);
        }
    }

    private Integer getLotSize(Long instrumentToken) {
        // NIFTY lot size is typically 25
        if (NIFTY_INSTRUMENT_TOKEN.equals(instrumentToken)) return 25;
        return 1;
    }

    private List<HistoricalCandle> fetchHistoricalCandles(Long instrumentToken, String timeframe) {
        try {
            LocalDate today = LocalDate.now();
            LocalDateTime fromDate = today.minusDays(5).atStartOfDay();
            LocalDateTime toDate = today.atTime(23, 59, 59);

            HistoricalDataRequest request = new HistoricalDataRequest();
            request.setInstrumentToken(String.valueOf(instrumentToken));
            request.setInterval(timeframe.replace("min", "minute"));
            request.setFromDate(fromDate);
            request.setToDate(toDate);

            HistoricalDataResponse response = instrumentService.getHistoricalData(request);
            return response != null && response.isSuccess() ? response.getCandles() : null;
        } catch (Exception e) {
            logger.error("Error fetching candles: {}", e.getMessage());
            return null;
        }
    }
}
