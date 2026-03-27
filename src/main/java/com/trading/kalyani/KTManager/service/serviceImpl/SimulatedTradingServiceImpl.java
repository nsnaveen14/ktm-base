package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.entity.SimulatedTrade;
import com.trading.kalyani.KTManager.entity.TradingLedger;
import com.trading.kalyani.KTManager.entity.TradeSetupEntity;
import com.trading.kalyani.KTManager.entity.InternalOrderBlock;
import com.trading.kalyani.KTManager.repository.SimulatedTradeRepository;
import com.trading.kalyani.KTManager.repository.TradingLedgerRepository;
import com.trading.kalyani.KTManager.repository.InternalOrderBlockRepository;
import com.trading.kalyani.KTManager.service.CandlePredictionService;

import com.trading.kalyani.KTManager.service.LiquiditySweepService;
import com.trading.kalyani.KTManager.service.InternalOrderBlockService;
import com.trading.kalyani.KTManager.service.SimulatedTradingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.annotation.PostConstruct;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;

/**
 * Implementation of SimulatedTradingService.
 * Provides automated options trading simulation with signal detection,
 * trade placement, management, and P&L tracking.
 */
@Service
public class SimulatedTradingServiceImpl implements SimulatedTradingService {

    private static final Logger logger = LoggerFactory.getLogger(SimulatedTradingServiceImpl.class);

    // Trading constants
    private static final int NIFTY_LOT_SIZE = 65;
    private static final int DEFAULT_NUM_LOTS = 4;
    private static final int DEFAULT_QUANTITY = NIFTY_LOT_SIZE * DEFAULT_NUM_LOTS; // 260 units
    private static final double DEFAULT_TARGET_PERCENT = 20.0; // 20% target
    private static final double DEFAULT_STOPLOSS_PERCENT = 10.0; // 10% stop loss
    private static final LocalTime MARKET_OPEN = LocalTime.of(9, 0);
    private static final LocalTime MARKET_CLOSE = LocalTime.of(15, 30);
    private static final LocalTime NO_NEW_TRADES_AFTER = LocalTime.of(15, 15); // No new trades after 3 PM

    // Trading configuration
    private final AtomicBoolean autoTradingEnabled = new AtomicBoolean(true); // Enabled by default for auto trading
    private volatile double targetPercent = DEFAULT_TARGET_PERCENT;
    private volatile double stoplossPercent = DEFAULT_STOPLOSS_PERCENT;
    private volatile int numLots = DEFAULT_NUM_LOTS;
    private volatile int quantity = DEFAULT_QUANTITY;
    private volatile double maxDailyLoss = -50000.0; // Max daily loss limit
    private volatile int maxDailyTrades = 100; // Max trades per day

    // Signal cooldown per signal key — tracks each source independently
    private final Map<String, LocalDateTime> signalCooldowns = new ConcurrentHashMap<>();
    private static final int SIGNAL_COOLDOWN_MINUTES = 5;

    @Autowired
    private SimulatedTradeRepository tradeRepository;

    @Autowired
    private TradingLedgerRepository ledgerRepository;

    @Lazy
    @Autowired
    private CandlePredictionService candlePredictionService;


    @Autowired
    private LiquiditySweepService liquiditySweepService;

    @Lazy
    @Autowired
    private InternalOrderBlockService internalOrderBlockService;

    @Autowired
    private InternalOrderBlockRepository internalOrderBlockRepository;

    @Autowired
    private com.trading.kalyani.KTManager.config.TradingProperties tradingProperties;

    // IOB trade tracking - tracks which IOBs have active trades to prevent duplicates
    private final Map<Long, String> activeIOBTrades = new ConcurrentHashMap<>();

    // ============= Trade Signal Detection =============

    @Override
    public Map<String, Object> checkForTradeSignals() {
        Map<String, Object> result = new HashMap<>();
        result.put("hasSignal", false);

        LocalTime now = LocalTime.now(IST);
        logger.debug("Checking for trade signals at {}", now);

        if (now.isBefore(MARKET_OPEN) || now.isAfter(NO_NEW_TRADES_AFTER)) {
            result.put("reason", "Outside trading hours");
            return result;
        }

        if (isDailyLimitBreached()) {
            result.put("reason", "Daily limits reached");
            return result;
        }

        // Each source is checked independently and can have its own open trade
        List<Map<String, Object>> actionableSignals = new ArrayList<>();
        List<Map<String, Object>> reverseSignals    = new ArrayList<>();

        collectSignal(checkLiquiditySweepSignalWithReverseCheck(), actionableSignals, reverseSignals);
        collectSignal(checkEMACrossoverSignalWithReverseCheck(),    actionableSignals, reverseSignals);
        collectSignal(checkIOBSignalWithReverseCheck(),             actionableSignals, reverseSignals);

        result.put("allSignals",         actionableSignals);
        result.put("reverseSignals",     reverseSignals);
        result.put("signalCount",        actionableSignals.size());
        result.put("reverseSignalCount", reverseSignals.size());

        if (!reverseSignals.isEmpty()) {
            result.putAll(reverseSignals.get(0));
            result.put("hasSignal", true);
            result.put("hasReverseSignal", true);
        } else if (!actionableSignals.isEmpty()) {
            result.putAll(actionableSignals.get(0));
            result.put("hasSignal", true);
        } else {
            result.put("reason", "No valid signals detected");
        }

        return result;
    }

    private void collectSignal(Map<String, Object> signal,
                               List<Map<String, Object>> actionable,
                               List<Map<String, Object>> reverse) {
        if (Boolean.TRUE.equals(signal.get("hasReverseSignal"))) {
            reverse.add(signal);
        } else if (Boolean.TRUE.equals(signal.get("hasSignal"))) {
            actionable.add(signal);
        }
    }

    /**
     * Check EMA crossover signal with reverse signal detection.
     * If there's an open EMA trade and a reverse signal comes, mark it for exit.
     */
    private Map<String, Object> checkEMACrossoverSignalWithReverseCheck() {
        Map<String, Object> result = signalResult(SOURCE_EMA_CROSSOVER);

        try {
            Map<String, Object> emaData = candlePredictionService.getEMAChartData();
            if (emaData == null || emaData.containsKey("error")) {
                return result;
            }

            String emaAlignment = (String) emaData.get("emaAlignment");
            String tradeSignal  = (String) emaData.get("tradeSignal"); // for crossover strength detection

            if (emaAlignment == null) return result;

            // Direction from current EMA alignment — fires on sustained trend, not just crossover candle
            String signalType     = "BULLISH".equals(emaAlignment) ? "BUY" : "SELL";
            // STRONG on the fresh crossover candle, MODERATE on sustained trend
            String signalStrength = signalType.equals(tradeSignal) ? "STRONG" : "MODERATE";

            logger.debug("EMA - type: {}, strength: {}, alignment: {}", signalType, signalStrength, emaAlignment);

            Optional<SimulatedTrade> openTrade = tradeRepository.findOpenTradeBySignalSource(SOURCE_EMA_CROSSOVER);

            if (openTrade.isPresent()) {
                String existingDirection = openTrade.get().getSignalType();
                if (isReverseSignal(existingDirection, signalType)) {
                    result.put("hasReverseSignal", true);
                    result.put("reverseTradeId", openTrade.get().getTradeId());
                    result.put("signalType", signalType);
                    result.put("signalStrength", signalStrength);
                    result.put("emaAlignment", emaAlignment);
                    logger.info("EMA reverse signal: {} -> {} for trade {}",
                            existingDirection, signalType, openTrade.get().getTradeId());
                }
                return result;
            }

            // No open trade — always surface the alignment signal; cooldown is enforced in processNewSignals (placement path only)
            result.put("hasSignal", true);
            result.put("signalType", signalType);
            result.put("signalStrength", signalStrength);
            result.put("emaAlignment", emaAlignment);
            result.put("ema9",  emaData.get("currentEMA9"));
            result.put("ema21", emaData.get("currentEMA21"));
            logger.info("EMA alignment signal: {} ({}), alignment: {}", signalType, signalStrength, emaAlignment);

        } catch (Exception e) {
            logger.error("Error checking EMA crossover signal: {}", e.getMessage(), e);
        }

        return result;
    }

    private Map<String, Object> checkLiquiditySweepSignalWithReverseCheck() {
        Map<String, Object> result = signalResult(SOURCE_LIQUIDITY_SWEEP);

        try {
            Map<String, Object> sweepSignal = liquiditySweepService.checkLiquiditySweepSignal(1);
            if (sweepSignal == null || !Boolean.TRUE.equals(sweepSignal.get("hasSignal"))) {
                return result;
            }

            String signalType = (String) sweepSignal.get("signalType");
            if (signalType == null || signalType.equals("NONE")) {
                return result;
            }

            String signalStrength = sweepSignal.get("signalStrength") != null
                    ? (String) sweepSignal.get("signalStrength") : "MODERATE";
            Double confidence     = toDouble(sweepSignal, "confidence");
            String whaleType      = (String) sweepSignal.get("whaleType");
            String sweepType      = (String) sweepSignal.get("sweepType");

            logger.debug("Liquidity Sweep - type: {}, strength: {}, confidence: {}, whale: {}",
                    signalType, signalStrength, confidence, whaleType);

            Optional<SimulatedTrade> openTrade = tradeRepository.findOpenTradeBySignalSource(SOURCE_LIQUIDITY_SWEEP);

            if (openTrade.isPresent()) {
                String existingDirection = openTrade.get().getSignalType();
                if (isReverseSignal(existingDirection, signalType)) {
                    result.put("hasReverseSignal", true);
                    result.put("reverseTradeId", openTrade.get().getTradeId());
                    result.put("signalType", signalType);
                    result.put("signalStrength", signalStrength);
                    result.put("confidence", confidence);
                    result.put("whaleType", whaleType);
                    result.put("sweepType", sweepType);
                    logger.info("Liquidity Sweep reverse signal: {} -> {} for trade {}",
                            existingDirection, signalType, openTrade.get().getTradeId());
                }
                return result; // open trade exists — no new entry regardless
            }

            // No open trade — new signal requires confidence >= 60%; cooldown enforced in processNewSignals
            if (confidence != null && confidence >= 60) {
                result.put("hasSignal", true);
                result.put("signalType", signalType);
                result.put("signalStrength", signalStrength);
                result.put("confidence", confidence);
                result.put("whaleType", whaleType);
                result.put("sweepType", sweepType);
                result.put("entryPrice",      sweepSignal.get("entryPrice"));
                result.put("stopLoss",         sweepSignal.get("stopLoss"));
                result.put("takeProfit",       sweepSignal.get("takeProfit"));
                result.put("suggestedOption",  sweepSignal.get("suggestedOption"));
                logger.info("Liquidity Sweep signal: {} ({}) confidence: {}% whale: {} sweep: {}",
                        signalType, signalStrength, confidence, whaleType, sweepType);
            }

        } catch (Exception e) {
            logger.error("Error checking Liquidity Sweep signal: {}", e.getMessage(), e);
        }

        return result;
    }

    private Map<String, Object> signalResult(String source) {
        Map<String, Object> result = new HashMap<>();
        result.put("hasSignal", false);
        result.put("hasReverseSignal", false);
        result.put("signalSource", source);
        return result;
    }

    private boolean isReverseSignal(String existingDirection, String newDirection) {
        return ("BUY".equals(existingDirection) && "SELL".equals(newDirection))
            || ("SELL".equals(existingDirection) && "BUY".equals(newDirection));
    }


    @Override
    public Map<String, Object> checkEMACrossoverSignal() {
        return checkEMACrossoverSignalWithReverseCheck();
    }

    @Override
    public Map<String, Object> checkTradeSetupSignal() {
        Map<String, Object> result = signalResult(SOURCE_TRADE_SETUP);

        try {
            Optional<SimulatedTrade> openSetupTrade = tradeRepository.findOpenTradeBySignalSource(SOURCE_TRADE_SETUP);
            Optional<TradeSetupEntity> optTradeSetup = candlePredictionService.getLatestTradeSetup();

            if (optTradeSetup.isEmpty()) return result;

            TradeSetupEntity tradeSetup = optTradeSetup.get();
            logger.debug("Trade Setup - direction: {}, confidence: {}, valid: {}, createdAt: {}",
                    tradeSetup.getTradeDirection(), tradeSetup.getConfidence(),
                    tradeSetup.getIsValid(), tradeSetup.getCreatedAt());

            if (!Boolean.TRUE.equals(tradeSetup.getIsValid()) || tradeSetup.getCreatedAt() == null) return result;
            if (!tradeSetup.getCreatedAt().isAfter(LocalDateTime.now().minusMinutes(10))) return result;

            String direction = tradeSetup.getTradeDirection();
            Double confidence = tradeSetup.getConfidence();
            if (confidence == null || confidence < 50) return result;

            String signalStrength = getSignalStrengthFromConfidence(confidence);

            if (openSetupTrade.isPresent()) {
                String existingDirection = openSetupTrade.get().getSignalType();
                if (isReverseSignal(existingDirection, direction)) {
                    result.put("hasReverseSignal", true);
                    result.put("reverseTradeId", openSetupTrade.get().getTradeId());
                    result.put("signalType", direction);
                    result.put("signalStrength", signalStrength);
                    result.put("confidence", confidence);
                    result.put("setupId", tradeSetup.getId());
                    logger.info("Trade Setup reverse signal: {} -> {} for trade {}",
                            existingDirection, direction, openSetupTrade.get().getTradeId());
                }
                return result;
            }

            if (isNewSignal("SETUP_" + direction + "_" + tradeSetup.getId())) {
                result.put("hasSignal", true);
                result.put("signalType", direction);
                result.put("signalStrength", signalStrength);
                result.put("confidence", confidence);
                result.put("entryPrice", tradeSetup.getEntryPrice());
                result.put("targetPrice", tradeSetup.getTarget1());
                result.put("stopLoss", tradeSetup.getStopLoss());
                result.put("setupId", tradeSetup.getId());
                logger.info("Trade Setup signal: {} ({}%)", direction, confidence);
            }
        } catch (Exception e) {
            logger.error("Error checking trade setup signal: {}", e.getMessage(), e);
        }
        return result;
    }

    /**
     * Check for IOB (Internal Order Block) based trade signals.
     * Automatically detects fresh IOBs and creates trade signals when:
     * - IOB has confidence >= 50%
     * - Price is within or near the IOB zone
     * - IOB is not yet mitigated
     *
     * Target selection based on confidence:
     * - 85%+ confidence: Target 3
     * - 70-84% confidence: Target 2
     * - 50-69% confidence: Target 1
     */
    @Override
    public Map<String, Object> checkIOBSignal() {
        return checkIOBSignalWithReverseCheck();
    }

    private static final Set<String> TERMINAL_IOB_STATUSES = Set.of("MITIGATED", "EXPIRED", "STOPPED", "COMPLETED");

    private Map<String, Object> checkIOBSignalWithReverseCheck() {
        Map<String, Object> result = signalResult(SOURCE_IOB_SIGNAL);

        try {
            Double niftyLTP = toDouble(candlePredictionService.getLiveTickData(), "niftyLTP");
            if (niftyLTP == null) return result;

            List<InternalOrderBlock> freshIOBs = internalOrderBlockService.getFreshIOBs(NIFTY_INSTRUMENT_TOKEN);
            logger.debug("IOB signal check — fresh IOBs: {}, price: {}", freshIOBs.size(), niftyLTP);

            Optional<SimulatedTrade> openTrade = tradeRepository.findOpenTradeBySignalSource(SOURCE_IOB_SIGNAL);
            if (openTrade.isPresent()) {
                return checkOpenIOBTradeForReversal(result, openTrade.get(), freshIOBs, niftyLTP);
            }

            // No open trade — pick the highest-confidence tradable IOB near price
            InternalOrderBlock bestIOB = null;
            for (InternalOrderBlock iob : freshIOBs) {
                if (iob.getSignalConfidence() == null || iob.getSignalConfidence() < 50) continue;
                if (activeIOBTrades.containsKey(iob.getId())) continue;
                if (isPriceOutsideIOBZone(niftyLTP, iob)) continue;
                if (!isNewSignal("IOB_" + iob.getId() + "_" + iobDirection(iob))) continue;
                if (bestIOB == null || iob.getSignalConfidence() > bestIOB.getSignalConfidence()) bestIOB = iob;
            }

            if (bestIOB != null) {
                result.put("hasSignal", true);
                result.put("signalType",     iobDirection(bestIOB));
                result.put("signalStrength", getSignalStrengthFromConfidence(bestIOB.getSignalConfidence()));
                result.put("confidence",     bestIOB.getSignalConfidence());
                result.put("iobId",          bestIOB.getId());
                result.put("iob",            bestIOB);
                result.put("entryPrice",     bestIOB.getEntryPrice());
                result.put("stopLoss",       bestIOB.getStopLoss());
                result.put("target1",        bestIOB.getTarget1());
                result.put("target2",        bestIOB.getTarget2());
                result.put("target3",        bestIOB.getTarget3());
                result.put("zoneHigh",       bestIOB.getZoneHigh());
                result.put("zoneLow",        bestIOB.getZoneLow());
                logger.info("IOB signal: {} IOB {} with {}% confidence at zone {}-{}",
                        bestIOB.getObType(), bestIOB.getId(), bestIOB.getSignalConfidence(),
                        bestIOB.getZoneLow(), bestIOB.getZoneHigh());
            }

        } catch (Exception e) {
            logger.error("Error checking IOB signal: {}", e.getMessage(), e);
        }

        return result;
    }

    private Map<String, Object> checkOpenIOBTradeForReversal(
            Map<String, Object> result, SimulatedTrade existingTrade,
            List<InternalOrderBlock> freshIOBs, Double niftyLTP) {

        String existingDirection = existingTrade.getSignalType();

        // Path 1: IOB that spawned this trade has reached a terminal status
        Long iobId = existingTrade.getIobId();
        if (iobId != null) {
            Optional<InternalOrderBlock> iobOpt = internalOrderBlockRepository.findById(iobId);
            if (iobOpt.isPresent() && TERMINAL_IOB_STATUSES.contains(iobOpt.get().getStatus())) {
                InternalOrderBlock iob = iobOpt.get();
                result.put("hasReverseSignal", true);
                result.put("reverseTradeId",   existingTrade.getTradeId());
                result.put("signalType",        "BUY".equals(existingDirection) ? "SELL" : "BUY");
                result.put("signalStrength",    "MODERATE");
                result.put("iobId",             iobId);
                result.put("exitReason",        "IOB_" + iob.getStatus());
                logger.info("IOB reverse signal: IOB {} is {} — exit trade {}",
                        iobId, iob.getStatus(), existingTrade.getTradeId());
                return result;
            }
        }

        // Path 2: Fresh opposite-direction IOB near current price
        for (InternalOrderBlock iob : freshIOBs) {
            if (iob.getSignalConfidence() == null || iob.getSignalConfidence() < 50) continue;
            if (!isReverseSignal(existingDirection, iobDirection(iob))) continue;
            if (isPriceOutsideIOBZone(niftyLTP, iob)) continue;

            result.put("hasReverseSignal", true);
            result.put("reverseTradeId",   existingTrade.getTradeId());
            result.put("signalType",        iobDirection(iob));
            result.put("signalStrength",    getSignalStrengthFromConfidence(iob.getSignalConfidence()));
            result.put("iobId",             iob.getId());
            result.put("confidence",        iob.getSignalConfidence());
            result.put("newIOB",            iob);
            logger.info("IOB reverse signal: {} IOB {} with {}% confidence — exit trade {}",
                    iob.getObType(), iob.getId(), iob.getSignalConfidence(), existingTrade.getTradeId());
            return result;
        }

        return result; // open trade exists, no reversal triggered
    }

    private static String iobDirection(InternalOrderBlock iob) {
        return "BULLISH_IOB".equals(iob.getObType()) ? "BUY" : "SELL";
    }

    /**
     * Check if price is near or within the IOB zone
     */
    private boolean isPriceOutsideIOBZone(Double currentPrice, InternalOrderBlock iob) {
        if (currentPrice == null || iob.getZoneHigh() == null || iob.getZoneLow() == null) {
            return true;
        }
        double tolerance = currentPrice * 0.005; // 0.5% of current price
        return currentPrice < (iob.getZoneLow() - tolerance) || currentPrice > (iob.getZoneHigh() + tolerance);
    }

    /**
     * Get signal strength based on confidence level
     */
    private String getSignalStrengthFromConfidence(Double confidence) {
        if (confidence == null) return "WEAK";
        if (confidence >= 85) return "STRONG";
        if (confidence >= 70) return "MODERATE";
        return "WEAK";
    }

    /**
     * Determine target price based on IOB confidence level
     */
    private Double getIOBTargetPrice(InternalOrderBlock iob) {
        Double confidence = iob.getSignalConfidence();
        if (confidence == null) {
            return iob.getTarget1(); // Default to T1
        }

        // Target selection based on confidence:
        // - 85%+ confidence: Target 3
        // - 70-84% confidence: Target 2
        // - 50-69% confidence: Target 1
        if (confidence >= 85 && iob.getTarget3() != null) {
            return iob.getTarget3();
        } else if (confidence >= 70 && iob.getTarget2() != null) {
            return iob.getTarget2();
        } else {
            return iob.getTarget1();
        }
    }

    /**
     * Place a trade based on IOB signal with appropriate targets.
     */
    @Override
    public SimulatedTrade placeIOBTrade(InternalOrderBlock iob) {
        try {
            if (iob == null) {
                logger.warn("Cannot place IOB trade: IOB is null");
                return null;
            }
            if (!"FRESH".equals(iob.getStatus())) {
                logger.warn("Cannot place IOB trade: IOB {} is not fresh (status: {})", iob.getId(), iob.getStatus());
                return null;
            }

            Map<String, Object> liveData = candlePredictionService.getLiveTickData();
            if (liveData == null) {
                logger.warn("Cannot place IOB trade: Live data not available");
                return null;
            }

            Double niftyLTP = toDouble(liveData, "niftyLTP");
            Double vixValue = toDouble(liveData, "vixValue");
            Integer atmStrike = toInt(liveData, "atmStrike");
            if (niftyLTP == null || atmStrike == null) {
                logger.warn("Cannot place IOB trade: Missing NIFTY LTP or ATM strike");
                return null;
            }

            String signalType = "BULLISH_IOB".equals(iob.getObType()) ? "BUY" : "SELL";
            String optionType = "BUY".equals(signalType) ? "CE" : "PE";
            String signalStrength = getSignalStrengthFromConfidence(iob.getSignalConfidence());

            AtmOptionData atmOption = resolveAtmOption(liveData, optionType);
            if (atmOption.ltp() == null || atmOption.ltp() <= 0) {
                logger.warn("Cannot place IOB trade: ATM {} LTP not available", optionType);
                return null;
            }

            Double iobTargetPrice = getIOBTargetPrice(iob);
            Double iobStopLoss    = iob.getStopLoss();
            Double iobEntryPrice  = iob.getEntryPrice();
            if (iobEntryPrice == null || iobStopLoss == null || iobTargetPrice == null) {
                logger.warn("Cannot place IOB trade: missing entry/stop/target price for IOB {}", iob.getId());
                return null;
            }

            IobOptionPricing pricing = calculateIOBOptionPricing(iobEntryPrice, iobStopLoss, iobTargetPrice, atmOption.ltp(), vixValue,
                    iob.getTarget1(), iob.getTarget2(), iob.getTarget3());
            SimulatedTrade trade = buildIOBTradeEntity(iob, signalType, signalStrength, optionType, atmStrike, atmOption, niftyLTP, vixValue, pricing);
            return registerAndLogIOBTrade(iob, trade, pricing, signalStrength);

        } catch (Exception e) {
            logger.error("Error placing IOB trade: {}", e.getMessage(), e);
            return null;
        }
    }

    /** Converts IOB index-level risk/reward into option target and stop-loss prices. */
    private IobOptionPricing calculateIOBOptionPricing(double iobEntryPrice, double iobStopLoss,
                                                        double iobTargetPrice, double optionLTP,
                                                        Double vixValue,
                                                        Double indexT1, Double indexT2, Double indexT3) {
        // More granular scaling for India VIX
        double delta;
        if (vixValue == null) delta = 0.5;
        else if (vixValue > 25) delta = 0.65; // High Volatility (Gamma risk)
        else if (vixValue > 18) delta = 0.55; // Elevated
        else delta = 0.45;

        double indexRisk   = Math.abs(iobEntryPrice - iobStopLoss);
        double indexReward = Math.abs(iobTargetPrice - iobEntryPrice);

        double optionTargetMove   = indexReward * delta;
        double optionStopLossMove = indexRisk   * delta;

        double targetPrice   = Math.min(optionLTP + optionTargetMove, optionLTP * 1.6); // 60% gain cap
        double stopLossPrice = Math.max(optionLTP - optionStopLossMove, optionLTP * 0.80); // 20% loss floor
        double actualStopMove  = optionLTP - stopLossPrice;
        double riskRewardRatio = actualStopMove > 0 ? optionTargetMove / actualStopMove : 0.0;

        // Premium equivalent at each index target level
        double premiumT1 = indexT1 != null ? Math.min(optionLTP + Math.abs(indexT1 - iobEntryPrice) * delta, optionLTP * 1.6) : 0.0;
        double premiumT2 = indexT2 != null ? Math.min(optionLTP + Math.abs(indexT2 - iobEntryPrice) * delta, optionLTP * 1.6) : 0.0;
        double premiumT3 = indexT3 != null ? Math.min(optionLTP + Math.abs(indexT3 - iobEntryPrice) * delta, optionLTP * 1.6) : 0.0;

        return new IobOptionPricing(targetPrice, stopLossPrice, riskRewardRatio, iobStopLoss, premiumT1, premiumT2, premiumT3);
    }

    /** Builds the SimulatedTrade entity (not yet persisted). */
    private SimulatedTrade buildIOBTradeEntity(InternalOrderBlock iob, String signalType, String signalStrength,
                                                String optionType, Integer atmStrike, AtmOptionData atmOption,
                                                Double niftyLTP, Double vixValue, IobOptionPricing pricing) {
        LocalDateTime now = LocalDateTime.now();
        Double confidence = iob.getSignalConfidence();
        String confStr = confidence != null ? String.format("%.1f%%", confidence) : "N/A";

        return SimulatedTrade.builder()
                .tradeId(generateTradeId())
                .tradeDate(now)
                .signalSource(SOURCE_IOB_SIGNAL)
                .signalType(signalType)
                .signalStrength(signalStrength)
                .optionType(optionType)
                .strikePrice(atmStrike.doubleValue())
                .optionToken(atmOption.token())
                .optionSymbol(atmOption.symbol())
                .underlyingPriceAtEntry(niftyLTP)
                .underlyingStopLoss(pricing.underlyingStopLoss())
                .iobId(iob.getId())
                .quantity(quantity)
                .lotSize(NIFTY_LOT_SIZE)
                .numLots(numLots)
                .entryPrice(atmOption.ltp())
                .entryTime(now)
                .entryReason(String.format("%s signal from IOB_SIGNAL (%s) - Confidence: %s",
                        signalType, signalStrength, confStr))
                .targetPrice(pricing.targetPrice())
                .stopLossPrice(pricing.stopLossPrice())
                .riskRewardRatio(pricing.riskRewardRatio())
                .premiumT1(pricing.premiumT1() > 0 ? pricing.premiumT1() : null)
                .premiumT2(pricing.premiumT2() > 0 ? pricing.premiumT2() : null)
                .premiumT3(pricing.premiumT3() > 0 ? pricing.premiumT3() : null)
                .status(TRADE_STATUS_OPEN)
                .vixAtEntry(vixValue)
                .marketTrend("BUY".equals(signalType) ? "BULLISH" : "BEARISH")
                .peakPrice(atmOption.ltp())
                .build();
    }

    /** Persists the trade, registers it in activeIOBTrades, marks IOB, subscribes token, and logs. */
    private SimulatedTrade registerAndLogIOBTrade(InternalOrderBlock iob, SimulatedTrade trade,
                                                   IobOptionPricing pricing, String signalStrength) {
        trade = tradeRepository.save(trade);
        internalOrderBlockService.markAsTraded(iob.getId(), trade.getTradeId(),
                pricing.premiumT1() > 0 ? pricing.premiumT1() : null,
                pricing.premiumT2() > 0 ? pricing.premiumT2() : null,
                pricing.premiumT3() > 0 ? pricing.premiumT3() : null); // DB first
        activeIOBTrades.put(iob.getId(), trade.getTradeId());                    // then in-memory
        subscribeOptionToken(trade.getOptionToken());

        String tier = "STRONG".equals(signalStrength) ? "3" : "MODERATE".equals(signalStrength) ? "2" : "1";
        logger.info("IOB Trade placed: {} | {} {} @ {} | Strike: {} | Active Target: {} (T{}) | PremiumSL: {} | IndexSL: {} | IOB: {} | Conf: {}%",
                trade.getTradeId(), trade.getOptionType(), trade.getSignalType(), trade.getEntryPrice(),
                trade.getStrikePrice().intValue(), pricing.targetPrice(), tier,
                pricing.stopLossPrice(), pricing.underlyingStopLoss(), iob.getId(), iob.getSignalConfidence());
        logger.info("IOB Premium Targets — T1: {} (idx: {}) | T2: {} (idx: {}) | T3: {} (idx: {})",
                String.format("%.2f", pricing.premiumT1()), iob.getTarget1(),
                String.format("%.2f", pricing.premiumT2()), iob.getTarget2(),
                String.format("%.2f", pricing.premiumT3()), iob.getTarget3());
        return trade;
    }

    // ============= Trade Placement =============

    @Override
    public SimulatedTrade placeTrade(String signalType, String signalSource, String signalStrength) {
        try {
            // Validate inputs
            if (signalType == null || (!signalType.equals("BUY") && !signalType.equals("SELL"))) {
                logger.warn("Invalid signal type: {}", signalType);
                return null;
            }

            // Get current market data
            Map<String, Object> liveData = candlePredictionService.getLiveTickData();
            if (liveData == null) {
                logger.warn("Cannot place trade: Live data not available");
                return null;
            }

            Double niftyLTP = toDouble(liveData, "niftyLTP");
            Double vixValue = toDouble(liveData, "vixValue");
            Integer atmStrike = toInt(liveData, "atmStrike");

            if (niftyLTP == null || niftyLTP <= 0 || atmStrike == null || atmStrike == 0) {
                logger.warn("Cannot place trade: Missing/zero NIFTY LTP ({}) or ATM strike ({})", niftyLTP, atmStrike);
                return null;
            }

            // Determine option type based on signal
            String optionType = signalType.equals("BUY") ? "CE" : "PE";

            AtmOptionData atmOption = resolveAtmOption(liveData, optionType);
            Double optionLTP = atmOption.ltp();
            Long optionToken = atmOption.token();
            String optionSymbol = atmOption.symbol();

            if (optionLTP == null || optionLTP <= 0) {
                logger.warn("Cannot place trade: ATM {} LTP not available", optionType);
                return null;
            }

            // Calculate target and stop loss prices
            double targetPrice = optionLTP * (1 + targetPercent / 100);
            double stopLossPrice = optionLTP * (1 - stoplossPercent / 100);
            double riskRewardRatio = targetPercent / stoplossPercent;

            // Generate trade ID
            String tradeId = generateTradeId();

            // Create trade entity
            SimulatedTrade trade = SimulatedTrade.builder()
                    .tradeId(tradeId)
                    .tradeDate(LocalDateTime.now())
                    .signalSource(signalSource)
                    .signalType(signalType)
                    .signalStrength(signalStrength)
                    .optionType(optionType)
                    .strikePrice(atmStrike.doubleValue())
                    .optionToken(optionToken)
                    .optionSymbol(optionSymbol)
                    .underlyingPriceAtEntry(niftyLTP)
                    .quantity(quantity)
                    .lotSize(NIFTY_LOT_SIZE)
                    .numLots(numLots)
                    .entryPrice(optionLTP)
                    .entryTime(LocalDateTime.now())
                    .entryReason(String.format("%s signal from %s (%s)", signalType, signalSource, signalStrength))
                    .targetPrice(targetPrice)
                    .stopLossPrice(stopLossPrice)
                    .riskRewardRatio(riskRewardRatio)
                    .status(TRADE_STATUS_OPEN)
                    .vixAtEntry(vixValue)
                    .marketTrend(signalType.equals("BUY") ? "BULLISH" : "BEARISH")
                    .peakPrice(optionLTP)
                    .build();

            // Save trade
            trade = tradeRepository.save(trade);

            subscribeOptionToken(trade.getOptionToken());

            logger.info("Trade placed successfully: {} | {} {} @ {} | Strike: {} | Target: {} | SL: {}",
                    tradeId, optionType, signalType, optionLTP, atmStrike, targetPrice, stopLossPrice);

            return trade;

        } catch (Exception e) {
            logger.error("Error placing trade: {}", e.getMessage(), e);
            return null;
        }
    }

    @Override
    public Map<String, Object> autoPlaceTrade() {
        Map<String, Object> result = new HashMap<>();
        result.put("tradePlaced", false);
        result.put("tradeExited", false);

        if (!autoTradingEnabled.get()) {
            logger.info("autoPlaceTrade: skipped — auto-trading is disabled");
            result.put("reason", "Auto-trading is disabled");
            return result;
        }

        closeStaleOpenTrades(); // close any OPEN trades left over from a previous day before checking signals
        monitorOpenTradeExits();

        Map<String, Object> signalCheck = checkForTradeSignals();
        logger.debug("Signal check: hasSignal={}, hasReverseSignal={}, reason={}",
                signalCheck.get("hasSignal"), signalCheck.get("hasReverseSignal"), signalCheck.get("reason"));

        List<SimulatedTrade> exitedTrades = processReverseSignalExits(getSignalList(signalCheck, "reverseSignals"));
        if (!exitedTrades.isEmpty()) {
            result.put("tradeExited", true);
            result.put("exitedTradesCount", exitedTrades.size());
            result.put("exitedTrades", exitedTrades);
            result.put("exitedTradeId", exitedTrades.get(0).getTradeId());
            result.put("exitPnl", exitedTrades.get(0).getNetPnl());
        }

        List<Map<String, Object>> allSignals = resolveAllSignals(signalCheck);
        List<SimulatedTrade> placedTrades = processNewSignals(allSignals);
        if (!placedTrades.isEmpty()) {
            result.put("tradePlaced", true);
            result.put("placedTradesCount", placedTrades.size());
            result.put("placedTrades", placedTrades);
            result.put("trade", placedTrades.get(0));
            result.put("tradeId", placedTrades.get(0).getTradeId());
            result.put("message", String.format("Placed %d trade(s) from different sources", placedTrades.size()));
        } else if (allSignals.isEmpty()) {
            result.put("reason", signalCheck.get("reason"));
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getSignalList(Map<String, Object> signalCheck, String key) {
        Object val = signalCheck.get(key);
        return val instanceof List ? (List<Map<String, Object>>) val : new ArrayList<>();
    }

    /** Falls back to single-signal map when allSignals list is absent (backward compatibility). */
    private List<Map<String, Object>> resolveAllSignals(Map<String, Object> signalCheck) {
        List<Map<String, Object>> all = getSignalList(signalCheck, "allSignals");
        if (all.isEmpty() && Boolean.TRUE.equals(signalCheck.get("hasSignal"))) {
            all = List.of(signalCheck);
        }
        return all;
    }

    private List<SimulatedTrade> processReverseSignalExits(List<Map<String, Object>> reverseSignals) {
        List<SimulatedTrade> exited = new ArrayList<>();
        if (reverseSignals.isEmpty()) return exited;

        Map<String, Object> liveData = candlePredictionService.getLiveTickData();
        for (Map<String, Object> signal : reverseSignals) {
            String reverseTradeId = (String) signal.get("reverseTradeId");
            if (reverseTradeId == null) continue;
            try {
                tradeRepository.findByTradeId(reverseTradeId).ifPresent(trade -> {
                    Double currentPrice = liveData != null ? getOptionCurrentPrice(trade, liveData) : null;
                    SimulatedTrade exitedTrade = exitTrade(reverseTradeId, EXIT_REVERSE_SIGNAL, currentPrice);
                    if (exitedTrade != null) {
                        exited.add(exitedTrade);
                        logger.info("Reverse exit: trade {} from {} — P&L {}",
                                reverseTradeId, signal.get("signalSource"), exitedTrade.getNetPnl());
                    }
                });
            } catch (Exception e) {
                logger.error("Error processing reverse signal exit for trade {}: {}", reverseTradeId, e.getMessage(), e);
            }
        }
        return exited;
    }

    private List<SimulatedTrade> processNewSignals(List<Map<String, Object>> allSignals) {
        List<SimulatedTrade> placed = new ArrayList<>();
        if (allSignals.isEmpty() || LocalTime.now(IST).isAfter(NO_NEW_TRADES_AFTER)) return placed;

        for (Map<String, Object> signal : allSignals) {
            if (isDailyLimitBreached()) {
                logger.debug("Daily limit reached mid-cycle — stopping further placement");
                break;
            }
            String signalType   = (String) signal.get("signalType");
            String signalSource = (String) signal.get("signalSource");

            if (tradeRepository.findOpenTradeBySignalSourceAndType(signalSource, signalType).isPresent()) {
                logger.debug("Skipping {} {} — open trade already exists for this source/direction", signalSource, signalType);
                continue;
            }

            // Cooldown gate — only consumed here (placement path), never in read-only signal detectors
            if (SOURCE_EMA_CROSSOVER.equals(signalSource) || SOURCE_LIQUIDITY_SWEEP.equals(signalSource)) {
                if (!isNewSignal(signalSource + "_" + signalType)) {
                    logger.debug("Skipping {} {} — on cooldown", signalSource, signalType);
                    continue;
                }
            }

            SimulatedTrade trade = placeTradeFromSignal(signal);
            if (trade != null) {
                placed.add(trade);
                logger.info("Trade placed from {}: {} {} @ {}", signalSource, trade.getOptionType(), trade.getSignalType(), trade.getEntryPrice());
            }
        }
        return placed;
    }

    private SimulatedTrade placeTradeFromSignal(Map<String, Object> signal) {
        String signalSource   = (String) signal.get("signalSource");
        String signalType     = (String) signal.get("signalType");
        String signalStrength = (String) signal.get("signalStrength");

        if (SOURCE_IOB_SIGNAL.equals(signalSource)) {
            InternalOrderBlock iob = (InternalOrderBlock) signal.get("iob");
            if (iob != null) return placeIOBTrade(iob);
            Long iobId = signal.get("iobId") instanceof Number ? ((Number) signal.get("iobId")).longValue() : null;
            return iobId != null ? internalOrderBlockRepository.findById(iobId).map(this::placeIOBTrade).orElse(null) : null;
        }

        return placeTrade(signalType, signalSource, signalStrength);
    }

    /**
     * Checks all open trades for SL hit, target hit, trailing SL, and market-close conditions.
     * Called at the start of every autoPlaceTrade cycle so price-based exits fire every minute.
     * Reverse-signal exits are handled separately by checkForTradeSignals().
     */
    private void monitorOpenTradeExits() {
        List<SimulatedTrade> openTrades = tradeRepository.findOpenTrades();
        if (openTrades.isEmpty()) return;

        try {
            Map<String, Object> liveData = candlePredictionService.getLiveTickData();
            LocalTime now = LocalTime.now(IST);

            for (SimulatedTrade trade : openTrades) {
                Double currentPrice = liveData != null ? getOptionCurrentPrice(trade, liveData) : null;
                String exitReason = evaluateExitReason(trade, liveData, currentPrice, now);
                if (exitReason != null) {
                    exitTrade(trade.getTradeId(), exitReason, currentPrice);
                }
            }
        } catch (Exception e) {
            logger.error("Error monitoring open trade exits: {}", e.getMessage(), e);
        }
    }

    private String resolveReverseSignalExit(SimulatedTrade trade,
                                              Map<String, Map<String, Object>> reverseSignals) {
        Map<String, Object> reverseSignal = reverseSignals.get(trade.getSignalSource());
        if (reverseSignal != null && trade.getTradeId().equals(reverseSignal.get("reverseTradeId"))) {
            logger.info("Reverse signal exit for trade {} ({})", trade.getTradeId(), trade.getSignalSource());
            return EXIT_REVERSE_SIGNAL;
        }
        return null;
    }

    /**
     * Evaluates whether a trade should exit based on:
     * IOB index SL → trailing SL / target / SL hit → market close.
     * Also persists trailing SL advances as a side effect.
     *
     * @return exit reason string, or null if no exit condition met
     */
    private String evaluateExitReason(SimulatedTrade trade, Map<String, Object> liveData,
                                       Double currentPrice, LocalTime now) {
        // IOB index-level SL: NIFTY breaching the IOB zone boundary exits immediately
        if (SOURCE_IOB_SIGNAL.equals(trade.getSignalSource())
                && trade.getUnderlyingStopLoss() != null && liveData != null) {
            Double niftyLTP = toDouble(liveData, "niftyLTP");
            if (niftyLTP != null) {
                boolean indexSlHit = "BUY".equals(trade.getSignalType())
                        ? niftyLTP <= trade.getUnderlyingStopLoss()
                        : niftyLTP >= trade.getUnderlyingStopLoss();
                if (indexSlHit) return EXIT_INDEX_SL_HIT;
            }
        }

        if (currentPrice != null) {
            Double prevTrailingSL = trade.getTrailingStopLoss();
            trade.updateTrailingStopLoss(currentPrice,
                    tradingProperties.getActivationThresholdPercent(),
                    tradingProperties.getTrailPercentOfProfit());

            if (trade.isTargetHit(currentPrice)) {
                return EXIT_TARGET_HIT;
            } else if (trade.isStopLossHit(currentPrice)) {
                return (trade.getTrailingStopLoss() != null
                        && trade.getStopLossPrice() != null
                        && currentPrice <= trade.getTrailingStopLoss()
                        && trade.getTrailingStopLoss() > trade.getStopLossPrice())
                        ? EXIT_TRAILING_SL : EXIT_STOPLOSS_HIT;
            }

            // Persist trailing SL only if it actually advanced (avoids unnecessary DB writes)
            if (!Objects.equals(prevTrailingSL, trade.getTrailingStopLoss())) {
                tradeRepository.save(trade);
            }
        }

        // Market-close fires regardless of price availability — exitTrade falls back to entryPrice if null
        if (now.isAfter(MARKET_CLOSE.minusMinutes(5))) return EXIT_MARKET_CLOSE;

        return null;
    }

    // ============= Trade Management =============

    @Override
    public List<SimulatedTrade> monitorAndManageOpenTrades() {
        List<SimulatedTrade> exitedTrades = new ArrayList<>();
        List<SimulatedTrade> openTrades = tradeRepository.findOpenTrades();

        if (openTrades.isEmpty()) {
            return exitedTrades;
        }

        try {
            Map<String, Object> liveData = candlePredictionService.getLiveTickData();
            LocalTime now = LocalTime.now(IST);

            // Determine which signal sources have open trades — only check those for reverse signals
            Set<String> openTradeSources = openTrades.stream()
                    .map(SimulatedTrade::getSignalSource)
                    .filter(Objects::nonNull)
                    .collect(java.util.stream.Collectors.toSet());

            Map<String, Map<String, Object>> reverseSignals = new HashMap<>();

            if (openTradeSources.contains(SOURCE_EMA_CROSSOVER)) {
                Map<String, Object> emaCheck = checkEMACrossoverSignalWithReverseCheck();
                if (Boolean.TRUE.equals(emaCheck.get("hasReverseSignal"))) {
                    reverseSignals.put(SOURCE_EMA_CROSSOVER, emaCheck);
                }
            }

            if (openTradeSources.contains(SOURCE_LIQUIDITY_SWEEP)) {
                Map<String, Object> liquiditySweepCheck = checkLiquiditySweepSignalWithReverseCheck();
                if (Boolean.TRUE.equals(liquiditySweepCheck.get("hasReverseSignal"))) {
                    reverseSignals.put(SOURCE_LIQUIDITY_SWEEP, liquiditySweepCheck);
                }
            }

            if (openTradeSources.contains(SOURCE_IOB_SIGNAL)) {
                Map<String, Object> iobCheck = checkIOBSignalWithReverseCheck();
                if (Boolean.TRUE.equals(iobCheck.get("hasReverseSignal"))) {
                    reverseSignals.put(SOURCE_IOB_SIGNAL, iobCheck);
                }
            }

            for (SimulatedTrade trade : openTrades) {
                Double currentPrice = liveData != null ? getOptionCurrentPrice(trade, liveData) : null;

                // Reverse signal takes priority over price-based exits
                String exitReason = resolveReverseSignalExit(trade, reverseSignals);

                if (exitReason == null) {
                    exitReason = evaluateExitReason(trade, liveData, currentPrice, now);
                }

                if (exitReason != null) {
                    SimulatedTrade exitedTrade = exitTrade(trade.getTradeId(), exitReason, currentPrice);
                    if (exitedTrade != null) exitedTrades.add(exitedTrade);
                }
            }

        } catch (Exception e) {
            logger.error("Error monitoring open trades: {}", e.getMessage(), e);
        }

        return exitedTrades;
    }

    @Override
    @Transactional
    public SimulatedTrade exitTrade(String tradeId, String exitReason, Double exitPrice) {
        try {
            Optional<SimulatedTrade> optTrade = tradeRepository.findByTradeId(tradeId);
            if (optTrade.isEmpty()) {
                logger.warn("Trade not found: {}", tradeId);
                return null;
            }

            SimulatedTrade trade = optTrade.get();
            if (!TRADE_STATUS_OPEN.equals(trade.getStatus())) {
                logger.warn("Trade {} is not open (status: {})", tradeId, trade.getStatus());
                return null;
            }

            exitPrice = resolveExitPrice(trade, exitPrice);

            trade.setExitPrice(exitPrice);
            trade.setExitTime(LocalDateTime.now());
            trade.setExitReason(exitReason);
            trade.setStatus(TRADE_STATUS_CLOSED);
            trade.calculatePnL();
            trade = tradeRepository.save(trade);

            logger.info("Trade exited: {} | {} | Entry: {} Exit: {} | P&L: {} ({})",
                    tradeId, exitReason, trade.getEntryPrice(), exitPrice,
                    trade.getNetPnl() != null ? String.format("%.2f", trade.getNetPnl()) : "N/A",
                    Boolean.TRUE.equals(trade.getIsProfitable()) ? "PROFIT" : "LOSS");

            if (SOURCE_IOB_SIGNAL.equals(trade.getSignalSource()) && trade.getIobId() != null) {
                activeIOBTrades.remove(trade.getIobId());
            }

            updateTodaysLedger();
            return trade;

        } catch (Exception e) {
            logger.error("Error exiting trade {}: {}", tradeId, e.getMessage(), e);
            return null;
        }
    }

    /**
     * Resolves the best available exit price for a trade.
     * Priority: tokenLTP map → getLTPForToken (fresh) → getLTPForToken (stale) → entryPrice fallback.
     */
    private Double resolveExitPrice(SimulatedTrade trade, Double providedPrice) {
        if (trade.getOptionToken() == null) {
            return providedPrice != null ? providedPrice : trade.getEntryPrice();
        }
        try {
            Map<String, Object> liveData = candlePredictionService.getLiveTickData();
            if (liveData != null) {
                Object tokenMapObj = liveData.get("tokenLTP");
                if (tokenMapObj instanceof Map) {
                    Map<?, ?> tokenMap = (Map<?, ?>) tokenMapObj;
                    Object ltpObj = tokenMap.get(trade.getOptionToken());
                    if (ltpObj == null) ltpObj = tokenMap.get(String.valueOf(trade.getOptionToken()));
                    if (ltpObj instanceof Number) {
                        double liveLtp = ((Number) ltpObj).doubleValue();
                        if (liveLtp > 0) {
                            logger.info("Exit LTP from tokenLTP map — token {}: {}", trade.getOptionToken(), liveLtp);
                            return liveLtp;
                        }
                    }
                }
            }

            // tokenLTP miss — fall back to service method
            Double svcLtp = candlePredictionService.getLTPForToken(trade.getOptionToken());
            Long svcTs    = candlePredictionService.getTickTimestampForToken(trade.getOptionToken());
            if (svcLtp != null && svcLtp > 0) {
                long ageMs = svcTs != null ? System.currentTimeMillis() - svcTs : 0;
                boolean fresh = svcTs == null || ageMs <= 30_000;
                if (!fresh) logger.warn("Using stale LTP for token {} (age {} ms): {}", trade.getOptionToken(), ageMs, svcLtp);
                else        logger.info("Exit LTP from getLTPForToken — token {}: {}", trade.getOptionToken(), svcLtp);
                return svcLtp;
            }
        } catch (Exception e) {
            logger.warn("Error resolving live LTP for exit (token {}): {}", trade.getOptionToken(), e.getMessage());
        }

        // Final fallback
        if (providedPrice != null) return providedPrice;
        logger.info("Exit price unavailable for trade {} — using entryPrice as fallback", trade.getTradeId());
        return trade.getEntryPrice();
    }

    @Override
    public List<SimulatedTrade> exitAllOpenTrades(String exitReason) {
        List<SimulatedTrade> exitedTrades = new ArrayList<>();
        List<SimulatedTrade> openTrades = tradeRepository.findOpenTrades();

        try {
            Map<String, Object> liveData = candlePredictionService.getLiveTickData();

            for (SimulatedTrade trade : openTrades) {
                Double currentPrice = getOptionCurrentPrice(trade, liveData);
                // exitTrade handles null currentPrice by falling back to entryPrice — always attempt exit
                SimulatedTrade exitedTrade = exitTrade(trade.getTradeId(), exitReason, currentPrice);
                if (exitedTrade != null) {
                    exitedTrades.add(exitedTrade);
                }
            }
        } catch (Exception e) {
            logger.error("Error exiting all trades: {}", e.getMessage(), e);
        }

        return exitedTrades;
    }

    @Override
    public List<SimulatedTrade> closeStaleOpenTrades() {
        List<SimulatedTrade> closed = new ArrayList<>();
        try {
            LocalDateTime startOfToday = getStartOfToday();
            List<SimulatedTrade> openTrades = tradeRepository.findOpenTrades();

            for (SimulatedTrade trade : openTrades) {
                // Stale = OPEN trade whose entry was before today's market open
                if (trade.getEntryTime() != null && trade.getEntryTime().isBefore(startOfToday)) {
                    logger.warn("Stale open trade detected: {} | source={} | direction={} | entered={} — closing as STALE_CARRY_OVER",
                            trade.getTradeId(), trade.getSignalSource(), trade.getSignalType(), trade.getEntryTime());
                    // Use entry price as exit price — option has expired, real price is 0
                    // but entryPrice avoids a misleading P&L spike in reports
                    SimulatedTrade exited = exitTrade(trade.getTradeId(), "STALE_CARRY_OVER", trade.getEntryPrice());
                    if (exited != null) {
                        closed.add(exited);
                        if (SOURCE_IOB_SIGNAL.equals(trade.getSignalSource()) && trade.getIobId() != null) {
                            activeIOBTrades.remove(trade.getIobId());
                        }
                    }
                }
            }

            if (!closed.isEmpty()) {
                logger.info("Closed {} stale carry-over trade(s): {}",
                        closed.size(),
                        closed.stream().map(SimulatedTrade::getTradeId).toList());
            }
        } catch (Exception e) {
            logger.error("Error closing stale open trades: {}", e.getMessage(), e);
        }
        return closed;
    }

    @Override
    public void updateTrailingStopLosses() {
        List<SimulatedTrade> openTrades = tradeRepository.findOpenTrades();

        try {
            Map<String, Object> liveData = candlePredictionService.getLiveTickData();
            if (liveData == null) return;

            for (SimulatedTrade trade : openTrades) {
                Double currentPrice = getOptionCurrentPrice(trade, liveData);
                if (currentPrice != null) {
                    trade.updateTrailingStopLoss(currentPrice, tradingProperties.getActivationThresholdPercent(), tradingProperties.getTrailPercentOfProfit());
                    tradeRepository.save(trade);
                }
            }
        } catch (Exception e) {
            logger.error("Error updating trailing stop losses: {}", e.getMessage());
        }
    }

    // ============= P&L and Reporting =============

    @Override
    public Map<String, Object> getTodaysSummary() {
        Map<String, Object> summary = new HashMap<>();

        try {
            LocalDateTime startOfDay = getStartOfToday();
            LocalDateTime endOfDay = getEndOfToday();

            List<SimulatedTrade> todaysTrades = tradeRepository.findTodaysTrades(startOfDay, endOfDay);
            // Exclude discarded trades from today's calculations
            todaysTrades = todaysTrades.stream().filter(t -> !TRADE_STATUS_DISCARDED.equals(t.getStatus())).toList();

            List<SimulatedTrade> todaysClosedTrades = todaysTrades.stream()
                    .filter(t -> TRADE_STATUS_CLOSED.equals(t.getStatus())).toList();
            int totalTrades = todaysTrades.size();
            int closedTrades = todaysClosedTrades.size();
            int openTradesCount = totalTrades - closedTrades;
            int winningTrades = (int) todaysClosedTrades.stream()
                    .filter(t -> Boolean.TRUE.equals(t.getIsProfitable())).count();
            int losingTrades = (int) todaysClosedTrades.stream()
                    .filter(t -> t.getNetPnl() != null && t.getNetPnl() < 0).count();

            double totalPnl = todaysClosedTrades.stream()
                    .filter(t -> t.getNetPnl() != null)
                    .mapToDouble(SimulatedTrade::getNetPnl).sum();

            double grossPnl = todaysClosedTrades.stream()
                    .filter(t -> t.getGrossPnl() != null)
                    .mapToDouble(SimulatedTrade::getGrossPnl).sum();

            double winRate = closedTrades > 0 ? (double) winningTrades / closedTrades * 100 : 0;

            // Calculate unrealized P&L across ALL currently open positions (including cross-day)
            double unrealizedPnl = 0;
            List<SimulatedTrade> allOpenTrades = tradeRepository.findOpenTrades();
            Map<String, Object> liveData = candlePredictionService.getLiveTickData();
            if (liveData != null) {
                for (SimulatedTrade trade : allOpenTrades) {
                    Double currentPrice = getOptionCurrentPrice(trade, liveData);
                    if (currentPrice != null && trade.getEntryPrice() != null) {
                        unrealizedPnl += (currentPrice - trade.getEntryPrice()) * trade.getQuantity();
                    }
                }
            }

            summary.put("totalTrades", totalTrades);
            summary.put("closedTrades", closedTrades);
            summary.put("openTrades", openTradesCount);
            summary.put("allOpenPositions", allOpenTrades.size()); // includes cross-day open trades
            summary.put("winningTrades", winningTrades);
            summary.put("losingTrades", losingTrades);
            summary.put("breakevenTrades", closedTrades - winningTrades - losingTrades);
            summary.put("winRate", winRate);
            summary.put("grossPnl", grossPnl);
            summary.put("netPnl", totalPnl);
            summary.put("unrealizedPnl", unrealizedPnl);
            summary.put("autoTradingEnabled", autoTradingEnabled.get());
            summary.put("lastUpdated", LocalDateTime.now().toString());

        } catch (Exception e) {
            logger.error("Error getting today's summary: {}", e.getMessage(), e);
            summary.put("error", e.getMessage());
        }

        return summary;
    }

    @Override
    public List<SimulatedTrade> getOpenTrades() {
        return tradeRepository.findOpenTrades();
    }

    @Override
    public List<SimulatedTrade> getTodaysTrades() {
        return tradeRepository.findTodaysTrades(getStartOfToday(), getEndOfToday());
    }

    @Override
    public List<SimulatedTrade> getTradeHistory(LocalDate startDate, LocalDate endDate) {
        return tradeRepository.findByTradeDateBetween(
                startDate.atStartOfDay(),
                endDate.atTime(23, 59, 59)
        );
    }

    @Override
    public TradingLedger getTodaysLedger() {
        return ledgerRepository.findByTradeDate(LocalDate.now()).orElse(null);
    }

    @Override
    public List<TradingLedger> getLedgerHistory(LocalDate startDate, LocalDate endDate) {
        return ledgerRepository.findByTradeDateBetween(startDate, endDate);
    }

    @Override
    public TradingLedger updateTodaysLedger() {
        try {
            LocalDate today = LocalDate.now();
            LocalDateTime startOfDay = getStartOfToday();
            LocalDateTime endOfDay = getEndOfToday();

            TradingLedger ledger = ledgerRepository.findByTradeDate(today)
                    .orElse(TradingLedger.builder().tradeDate(today).build());

            List<SimulatedTrade> todaysTrades = tradeRepository.findTodaysTrades(startOfDay, endOfDay);
            // Exclude discarded trades from ledger calculations
            todaysTrades = todaysTrades.stream().filter(t -> !TRADE_STATUS_DISCARDED.equals(t.getStatus())).toList();
            List<SimulatedTrade> closedTrades = todaysTrades.stream()
                    .filter(t -> TRADE_STATUS_CLOSED.equals(t.getStatus()))
                    .toList();

            // Update counts
            ledger.setTotalTrades(todaysTrades.size());
            ledger.setWinningTrades((int) closedTrades.stream()
                    .filter(t -> Boolean.TRUE.equals(t.getIsProfitable())).count());
            ledger.setLosingTrades((int) closedTrades.stream()
                    .filter(t -> Boolean.FALSE.equals(t.getIsProfitable())).count());

            // Calculate P&L
            double grossPnl = closedTrades.stream()
                    .filter(t -> t.getGrossPnl() != null)
                    .mapToDouble(SimulatedTrade::getGrossPnl).sum();
            double brokerage = closedTrades.stream()
                    .filter(t -> t.getBrokerage() != null)
                    .mapToDouble(SimulatedTrade::getBrokerage).sum();

            ledger.setGrossPnl(grossPnl);
            ledger.setTotalBrokerage(brokerage);
            ledger.setNetPnl(grossPnl - brokerage);

            // By signal source
            ledger.setTradeSetupTrades((int) todaysTrades.stream()
                    .filter(t -> SOURCE_TRADE_SETUP.equals(t.getSignalSource())).count());
            ledger.setEmaCrossoverTrades((int) todaysTrades.stream()
                    .filter(t -> SOURCE_EMA_CROSSOVER.equals(t.getSignalSource())).count());

            ledger.setTradeSetupPnl(closedTrades.stream()
                    .filter(t -> SOURCE_TRADE_SETUP.equals(t.getSignalSource()) && t.getNetPnl() != null)
                    .mapToDouble(SimulatedTrade::getNetPnl).sum());
            ledger.setEmaCrossoverPnl(closedTrades.stream()
                    .filter(t -> SOURCE_EMA_CROSSOVER.equals(t.getSignalSource()) && t.getNetPnl() != null)
                    .mapToDouble(SimulatedTrade::getNetPnl).sum());

            // Liquidity Sweep trades
            ledger.setLiquiditySweepTrades((int) todaysTrades.stream()
                    .filter(t -> SOURCE_LIQUIDITY_SWEEP.equals(t.getSignalSource())).count());
            ledger.setLiquiditySweepPnl(closedTrades.stream()
                    .filter(t -> SOURCE_LIQUIDITY_SWEEP.equals(t.getSignalSource()) && t.getNetPnl() != null)
                    .mapToDouble(SimulatedTrade::getNetPnl).sum());

            // By direction
            ledger.setBuyTrades((int) todaysTrades.stream()
                    .filter(t -> "BUY".equals(t.getSignalType())).count());
            ledger.setSellTrades((int) todaysTrades.stream()
                    .filter(t -> "SELL".equals(t.getSignalType())).count());
            ledger.setBuyPnl(closedTrades.stream()
                    .filter(t -> "BUY".equals(t.getSignalType()) && t.getNetPnl() != null)
                    .mapToDouble(SimulatedTrade::getNetPnl).sum());
            ledger.setSellPnl(closedTrades.stream()
                    .filter(t -> "SELL".equals(t.getSignalType()) && t.getNetPnl() != null)
                    .mapToDouble(SimulatedTrade::getNetPnl).sum());

            // By option type
            ledger.setCeTrades((int) todaysTrades.stream()
                    .filter(t -> "CE".equals(t.getOptionType())).count());
            ledger.setPeTrades((int) todaysTrades.stream()
                    .filter(t -> "PE".equals(t.getOptionType())).count());
            ledger.setCePnl(closedTrades.stream()
                    .filter(t -> "CE".equals(t.getOptionType()) && t.getNetPnl() != null)
                    .mapToDouble(SimulatedTrade::getNetPnl).sum());
            ledger.setPePnl(closedTrades.stream()
                    .filter(t -> "PE".equals(t.getOptionType()) && t.getNetPnl() != null)
                    .mapToDouble(SimulatedTrade::getNetPnl).sum());

            // Exit reasons
            ledger.setTargetHitCount((int) closedTrades.stream()
                    .filter(t -> EXIT_TARGET_HIT.equals(t.getExitReason())).count());
            ledger.setStoplossHitCount((int) closedTrades.stream()
                    .filter(t -> EXIT_STOPLOSS_HIT.equals(t.getExitReason())
                              || EXIT_INDEX_SL_HIT.equals(t.getExitReason())).count());
            ledger.setTrailingSlCount((int) closedTrades.stream()
                    .filter(t -> EXIT_TRAILING_SL.equals(t.getExitReason())).count());
            // Count both TIME_EXIT and MARKET_CLOSE as time-based exits
            ledger.setTimeExitCount((int) closedTrades.stream()
                    .filter(t -> "TIME_EXIT".equals(t.getExitReason()) || EXIT_MARKET_CLOSE.equals(t.getExitReason())).count());

            // Reverse signal exits
            ledger.setReverseSignalCount((int) closedTrades.stream()
                    .filter(t -> EXIT_REVERSE_SIGNAL.equals(t.getExitReason())).count());

            ledger.calculateMetrics();

            return ledgerRepository.save(ledger);

        } catch (Exception e) {
            logger.error("Error updating today's ledger: {}", e.getMessage(), e);
            return null;
        }
    }

    @Override
    public Map<String, Object> getTradingStatistics() {
        Map<String, Object> stats = new HashMap<>();

        try {
            // Today's stats
            stats.put("today", getTodaysSummary());

            // Weekly stats
            LocalDate startOfWeek = LocalDate.now().minusDays(LocalDate.now().getDayOfWeek().getValue() - 1);
            Double weeklyPnl = tradeRepository.calculateWeeklyPnl(startOfWeek.atStartOfDay());
            stats.put("weeklyPnl", weeklyPnl != null ? weeklyPnl : 0.0);

            // Monthly stats
            LocalDate startOfMonth = LocalDate.now().withDayOfMonth(1);
            Double monthlyPnl = tradeRepository.calculateMonthlyPnl(startOfMonth.atStartOfDay());
            stats.put("monthlyPnl", monthlyPnl != null ? monthlyPnl : 0.0);

            // Last 10 trades
            stats.put("recentTrades", tradeRepository.findLastNTrades(10));

            // Open positions
            stats.put("openPositions", tradeRepository.findOpenTrades());

        } catch (Exception e) {
            logger.error("Error getting trading statistics: {}", e.getMessage());
            stats.put("error", e.getMessage());
        }

        return stats;
    }

    // ============= Configuration =============

    @Override
    public Map<String, Object> getTradingConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("autoTradingEnabled", autoTradingEnabled.get());
        config.put("numLots", numLots);
        config.put("quantity", quantity);
        config.put("targetPercent", targetPercent);
        config.put("stoplossPercent", stoplossPercent);
        config.put("maxDailyLoss", maxDailyLoss);
        config.put("maxDailyTrades", maxDailyTrades);
        config.put("lotSize", NIFTY_LOT_SIZE);
        // Expose trailing SL runtime config
        config.put("trailingActivationThresholdPercent", tradingProperties.getActivationThresholdPercent());
        config.put("trailingTrailPercentOfProfit", tradingProperties.getTrailPercentOfProfit());
        return config;
    }

    @Override
    public void updateTradingConfig(Map<String, Object> config) {
        if (config == null || config.isEmpty()) return;

        if (config.containsKey("numLots")) {
            Integer val = toInt(config, "numLots");
            if (val != null && val > 0) {
                numLots = val;
                quantity = numLots * NIFTY_LOT_SIZE;
                logger.info("Config updated: numLots={}, quantity={}", numLots, quantity);
            } else {
                logger.warn("Ignoring invalid numLots value: {}", config.get("numLots"));
            }
        }
        if (config.containsKey("targetPercent")) {
            Double val = toDouble(config, "targetPercent");
            if (val != null && val > 0) {
                targetPercent = val;
                logger.info("Config updated: targetPercent={}", targetPercent);
            } else {
                logger.warn("Ignoring invalid targetPercent value: {}", config.get("targetPercent"));
            }
        }
        if (config.containsKey("stoplossPercent")) {
            Double val = toDouble(config, "stoplossPercent");
            if (val != null && val > 0) {
                stoplossPercent = val;
                logger.info("Config updated: stoplossPercent={}", stoplossPercent);
            } else {
                logger.warn("Ignoring invalid stoplossPercent value: {}", config.get("stoplossPercent"));
            }
        }
        if (config.containsKey("maxDailyLoss")) {
            Double val = toDouble(config, "maxDailyLoss");
            if (val != null) {
                // Normalize to negative — maxDailyLoss is a loss limit checked as: todayPnl <= maxDailyLoss
                maxDailyLoss = -Math.abs(val);
                logger.info("Config updated: maxDailyLoss={}", maxDailyLoss);
            }
        }
        if (config.containsKey("maxDailyTrades")) {
            Integer val = toInt(config, "maxDailyTrades");
            if (val != null && val > 0) {
                maxDailyTrades = val;
                logger.info("Config updated: maxDailyTrades={}", maxDailyTrades);
            } else {
                logger.warn("Ignoring invalid maxDailyTrades value: {}", config.get("maxDailyTrades"));
            }
        }
        if (config.containsKey("autoTradingEnabled")) {
            autoTradingEnabled.set(Boolean.TRUE.equals(config.get("autoTradingEnabled")));
            logger.info("Config updated: autoTradingEnabled={}", autoTradingEnabled.get());
        }

        // Allow updating trailing config at runtime via trading config API
        if (config.containsKey("trailingActivationThresholdPercent")) {
            Object v = config.get("trailingActivationThresholdPercent");
            if (v != null) {
                double val = v instanceof Number ? ((Number) v).doubleValue() : Double.parseDouble(v.toString());
                if (val > 0) {
                    tradingProperties.setActivationThresholdPercent(val);
                    logger.info("Config updated: trailingActivationThresholdPercent={}", val);
                } else {
                    logger.warn("Ignoring invalid trailingActivationThresholdPercent value: {}", v);
                }
            }
        }
        if (config.containsKey("trailingTrailPercentOfProfit")) {
            Object v = config.get("trailingTrailPercentOfProfit");
            if (v != null) {
                double val = v instanceof Number ? ((Number) v).doubleValue() : Double.parseDouble(v.toString());
                if (val > 0) {
                    tradingProperties.setTrailPercentOfProfit(val);
                    logger.info("Config updated: trailingTrailPercentOfProfit={}", val);
                } else {
                    logger.warn("Ignoring invalid trailingTrailPercentOfProfit value: {}", v);
                }
            }
        }
    }

    @Override
    public void setAutoTradingEnabled(boolean enabled) {
        autoTradingEnabled.set(enabled);
        logger.info("Auto-trading {}", enabled ? "enabled" : "disabled");
    }

    @Override
    public boolean isAutoTradingEnabled() {
        return autoTradingEnabled.get();
    }

    // ============= Manual Controls =============

    @Override
    public SimulatedTrade manualBuyTrade(String source) {
        return placeTrade("BUY", source != null ? source : EXIT_MANUAL, "MODERATE");
    }

    @Override
    public SimulatedTrade manualSellTrade(String source) {
        return placeTrade("SELL", source != null ? source : EXIT_MANUAL, "MODERATE");
    }

    @Override
    public SimulatedTrade cancelTrade(String tradeId) {
        try {
            Optional<SimulatedTrade> optTrade = tradeRepository.findByTradeId(tradeId);
            if (optTrade.isEmpty()) {
                return null;
            }

            SimulatedTrade trade = optTrade.get();
            if ("PENDING".equals(trade.getStatus())) {
                trade.setStatus("CANCELLED");
                return tradeRepository.save(trade);
            }

            return null;
        } catch (Exception e) {
            logger.error("Error cancelling trade: {}", e.getMessage());
            return null;
        }
    }

    @Override
    @Transactional
    public List<SimulatedTrade> discardTrades(List<String> tradeIds) {
        List<SimulatedTrade> discarded = new ArrayList<>();
        if (tradeIds == null || tradeIds.isEmpty()) return discarded;

        try {
            List<SimulatedTrade> trades = tradeRepository.findAllByTradeIdIn(tradeIds);
            LocalDateTime now = LocalDateTime.now();

            for (SimulatedTrade trade : trades) {
                if (TRADE_STATUS_DISCARDED.equals(trade.getStatus())) continue;

                boolean wasOpen = TRADE_STATUS_OPEN.equals(trade.getStatus());
                trade.setStatus(TRADE_STATUS_DISCARDED);
                trade.setExitReason(TRADE_STATUS_DISCARDED);
                if (wasOpen) trade.setExitTime(now);
                // Zero out P&L so ledger recalculation ignores it
                trade.setGrossPnl(0.0);
                trade.setNetPnl(0.0);
                trade.setIsProfitable(false);

                // Release IOB lock so new trades can be placed on this IOB
                if (SOURCE_IOB_SIGNAL.equals(trade.getSignalSource()) && trade.getIobId() != null) {
                    activeIOBTrades.remove(trade.getIobId());
                }

                discarded.add(tradeRepository.save(trade));
            }

            // Recalculate ledger after discards
            updateTodaysLedger();

        } catch (Exception e) {
            logger.error("Error discarding trades: {}", e.getMessage(), e);
            throw e;
        }

        return discarded;
    }

    // ============= Helper Methods =============

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    private LocalDateTime getStartOfToday() {
        return LocalDate.now(IST).atStartOfDay();
    }

    private LocalDateTime getEndOfToday() {
        return LocalDate.now(IST).plusDays(1).atStartOfDay();
    }

    private String generateTradeId() {
        return "SIM_" + LocalDateTime.now(IST).format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")) +
                "_" + UUID.randomUUID().toString().substring(0, 4).toUpperCase();
    }

    private boolean isNewSignal(String signalKey) {
        LocalDateTime now = LocalDateTime.now(IST);
        LocalDateTime lastTime = signalCooldowns.get(signalKey);
        if (lastTime != null && lastTime.plusMinutes(SIGNAL_COOLDOWN_MINUTES).isAfter(now)) {
            return false;
        }
        signalCooldowns.put(signalKey, now);
        return true;
    }

    private boolean isDailyLimitBreached() {
        LocalDateTime startOfDay = getStartOfToday();
        LocalDateTime endOfDay = getEndOfToday();

        Long todayTradeCount = tradeRepository.countTodaysTrades(startOfDay, endOfDay);
        if (todayTradeCount >= maxDailyTrades) {
            logger.info("Max daily trades limit reached: {}", todayTradeCount);
            return true;
        }

        Double todayPnl = tradeRepository.calculateTodaysTotalPnl(startOfDay, endOfDay);
        if (todayPnl != null && todayPnl <= maxDailyLoss) {
            logger.info("Max daily loss limit reached: {}", todayPnl);
            return true;
        }

        return false;
    }

    private Double getOptionCurrentPrice(SimulatedTrade trade, Map<String, Object> liveData) {
        if (trade == null || liveData == null) return null;

        // Prefer retrieving the original option LTP using the stored instrument token if available
        if (trade.getOptionToken() != null) {
            // Try tokenLTP map (Long key, then String key fallback)
            Object tokenLtpMapObj = liveData.get("tokenLTP");
            if (tokenLtpMapObj instanceof Map) {
                Map<?, ?> tokenLtpMap = (Map<?, ?>) tokenLtpMapObj;
                Object ltpObj = tokenLtpMap.get(trade.getOptionToken());
                if (ltpObj == null) {
                    ltpObj = tokenLtpMap.get(String.valueOf(trade.getOptionToken()));
                }
                if (ltpObj instanceof Number) {
                    return ((Number) ltpObj).doubleValue();
                }
            }

            // Fallback: match against stored atmCEToken/atmPEToken
            Long atmCet = toLong(liveData, "atmCEToken");
            Long atmPet = toLong(liveData, "atmPEToken");
            if (trade.getOptionToken().equals(atmCet)) {
                return toDouble(liveData, "atmCELTP");
            } else if (trade.getOptionToken().equals(atmPet)) {
                return toDouble(liveData, "atmPELTP");
            }

            // Token known but tick not yet received — do NOT fall back to a different instrument's LTP
            logger.debug("Live LTP not available for trade token {} (symbol={}). Skipping price update.",
                    trade.getOptionToken(), trade.getOptionSymbol());
            return null;
        }

        // No stored token — best-effort fallback to current ATM LTP by option type
        if ("CE".equals(trade.getOptionType())) {
            return toDouble(liveData, "atmCELTP");
        } else if ("PE".equals(trade.getOptionType())) {
            return toDouble(liveData, "atmPELTP");
        }
        return null;
    }

    // ============= Live Data Helpers =============

    private record AtmOptionData(Double ltp, Long token, String symbol) {}

    private record IobOptionPricing(double targetPrice, double stopLossPrice, double riskRewardRatio,
                                    double underlyingStopLoss,
                                    double premiumT1, double premiumT2, double premiumT3) {}

    private static Double toDouble(Map<String, Object> map, String key) {
        Object v = map.get(key);
        return v instanceof Number ? ((Number) v).doubleValue() : null;
    }

    private static Long toLong(Map<String, Object> map, String key) {
        Object v = map.get(key);
        return v instanceof Number ? ((Number) v).longValue() : null;
    }

    private static Integer toInt(Map<String, Object> map, String key) {
        Object v = map.get(key);
        return v instanceof Number ? ((Number) v).intValue() : null;
    }

    private AtmOptionData resolveAtmOption(Map<String, Object> liveData, String optionType) {
        String ltpKey    = "CE".equals(optionType) ? "atmCELTP"    : "atmPELTP";
        String tokenKey  = "CE".equals(optionType) ? "atmCEToken"  : "atmPEToken";
        String symbolKey = "CE".equals(optionType) ? "atmCESymbol" : "atmPESymbol";
        return new AtmOptionData(toDouble(liveData, ltpKey), toLong(liveData, tokenKey), (String) liveData.get(symbolKey));
    }

    private void subscribeOptionToken(Long optionToken) {
        if (optionToken == null) return;
        try {
            candlePredictionService.subscribeTokenForJob(List.of(optionToken));
            logger.info("Requested subscription for option token: {}", optionToken);
        } catch (Exception e) {
            logger.warn("Failed to subscribe option token {}: {}", optionToken, e.getMessage());
        }
    }

    // ============= Performance Statistics =============

    @Override
    public Map<String, Object> getPerformanceBySignalSource(String period) {
        Map<String, Object> result = new HashMap<>();

        LocalDateTime startDate;
        LocalDateTime endDate = LocalDateTime.now().plusDays(1).toLocalDate().atStartOfDay();

        switch (period.toLowerCase()) {
            case "weekly":
                startDate = LocalDate.now().minusDays(LocalDate.now().getDayOfWeek().getValue() - 1).atStartOfDay();
                break;
            case "monthly":
                startDate = LocalDate.now().withDayOfMonth(1).atStartOfDay();
                break;
            case "daily":
            default:
                startDate = LocalDate.now().atStartOfDay();
                break;
        }

        result.put("period", period);
        result.put("startDate", startDate.toString());
        result.put("endDate", endDate.toString());

        // Get all signal sources
        List<String> signalSources = List.of(SOURCE_IOB_SIGNAL, SOURCE_TRADE_SETUP, SOURCE_EMA_CROSSOVER, SOURCE_LIQUIDITY_SWEEP, EXIT_MANUAL);
        List<Map<String, Object>> sourceStats = new ArrayList<>();

        for (String source : signalSources) {
            Map<String, Object> stats = calculateSourceStats(source, startDate, endDate);
            sourceStats.add(stats);
        }

        result.put("bySource", sourceStats);

        // Calculate totals
        Long totalTrades = tradeRepository.countClosedTradesInRange(startDate, endDate);
        Long winningTrades = tradeRepository.countWinningTradesInRange(startDate, endDate);
        Double totalPnl = tradeRepository.calculatePnlInRange(startDate, endDate);

        result.put("totalTrades", totalTrades != null ? totalTrades : 0);
        result.put("winningTrades", winningTrades != null ? winningTrades : 0);
        result.put("losingTrades", totalTrades != null && winningTrades != null ? totalTrades - winningTrades : 0);
        result.put("totalPnl", totalPnl != null ? totalPnl : 0.0);
        result.put("winRate", totalTrades != null && totalTrades > 0 ? (double) winningTrades / totalTrades * 100 : 0.0);

        return result;
    }

    @Override
    public Map<String, Object> getPerformanceChartData() {
        Map<String, Object> result = new HashMap<>();

        // Get daily, weekly, and monthly performance
        Map<String, Object> dailyPerf = getPerformanceBySignalSource("daily");
        Map<String, Object> weeklyPerf = getPerformanceBySignalSource("weekly");
        Map<String, Object> monthlyPerf = getPerformanceBySignalSource("monthly");

        result.put("daily", dailyPerf);
        result.put("weekly", weeklyPerf);
        result.put("monthly", monthlyPerf);

        // Get historical daily data for the last 7 days for trend chart
        List<Map<String, Object>> dailyTrend = new ArrayList<>();
        for (int i = 6; i >= 0; i--) {
            LocalDate date = LocalDate.now().minusDays(i);
            LocalDateTime dayStart = date.atStartOfDay();
            LocalDateTime dayEnd = date.plusDays(1).atStartOfDay();

            Long totalTrades = tradeRepository.countClosedTradesInRange(dayStart, dayEnd);
            Long winningTrades = tradeRepository.countWinningTradesInRange(dayStart, dayEnd);
            Double pnl = tradeRepository.calculatePnlInRange(dayStart, dayEnd);

            Map<String, Object> dayData = new HashMap<>();
            dayData.put("date", date.toString());
            dayData.put("totalTrades", totalTrades != null ? totalTrades : 0);
            dayData.put("winningTrades", winningTrades != null ? winningTrades : 0);
            dayData.put("pnl", pnl != null ? pnl : 0.0);
            dayData.put("winRate", totalTrades != null && totalTrades > 0 ? (double) winningTrades / totalTrades * 100 : 0.0);

            dailyTrend.add(dayData);
        }
        result.put("dailyTrend", dailyTrend);

        // Get signal source breakdown for pie chart
        List<Map<String, Object>> sourceBreakdown = new ArrayList<>();
        LocalDateTime monthStart = LocalDate.now().withDayOfMonth(1).atStartOfDay();
        LocalDateTime monthEnd = LocalDateTime.now().plusDays(1).toLocalDate().atStartOfDay();

        for (String source : List.of(SOURCE_IOB_SIGNAL, SOURCE_TRADE_SETUP, SOURCE_EMA_CROSSOVER, SOURCE_LIQUIDITY_SWEEP)) {
            Map<String, Object> breakdown = calculateSourceStats(source, monthStart, monthEnd);
            sourceBreakdown.add(breakdown);
        }
        result.put("sourceBreakdown", sourceBreakdown);

        return result;
    }

    private Map<String, Object> calculateSourceStats(String source, LocalDateTime startDate, LocalDateTime endDate) {
        Map<String, Object> stats = new HashMap<>();
        stats.put("signalSource", source);
        stats.put("displayName", getDisplayName(source));

        Long totalTrades = tradeRepository.countClosedTradesBySignalSource(source, startDate, endDate);
        Long winningTrades = tradeRepository.countWinningTradesBySignalSource(source, startDate, endDate);
        Double pnl = tradeRepository.calculatePnlBySignalSource(source, startDate, endDate);

        totalTrades = totalTrades != null ? totalTrades : 0;
        winningTrades = winningTrades != null ? winningTrades : 0;
        pnl = pnl != null ? pnl : 0.0;

        stats.put("totalTrades", totalTrades);
        stats.put("winningTrades", winningTrades);
        stats.put("losingTrades", totalTrades - winningTrades);
        stats.put("pnl", pnl);
        stats.put("winRate", totalTrades > 0 ? (double) winningTrades / totalTrades * 100 : 0.0);
        stats.put("profitFactor", calculateProfitFactor(source, startDate, endDate));

        return stats;
    }

    private String getDisplayName(String source) {
        switch (source) {
            case SOURCE_IOB_SIGNAL: return "IOB";
            case SOURCE_TRADE_SETUP: return "Trade Setup";
            case SOURCE_EMA_CROSSOVER: return "EMA Crossover";
            case SOURCE_LIQUIDITY_SWEEP: return "Liquidity Sweep";
            case EXIT_MANUAL: return "Manual";
            default: return source;
        }
    }

    private double calculateProfitFactor(String source, LocalDateTime startDate, LocalDateTime endDate) {
        List<SimulatedTrade> trades = tradeRepository.findClosedTradesBySignalSource(source, startDate, endDate);
        if (trades == null || trades.isEmpty()) return 0.0;

        double totalProfit = 0;
        double totalLoss = 0;

        for (SimulatedTrade trade : trades) {
            if (trade.getNetPnl() != null) {
                if (trade.getNetPnl() > 0) {
                    totalProfit += trade.getNetPnl();
                } else {
                    totalLoss += Math.abs(trade.getNetPnl());
                }
            }
        }

        return totalLoss > 0 ? totalProfit / totalLoss : totalProfit > 0 ? Double.MAX_VALUE : 0.0;
    }

    @PostConstruct
    public void initLoggingContext() {
        // Log a startup line to ensure dedicated appender creates the file
        try {
            MDC.put("appJobConfigNum", "default");
            logger.info("SimulatedTradingServiceImpl initialized - logging test entry");

            // Subscribe to tokens for currently open trades so we immediately receive their LTPs
            try {
                List<SimulatedTrade> openTrades = tradeRepository.findOpenTrades();
                if (openTrades != null && !openTrades.isEmpty()) {
                    List<Long> tokens = new ArrayList<>();
                    for (SimulatedTrade t : openTrades) {
                        if (t.getOptionToken() != null) tokens.add(t.getOptionToken());
                    }
                    if (!tokens.isEmpty()) {
                        // Check if within trading hours
                        LocalTime now = LocalTime.now(IST);
                        if (now.isAfter(MARKET_OPEN) && now.isBefore(MARKET_CLOSE)) {
                            candlePredictionService.subscribeTokenForJob(tokens);
                            logger.info("Requested subscription for {} open trade tokens on startup", tokens.size());
                        }

                    }
                }
            } catch (Exception e) {
                logger.warn("Error subscribing open trade tokens on startup: {}", e.getMessage());
            }

        } finally {
            MDC.remove("appJobConfigNum");
        }
    }
}
