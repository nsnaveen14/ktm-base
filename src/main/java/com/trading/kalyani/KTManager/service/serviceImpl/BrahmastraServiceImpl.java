package com.trading.kalyani.KTManager.service.serviceImpl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.kalyani.KTManager.dto.brahmastra.*;
import com.trading.kalyani.KTManager.entity.*;
import com.trading.kalyani.KTManager.model.HistoricalDataRequest;
import com.trading.kalyani.KTManager.model.HistoricalDataResponse;
import com.trading.kalyani.KTManager.repository.*;
import com.trading.kalyani.KTManager.service.BrahmastraService;

import com.trading.kalyani.KTManager.service.InstrumentService;
import com.trading.kalyani.KTManager.service.MessagingService;
import com.trading.kalyani.KTManager.service.TelegramNotificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;

/**
 * Implementation of Brahmastra Triple Confirmation Trading Strategy.
 *
 * Signal Generation Logic:
 * - BUY Signal: Supertrend Bullish + MACD Bullish (Line > Signal) + Price at/below VWAP (±0.2%)
 * - SELL Signal: Supertrend Bearish + MACD Bearish (Line < Signal) + Price at/above VWAP (±0.2%)
 *
 * With optional PCR filter:
 * - PCR > 1.2: Bullish bias - Only BUY signals allowed
 * - PCR < 0.8: Bearish bias - Only SELL signals allowed
 * - PCR 0.8-1.2: Neutral - Both signals allowed
 */
@Service
public class BrahmastraServiceImpl implements BrahmastraService {

    private static final Logger logger = LoggerFactory.getLogger(BrahmastraServiceImpl.class);

    private static final String SIGNAL_BUY = "BUY";
    private static final String SIGNAL_SELL = "SELL";
    private static final String SIGNAL_NONE = "NO_SIGNAL";
    private static final String TREND_BULLISH = "BULLISH";
    private static final String TREND_BEARISH = "BEARISH";
    private static final String TREND_NEUTRAL = "NEUTRAL";
    private static final String STATUS_ACTIVE = "ACTIVE";
    private static final String STATUS_CLOSED = "CLOSED";
    private static final String STATUS_STOPPED_OUT = "STOPPED_OUT";
    private static final String STATUS_TARGET_HIT = "TARGET_HIT";

    private static final Double PCR_BULLISH_THRESHOLD = 1.2;
    private static final Double PCR_BEARISH_THRESHOLD = 0.8;

    // Z pattern (no colon) handles Kite API offset format: +0530
    // XXX pattern (with colon) handles ISO offset format: +05:30
    private static final DateTimeFormatter KITE_OFFSET_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ");
    private static final List<DateTimeFormatter> LOCAL_TIMESTAMP_FORMATTERS = List.of(
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd")
    );

    // Cache for indicator calculations (candle cache entries expire after 2 minutes)
    private final Map<String, List<CandleData>> candleCache = new ConcurrentHashMap<>();
    private final Map<String, Long> candleCacheTimestamp = new ConcurrentHashMap<>();
    private static final long CANDLE_CACHE_TTL_MS = 2 * 60 * 1000L; // 2 minutes
    private final Map<String, BrahmastraSignal> lastSignalCache = new ConcurrentHashMap<>();

    @Autowired
    private BrahmastraSignalRepository signalRepository;

    @Autowired
    private BrahmastraBacktestResultRepository backtestRepository;

    @Autowired
    private CandleStickRepository candleStickRepository;

    @Autowired
    private IndexLTPRepository indexLTPRepository;

    @Autowired
    private MessagingService messagingService;

    @Autowired
    private TelegramNotificationService telegramNotificationService;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private InstrumentService instrumentService;

    @Autowired
    private MiniDeltaRepository miniDeltaRepository;

    // ==================== Symbol to Token Mapping ====================

    private static final Map<String, Long> SYMBOL_TOKEN_MAP = Map.of(
            "NIFTY", NIFTY_INSTRUMENT_TOKEN
    );

    private static final Map<Long, String> TOKEN_SYMBOL_MAP = Map.of(
            NIFTY_INSTRUMENT_TOKEN, "NIFTY"
    );

    // Default indicator parameters
    private static final int DEFAULT_SUPERTREND_PERIOD = 20;
    private static final double DEFAULT_SUPERTREND_MULTIPLIER = 2.0;
    private static final int DEFAULT_MACD_FAST_PERIOD = 12;
    private static final int DEFAULT_MACD_SLOW_PERIOD = 26;
    private static final int DEFAULT_MACD_SIGNAL_PERIOD = 9;
    private static final double DEFAULT_VWAP_TOLERANCE = 0.002;

    /**
     * Create a SignalRequest with default indicator parameters.
     */
    private SignalRequest createDefaultSignalRequest(String symbol, String timeframe) {
        return SignalRequest.builder()
                .symbol(symbol)
                .timeframe(timeframe)
                .usePCR(true)
                .supertrendPeriod(DEFAULT_SUPERTREND_PERIOD)
                .supertrendMultiplier(DEFAULT_SUPERTREND_MULTIPLIER)
                .macdFastPeriod(DEFAULT_MACD_FAST_PERIOD)
                .macdSlowPeriod(DEFAULT_MACD_SLOW_PERIOD)
                .macdSignalPeriod(DEFAULT_MACD_SIGNAL_PERIOD)
                .vwapTolerance(DEFAULT_VWAP_TOLERANCE)
                .build();
    }

    /**
     * Apply default values to a SignalRequest if any indicator parameter is null.
     */
    private SignalRequest applyDefaults(SignalRequest request) {
        if (request == null) {
            return createDefaultSignalRequest("NIFTY", "5m");
        }
        return SignalRequest.builder()
                .symbol(request.getSymbol())
                .timeframe(request.getTimeframe() != null ? request.getTimeframe() : "5m")
                .fromDate(request.getFromDate())
                .toDate(request.getToDate())
                .usePCR(request.getUsePCR() != null ? request.getUsePCR() : true)
                .supertrendPeriod(request.getSupertrendPeriod() != null ? request.getSupertrendPeriod() : DEFAULT_SUPERTREND_PERIOD)
                .supertrendMultiplier(request.getSupertrendMultiplier() != null ? request.getSupertrendMultiplier() : DEFAULT_SUPERTREND_MULTIPLIER)
                .macdFastPeriod(request.getMacdFastPeriod() != null ? request.getMacdFastPeriod() : DEFAULT_MACD_FAST_PERIOD)
                .macdSlowPeriod(request.getMacdSlowPeriod() != null ? request.getMacdSlowPeriod() : DEFAULT_MACD_SLOW_PERIOD)
                .macdSignalPeriod(request.getMacdSignalPeriod() != null ? request.getMacdSignalPeriod() : DEFAULT_MACD_SIGNAL_PERIOD)
                .vwapTolerance(request.getVwapTolerance() != null ? request.getVwapTolerance() : DEFAULT_VWAP_TOLERANCE)
                .build();
    }

    // ==================== Signal Generation ====================

    @Override
    public List<SignalDTO> generateSignals(SignalRequest request) {
        // Apply default values for any null parameters
        request = applyDefaults(request);

        logger.info("Generating signals for {} from {} to {} on {} timeframe",
                request.getSymbol(), request.getFromDate(), request.getToDate(), request.getTimeframe());

        Long instrumentToken = SYMBOL_TOKEN_MAP.getOrDefault(request.getSymbol().toUpperCase(), NIFTY_INSTRUMENT_TOKEN);

        // Fetch candle data for the period
        List<CandleData> candles = fetchCandleData(instrumentToken, request.getTimeframe(),
                request.getFromDate().atStartOfDay(), request.getToDate().atTime(23, 59, 59));

        if (candles.isEmpty()) {
            logger.warn("No candle data found for {} between {} and {}", request.getSymbol(), request.getFromDate(), request.getToDate());
            return Collections.emptyList();
        }

        // Calculate indicators
        calculateIndicators(candles, request);

        // Generate signals based on triple confirmation
        List<SignalDTO> signals = new ArrayList<>();

        for (int i = Math.max(26, request.getSupertrendPeriod()); i < candles.size(); i++) {
            CandleData candle = candles.get(i);
            CandleData prevCandle = candles.get(i - 1);

            // Check for triple confirmation
            String signalType = checkTripleConfirmation(candle, prevCandle, request);

            if (!SIGNAL_NONE.equals(signalType)) {
                // Apply PCR filter if enabled
                if (Boolean.TRUE.equals(request.getUsePCR())) {
                    Double pcr = getPCRForTime(instrumentToken, candle.getTimestamp());
                    if (!isSignalAllowedByPCR(signalType, pcr)) {
                        continue; // Skip signal
                    }
                }

                // Check for sideways market (skip if too many flips)
                if (isSidewaysMarket(candles, i, 10, 3)) {
                    continue;
                }

                SignalDTO signal = buildSignal(candle, prevCandle, signalType, request);
                signals.add(signal);
            }
        }

        // Batch-save all signals in a single DB call
        if (!signals.isEmpty()) {
            saveSignals(signals, instrumentToken, request);
        }

        logger.info("Generated {} signals for {}", signals.size(), request.getSymbol());
        return signals;
    }

    @Override
    @Async
    public CompletableFuture<List<SignalDTO>> generateSignalsAsync(SignalRequest request) {
        return CompletableFuture.completedFuture(generateSignals(request));
    }

    @Override
    public List<LiveScanResult> scanLiveSignals(List<String> symbols) {
        return symbols.stream()
                .map(symbol -> scanSymbol(symbol, "5m"))
                .filter(result -> !SIGNAL_NONE.equals(result.getSignalType()))
                .toList();
    }

    @Override
    public LiveScanResult scanSymbol(String symbol, String timeframe) {
        Long instrumentToken = SYMBOL_TOKEN_MAP.getOrDefault(symbol.toUpperCase(), NIFTY_INSTRUMENT_TOKEN);

        // Get recent candles from cache or fetch (cache expires after CANDLE_CACHE_TTL_MS)
        String cacheKey = symbol + "_" + timeframe;
        long now = System.currentTimeMillis();
        Long cacheTs = candleCacheTimestamp.get(cacheKey);
        List<CandleData> candles = candleCache.get(cacheKey);

        if (candles == null || candles.isEmpty() || cacheTs == null || (now - cacheTs) > CANDLE_CACHE_TTL_MS) {
            candles = fetchRecentCandles(instrumentToken, timeframe);
            candleCache.put(cacheKey, candles);
            candleCacheTimestamp.put(cacheKey, now);
        }

        if (candles.isEmpty()) {
            return buildNoSignalResult(symbol, instrumentToken);
        }

        // Calculate indicators on latest data with default parameters
        SignalRequest request = createDefaultSignalRequest(symbol, timeframe);

        calculateIndicators(candles, request);

        CandleData current = candles.get(candles.size() - 1);
        CandleData prev = candles.size() > 1 ? candles.get(candles.size() - 2) : current;

        String signalType = checkTripleConfirmation(current, prev, request);

        // Get PCR
        Double pcr = getCurrentPCR(symbol);
        String pcrBias = getPCRBias(pcr);

        // Compute SL first, then derive targets from same risk basis
        Double stopLoss = calculateStopLoss(current, prev, signalType);

        // Build result
        return LiveScanResult.builder()
                .symbol(symbol)
                .instrumentToken(instrumentToken)
                .scanTime(LocalDateTime.now())
                .signalType(signalType)
                .currentPrice(current.getClose())
                .entryPrice(!SIGNAL_NONE.equals(signalType) ? current.getClose() : null)
                .stopLoss(stopLoss)
                .target1(calculateTarget(current, signalType, 1.0, stopLoss))
                .target2(calculateTarget(current, signalType, 2.0, stopLoss))
                .riskReward(2.0)
                .confidenceScore(calculateConfidence(current, signalType))
                .supertrendStatus(current.getSupertrendTrend())
                .macdStatus(current.getMacdLine() != null && current.getMacdSignal() != null
                        ? (current.getMacdLine() > current.getMacdSignal() ? TREND_BULLISH : TREND_BEARISH)
                        : TREND_NEUTRAL)
                .vwapStatus(getVwapStatus(current))
                .pcrBias(pcrBias)
                .vwap(current.getVwap())
                .supertrend(current.getSupertrend())
                .macdLine(current.getMacdLine())
                .macdSignal(current.getMacdSignal())
                .pcr(pcr)
                .isNewSignal(!signalType.equals(SIGNAL_NONE) && isNewSignal(symbol, signalType))
                .message(buildSignalMessage(symbol, signalType, current))
                .alertLevel(!signalType.equals(SIGNAL_NONE) ? "HIGH" : "LOW")
                .build();
    }

    // ==================== Backtesting ====================

    @Override
    public BacktestResult runBacktest(BacktestRequest request) {
        logger.info("Running backtest for {} from {} to {}", request.getSymbol(), request.getFromDate(), request.getToDate());

        Long instrumentToken = SYMBOL_TOKEN_MAP.getOrDefault(request.getSymbol().toUpperCase(), NIFTY_INSTRUMENT_TOKEN);

        // Fetch candle data
        List<CandleData> candles = fetchCandleData(instrumentToken, request.getTimeframe(),
                request.getFromDate().atStartOfDay(), request.getToDate().atTime(23, 59, 59));

        if (candles.isEmpty()) {
            return BacktestResult.builder()
                    .symbol(request.getSymbol())
                    .totalTrades(0)
                    .build();
        }

        SignalRequest signalRequest = applyDefaults(SignalRequest.builder()
                .symbol(request.getSymbol())
                .timeframe(request.getTimeframe())
                .supertrendPeriod(request.getSupertrendPeriod())
                .supertrendMultiplier(request.getSupertrendMultiplier())
                .macdFastPeriod(request.getMacdFastPeriod())
                .macdSlowPeriod(request.getMacdSlowPeriod())
                .macdSignalPeriod(request.getMacdSignalPeriod())
                .vwapTolerance(request.getVwapTolerance())
                .usePCR(request.getUsePCR())
                .build());

        calculateIndicators(candles, signalRequest);

        // Run simulation
        BacktestEngine engine = new BacktestEngine(request);
        BacktestResult result = engine.runSimulation(candles, signalRequest);

        // Save result
        saveBacktestResult(result, request);

        logger.info("Backtest complete. Total trades: {}, Win rate: {}%", result.getTotalTrades(), result.getWinRate());
        return result;
    }

    @Override
    @Async
    public CompletableFuture<BacktestResult> runBacktestAsync(BacktestRequest request) {
        return CompletableFuture.completedFuture(runBacktest(request));
    }

    // ==================== Dashboard ====================

    @Override
    public DashboardSummary getDashboardSummary() {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime todayStart = now.toLocalDate().atStartOfDay();
        LocalDateTime todayEnd = now.toLocalDate().atTime(23, 59, 59);

        List<BrahmastraSignal> todaysSignals = signalRepository.findBySignalTimeBetweenOrderBySignalTimeDesc(todayStart, todayEnd);
        List<BrahmastraSignal> activeSignals = signalRepository.findByStatusOrderBySignalTimeDesc(STATUS_ACTIVE);

        // Calculate overall stats
        double totalPnL = todaysSignals.stream()
                .filter(s -> s.getPnl() != null)
                .mapToDouble(BrahmastraSignal::getPnl)
                .sum();

        int todayWins = (int) todaysSignals.stream()
                .filter(s -> s.getPnl() != null && s.getPnl() > 0)
                .count();

        int todayLosses = (int) todaysSignals.stream()
                .filter(s -> s.getPnl() != null && s.getPnl() < 0)
                .count();

        // Get PCR values (NIFTY only)
        Double niftyPCR = getCurrentPCR("NIFTY");

        // Build symbol summaries (NIFTY only)
        List<SymbolSummary> symbolSummaries = List.of("NIFTY")
                .stream()
                .map(this::getSymbolSummary)
                .toList();

        // Get option chain metrics
        List<OptionChainMetrics> optionChainMetricsList = getAllOptionChainMetrics();
        OptionChainMetrics niftyOC = optionChainMetricsList.stream()
                .filter(m -> "NIFTY".equals(m.getSymbol())).findFirst().orElse(null);

        // Determine overall option chain bias
        String optionChainOverallBias = determineOptionChainOverallBias(optionChainMetricsList);

        return DashboardSummary.builder()
                .generatedAt(now)
                .totalPnL(totalPnL)
                .totalPnLPercent(0.0) // Would need initial capital to calculate
                .totalTrades(todaysSignals.size())
                .overallWinRate((todayWins + todayLosses) > 0 ?
                        (double) todayWins / (todayWins + todayLosses) * 100 : 0.0)
                .todaysTrades(todaysSignals.size())
                .todaysPnL(totalPnL)
                .todaysWinningTrades(todayWins)
                .todaysLosingTrades(todayLosses)
                .activeSignals(activeSignals.size())
                .symbolSummaries(symbolSummaries)
                .currentMarketBias(determineOverallBias(niftyPCR))
                .niftyPCR(niftyPCR)
                .liveSignals(activeSignals.stream().map(this::convertToDTO).toList())
                .strategyStatus(determineStrategyStatus(todaysSignals))
                // Option Chain Integration
                .optionChainMetrics(optionChainMetricsList)
                .optionChainOverallBias(optionChainOverallBias)
                .maxPainNifty(niftyOC != null && niftyOC.getMaxPain() != null ? niftyOC.getMaxPain().getMaxPainStrike() : null)
                .build();
    }

    private String determineOptionChainOverallBias(List<OptionChainMetrics> metricsList) {
        if (metricsList == null || metricsList.isEmpty()) return TREND_NEUTRAL;

        int bullishCount = 0;
        int bearishCount = 0;

        for (OptionChainMetrics m : metricsList) {
            if (TREND_BULLISH.equals(m.getOptionChainBias())) bullishCount++;
            if (TREND_BEARISH.equals(m.getOptionChainBias())) bearishCount++;
        }

        if (bullishCount > bearishCount) return TREND_BULLISH;
        if (bearishCount > bullishCount) return TREND_BEARISH;
        return TREND_NEUTRAL;
    }

    @Override
    public SymbolSummary getSymbolSummary(String symbol) {
        Long instrumentToken = SYMBOL_TOKEN_MAP.getOrDefault(symbol.toUpperCase(), NIFTY_INSTRUMENT_TOKEN);
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime last30Days = now.minusDays(30);

        List<BrahmastraSignal> signals = signalRepository.findBySymbolAndSignalTimeBetweenOrderBySignalTimeDesc(
                symbol, last30Days, now);

        int wins = (int) signals.stream().filter(s -> s.getPnl() != null && s.getPnl() > 0).count();
        int losses = (int) signals.stream().filter(s -> s.getPnl() != null && s.getPnl() < 0).count();
        double totalPnL = signals.stream()
                .filter(s -> s.getPnl() != null)
                .mapToDouble(BrahmastraSignal::getPnl)
                .sum();

        BrahmastraSignal lastSignal = signals.isEmpty() ? null : signals.get(0);

        return SymbolSummary.builder()
                .symbol(symbol)
                .instrumentToken(instrumentToken)
                .totalTrades(signals.size())
                .winningTrades(wins)
                .losingTrades(losses)
                .winRate((wins + losses) > 0 ? (double) wins / (wins + losses) * 100 : 0.0)
                .totalPnL(totalPnL)
                .activeSignals((int) signals.stream().filter(s -> STATUS_ACTIVE.equals(s.getStatus())).count())
                .lastSignalType(lastSignal != null ? lastSignal.getSignalType() : null)
                .lastSignalPrice(lastSignal != null ? lastSignal.getEntryPrice() : null)
                .currentTrend(determineTrend(symbol))
                .build();
    }

    // ==================== Signal Management ====================

    @Override
    public List<SignalDTO> getActiveSignals() {
        return signalRepository.findByStatusOrderBySignalTimeDesc(STATUS_ACTIVE)
                .stream()
                .map(this::convertToDTO)
                .toList();
    }

    @Override
    public List<SignalDTO> getSignalsByDateRange(String symbol, LocalDate from, LocalDate to) {
        return signalRepository.findBySymbolAndSignalTimeBetweenOrderBySignalTimeDesc(
                        symbol, from.atStartOfDay(), to.atTime(23, 59, 59))
                .stream()
                .map(this::convertToDTO)
                .toList();
    }

    @Override
    public BrahmastraSignal updateSignalStatus(Long signalId, String status, Double exitPrice, String exitReason) {
        BrahmastraSignal signal = signalRepository.findById(signalId)
                .orElseThrow(() -> new RuntimeException("Signal not found: " + signalId));

        signal.setStatus(status);
        signal.setExitPrice(exitPrice);
        signal.setExitReason(exitReason);
        signal.setExitTime(LocalDateTime.now());

        // Calculate PnL
        if (exitPrice != null && signal.getEntryPrice() != null) {
            double pnl = SIGNAL_BUY.equals(signal.getSignalType())
                    ? exitPrice - signal.getEntryPrice()
                    : signal.getEntryPrice() - exitPrice;
            signal.setPnl(pnl);
            signal.setPnlPercent(pnl / signal.getEntryPrice() * 100);
        }

        return signalRepository.save(signal);
    }

    @Override
    public SignalDTO getSignalById(Long id) {
        return signalRepository.findById(id)
                .map(this::convertToDTO)
                .orElse(null);
    }

    // ==================== Real-time Scanning ====================

    @Override
    public void processLiveTick(Long instrumentToken, Double ltp, Integer appJobConfigNum) {
        String symbol = TOKEN_SYMBOL_MAP.getOrDefault(instrumentToken, "UNKNOWN");
        String timeframe = "5m"; // Default timeframe
        LocalDateTime now = LocalDateTime.now();

        // Update candle cache with new tick
        updateCandleCache(instrumentToken, ltp, now);

        // Scan for signals
        LiveScanResult result = scanSymbol(symbol, timeframe);

        if (!SIGNAL_NONE.equals(result.getSignalType()) && Boolean.TRUE.equals(result.getIsNewSignal())) {
            logger.info("New Brahmastra signal detected: {} {} at {}", result.getSignalType(), symbol, ltp);

            // Save signal
            BrahmastraSignal signal = BrahmastraSignal.builder()
                    .instrumentToken(instrumentToken)
                    .symbol(symbol)
                    .timeframe(timeframe)
                    .signalType(result.getSignalType())
                    .signalTime(now)
                    .entryPrice(ltp)
                    .stopLoss(result.getStopLoss())
                    .target1(result.getTarget1())
                    .target2(result.getTarget2())
                    .confidenceScore(result.getConfidenceScore())
                    .supertrendValue(result.getSupertrend())
                    .supertrendTrend(result.getSupertrendStatus())
                    .macdLine(result.getMacdLine())
                    .macdSignalLine(result.getMacdSignal())
                    .vwapValue(result.getVwap())
                    .pcrValue(result.getPcr())
                    .pcrBias(result.getPcrBias())
                    .status(STATUS_ACTIVE)
                    .appJobConfigNum(appJobConfigNum)
                    .build();

            signalRepository.save(signal);
            lastSignalCache.put(symbol, signal);

            // Push to WebSocket
            try {
                messagingService.sendBrahmastraSignal(result);
            } catch (Exception e) {
                logger.error("Error sending WebSocket message: {}", e.getMessage());
            }

            // Send Telegram notification
            sendTelegramAlert(result);
        }
    }

    @Override
    public boolean isTripleConfirmation(Long instrumentToken, String signalType) {
        String symbol = TOKEN_SYMBOL_MAP.getOrDefault(instrumentToken, "UNKNOWN");
        LiveScanResult result = scanSymbol(symbol, "5m");
        return signalType.equals(result.getSignalType());
    }

    // ==================== PCR Integration ====================

    @Override
    public Double getCurrentPCR(String symbol) {
        try {
            // Get PCR from IndexLTP (meanStrikePCR field) - use display=true query for accurate data
            Integer appJobConfigNum = getAppJobConfigNum(symbol);
            List<IndexLTP> indexLTPList = indexLTPRepository
                    .findLatestIndexDataByAppJobConfigNum(appJobConfigNum);

            if (!indexLTPList.isEmpty()) {
                IndexLTP latestLTP = indexLTPList.get(0);
                if (latestLTP.getMeanStrikePCR() != null) {
                    logger.debug("PCR for {} (config={}): {} from IndexLTP id={}, ts={}",
                            symbol, appJobConfigNum, latestLTP.getMeanStrikePCR(),
                            latestLTP.getId(), latestLTP.getIndexTS());
                    return latestLTP.getMeanStrikePCR();
                } else {
                    logger.warn("PCR is null for {} in IndexLTP id={}", symbol, latestLTP.getId());
                }
            } else {
                logger.warn("No display=true IndexLTP records found for {} (config={})", symbol, appJobConfigNum);
            }
        } catch (Exception e) {
            logger.warn("Error fetching PCR for {}: {}", symbol, e.getMessage());
        }
        return 1.0; // Neutral default
    }

    @Override
    public String getPCRBias(Double pcr) {
        if (pcr == null) return TREND_NEUTRAL;
        if (pcr > PCR_BULLISH_THRESHOLD) return TREND_BULLISH;
        if (pcr < PCR_BEARISH_THRESHOLD) return TREND_BEARISH;
        return TREND_NEUTRAL;
    }

    @Override
    public boolean isSignalAllowedByPCR(String signalType, Double pcr) {
        if (pcr == null) return true;

        String bias = getPCRBias(pcr);

        // PCR > 1.2: Bullish - Only allow BUY
        if (TREND_BULLISH.equals(bias) && SIGNAL_SELL.equals(signalType)) {
            return false;
        }

        // PCR < 0.8: Bearish - Only allow SELL
        if (TREND_BEARISH.equals(bias) && SIGNAL_BUY.equals(signalType)) {
            return false;
        }

        return true;
    }

    // ==================== Indicator Calculations ====================

    /**
     * Calculate all indicators for the candle list.
     */
    private void calculateIndicators(List<CandleData> candles, SignalRequest request) {
        calculateSupertrend(candles, request.getSupertrendPeriod(), request.getSupertrendMultiplier());
        calculateMACD(candles, request.getMacdFastPeriod(), request.getMacdSlowPeriod(), request.getMacdSignalPeriod());
        calculateVWAP(candles);
    }

    /**
     * Calculate Supertrend indicator (ATR-based).
     *
     * Formula:
     * Basic Upper Band = (HL2 + (Multiplier * ATR))
     * Basic Lower Band = (HL2 - (Multiplier * ATR))
     * Final Upper = price > prev Upper ? min(Basic Upper, prev Final Upper) : Basic Upper
     * Final Lower = price < prev Lower ? max(Basic Lower, prev Final Lower) : Basic Lower
     * Trend = Close <= Final Lower ? DOWN : UP
     */
    private void calculateSupertrend(List<CandleData> candles, int period, double multiplier) {
        if (candles == null || candles.isEmpty()) return;

        // First calculate ATR
        calculateATR(candles, period);

        double prevFinalUpper = 0;
        double prevFinalLower = 0;
        String prevTrend = TREND_BULLISH;

        for (int i = 0; i < candles.size(); i++) {
            CandleData candle = candles.get(i);

            // Skip candles with null values
            if (candle == null || candle.getClose() == null || candle.getHigh() == null || candle.getLow() == null) {
                continue;
            }

            if (i < period || candle.getAtr() == null) {
                candle.setSupertrend(candle.getClose());
                candle.setSupertrendTrend(TREND_NEUTRAL);
                continue;
            }

            double hl2 = (candle.getHigh() + candle.getLow()) / 2;
            double atr = candle.getAtr();

            double basicUpper = hl2 + (multiplier * atr);
            double basicLower = hl2 - (multiplier * atr);

            double finalUpper, finalLower;

            if (i == period) {
                finalUpper = basicUpper;
                finalLower = basicLower;
            } else {
                CandleData prevCandle = candles.get(i - 1);

                // Final Upper: constrain band (min) only while price stays at/below it (resistance).
                // When price breaks above, reset to basicUpper (no constraint needed).
                finalUpper = (prevCandle.getClose() <= prevFinalUpper)
                        ? Math.min(basicUpper, prevFinalUpper)
                        : basicUpper;

                // Final Lower: constrain band (max) only while price stays at/above it (support).
                // When price breaks below, reset to basicLower (no constraint needed).
                finalLower = (prevCandle.getClose() >= prevFinalLower)
                        ? Math.max(basicLower, prevFinalLower)
                        : basicLower;
            }

            // Determine trend
            String trend;
            double supertrendValue;

            if (prevTrend.equals(TREND_BULLISH)) {
                // Was in uptrend
                if (candle.getClose() < finalLower) {
                    trend = TREND_BEARISH;
                    supertrendValue = finalUpper;
                } else {
                    trend = TREND_BULLISH;
                    supertrendValue = finalLower;
                }
            } else {
                // Was in downtrend
                if (candle.getClose() > finalUpper) {
                    trend = TREND_BULLISH;
                    supertrendValue = finalLower;
                } else {
                    trend = TREND_BEARISH;
                    supertrendValue = finalUpper;
                }
            }

            candle.setSupertrend(supertrendValue);
            candle.setSupertrendTrend(trend);

            prevFinalUpper = finalUpper;
            prevFinalLower = finalLower;
            prevTrend = trend;
        }
    }

    /**
     * Calculate ATR (Average True Range).
     */
    private void calculateATR(List<CandleData> candles, int period) {
        if (candles == null || candles.isEmpty()) return;

        double[] tr = new double[candles.size()];

        for (int i = 0; i < candles.size(); i++) {
            CandleData candle = candles.get(i);

            // Skip if candle has null values
            if (candle == null || candle.getHigh() == null || candle.getLow() == null || candle.getClose() == null) {
                tr[i] = 0;
                continue;
            }

            if (i == 0) {
                tr[i] = candle.getHigh() - candle.getLow();
            } else {
                CandleData prevCandle = candles.get(i - 1);
                // Skip if previous candle has null values
                if (prevCandle == null || prevCandle.getClose() == null) {
                    tr[i] = candle.getHigh() - candle.getLow();
                    continue;
                }
                double hl = candle.getHigh() - candle.getLow();
                double hpc = Math.abs(candle.getHigh() - prevCandle.getClose());
                double lpc = Math.abs(candle.getLow() - prevCandle.getClose());
                tr[i] = Math.max(hl, Math.max(hpc, lpc));
            }
        }

        // Calculate ATR using EMA
        double atr = 0;
        for (int i = 0; i < candles.size(); i++) {
            if (i < period) {
                atr += tr[i];
                if (i == period - 1) {
                    atr /= period;
                    candles.get(i).setAtr(atr);
                }
            } else {
                atr = ((atr * (period - 1)) + tr[i]) / period;
                candles.get(i).setAtr(atr);
            }
        }
    }

    /**
     * Calculate MACD (12, 26, 9).
     *
     * MACD Line = EMA12 - EMA26
     * Signal Line = EMA9 of MACD Line
     * Histogram = MACD Line - Signal Line
     */
    private void calculateMACD(List<CandleData> candles, int fastPeriod, int slowPeriod, int signalPeriod) {
        // Calculate EMAs
        double[] emaFast = calculateEMA(candles, fastPeriod);
        double[] emaSlow = calculateEMA(candles, slowPeriod);

        // Calculate MACD line
        double[] macdLine = new double[candles.size()];
        for (int i = 0; i < candles.size(); i++) {
            if (i >= slowPeriod - 1) {
                macdLine[i] = emaFast[i] - emaSlow[i];
            }
        }

        // Calculate Signal line (EMA of MACD)
        double[] signalLine = new double[candles.size()];
        double emaMultiplier = 2.0 / (signalPeriod + 1);
        double signal = 0;

        // Seed: accumulate exactly signalPeriod MACD values (slowPeriod-1 .. slowPeriod+signalPeriod-2)
        for (int i = 0; i < candles.size(); i++) {
            if (i < slowPeriod + signalPeriod - 1) {
                if (i >= slowPeriod - 1) {
                    signal += macdLine[i];
                    if (i == slowPeriod + signalPeriod - 2) {
                        signal /= signalPeriod;
                    }
                }
            } else {
                signal = (macdLine[i] * emaMultiplier) + (signal * (1 - emaMultiplier));
            }
            signalLine[i] = signal;
        }

        // Set values on candles (valid from the end of the signal seed period)
        for (int i = slowPeriod + signalPeriod - 1; i < candles.size(); i++) {
            CandleData candle = candles.get(i);
            candle.setMacdLine(macdLine[i]);
            candle.setMacdSignal(signalLine[i]);
            candle.setMacdHistogram(macdLine[i] - signalLine[i]);
        }
    }

    /**
     * Calculate EMA (Exponential Moving Average).
     */
    private double[] calculateEMA(List<CandleData> candles, int period) {
        if (candles == null || candles.isEmpty()) return new double[0];
        double[] ema = new double[candles.size()];

        double multiplier = 2.0 / (period + 1);

        double sum = 0;
        int validCount = 0;
        for (int i = 0; i < candles.size(); i++) {
            CandleData candle = candles.get(i);
            double closePrice = (candle != null && candle.getClose() != null) ? candle.getClose() : 0.0;

            if (i < period) {
                sum += closePrice;
                validCount++;
                if (i == period - 1) {
                    ema[i] = validCount > 0 ? sum / validCount : 0;
                }
            } else {
                ema[i] = (closePrice * multiplier) + (ema[i - 1] * (1 - multiplier));
            }
        }

        return ema;
    }

    /**
     * Calculate VWAP (Volume Weighted Average Price).
     * VWAP = Cumulative (Price * Volume) / Cumulative Volume
     * Resets at market open (new trading day).
     *
     * For index instruments (NIFTY) that have no volume data,
     * uses candle range (high - low) as a volume proxy so VWAP remains responsive
     * to price action instead of degenerating to a flat average.
     */
    private void calculateVWAP(List<CandleData> candles) {
        if (candles == null || candles.isEmpty()) return;

        double cumulativePV = 0;
        double cumulativeVolume = 0;
        LocalDate currentDay = null;

        // Detect if this data has real volume (non-index instruments)
        boolean hasRealVolume = candles.stream()
                .anyMatch(c -> c != null && c.getVolume() != null && c.getVolume() > 0);

        for (CandleData candle : candles) {
            if (candle == null || candle.getTimestamp() == null ||
                candle.getHigh() == null || candle.getLow() == null || candle.getClose() == null) {
                continue;
            }

            LocalDate candleDay = candle.getTimestamp().toLocalDate();

            // Reset at new day
            if (!candleDay.equals(currentDay)) {
                cumulativePV = 0;
                cumulativeVolume = 0;
                currentDay = candleDay;
            }

            double typicalPrice = (candle.getHigh() + candle.getLow() + candle.getClose()) / 3;

            double volume;
            if (hasRealVolume) {
                // Use actual volume for stocks/futures
                volume = candle.getVolume() != null && candle.getVolume() > 0 ? candle.getVolume() : 1;
            } else {
                // For indices without volume: use candle range as volume proxy
                // This makes VWAP weight wider-range (higher activity) candles more
                double range = candle.getHigh() - candle.getLow();
                volume = Math.max(range, 0.01); // Minimum proxy volume to avoid zero
            }

            cumulativePV += typicalPrice * volume;
            cumulativeVolume += volume;

            candle.setVwap(cumulativeVolume > 0 ? cumulativePV / cumulativeVolume : typicalPrice);
        }
    }

    // ==================== Signal Logic ====================

    /**
     * Check for Triple Confirmation signal.
     *
     * BUY: Supertrend Bullish + MACD Bullish + Price at/below VWAP
     * SELL: Supertrend Bearish + MACD Bearish + Price at/above VWAP
     */
    private String checkTripleConfirmation(CandleData current, CandleData prev, SignalRequest request) {
        if (current.getSupertrend() == null || current.getMacdLine() == null || current.getVwap() == null) {
            return SIGNAL_NONE;
        }

        boolean supertrendBullish = TREND_BULLISH.equals(current.getSupertrendTrend());
        boolean supertrendBearish = TREND_BEARISH.equals(current.getSupertrendTrend());

        // MACD crossover detection
        boolean macdBullish = current.getMacdLine() > current.getMacdSignal();
        boolean macdBearish = current.getMacdLine() < current.getMacdSignal();

        // Check for fresh crossover (not just existing position)
        boolean macdBullishCrossover = macdBullish &&
                (prev.getMacdLine() == null || prev.getMacdLine() <= prev.getMacdSignal());
        boolean macdBearishCrossover = macdBearish &&
                (prev.getMacdLine() == null || prev.getMacdLine() >= prev.getMacdSignal());

        // VWAP condition (±tolerance for "near VWAP" zone)
        // BUY: Price should be at or below VWAP (discount zone) - allows slight tolerance above
        // SELL: Price should be at or above VWAP (premium zone) - allows slight tolerance below
        double vwapTolerance = request.getVwapTolerance() != null ? request.getVwapTolerance() : 0.002;
        double vwapUpper = current.getVwap() * (1 + vwapTolerance);
        double vwapLower = current.getVwap() * (1 - vwapTolerance);

        // For BUY: price must be at/below VWAP (or slightly above within tolerance)
        boolean priceInBuyZone = current.getClose() <= vwapUpper;
        // For SELL: price must be at/above VWAP (or slightly below within tolerance)
        boolean priceInSellZone = current.getClose() >= vwapLower;

        // Triple Confirmation BUY
        if (supertrendBullish && macdBullishCrossover && priceInBuyZone) {
            return SIGNAL_BUY;
        }

        // Triple Confirmation SELL
        if (supertrendBearish && macdBearishCrossover && priceInSellZone) {
            return SIGNAL_SELL;
        }

        return SIGNAL_NONE;
    }

    /**
     * Check if market is sideways (too many Supertrend flips).
     */
    private boolean isSidewaysMarket(List<CandleData> candles, int currentIndex, int lookback, int maxFlips) {
        if (currentIndex < lookback) return false;

        int flips = 0;
        String prevTrend = null;

        for (int i = currentIndex - lookback; i <= currentIndex; i++) {
            String trend = candles.get(i).getSupertrendTrend();
            // Only count flips between BULLISH and BEARISH — skip NEUTRAL (warm-up candles)
            if (TREND_NEUTRAL.equals(trend)) continue;
            if (prevTrend != null && !prevTrend.equals(trend)) {
                flips++;
            }
            prevTrend = trend;
        }

        return flips > maxFlips;
    }

    // ==================== Helper Methods ====================

    /**
     * Fetch candle data using Kite Historical Data API.
     * Falls back to database if API fails.
     */
    private List<CandleData> fetchCandleData(Long instrumentToken, String timeframe,
                                              LocalDateTime from, LocalDateTime to) {
        logger.info("Fetching candle data for instrument {} from {} to {} with timeframe {}",
                instrumentToken, from, to, timeframe);

        // 1. PRIMARY: Fetch 1-minute candles from CandleStick database
        //    (KiteTickerProvider.processCandleForNifty50 persists live candles to this table every minute)
        try {
            List<CandleStick> dbCandles = candleStickRepository.findByInstrumentTokenAndCandleStartTimeBetween(
                    instrumentToken, from, to);

            if (dbCandles != null && !dbCandles.isEmpty()) {
                logger.info("Found {} 1-minute candles from CandleStick database", dbCandles.size());

                List<CandleData> oneMinCandles = dbCandles.stream()
                        .filter(cs -> cs != null &&
                                      cs.getOpenPrice() != null &&
                                      cs.getHighPrice() != null &&
                                      cs.getLowPrice() != null &&
                                      cs.getClosePrice() != null)
                        .map(this::convertToCandleData)
                        .sorted(Comparator.comparing(CandleData::getTimestamp))
                        .toList();

                if (!oneMinCandles.isEmpty()) {
                    List<CandleData> aggregated = aggregateCandlesToTimeframe(oneMinCandles, timeframe);
                    logger.info("Aggregated {} 1-min candles to {} candles in {} timeframe",
                            oneMinCandles.size(), aggregated.size(), timeframe);
                    return aggregated;
                }
            }
        } catch (Exception e) {
            logger.warn("Error fetching candle data from CandleStick database: {}", e.getMessage());
        }

        // 2. FALLBACK: Try Kite Historical Data API
        try {
            List<CandleData> kiteCandles = fetchFromKiteHistoricalApi(instrumentToken, timeframe, from, to);
            if (!kiteCandles.isEmpty()) {
                logger.info("Successfully fetched {} candles from Kite Historical API", kiteCandles.size());
                return kiteCandles;
            }
        } catch (Exception e) {
            logger.warn("Failed to fetch from Kite Historical API: {}", e.getMessage());
        }

        logger.warn("No candle data available from any source for instrument {}", instrumentToken);
        return new ArrayList<>();
    }

    /**
     * Fetch candle data from Kite Historical Data API.
     */
    private List<CandleData> fetchFromKiteHistoricalApi(Long instrumentToken, String timeframe,
                                                         LocalDateTime from, LocalDateTime to) {
        // Convert timeframe to Kite API interval format
        String kiteInterval = convertToKiteInterval(timeframe);

        HistoricalDataRequest request = HistoricalDataRequest.builder()
                .instrumentToken(String.valueOf(instrumentToken))
                .fromDate(from)
                .toDate(to)
                .interval(kiteInterval)
                .continuous(false)
                .oi(false)
                .build();

        HistoricalDataResponse response = instrumentService.getHistoricalData(request);

        if (response == null || !response.isSuccess() || response.getCandles() == null) {
            logger.warn("Kite Historical API returned unsuccessful response: {}",
                    response != null ? response.getMessage() : "null response");
            return Collections.emptyList();
        }

        return response.getCandles().stream()
                .map(this::convertHistoricalCandleToCandleData)
                .filter(cd -> cd != null && cd.getClose() != null)
                .sorted(Comparator.comparing(CandleData::getTimestamp))
                .toList();
    }

    /**
     * Convert Brahmastra timeframe format to Kite API interval format.
     * Brahmastra: "5m", "15m", "1h", "1d"
     * Kite API: "minute", "3minute", "5minute", "10minute", "15minute", "30minute", "60minute", "day"
     */
    private String convertToKiteInterval(String timeframe) {
        if (timeframe == null) {
            return "5minute"; // Default to 5 minute
        }

        String tf = timeframe.toLowerCase().trim();

        // Direct Kite format
        if (tf.contains("minute") || tf.equals("day")) {
            return tf;
        }

        // Convert from short format (e.g., "5m", "1h", "1d")
        switch (tf) {
            case "1m":
            case "1min":
                return "minute";
            case "3m":
            case "3min":
                return "3minute";
            case "5m":
            case "5min":
                return "5minute";
            case "10m":
            case "10min":
                return "10minute";
            case "15m":
            case "15min":
                return "15minute";
            case "30m":
            case "30min":
                return "30minute";
            case "1h":
            case "60m":
            case "60min":
                return "60minute";
            case "1d":
            case "d":
            case "day":
                return "day";
            default:
                return "5minute"; // Default
        }
    }

    /**
     * Convert Kite HistoricalCandle to CandleData.
     */
    private CandleData convertHistoricalCandleToCandleData(HistoricalDataResponse.HistoricalCandle candle) {
        if (candle == null) {
            return null;
        }

        LocalDateTime timestamp = parseTimestamp(candle.getTimestamp());
        if (timestamp == null) {
            logger.warn("Skipping candle with unparseable timestamp: {}", candle.getTimestamp());
            return null;
        }

        return CandleData.builder()
                .timestamp(timestamp)
                .open(candle.getOpen())
                .high(candle.getHigh())
                .low(candle.getLow())
                .close(candle.getClose())
                .volume(candle.getVolume())
                .build();
    }

    /**
     * Parse timestamp string to LocalDateTime.
     * Supports multiple formats from Kite API.
     */
    private LocalDateTime parseTimestamp(String timestampStr) {
        if (timestampStr == null || timestampStr.isEmpty()) return null;
        // Kite API sends +0530 (no colon) — try offset-aware parse first
        try {
            return java.time.ZonedDateTime.parse(timestampStr, KITE_OFFSET_FORMATTER).toLocalDateTime();
        } catch (DateTimeParseException ignored) {}
        // ISO offset with colon (+05:30) or Z
        try {
            return java.time.ZonedDateTime.parse(timestampStr).toLocalDateTime();
        } catch (DateTimeParseException ignored) {}
        // Local (no offset) formats
        for (DateTimeFormatter formatter : LOCAL_TIMESTAMP_FORMATTERS) {
            try {
                return LocalDateTime.parse(timestampStr, formatter);
            } catch (DateTimeParseException ignored) {}
        }
        // Date-only fallback
        try {
            return LocalDate.parse(timestampStr.substring(0, 10)).atStartOfDay();
        } catch (Exception e) {
            logger.warn("Could not parse timestamp: {}", timestampStr);
            return null;
        }
    }

    private List<CandleData> fetchRecentCandles(Long instrumentToken, String timeframe) {
        LocalDateTime to = LocalDateTime.now();
        LocalDateTime from = to.minusDays(5); // Fetch 5 days for proper indicator warmup (Supertrend needs ~20 bars + ATR warmup)

        return fetchCandleData(instrumentToken, timeframe, from, to);
    }

    private CandleData convertToCandleData(CandleStick candleStick) {
        return CandleData.builder()
                .timestamp(candleStick.getCandleStartTime())
                .open(candleStick.getOpenPrice())
                .high(candleStick.getHighPrice())
                .low(candleStick.getLowPrice())
                .close(candleStick.getClosePrice())
                .volume(0L) // Volume not available in CandleStick entity
                .build();
    }

    /**
     * Aggregate 1-minute CandleData to a higher timeframe (e.g., 5m, 15m, 30m, 1h).
     * Groups candles into buckets based on the requested interval and computes OHLC for each bucket.
     */
    private List<CandleData> aggregateCandlesToTimeframe(List<CandleData> oneMinCandles, String timeframe) {
        if (oneMinCandles == null || oneMinCandles.isEmpty()) {
            return new ArrayList<>();
        }

        int intervalMinutes = getIntervalMinutes(timeframe);
        if (intervalMinutes <= 1) {
            // Already 1-minute or unknown — return as-is
            return oneMinCandles;
        }

        List<CandleData> aggregated = new ArrayList<>();
        int i = 0;

        while (i < oneMinCandles.size()) {
            CandleData first = oneMinCandles.get(i);
            LocalDateTime bucketStart = alignToBucket(first.getTimestamp(), intervalMinutes);
            LocalDateTime bucketEnd = bucketStart.plusMinutes(intervalMinutes);

            double open = first.getOpen();
            double high = first.getHigh();
            double low = first.getLow();
            double close = first.getClose();
            long volume = first.getVolume() != null ? first.getVolume() : 0L;

            i++;

            // Collect all 1-minute candles within this bucket
            while (i < oneMinCandles.size()) {
                CandleData c = oneMinCandles.get(i);
                if (c.getTimestamp().isBefore(bucketEnd)) {
                    if (c.getHigh() > high) high = c.getHigh();
                    if (c.getLow() < low) low = c.getLow();
                    close = c.getClose();
                    volume += (c.getVolume() != null ? c.getVolume() : 0L);
                    i++;
                } else {
                    break;
                }
            }

            aggregated.add(CandleData.builder()
                    .timestamp(bucketStart)
                    .open(open)
                    .high(high)
                    .low(low)
                    .close(close)
                    .volume(volume)
                    .build());
        }

        return aggregated;
    }

    /**
     * Get the number of minutes for a given timeframe string.
     */
    private int getIntervalMinutes(String timeframe) {
        if (timeframe == null) return 5;
        String tf = timeframe.toLowerCase().trim();
        switch (tf) {
            case "1m": case "1min": case "minute": return 1;
            case "3m": case "3min": case "3minute": return 3;
            case "5m": case "5min": case "5minute": return 5;
            case "10m": case "10min": case "10minute": return 10;
            case "15m": case "15min": case "15minute": return 15;
            case "30m": case "30min": case "30minute": return 30;
            case "1h": case "60m": case "60min": case "60minute": return 60;
            case "1d": case "day": return 1440;
            default: return 5;
        }
    }

    /**
     * Align a timestamp to the start of its bucket (e.g., 09:17 aligned to 5-min bucket = 09:15).
     * Market starts at 09:15 IST, so buckets align relative to that.
     */
    private LocalDateTime alignToBucket(LocalDateTime timestamp, int intervalMinutes) {
        int hour = timestamp.getHour();
        int minute = timestamp.getMinute();
        int totalMinutes = hour * 60 + minute;

        // Align to market open at 09:15 (= 555 minutes from midnight)
        int marketOpenMinutes = 9 * 60 + 15;
        int minutesSinceOpen = totalMinutes - marketOpenMinutes;
        if (minutesSinceOpen < 0) minutesSinceOpen = 0;

        int bucketIndex = minutesSinceOpen / intervalMinutes;
        int alignedMinutesSinceOpen = bucketIndex * intervalMinutes;
        int alignedTotalMinutes = marketOpenMinutes + alignedMinutesSinceOpen;

        int alignedHour = alignedTotalMinutes / 60;
        int alignedMinute = alignedTotalMinutes % 60;

        return timestamp.toLocalDate().atTime(alignedHour, alignedMinute, 0);
    }

    private SignalDTO buildSignal(CandleData candle, CandleData prev, String signalType, SignalRequest request) {
        // Use prev candle's low/high for SL (consistent with calculateStopLoss logic)
        double stopLoss = SIGNAL_BUY.equals(signalType)
                ? (prev != null ? prev.getLow() : candle.getLow())
                : (prev != null ? prev.getHigh() : candle.getHigh());

        // Risk = distance from entry to SL
        double riskPerPoint = Math.abs(candle.getClose() - stopLoss);

        // Ensure minimum risk to avoid zero-risk scenarios
        if (riskPerPoint < 0.01) {
            riskPerPoint = SIGNAL_BUY.equals(signalType)
                    ? candle.getClose() - candle.getLow()
                    : candle.getHigh() - candle.getClose();
        }

        return SignalDTO.builder()
                .signalType(signalType)
                .signalTime(candle.getTimestamp())
                .entryPrice(candle.getClose())
                .stopLoss(stopLoss)
                .target1(SIGNAL_BUY.equals(signalType)
                        ? candle.getClose() + riskPerPoint
                        : candle.getClose() - riskPerPoint)
                .target2(SIGNAL_BUY.equals(signalType)
                        ? candle.getClose() + (2 * riskPerPoint)
                        : candle.getClose() - (2 * riskPerPoint))
                .target3(SIGNAL_BUY.equals(signalType)
                        ? candle.getClose() + (3 * riskPerPoint)
                        : candle.getClose() - (3 * riskPerPoint))
                .riskRewardRatio(2.0)
                .confidenceScore(calculateConfidence(candle, signalType))
                .supertrendValue(candle.getSupertrend())
                .supertrendTrend(candle.getSupertrendTrend())
                .macdLine(candle.getMacdLine())
                .macdSignalLine(candle.getMacdSignal())
                .macdHistogram(candle.getMacdHistogram())
                .vwapValue(candle.getVwap())
                .priceToVwapPercent(candle.getVwap() != null && candle.getVwap() != 0
                        ? (candle.getClose() - candle.getVwap()) / candle.getVwap() * 100
                        : null)
                .status(STATUS_ACTIVE)
                .open(candle.getOpen())
                .high(candle.getHigh())
                .low(candle.getLow())
                .close(candle.getClose())
                .build();
    }

    private BrahmastraSignal toEntity(SignalDTO dto, Long instrumentToken, SignalRequest request) {
        return BrahmastraSignal.builder()
                .instrumentToken(instrumentToken)
                .symbol(request.getSymbol())
                .timeframe(request.getTimeframe())
                .signalType(dto.getSignalType())
                .signalTime(dto.getSignalTime())
                .entryPrice(dto.getEntryPrice())
                .stopLoss(dto.getStopLoss())
                .target1(dto.getTarget1())
                .target2(dto.getTarget2())
                .target3(dto.getTarget3())
                .riskRewardRatio(dto.getRiskRewardRatio())
                .confidenceScore(dto.getConfidenceScore())
                .supertrendValue(dto.getSupertrendValue())
                .supertrendTrend(dto.getSupertrendTrend())
                .macdLine(dto.getMacdLine())
                .macdSignalLine(dto.getMacdSignalLine())
                .vwapValue(dto.getVwapValue())
                .priceToVwapPercent(dto.getPriceToVwapPercent())
                .pcrFilterApplied(request.getUsePCR())
                .candleOpen(dto.getOpen())
                .candleHigh(dto.getHigh())
                .candleLow(dto.getLow())
                .candleClose(dto.getClose())
                .status(STATUS_ACTIVE)
                .build();
    }

    private void saveSignals(List<SignalDTO> dtos, Long instrumentToken, SignalRequest request) {
        List<BrahmastraSignal> entities = dtos.stream()
                .map(dto -> toEntity(dto, instrumentToken, request))
                .toList();
        signalRepository.saveAll(entities);
    }

    private void saveBacktestResult(BacktestResult result, BacktestRequest request) {
        try {
            BrahmastraBacktestResult entity = BrahmastraBacktestResult.builder()
                    .symbol(request.getSymbol())
                    .timeframe(request.getTimeframe())
                    .fromDate(request.getFromDate().atStartOfDay())
                    .toDate(request.getToDate().atTime(23, 59, 59))
                    .initialCapital(request.getInitialCapital())
                    .riskPerTrade(request.getRiskPerTrade())
                    .usePCR(request.getUsePCR())
                    .finalCapital(result.getFinalCapital())
                    .netPnL(result.getNetPnL())
                    .netPnLPercent(result.getNetPnLPercent())
                    .totalTrades(result.getTotalTrades())
                    .winningTrades(result.getWinningTrades())
                    .losingTrades(result.getLosingTrades())
                    .winRate(result.getWinRate())
                    .averageWin(result.getAverageWin())
                    .averageLoss(result.getAverageLoss())
                    .profitFactor(result.getProfitFactor())
                    .averageRR(result.getAverageRiskReward())
                    .expectancy(result.getExpectancy())
                    .maxDrawdown(result.getMaxDrawdown())
                    .maxDrawdownPercent(result.getMaxDrawdownPercent())
                    .sharpeRatio(result.getSharpeRatio())
                    .sortinoRatio(result.getSortinoRatio())
                    .calmarRatio(result.getCalmarRatio())
                    .volatility(result.getVolatility())
                    .tradeLogJson(objectMapper.writeValueAsString(result.getTradeLog()))
                    .equityCurveJson(objectMapper.writeValueAsString(result.getEquityCurve()))
                    .build();

            backtestRepository.save(entity);
        } catch (JsonProcessingException e) {
            logger.error("Error saving backtest result: {}", e.getMessage());
        }
    }

    private SignalDTO convertToDTO(BrahmastraSignal signal) {
        return SignalDTO.builder()
                .id(signal.getId())
                .symbol(signal.getSymbol())
                .timeframe(signal.getTimeframe())
                .signalType(signal.getSignalType())
                .signalTime(signal.getSignalTime())
                .entryPrice(signal.getEntryPrice())
                .stopLoss(signal.getStopLoss())
                .target1(signal.getTarget1())
                .target2(signal.getTarget2())
                .target3(signal.getTarget3())
                .riskRewardRatio(signal.getRiskRewardRatio())
                .confidenceScore(signal.getConfidenceScore())
                .supertrendValue(signal.getSupertrendValue())
                .supertrendTrend(signal.getSupertrendTrend())
                .macdLine(signal.getMacdLine())
                .macdSignalLine(signal.getMacdSignalLine())
                .vwapValue(signal.getVwapValue())
                .priceToVwapPercent(signal.getPriceToVwapPercent())
                .pcrValue(signal.getPcrValue())
                .pcrBias(signal.getPcrBias())
                .status(signal.getStatus())
                .exitTime(signal.getExitTime())
                .exitPrice(signal.getExitPrice())
                .pnl(signal.getPnl())
                .pnlPercent(signal.getPnlPercent())
                .open(signal.getCandleOpen())
                .high(signal.getCandleHigh())
                .low(signal.getCandleLow())
                .close(signal.getCandleClose())
                .build();
    }

    private Double calculateStopLoss(CandleData current, CandleData prev, String signalType) {
        if (SIGNAL_BUY.equals(signalType)) {
            return prev != null ? prev.getLow() : current.getLow();
        } else if (SIGNAL_SELL.equals(signalType)) {
            return prev != null ? prev.getHigh() : current.getHigh();
        }
        return null;
    }

    private Double calculateTarget(CandleData current, String signalType, double rrRatio, Double stopLoss) {
        if (current == null || stopLoss == null) return null;

        // Risk = distance from entry (close) to stop loss
        double risk = Math.abs(current.getClose() - stopLoss);

        // Ensure minimum risk
        if (risk < 0.01) {
            risk = SIGNAL_BUY.equals(signalType)
                    ? current.getClose() - current.getLow()
                    : current.getHigh() - current.getClose();
        }

        if (SIGNAL_BUY.equals(signalType)) {
            return current.getClose() + (risk * rrRatio);
        } else if (SIGNAL_SELL.equals(signalType)) {
            return current.getClose() - (risk * rrRatio);
        }
        return null;
    }

    private Double calculateConfidence(CandleData candle, String signalType) {
        if (candle == null || SIGNAL_NONE.equals(signalType)) return 0.0;

        double confidence = 60.0; // Base confidence for triple confirmation

        // Add confidence for strong MACD
        if (candle.getMacdHistogram() != null) {
            double histogramStrength = Math.abs(candle.getMacdHistogram());
            confidence += Math.min(10, histogramStrength * 2);
        }

        // Add confidence for price distance from Supertrend
        if (candle.getSupertrend() != null) {
            double stDistance = Math.abs(candle.getClose() - candle.getSupertrend()) / candle.getClose() * 100;
            confidence += Math.min(10, stDistance * 5);
        }

        // Add confidence for VWAP proximity
        if (candle.getVwap() != null) {
            double vwapDistance = Math.abs(candle.getClose() - candle.getVwap()) / candle.getVwap() * 100;
            confidence += Math.max(0, Math.min(10, (1 - vwapDistance) * 10));
        }

        // Cap at 95%
        return Math.min(95, confidence);
    }

    private String getVwapStatus(CandleData candle) {
        if (candle.getVwap() == null) return TREND_NEUTRAL;

        double diff = (candle.getClose() - candle.getVwap()) / candle.getVwap();
        if (diff > 0.002) return "ABOVE";
        if (diff < -0.002) return "BELOW";
        return "NEAR";
    }

    private boolean isNewSignal(String symbol, String signalType) {
        BrahmastraSignal lastSignal = lastSignalCache.get(symbol);
        if (lastSignal == null) return true;

        // Check if last signal is more than 30 minutes old or different type
        return lastSignal.getSignalTime().isBefore(LocalDateTime.now().minusMinutes(30))
                || !signalType.equals(lastSignal.getSignalType());
    }

    private String buildSignalMessage(String symbol, String signalType, CandleData candle) {
        if (SIGNAL_NONE.equals(signalType)) {
            return "No signal";
        }
        return String.format("Brahmastra %s Signal on %s at %.2f (ST: %s, MACD: %.2f, VWAP: %.2f)",
                signalType, symbol, candle.getClose(),
                candle.getSupertrendTrend(),
                candle.getMacdLine() != null ? candle.getMacdLine() : 0.0,
                candle.getVwap() != null ? candle.getVwap() : 0.0);
    }

    private LiveScanResult buildNoSignalResult(String symbol, Long instrumentToken) {
        return LiveScanResult.builder()
                .symbol(symbol)
                .instrumentToken(instrumentToken)
                .scanTime(LocalDateTime.now())
                .signalType(SIGNAL_NONE)
                .isNewSignal(false)
                .alertLevel("LOW")
                .build();
    }

    private void updateCandleCache(Long instrumentToken, Double ltp, LocalDateTime time) {
        // Invalidate the candle cache for this instrument so that the next scan
        // fetches fresh data from the CandleStick database
        String symbol = TOKEN_SYMBOL_MAP.getOrDefault(instrumentToken, "NIFTY");
        // Remove all timeframe caches for this symbol so scanSymbol re-fetches
        candleCache.entrySet().removeIf(entry -> entry.getKey().startsWith(symbol + "_"));
        candleCacheTimestamp.entrySet().removeIf(entry -> entry.getKey().startsWith(symbol + "_"));
        logger.debug("Invalidated candle cache for {} on live tick at {}, LTP={}", symbol, time, ltp);
    }

    private Double getPCRForTime(Long instrumentToken, LocalDateTime time) {
        String symbol = TOKEN_SYMBOL_MAP.getOrDefault(instrumentToken, "NIFTY");
        return getCurrentPCR(symbol);
    }

    private Integer getAppJobConfigNum(String symbol) {
        return 1;
    }

    private String determineTrend(String symbol) {
        LiveScanResult result = scanSymbol(symbol, "5m");
        return result.getSupertrendStatus() != null ? result.getSupertrendStatus() : TREND_NEUTRAL;
    }

    private String determineOverallBias(Double niftyPCR) {
        if (niftyPCR == null) return TREND_NEUTRAL;
        return getPCRBias(niftyPCR);
    }

    private String determineStrategyStatus(List<BrahmastraSignal> recentSignals) {
        if (recentSignals.isEmpty()) return "HEALTHY";

        long wins = recentSignals.stream().filter(s -> s.getPnl() != null && s.getPnl() > 0).count();
        long total = recentSignals.stream().filter(s -> s.getPnl() != null).count();

        if (total == 0) return "HEALTHY";

        double winRate = (double) wins / total * 100;

        if (winRate >= 50) return "HEALTHY";
        if (winRate >= 35) return "CAUTION";
        return "PAUSE";
    }

    private void sendTelegramAlert(LiveScanResult result) {
        // Check if Brahmastra alerts are enabled in telegram settings
        if (!telegramNotificationService.isAlertTypeEnabled("TRADE", "BRAHMASTRA")) {
            logger.debug("Brahmastra telegram alerts are disabled, skipping notification");
            return;
        }

        try {
            double risk = result.getEntryPrice() != null && result.getStopLoss() != null
                    ? Math.abs(result.getEntryPrice() - result.getStopLoss()) : 0;
            String message = String.format(
                    "🎯 *BRAHMASTRA SIGNAL*\n\n" +
                    "Symbol: %s\n" +
                    "Signal: %s\n" +
                    "Entry: %.2f\n" +
                    "Stop Loss: %.2f\n" +
                    "Target 1 (1:1): %.2f\n" +
                    "Target 2 (1:2): %.2f\n" +
                    "Risk: %.2f pts\n" +
                    "R:R Ratio: 1:2\n" +
                    "Confidence: %.1f%%\n\n" +
                    "📊 Indicators:\n" +
                    "• Supertrend: %s\n" +
                    "• MACD: %s\n" +
                    "• VWAP: %s\n" +
                    "• PCR Bias: %s",
                    result.getSymbol(),
                    result.getSignalType(),
                    result.getEntryPrice(),
                    result.getStopLoss(),
                    result.getTarget1(),
                    result.getTarget2(),
                    risk,
                    result.getConfidenceScore(),
                    result.getSupertrendStatus(),
                    result.getMacdStatus(),
                    result.getVwapStatus(),
                    result.getPcrBias()
            );

            telegramNotificationService.sendMessage("Brahmastra Signal", message);
        } catch (Exception e) {
            logger.error("Error sending Telegram alert: {}", e.getMessage());
        }
    }

    // ==================== Inner Classes ====================

    /**
     * Candle data holder with indicator values.
     */
    @lombok.Data
    @lombok.Builder
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    public static class CandleData {
        private LocalDateTime timestamp;
        private Double open;
        private Double high;
        private Double low;
        private Double close;
        private Long volume;

        // Indicators
        private Double atr;
        private Double supertrend;
        private String supertrendTrend;
        private Double macdLine;
        private Double macdSignal;
        private Double macdHistogram;
        private Double vwap;
    }

    /**
     * Backtest simulation engine.
     */
    private class BacktestEngine {
        private final BacktestRequest request;
        private double equity;
        private double peakEquity;
        private double maxDrawdown;
        private int winCount;
        private int lossCount;
        private double totalWins;
        private double totalLosses;
        private final List<TradeLog> tradeLog = new ArrayList<>();
        private final List<EquityPoint> equityCurve = new ArrayList<>();
        private final List<Double> returns = new ArrayList<>();

        public BacktestEngine(BacktestRequest request) {
            this.request = request;
            this.equity = request.getInitialCapital();
            this.peakEquity = equity;
        }

        public BacktestResult runSimulation(List<CandleData> candles, SignalRequest signalRequest) {
            String currentPosition = null;
            Double entryPrice = null;
            Double stopLoss = null;
            Double target = null;
            CandleData entryCandle = null;
            int tradeNumber = 0;

            for (int i = Math.max(26, signalRequest.getSupertrendPeriod()); i < candles.size(); i++) {
                CandleData candle = candles.get(i);
                CandleData prevCandle = candles.get(i - 1);

                // Check for exit conditions if in position
                if (currentPosition != null) {
                    boolean exitSignal = false;
                    String exitReason = null;
                    Double exitPrice = null;

                    // Check stop loss
                    if (SIGNAL_BUY.equals(currentPosition) && candle.getLow() <= stopLoss) {
                        exitPrice = stopLoss;
                        exitReason = "STOP_LOSS";
                        exitSignal = true;
                    } else if (SIGNAL_SELL.equals(currentPosition) && candle.getHigh() >= stopLoss) {
                        exitPrice = stopLoss;
                        exitReason = "STOP_LOSS";
                        exitSignal = true;
                    }

                    // Check target
                    if (!exitSignal) {
                        if (SIGNAL_BUY.equals(currentPosition) && candle.getHigh() >= target) {
                            exitPrice = target;
                            exitReason = "TARGET";
                            exitSignal = true;
                        } else if (SIGNAL_SELL.equals(currentPosition) && candle.getLow() <= target) {
                            exitPrice = target;
                            exitReason = "TARGET";
                            exitSignal = true;
                        }
                    }

                    // Check for signal reversal
                    if (!exitSignal) {
                        String newSignal = checkTripleConfirmation(candle, prevCandle, signalRequest);
                        if (!SIGNAL_NONE.equals(newSignal) && !newSignal.equals(currentPosition)) {
                            exitPrice = candle.getClose();
                            exitReason = "SIGNAL_REVERSAL";
                            exitSignal = true;
                        }
                    }

                    // Check EOD (3:25 PM)
                    if (!exitSignal && candle.getTimestamp().getHour() == 15 && candle.getTimestamp().getMinute() >= 25) {
                        exitPrice = candle.getClose();
                        exitReason = "EOD";
                        exitSignal = true;
                    }

                    if (exitSignal) {
                        recordTrade(tradeNumber, currentPosition, entryCandle, candle, entryPrice, exitPrice, stopLoss, target, exitReason);
                        currentPosition = null;
                        entryPrice = null;
                        stopLoss = null;
                        target = null;
                        entryCandle = null;
                    }
                }

                // Check for new signal if not in position
                if (currentPosition == null) {
                    String signalType = checkTripleConfirmation(candle, prevCandle, signalRequest);

                    if (!SIGNAL_NONE.equals(signalType)) {
                        // Apply PCR filter
                        if (Boolean.TRUE.equals(request.getUsePCR())) {
                            Double pcr = getPCRForTime(SYMBOL_TOKEN_MAP.get(request.getSymbol()), candle.getTimestamp());
                            if (!isSignalAllowedByPCR(signalType, pcr)) {
                                continue;
                            }
                        }

                        tradeNumber++;
                        currentPosition = signalType;
                        entryPrice = candle.getClose();
                        entryCandle = candle;

                        if (SIGNAL_BUY.equals(signalType)) {
                            stopLoss = prevCandle.getLow();
                            target = entryPrice + (2 * (entryPrice - stopLoss)); // 2:1 RR
                        } else {
                            stopLoss = prevCandle.getHigh();
                            target = entryPrice - (2 * (stopLoss - entryPrice)); // 2:1 RR
                        }
                    }
                }

                // Record equity point
                equityCurve.add(EquityPoint.builder()
                        .timestamp(candle.getTimestamp())
                        .equity(equity)
                        .tradeNumber(tradeNumber)
                        .build());
            }

            return buildResult();
        }

        private void recordTrade(int tradeNum, String signalType, CandleData entry, CandleData exit,
                                  Double entryPrice, Double exitPrice, Double sl, Double tp, String exitReason) {
            double pnl = SIGNAL_BUY.equals(signalType)
                    ? exitPrice - entryPrice
                    : entryPrice - exitPrice;

            double riskAmount = equity * (request.getRiskPerTrade() / 100);
            double slDistance = Math.abs(entryPrice - sl);
            double positionSize = slDistance > 0.01 ? riskAmount / slDistance : 0;
            double tradePnL = pnl * positionSize;

            equity += tradePnL;
            returns.add(tradePnL / (equity - tradePnL) * 100);

            if (tradePnL > 0) {
                winCount++;
                totalWins += tradePnL;
            } else {
                lossCount++;
                totalLosses += Math.abs(tradePnL);
            }

            // Update peak and drawdown
            if (equity > peakEquity) {
                peakEquity = equity;
            }
            double currentDrawdown = (peakEquity - equity) / peakEquity * 100;
            if (currentDrawdown > maxDrawdown) {
                maxDrawdown = currentDrawdown;
            }

            tradeLog.add(TradeLog.builder()
                    .tradeNumber(tradeNum)
                    .signalType(signalType)
                    .entryTime(entry.getTimestamp())
                    .exitTime(exit.getTimestamp())
                    .entryPrice(entryPrice)
                    .exitPrice(exitPrice)
                    .stopLoss(sl)
                    .target(tp)
                    .positionSize(positionSize)
                    .pnl(tradePnL)
                    .pnlPercent(tradePnL / request.getInitialCapital() * 100)
                    .cumulativePnl(equity - request.getInitialCapital())
                    .cumulativePnlPercent((equity - request.getInitialCapital()) / request.getInitialCapital() * 100)
                    .exitReason(exitReason)
                    .riskReward(Math.abs(pnl / (entryPrice - sl)))
                    .drawdownAtExit(currentDrawdown)
                    .build());
        }

        private BacktestResult buildResult() {
            int totalTrades = winCount + lossCount;
            double winRate = totalTrades > 0 ? (double) winCount / totalTrades * 100 : 0;
            double avgWin = winCount > 0 ? totalWins / winCount : 0;
            double avgLoss = lossCount > 0 ? totalLosses / lossCount : 0;
            double profitFactor = totalLosses > 0 ? totalWins / totalLosses : totalWins > 0 ? Double.MAX_VALUE : 0;
            double expectancy = (winRate / 100 * avgWin) - ((1 - winRate / 100) * avgLoss);

            // Calculate Sharpe Ratio
            double avgReturn = returns.stream().mapToDouble(Double::doubleValue).average().orElse(0);
            double stdDev = calculateStdDev(returns, avgReturn);
            double sharpeRatio = stdDev > 0 ? (avgReturn * Math.sqrt(252)) / stdDev : 0;

            // Calculate Sortino Ratio (using only negative returns)
            List<Double> negativeReturns = returns.stream().filter(r -> r < 0).toList();
            double downstdDev = calculateStdDev(negativeReturns, avgReturn);
            double sortinoRatio = downstdDev > 0 ? (avgReturn * Math.sqrt(252)) / downstdDev : 0;

            // Calculate Calmar Ratio
            double annualizedReturn = (equity - request.getInitialCapital()) / request.getInitialCapital() * 100;
            double calmarRatio = maxDrawdown > 0 ? annualizedReturn / maxDrawdown : 0;

            return BacktestResult.builder()
                    .symbol(request.getSymbol())
                    .timeframe(request.getTimeframe())
                    .backtestStart(request.getFromDate().atStartOfDay())
                    .backtestEnd(request.getToDate().atTime(23, 59, 59))
                    .runTimestamp(LocalDateTime.now())
                    .initialCapital(request.getInitialCapital())
                    .finalCapital(equity)
                    .netPnL(equity - request.getInitialCapital())
                    .netPnLPercent((equity - request.getInitialCapital()) / request.getInitialCapital() * 100)
                    .riskPerTrade(request.getRiskPerTrade())
                    .totalTrades(totalTrades)
                    .winningTrades(winCount)
                    .losingTrades(lossCount)
                    .winRate(winRate)
                    .averageWin(avgWin)
                    .averageLoss(avgLoss)
                    .averageRiskReward(tradeLog.stream().mapToDouble(TradeLog::getRiskReward).average().orElse(0))
                    .profitFactor(profitFactor)
                    .expectancy(expectancy)
                    .maxDrawdown(maxDrawdown * request.getInitialCapital() / 100)
                    .maxDrawdownPercent(maxDrawdown)
                    .sharpeRatio(sharpeRatio)
                    .sortinoRatio(sortinoRatio)
                    .calmarRatio(calmarRatio)
                    .volatility(stdDev)
                    .buySignals((int) tradeLog.stream().filter(t -> SIGNAL_BUY.equals(t.getSignalType())).count())
                    .sellSignals((int) tradeLog.stream().filter(t -> SIGNAL_SELL.equals(t.getSignalType())).count())
                    .tradeLog(tradeLog)
                    .equityCurve(equityCurve)
                    .build();
        }

        private double calculateStdDev(List<Double> values, double mean) {
            if (values.isEmpty()) return 0;
            double variance = values.stream()
                    .mapToDouble(v -> Math.pow(v - mean, 2))
                    .average()
                    .orElse(0);
            return Math.sqrt(variance);
        }
    }

    // ==================== Indicator Metrics ====================

    @Override
    public IndicatorMetrics getIndicatorMetrics(String symbol, String timeframe, int historyBars) {
        logger.info("Getting indicator metrics for {} on {} timeframe with {} history bars", symbol, timeframe, historyBars);

        Long instrumentToken = SYMBOL_TOKEN_MAP.getOrDefault(symbol.toUpperCase(), NIFTY_INSTRUMENT_TOKEN);

        // Fetch candle data (uses KiteTickerProvider live data → DB → Kite Historical API)
        LocalDateTime endTime = LocalDateTime.now();
        LocalDateTime startTime = endTime.minusDays(5); // Fetch 5 days to ensure enough data

        List<CandleData> candles = fetchCandleData(instrumentToken, timeframe, startTime, endTime);

        if (candles.isEmpty() || candles.size() < 25) {
            logger.warn("Insufficient candle data for {} indicator metrics (got {} candles)", symbol, candles.size());
            return buildEmptyIndicatorMetrics(symbol, timeframe);
        }

        // Calculate indicators
        SignalRequest request = SignalRequest.builder()
                .symbol(symbol)
                .timeframe(timeframe)
                .supertrendPeriod(20)
                .supertrendMultiplier(2.0)
                .macdFastPeriod(12)
                .macdSlowPeriod(26)
                .macdSignalPeriod(9)
                .build();

        calculateIndicators(candles, request);

        // Get the latest candle with indicators
        CandleData latestCandle = candles.get(candles.size() - 1);
        CandleData prevCandle = candles.size() > 1 ? candles.get(candles.size() - 2) : null;

        // Use live price from IndexLTP table (written by DailyJobServiceImpl during market hours)
        Double currentPrice = latestCandle.getClose();
        try {
            Integer appJobConfigNum = getAppJobConfigNum(symbol);
            List<IndexLTP> latestIndexLTPList = indexLTPRepository.findLatestIndexDataByAppJobConfigNum(appJobConfigNum);
            if (!latestIndexLTPList.isEmpty() && latestIndexLTPList.get(0).getIndexLTP() > 0) {
                currentPrice = (double) latestIndexLTPList.get(0).getIndexLTP();
            }
        } catch (Exception e) {
            logger.debug("Could not fetch live price from IndexLTP for {}: {}", symbol, e.getMessage());
        }

        // Build Supertrend data
        IndicatorMetrics.SupertrendData supertrendData = buildSupertrendData(candles, latestCandle, prevCandle, request);

        // Build MACD data
        IndicatorMetrics.MACDData macdData = buildMACDData(candles, latestCandle, prevCandle, request);

        // Build VWAP data
        IndicatorMetrics.VWAPData vwapData = buildVWAPData(candles, latestCandle);

        // Build history points for charts
        int historyStart = Math.max(0, candles.size() - historyBars);
        List<IndicatorMetrics.IndicatorHistoryPoint> history = new ArrayList<>();

        for (int i = historyStart; i < candles.size(); i++) {
            CandleData candle = candles.get(i);
            history.add(IndicatorMetrics.IndicatorHistoryPoint.builder()
                    .timestamp(candle.getTimestamp())
                    .price(candle.getClose())
                    .open(candle.getOpen())
                    .high(candle.getHigh())
                    .low(candle.getLow())
                    .close(candle.getClose())
                    .volume(candle.getVolume())
                    .supertrend(candle.getSupertrend())
                    .supertrendTrend(candle.getSupertrendTrend())
                    .macdLine(candle.getMacdLine())
                    .macdSignal(candle.getMacdSignal())
                    .macdHistogram(candle.getMacdHistogram())
                    .vwap(candle.getVwap())
                    .vwapUpperBand(candle.getVwap() != null ? candle.getVwap() * 1.01 : null)
                    .vwapLowerBand(candle.getVwap() != null ? candle.getVwap() * 0.99 : null)
                    .build());
        }

        // Determine overall signal
        String overallSignal = determineOverallSignal(supertrendData, macdData, vwapData);
        Double confidenceScore = calculateConfidenceScore(supertrendData, macdData, vwapData);
        String recommendation = buildRecommendation(overallSignal, supertrendData, macdData, vwapData);

        return IndicatorMetrics.builder()
                .symbol(symbol)
                .timeframe(timeframe)
                .timestamp(latestCandle.getTimestamp())
                .currentPrice(currentPrice)
                .supertrend(supertrendData)
                .macd(macdData)
                .vwap(vwapData)
                .overallSignal(overallSignal)
                .confidenceScore(confidenceScore)
                .recommendation(recommendation)
                .history(history)
                .build();
    }

    @Override
    public List<IndicatorMetrics> getAllIndicatorMetrics(String timeframe, int historyBars) {
        logger.info("Getting indicator metrics for all symbols on {} timeframe", timeframe);

        List<IndicatorMetrics> allMetrics = new ArrayList<>();
        for (String symbol : SYMBOL_TOKEN_MAP.keySet()) {
            try {
                IndicatorMetrics metrics = getIndicatorMetrics(symbol, timeframe, historyBars);
                allMetrics.add(metrics);
            } catch (Exception e) {
                logger.error("Error getting indicator metrics for {}: {}", symbol, e.getMessage());
            }
        }
        return allMetrics;
    }

    private IndicatorMetrics buildEmptyIndicatorMetrics(String symbol, String timeframe) {
        return IndicatorMetrics.builder()
                .symbol(symbol)
                .timeframe(timeframe)
                .timestamp(LocalDateTime.now())
                .currentPrice(0.0)
                .supertrend(IndicatorMetrics.SupertrendData.builder()
                        .value(0.0)
                        .trend(TREND_NEUTRAL)
                        .period(10)
                        .multiplier(3.0)
                        .build())
                .macd(IndicatorMetrics.MACDData.builder()
                        .macdLine(0.0)
                        .signalLine(0.0)
                        .histogram(0.0)
                        .signal(TREND_NEUTRAL)
                        .fastPeriod(12)
                        .slowPeriod(26)
                        .signalPeriod(9)
                        .build())
                .vwap(IndicatorMetrics.VWAPData.builder()
                        .value(0.0)
                        .position(TREND_NEUTRAL)
                        .priceToVwapPercent(0.0)
                        .tradingZone("FAIR_VALUE")
                        .build())
                .overallSignal(SIGNAL_NONE)
                .confidenceScore(0.0)
                .recommendation("Insufficient data for analysis")
                .history(Collections.emptyList())
                .build();
    }

    private IndicatorMetrics.SupertrendData buildSupertrendData(List<CandleData> candles, CandleData latest, CandleData prev, SignalRequest request) {
        double priceDistance = latest.getClose() - (latest.getSupertrend() != null ? latest.getSupertrend() : latest.getClose());
        double priceDistancePercent = latest.getSupertrend() != null && latest.getSupertrend() != 0
                ? (priceDistance / latest.getSupertrend()) * 100 : 0;

        // Calculate consecutive bars in trend
        int consecutiveBars = 1;
        String currentTrend = latest.getSupertrendTrend();
        for (int i = candles.size() - 2; i >= 0 && consecutiveBars < candles.size(); i--) {
            if (currentTrend != null && currentTrend.equals(candles.get(i).getSupertrendTrend())) {
                consecutiveBars++;
            } else {
                break;
            }
        }

        boolean isTrendChange = prev != null && prev.getSupertrendTrend() != null
                && !prev.getSupertrendTrend().equals(latest.getSupertrendTrend());

        // Calculate upper and lower bands based on ATR if available
        Double upperBand = null;
        Double lowerBand = null;
        if (latest.getAtr() != null && latest.getSupertrend() != null) {
            double atrMultiplied = latest.getAtr() * request.getSupertrendMultiplier();
            upperBand = latest.getClose() + atrMultiplied;
            lowerBand = latest.getClose() - atrMultiplied;
        }

        return IndicatorMetrics.SupertrendData.builder()
                .value(latest.getSupertrend())
                .trend(latest.getSupertrendTrend())
                .atrValue(latest.getAtr())
                .upperBand(upperBand)
                .lowerBand(lowerBand)
                .period(request.getSupertrendPeriod())
                .multiplier(request.getSupertrendMultiplier())
                .priceDistance(priceDistance)
                .priceDistancePercent(priceDistancePercent)
                .consecutiveBars(consecutiveBars)
                .isTrendChange(isTrendChange)
                .build();
    }

    private IndicatorMetrics.MACDData buildMACDData(List<CandleData> candles, CandleData latest, CandleData prev, SignalRequest request) {
        String signal = TREND_NEUTRAL;
        String crossoverType = "NONE";
        boolean crossover = false;

        if (latest.getMacdLine() != null && latest.getMacdSignal() != null) {
            if (latest.getMacdLine() > latest.getMacdSignal()) {
                signal = TREND_BULLISH;
            } else if (latest.getMacdLine() < latest.getMacdSignal()) {
                signal = TREND_BEARISH;
            }

            // Check for crossover
            if (prev != null && prev.getMacdLine() != null && prev.getMacdSignal() != null) {
                if (prev.getMacdLine() <= prev.getMacdSignal() && latest.getMacdLine() > latest.getMacdSignal()) {
                    crossover = true;
                    crossoverType = "BULLISH_CROSSOVER";
                } else if (prev.getMacdLine() >= prev.getMacdSignal() && latest.getMacdLine() < latest.getMacdSignal()) {
                    crossover = true;
                    crossoverType = "BEARISH_CROSSOVER";
                }
            }
        }

        // Calculate bars since last crossover
        int barsSinceCrossover = 0;
        for (int i = candles.size() - 2; i >= 1; i--) {
            CandleData curr = candles.get(i);
            CandleData previous = candles.get(i - 1);
            barsSinceCrossover++;

            if (curr.getMacdLine() != null && curr.getMacdSignal() != null &&
                previous.getMacdLine() != null && previous.getMacdSignal() != null) {
                boolean currAbove = curr.getMacdLine() > curr.getMacdSignal();
                boolean prevAbove = previous.getMacdLine() > previous.getMacdSignal();
                if (currAbove != prevAbove) {
                    break;
                }
            }
        }

        double divergence = latest.getMacdLine() != null && latest.getMacdSignal() != null
                ? latest.getMacdLine() - latest.getMacdSignal() : 0;

        boolean isConverging = false;
        if (prev != null && prev.getMacdLine() != null && prev.getMacdSignal() != null) {
            double prevDivergence = prev.getMacdLine() - prev.getMacdSignal();
            isConverging = Math.abs(divergence) < Math.abs(prevDivergence);
        }

        return IndicatorMetrics.MACDData.builder()
                .macdLine(latest.getMacdLine())
                .signalLine(latest.getMacdSignal())
                .histogram(latest.getMacdHistogram())
                .signal(signal)
                .crossover(crossover)
                .crossoverType(crossoverType)
                .fastPeriod(request.getMacdFastPeriod())
                .slowPeriod(request.getMacdSlowPeriod())
                .signalPeriod(request.getMacdSignalPeriod())
                .divergence(divergence)
                .isConverging(isConverging)
                .barsSinceCrossover(barsSinceCrossover)
                .build();
    }

    private IndicatorMetrics.VWAPData buildVWAPData(List<CandleData> candles, CandleData latest) {
        String position = TREND_NEUTRAL;
        String tradingZone = "FAIR_VALUE";
        double priceToVwapPercent = 0;

        if (latest.getVwap() != null && latest.getVwap() != 0) {
            priceToVwapPercent = ((latest.getClose() - latest.getVwap()) / latest.getVwap()) * 100;

            if (latest.getClose() > latest.getVwap() * 1.002) {
                position = "ABOVE";
            } else if (latest.getClose() < latest.getVwap() * 0.998) {
                position = "BELOW";
            } else {
                position = "AT_VWAP";
            }

            if (priceToVwapPercent > 1.0) {
                tradingZone = "PREMIUM";
            } else if (priceToVwapPercent < -1.0) {
                tradingZone = "DISCOUNT";
            } else {
                tradingZone = "FAIR_VALUE";
            }
        }

        // Calculate VWAP bands (approximate standard deviation bands)
        double vwapStdDev = calculateVwapStdDev(candles);
        double upperBand1SD = latest.getVwap() != null ? latest.getVwap() + vwapStdDev : 0;
        double lowerBand1SD = latest.getVwap() != null ? latest.getVwap() - vwapStdDev : 0;
        double upperBand2SD = latest.getVwap() != null ? latest.getVwap() + (2 * vwapStdDev) : 0;
        double lowerBand2SD = latest.getVwap() != null ? latest.getVwap() - (2 * vwapStdDev) : 0;

        // Calculate cumulative volume from candles
        Double cumulativeVolume = candles.stream()
                .filter(c -> c.getVolume() != null)
                .mapToDouble(c -> c.getVolume().doubleValue())
                .sum();

        return IndicatorMetrics.VWAPData.builder()
                .value(latest.getVwap())
                .position(position)
                .priceToVwapPercent(priceToVwapPercent)
                .upperBand1SD(upperBand1SD)
                .lowerBand1SD(lowerBand1SD)
                .upperBand2SD(upperBand2SD)
                .lowerBand2SD(lowerBand2SD)
                .cumulativeVolume(cumulativeVolume)
                .cumulativeTpv(null) // Not tracked in CandleData
                .tradingZone(tradingZone)
                .build();
    }

    private double calculateVwapStdDev(List<CandleData> candles) {
        if (candles.isEmpty()) return 0;

        double avgVwap = candles.stream()
                .filter(c -> c.getVwap() != null)
                .mapToDouble(CandleData::getVwap)
                .average()
                .orElse(0);

        if (avgVwap == 0) return 0;

        double variance = candles.stream()
                .filter(c -> c.getVwap() != null && c.getClose() != null)
                .mapToDouble(c -> Math.pow(c.getClose() - c.getVwap(), 2))
                .average()
                .orElse(0);

        return Math.sqrt(variance);
    }

    private String determineOverallSignal(IndicatorMetrics.SupertrendData st, IndicatorMetrics.MACDData macd, IndicatorMetrics.VWAPData vwap) {
        int bullishCount = 0;
        int bearishCount = 0;

        // Check Supertrend
        if (TREND_BULLISH.equals(st.getTrend())) bullishCount++;
        else if (TREND_BEARISH.equals(st.getTrend())) bearishCount++;

        // Check MACD
        if (TREND_BULLISH.equals(macd.getSignal())) bullishCount++;
        else if (TREND_BEARISH.equals(macd.getSignal())) bearishCount++;

        // Check VWAP — independent vote; price at/below VWAP favours BUY, at/above favours SELL
        if ("BELOW".equals(vwap.getPosition()) || "AT_VWAP".equals(vwap.getPosition())) bullishCount++;
        if ("ABOVE".equals(vwap.getPosition()) || "AT_VWAP".equals(vwap.getPosition())) bearishCount++;

        // Triple confirmation
        if (bullishCount >= 3) return SIGNAL_BUY;
        if (bearishCount >= 3) return SIGNAL_SELL;

        return SIGNAL_NONE;
    }

    private Double calculateConfidenceScore(IndicatorMetrics.SupertrendData st, IndicatorMetrics.MACDData macd, IndicatorMetrics.VWAPData vwap) {
        double score = 0;

        // Supertrend contribution (40%)
        if (TREND_BULLISH.equals(st.getTrend()) || TREND_BEARISH.equals(st.getTrend())) {
            score += 20; // Base score for having a trend
            if (st.getConsecutiveBars() != null && st.getConsecutiveBars() > 3) {
                score += Math.min(20, st.getConsecutiveBars() * 2); // Up to 20 points for trend strength
            }
        }

        // MACD contribution (30%)
        if (macd.getCrossover() != null && macd.getCrossover()) {
            score += 15; // Fresh crossover
        }
        if (macd.getDivergence() != null) {
            double absDiv = Math.abs(macd.getDivergence());
            score += Math.min(15, absDiv * 0.5); // Up to 15 points for divergence strength
        }

        // VWAP contribution (30%)
        if ("AT_VWAP".equals(vwap.getPosition())) {
            score += 15; // Ideal entry zone
        } else if ("DISCOUNT".equals(vwap.getTradingZone()) && TREND_BULLISH.equals(st.getTrend())) {
            score += 15; // Buying at discount
        } else if ("PREMIUM".equals(vwap.getTradingZone()) && TREND_BEARISH.equals(st.getTrend())) {
            score += 15; // Selling at premium
        }
        if (vwap.getPriceToVwapPercent() != null && Math.abs(vwap.getPriceToVwapPercent()) < 0.5) {
            score += 15; // Close to VWAP
        }

        return Math.min(100, score);
    }

    private String buildRecommendation(String signal, IndicatorMetrics.SupertrendData st, IndicatorMetrics.MACDData macd, IndicatorMetrics.VWAPData vwap) {
        StringBuilder rec = new StringBuilder();

        if (SIGNAL_BUY.equals(signal)) {
            rec.append("✅ STRONG BUY SIGNAL - Triple confirmation achieved. ");
            rec.append("Supertrend: ").append(st.getTrend()).append(" (").append(st.getConsecutiveBars()).append(" bars). ");
            rec.append("MACD: ").append(macd.getSignal());
            if (Boolean.TRUE.equals(macd.getCrossover())) rec.append(" (Fresh Crossover!)");
            rec.append(". Price is ").append(vwap.getPosition()).append(" VWAP.");
        } else if (SIGNAL_SELL.equals(signal)) {
            rec.append("🔴 STRONG SELL SIGNAL - Triple confirmation achieved. ");
            rec.append("Supertrend: ").append(st.getTrend()).append(" (").append(st.getConsecutiveBars()).append(" bars). ");
            rec.append("MACD: ").append(macd.getSignal());
            if (Boolean.TRUE.equals(macd.getCrossover())) rec.append(" (Fresh Crossover!)");
            rec.append(". Price is ").append(vwap.getPosition()).append(" VWAP.");
        } else {
            rec.append("⏸️ NO SIGNAL - Waiting for triple confirmation. ");
            rec.append("Supertrend: ").append(st.getTrend() != null ? st.getTrend() : "N/A").append(". ");
            rec.append("MACD: ").append(macd.getSignal() != null ? macd.getSignal() : "N/A").append(". ");
            rec.append("VWAP: ").append(vwap.getPosition() != null ? vwap.getPosition() : "N/A").append(".");
        }

        return rec.toString();
    }

    // ==================== Option Chain Integration ====================

    @Override
    public OptionChainMetrics getOptionChainMetrics(String symbol) {
        logger.info("Getting option chain metrics for {}", symbol);

        Integer appJobConfigNum = getAppJobConfigNum(symbol);

        // Get latest IndexLTP for Max Pain and PCR data - must use display=true query
        List<IndexLTP> indexLTPList = indexLTPRepository.findLatestIndexDataByAppJobConfigNum(appJobConfigNum);
        if (indexLTPList.isEmpty()) {
            logger.warn("No IndexLTP data found for {}", symbol);
            return buildEmptyOptionChainMetrics(symbol);
        }
        IndexLTP indexLTP = indexLTPList.get(0);

        double spotPrice = indexLTP.getIndexLTP() != null ? indexLTP.getIndexLTP().doubleValue() : 0;
        double atmStrike = calculateATMStrike(spotPrice, appJobConfigNum);

        // Build Max Pain Data
        OptionChainMetrics.MaxPainData maxPainData = buildMaxPainData(indexLTP, spotPrice);

        // Build OI Analysis Data
        OptionChainMetrics.OIAnalysisData oiAnalysisData = buildOIAnalysisData(appJobConfigNum, indexLTP);

        // Calculate combined signal
        String optionChainSignal = calculateOptionChainSignal(maxPainData, oiAnalysisData);
        double optionChainConfidence = calculateOptionChainConfidence(maxPainData, oiAnalysisData);
        String optionChainBias = determineOptionChainBias(maxPainData, oiAnalysisData);
        String recommendedAction = buildOptionChainRecommendation(optionChainSignal, maxPainData, oiAnalysisData);

        return OptionChainMetrics.builder()
                .symbol(symbol)
                .timestamp(LocalDateTime.now())
                .spotPrice(spotPrice)
                .atmStrike(atmStrike)
                .maxPain(maxPainData)
                .oiAnalysis(oiAnalysisData)
                .optionChainSignal(optionChainSignal)
                .optionChainConfidence(optionChainConfidence)
                .optionChainBias(optionChainBias)
                .recommendedAction(recommendedAction)
                .build();
    }

    @Override
    public List<OptionChainMetrics> getAllOptionChainMetrics() {
        logger.info("Getting option chain metrics for all symbols");
        List<OptionChainMetrics> allMetrics = new ArrayList<>();

        for (String symbol : SYMBOL_TOKEN_MAP.keySet()) {
            try {
                OptionChainMetrics metrics = getOptionChainMetrics(symbol);
                allMetrics.add(metrics);
            } catch (Exception e) {
                logger.error("Error getting option chain metrics for {}: {}", symbol, e.getMessage());
            }
        }

        return allMetrics;
    }

    @Override
    public boolean doesOptionChainConfirmSignal(String symbol, String signalType) {
        OptionChainMetrics metrics = getOptionChainMetrics(symbol);
        if (metrics == null || SIGNAL_NONE.equals(metrics.getOptionChainSignal())) {
            return true; // No contraindication if data unavailable
        }

        // Strong confirmation if option chain signal matches
        if (signalType.equals(metrics.getOptionChainSignal())) {
            return true;
        }

        // Check individual components
        boolean maxPainConfirms = false;
        boolean oiConfirms = false;

        if (metrics.getMaxPain() != null) {
            if (SIGNAL_BUY.equals(signalType)) {
                maxPainConfirms = Boolean.TRUE.equals(metrics.getMaxPain().getConfirmsBullish());
            } else if (SIGNAL_SELL.equals(signalType)) {
                maxPainConfirms = Boolean.TRUE.equals(metrics.getMaxPain().getConfirmsBearish());
            }
        }

        if (metrics.getOiAnalysis() != null) {
            if (SIGNAL_BUY.equals(signalType)) {
                oiConfirms = Boolean.TRUE.equals(metrics.getOiAnalysis().getConfirmsUptrend());
            } else if (SIGNAL_SELL.equals(signalType)) {
                oiConfirms = Boolean.TRUE.equals(metrics.getOiAnalysis().getConfirmsDowntrend());
            }
        }

        return maxPainConfirms || oiConfirms;
    }

    @Override
    public String getOptionChainSignal(String symbol) {
        OptionChainMetrics metrics = getOptionChainMetrics(symbol);
        return metrics != null ? metrics.getOptionChainSignal() : SIGNAL_NONE;
    }

    // ============= Private Helper Methods for Option Chain =============

    private OptionChainMetrics buildEmptyOptionChainMetrics(String symbol) {
        return OptionChainMetrics.builder()
                .symbol(symbol)
                .timestamp(LocalDateTime.now())
                .spotPrice(0.0)
                .atmStrike(0.0)
                .maxPain(OptionChainMetrics.MaxPainData.builder().build())
                .oiAnalysis(OptionChainMetrics.OIAnalysisData.builder().build())
                .optionChainSignal(SIGNAL_NONE)
                .optionChainConfidence(0.0)
                .optionChainBias(TREND_NEUTRAL)
                .recommendedAction("Insufficient data for option chain analysis")
                .build();
    }

    private OptionChainMetrics.MaxPainData buildMaxPainData(IndexLTP indexLTP, double spotPrice) {
        Integer maxPainSP = indexLTP.getMaxPainSP();
        Integer maxPainSPSecond = indexLTP.getMaxPainSPSecond();

        if (maxPainSP == null || maxPainSP == 0) {
            return OptionChainMetrics.MaxPainData.builder()
                    .maxPainStrike(0.0)
                    .priceRelation("UNKNOWN")
                    .confirmsBullish(false)
                    .confirmsBearish(false)
                    .pullStrength(0.0)
                    .build();
        }

        double maxPain = maxPainSP.doubleValue();
        double distanceFromSpot = spotPrice - maxPain;
        double distanceFromSpotPercent = (distanceFromSpot / maxPain) * 100;

        String priceRelation;
        boolean confirmsBullish = false;
        boolean confirmsBearish = false;
        boolean actsAsSupport = false;
        boolean actsAsResistance = false;

        if (distanceFromSpotPercent > 0.3) {
            priceRelation = "ABOVE_MAX_PAIN";
            confirmsBearish = true; // Price tends to gravitate down towards max pain
            actsAsSupport = true;   // Max pain acts as support from below
        } else if (distanceFromSpotPercent < -0.3) {
            priceRelation = "BELOW_MAX_PAIN";
            confirmsBullish = true; // Price tends to gravitate up towards max pain
            actsAsResistance = true; // Max pain acts as resistance from above
        } else {
            priceRelation = "AT_MAX_PAIN";
        }

        // Pull strength based on distance (closer = stronger pull, but max at moderate distance)
        double absDistancePercent = Math.abs(distanceFromSpotPercent);
        double pullStrength;
        if (absDistancePercent < 0.2) {
            pullStrength = 20; // Very close, already at max pain
        } else if (absDistancePercent < 1.0) {
            pullStrength = 80; // Strong pull zone
        } else if (absDistancePercent < 2.0) {
            pullStrength = 60;
        } else {
            pullStrength = Math.max(20, 80 - (absDistancePercent - 1.0) * 20);
        }

        return OptionChainMetrics.MaxPainData.builder()
                .maxPainStrike(maxPain)
                .maxPainSecondStrike(maxPainSPSecond != null ? maxPainSPSecond.doubleValue() : null)
                .distanceFromSpot(distanceFromSpot)
                .distanceFromSpotPercent(distanceFromSpotPercent)
                .priceRelation(priceRelation)
                .confirmsBullish(confirmsBullish)
                .confirmsBearish(confirmsBearish)
                .pullStrength(pullStrength)
                .actsAsSupport(actsAsSupport)
                .actsAsResistance(actsAsResistance)
                .build();
    }

    private OptionChainMetrics.OIAnalysisData buildOIAnalysisData(Integer appJobConfigNum, IndexLTP indexLTP) {
        // Get MiniDelta data for OI analysis
        List<MiniDelta> miniDeltaList = miniDeltaRepository.findByAppJobConfigNumOrderByIdAsc(appJobConfigNum);

        Double pcr = indexLTP.getMeanStrikePCR();
        String pcrSignal = getPCRBias(pcr);

        // Calculate OI totals and changes
        double totalCallOI = 0;
        double totalPutOI = 0;
        double callOIChange = 0;
        double putOIChange = 0;
        Double highestCallOIStrike = null;
        Double highestPutOIStrike = null;
        double maxCallOI = 0;
        double maxPutOI = 0;

        List<OptionChainMetrics.StrikeOIChange> significantChanges = new ArrayList<>();

        for (MiniDelta delta : miniDeltaList) {
            if (delta.getStrikePrice() == null || "Total".equalsIgnoreCase(delta.getStrikePrice())) {
                continue;
            }

            try {
                double strike = Double.parseDouble(delta.getStrikePrice());
                double cOI = delta.getCallOI() != null ? delta.getCallOI() : 0;
                double pOI = delta.getPutOI() != null ? delta.getPutOI() : 0;
                double cOIChg = delta.getCallOIChange() != null ? delta.getCallOIChange() : 0;
                double pOIChg = delta.getPutOIChange() != null ? delta.getPutOIChange() : 0;

                totalCallOI += cOI;
                totalPutOI += pOI;
                callOIChange += cOIChg;
                putOIChange += pOIChg;

                if (cOI > maxCallOI) {
                    maxCallOI = cOI;
                    highestCallOIStrike = strike;
                }
                if (pOI > maxPutOI) {
                    maxPutOI = pOI;
                    highestPutOIStrike = strike;
                }

                // Track significant OI changes (>5% change)
                double cOIChgPct = cOI > 0 ? (cOIChg / cOI) * 100 : 0;
                double pOIChgPct = pOI > 0 ? (pOIChg / pOI) * 100 : 0;

                if (Math.abs(cOIChgPct) > 5 || Math.abs(pOIChgPct) > 5) {
                    String interpretation = interpretOIChange(cOIChg, pOIChg);
                    significantChanges.add(OptionChainMetrics.StrikeOIChange.builder()
                            .strikePrice(strike)
                            .callOIChange(cOIChg)
                            .putOIChange(pOIChg)
                            .callOIChangePercent(cOIChgPct)
                            .putOIChangePercent(pOIChgPct)
                            .interpretation(interpretation)
                            .build());
                }
            } catch (NumberFormatException e) {
                // Skip non-numeric strikes
            }
        }

        // Determine OI build-up type
        String oiBuildUpType = determineOIBuildUpType(callOIChange, putOIChange);
        boolean confirmsUptrend = "LONG_BUILD_UP".equals(oiBuildUpType) || "SHORT_COVERING".equals(oiBuildUpType);
        boolean confirmsDowntrend = "SHORT_BUILD_UP".equals(oiBuildUpType) || "LONG_UNWINDING".equals(oiBuildUpType);

        // PCR trend (would need historical data for actual trend, using current value for bias)
        String pcrTrend = "STABLE";
        if (pcr != null) {
            if (pcr > 1.2) pcrTrend = "HIGH";
            else if (pcr < 0.8) pcrTrend = "LOW";
        }

        return OptionChainMetrics.OIAnalysisData.builder()
                .pcr(pcr)
                .pcrChange(0.0) // Would need previous PCR for change
                .pcrTrend(pcrTrend)
                .pcrSignal(pcrSignal)
                .totalCallOI(totalCallOI)
                .totalPutOI(totalPutOI)
                .callOIChange(callOIChange)
                .putOIChange(putOIChange)
                .oiBuildUpType(oiBuildUpType)
                .confirmsUptrend(confirmsUptrend)
                .confirmsDowntrend(confirmsDowntrend)
                .highestCallOIStrike(highestCallOIStrike)
                .highestPutOIStrike(highestPutOIStrike)
                .callOIConcentration(totalCallOI > 0 ? (maxCallOI / totalCallOI) * 100 : 0)
                .putOIConcentration(totalPutOI > 0 ? (maxPutOI / totalPutOI) * 100 : 0)
                .significantOIChanges(significantChanges.size() > 10 ? significantChanges.subList(0, 10) : significantChanges)
                .build();
    }

    private String determineOIBuildUpType(double callOIChange, double putOIChange) {
        // Price rising + OI rising = Long Build-up (Bullish)
        // Price rising + OI falling = Short Covering (Bullish)
        // Price falling + OI rising = Short Build-up (Bearish)
        // Price falling + OI falling = Long Unwinding (Bearish)

        // Since we don't have price change here, we use OI changes to infer
        if (callOIChange > 0 && putOIChange > 0) {
            // Both increasing - look at ratio
            if (putOIChange > callOIChange * 1.2) {
                return "LONG_BUILD_UP"; // More puts being added = hedging for longs
            } else if (callOIChange > putOIChange * 1.2) {
                return "SHORT_BUILD_UP"; // More calls being added = hedging for shorts
            }
            return "MIXED_BUILD_UP";
        } else if (callOIChange < 0 && putOIChange < 0) {
            // Both decreasing
            if (Math.abs(putOIChange) > Math.abs(callOIChange) * 1.2) {
                return "SHORT_COVERING";
            } else if (Math.abs(callOIChange) > Math.abs(putOIChange) * 1.2) {
                return "LONG_UNWINDING";
            }
            return "MIXED_UNWINDING";
        } else if (putOIChange > 0 && callOIChange < 0) {
            return "LONG_BUILD_UP"; // Puts added, calls reduced = bullish positioning
        } else if (callOIChange > 0 && putOIChange < 0) {
            return "SHORT_BUILD_UP"; // Calls added, puts reduced = bearish positioning
        }

        return "NEUTRAL";
    }

    private String interpretOIChange(double callOIChange, double putOIChange) {
        if (callOIChange > 0 && putOIChange < 0) {
            return "BEARISH_SIGNAL"; // Call writers active
        } else if (callOIChange < 0 && putOIChange > 0) {
            return "BULLISH_SIGNAL"; // Put writers active
        } else if (callOIChange > 0 && putOIChange > 0) {
            return putOIChange > callOIChange ? "BULLISH_SIGNAL" : "BEARISH_SIGNAL";
        }
        return "NEUTRAL";
    }

    private String calculateOptionChainSignal(OptionChainMetrics.MaxPainData maxPain,
                                               OptionChainMetrics.OIAnalysisData oiAnalysis) {
        int bullishScore = 0;
        int bearishScore = 0;

        // Max Pain contribution
        if (maxPain != null) {
            if (Boolean.TRUE.equals(maxPain.getConfirmsBullish())) bullishScore += 2;
            if (Boolean.TRUE.equals(maxPain.getConfirmsBearish())) bearishScore += 2;
        }

        // OI Analysis contribution
        if (oiAnalysis != null) {
            if (Boolean.TRUE.equals(oiAnalysis.getConfirmsUptrend())) bullishScore += 2;
            if (Boolean.TRUE.equals(oiAnalysis.getConfirmsDowntrend())) bearishScore += 2;

            // PCR signal
            if (TREND_BULLISH.equals(oiAnalysis.getPcrSignal())) bullishScore += 1;
            if (TREND_BEARISH.equals(oiAnalysis.getPcrSignal())) bearishScore += 1;
        }

        if (bullishScore >= 4 && bullishScore > bearishScore + 1) return SIGNAL_BUY;
        if (bearishScore >= 4 && bearishScore > bullishScore + 1) return SIGNAL_SELL;
        return SIGNAL_NONE;
    }

    private double calculateOptionChainConfidence(OptionChainMetrics.MaxPainData maxPain,
                                                   OptionChainMetrics.OIAnalysisData oiAnalysis) {
        double confidence = 0;

        // Max Pain contribution (50%)
        if (maxPain != null && maxPain.getPullStrength() != null) {
            confidence += maxPain.getPullStrength() * 0.5;
        }

        // OI Analysis contribution (50%)
        if (oiAnalysis != null && oiAnalysis.getPcr() != null) {
            if (oiAnalysis.getPcr() > 1.3 || oiAnalysis.getPcr() < 0.7) {
                confidence += 50;
            } else if (oiAnalysis.getPcr() > 1.1 || oiAnalysis.getPcr() < 0.9) {
                confidence += 35;
            } else {
                confidence += 20;
            }
        }

        return Math.min(100, confidence);
    }

    private String determineOptionChainBias(OptionChainMetrics.MaxPainData maxPain,
                                             OptionChainMetrics.OIAnalysisData oiAnalysis) {
        int bullishVotes = 0;
        int bearishVotes = 0;

        if (maxPain != null) {
            if (Boolean.TRUE.equals(maxPain.getConfirmsBullish())) bullishVotes++;
            if (Boolean.TRUE.equals(maxPain.getConfirmsBearish())) bearishVotes++;
        }

        if (oiAnalysis != null) {
            if (Boolean.TRUE.equals(oiAnalysis.getConfirmsUptrend())) bullishVotes++;
            if (Boolean.TRUE.equals(oiAnalysis.getConfirmsDowntrend())) bearishVotes++;
        }

        if (bullishVotes > bearishVotes) return TREND_BULLISH;
        if (bearishVotes > bullishVotes) return TREND_BEARISH;
        return TREND_NEUTRAL;
    }

    private String buildOptionChainRecommendation(String signal,
                                                   OptionChainMetrics.MaxPainData maxPain,
                                                   OptionChainMetrics.OIAnalysisData oiAnalysis) {
        StringBuilder rec = new StringBuilder();

        if (SIGNAL_BUY.equals(signal)) {
            rec.append("📈 BULLISH OPTION CHAIN SIGNAL - ");
        } else if (SIGNAL_SELL.equals(signal)) {
            rec.append("📉 BEARISH OPTION CHAIN SIGNAL - ");
        } else {
            rec.append("⏸️ NEUTRAL OPTION CHAIN - ");
        }

        if (maxPain != null && maxPain.getMaxPainStrike() != null && maxPain.getMaxPainStrike() > 0) {
            rec.append(String.format("Max Pain at %.0f (%s). ", maxPain.getMaxPainStrike(), maxPain.getPriceRelation()));
        }

        if (oiAnalysis != null && oiAnalysis.getOiBuildUpType() != null) {
            rec.append(String.format("OI: %s. ", oiAnalysis.getOiBuildUpType().replace("_", " ")));
        }

        return rec.toString();
    }

    private double calculateATMStrike(double spotPrice, Integer appJobConfigNum) {
        double strikeGap = appJobConfigNum != null && appJobConfigNum <= 3 ? 50.0 : 100.0;
        return Math.round(spotPrice / strikeGap) * strikeGap;
    }
}

