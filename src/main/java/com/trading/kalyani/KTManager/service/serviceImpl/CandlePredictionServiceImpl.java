package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.config.KiteConnectConfig;
import com.trading.kalyani.KTManager.entity.*;
import com.trading.kalyani.KTManager.model.HistoricalDataRequest;
import com.trading.kalyani.KTManager.model.HistoricalDataResponse;
import com.trading.kalyani.KTManager.repository.*;
import com.trading.kalyani.KTManager.service.CandlePredictionService;
import com.trading.kalyani.KTManager.service.DailyJobService;
import com.trading.kalyani.KTManager.service.InstrumentService;
import com.trading.kalyani.KTManager.service.TelegramNotificationService;
import com.trading.kalyani.KTManager.utilities.KiteTickerProvider;
import com.zerodhatech.models.Tick;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.stereotype.Service;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;

/**
 * Implementation of CandlePredictionService for predicting NIFTY 50 candles.
 * Uses multiple technical indicators, option chain analysis, and market microstructure
 * to generate predictions for intraday option trading in Indian markets.
 *
 * Enhanced with:
 * - Previous and current day high/low analysis
 * - ATM strike price CE/PE behavior analysis
 */
@Service
public class CandlePredictionServiceImpl implements CandlePredictionService {

    private static final Logger logger = LoggerFactory.getLogger(CandlePredictionServiceImpl.class);

    // Constants for Indian market timing (IST)
    private static final LocalTime MARKET_OPEN = LocalTime.of(9, 15);
    private static final LocalTime MARKET_CLOSE = LocalTime.of(15, 30);

    // Technical analysis constants
    private static final int ATR_PERIOD = 14;
    private static final int EMA_SHORT_PERIOD = 9;
    private static final int EMA_LONG_PERIOD = 21;
    private static final int RSI_PERIOD = 14;
    private static final int NIFTY_STRIKE_GAP = 50; // NIFTY strikes are 50 points apart

    // Trend direction constants
    public static final String TREND_BULLISH = "BULLISH";
    public static final String TREND_BEARISH = "BEARISH";
    public static final String TREND_NEUTRAL = "NEUTRAL";

    // Prediction basis constants
    public static final String BASIS_TECHNICAL = "TECHNICAL";
    public static final String BASIS_OPTION_CHAIN = "OPTION_CHAIN";
    public static final String BASIS_COMBINED = "COMBINED";

    @Autowired
    private CandleStickRepository candleStickRepository;

    @Autowired
    private PredictedCandleRepository predictedCandleRepository;

    @Autowired
    private IndexLTPRepository indexLTPRepository;

    @Autowired
    private MiniDeltaRepository miniDeltaRepository;

    @Autowired
    private InstrumentRepository instrumentRepository;

    @Autowired
    private TradeSetupRepository tradeSetupRepository;

    @Autowired
    private PredictionDeviationRepository predictionDeviationRepository;

    @Autowired
    DailyJobService dailyJobService;

    @Autowired
    InstrumentService instrumentService;

    @Autowired(required = false)
    TelegramNotificationService telegramNotificationService;

    // Reference to KiteTickerProvider for live tick data - to be set by DailyJobServiceImpl
//    KiteTickerProvider kiteTickerProvider;
     volatile KiteTickerProvider kiteTickerProvider;

    // Thread-safe flag for ticker connection status
    private static final AtomicBoolean isTickerConnected = new AtomicBoolean(false);

    // Prediction job status
    private final AtomicBoolean predictionJobActive = new AtomicBoolean(false);
    private final AtomicInteger totalPredictionJobRuns = new AtomicInteger(0);
    private final AtomicInteger totalVerificationRuns = new AtomicInteger(0);
    private LocalDateTime lastPredictionTime = null;
    private LocalDateTime lastVerificationTime = null;

    // Date formatter for batch IDs
    private static final DateTimeFormatter BATCH_ID_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm");

    // Formatter for Kite API candle timestamps — thread-safe, reuse across all calls
    private static final DateTimeFormatter KITE_TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ");

    private static final ZoneId IST_ZONE_ID = ZoneId.of("Asia/Kolkata");

    // Thread-safe cache for recent predictions (CopyOnWriteArrayList for concurrent access)
    private List<PredictedCandleStick> latestPredictions = new CopyOnWriteArrayList<>();

    // Thread-safe cache for latest trade setup (AtomicReference for thread-safe access)
    private AtomicReference<TradeSetupEntity> latestTradeSetupCache = new AtomicReference<>(null);

    @Autowired
    private KiteConnectConfig kiteConnectConfig;


    public synchronized boolean startKiteTicker() {
        // Re-check under lock — another thread may have connected while we were waiting
        if (isTickerConnected.get())
            return true;

        // Close any existing provider before replacing — avoids leaked WebSocket connections on reconnect
        if (kiteTickerProvider != null) {
            try { kiteTickerProvider.disconnect(); } catch (Exception ignored) {}
            kiteTickerProvider = null;
        }

        kiteTickerProvider = new KiteTickerProvider(kiteConnectConfig.kiteConnect().getAccessToken(), kiteConnectConfig.kiteConnect().getApiKey(), candleStickRepository);
        isTickerConnected.set(kiteTickerProvider.startTickerConnection());

        // Subscribe to essential tokens (NIFTY 50 and India VIX) for live data
        if (isTickerConnected.get()) {
            ArrayList<Long> essentialTokens = new ArrayList<>();
            essentialTokens.add(NIFTY_INSTRUMENT_TOKEN);
            essentialTokens.add(INDIA_VIX_INSTRUMENT_TOKEN);
            kiteTickerProvider.subscribeTokenForJob(essentialTokens);
            logger.info("Subscribed to essential tokens: NIFTY={}, VIX={}", NIFTY_INSTRUMENT_TOKEN, INDIA_VIX_INSTRUMENT_TOKEN);
            // NOTE: open trade option tokens (CE/PE) must be re-subscribed by the caller or SimulatedTradingServiceImpl @PostConstruct
        }

        return isTickerConnected.get();
    }

    @Override
    public List<PredictedCandleStick> predictNextFiveCandles(Integer appJobConfigNum, LocalDateTime startTime, LocalDateTime endTime) {
        logger.info("Starting candle prediction for appJobConfigNum: {}", appJobConfigNum);

        // Check if within market hours
        if (!isWithinMarketHours()) {
            logger.warn("Candle prediction skipped - outside market hours or weekend");
            return Collections.emptyList();
        }

        // Check if ticker is connected
        if (!isTickerReady()) {
            logger.warn("Candle prediction skipped - ticker not connected. Attempting to connect...");
            if (!startKiteTicker()) {
                logger.error("Failed to connect ticker, cannot generate accurate predictions");
                return Collections.emptyList();
            }
        }

        try {
            // Step 1: Gather all input data - We need 36 candles for 3 hours of 5-minute data
            final int REQUIRED_CANDLES = 36;

            // Create request for last 2 hours of 1-minute candle data
            LocalDateTime now = LocalDateTime.now();
            LocalDateTime threeHoursAgo = now.minusHours(3);

            // Ensure we don't request data before market open
            LocalDate today = now.toLocalDate();
            LocalDateTime marketOpenToday = LocalDateTime.of(today, MARKET_OPEN);
            if (threeHoursAgo.isBefore(marketOpenToday)) {
                threeHoursAgo = marketOpenToday;
            }

            HistoricalDataRequest request = HistoricalDataRequest.builder()
                    .instrumentToken(String.valueOf(NIFTY_INSTRUMENT_TOKEN))
                    .fromDate(threeHoursAgo)
                    .toDate(now)
                    .interval("5minute")
                    .continuous(false)
                    .oi(false)
                    .build();

            logger.info("Fetching historical data from {} to {} for NIFTY ({})",
                    threeHoursAgo, now, NIFTY_INSTRUMENT_TOKEN);

            HistoricalDataResponse historicalDataResponse = instrumentService.getHistoricalData(request);

            // Convert HistoricalDataResponse to List<CandleStick>
            List<CandleStick> historicalCandles = new ArrayList<>();

            if (historicalDataResponse != null && historicalDataResponse.isSuccess()
                    && historicalDataResponse.getCandles() != null) {

                for (HistoricalDataResponse.HistoricalCandle candle : historicalDataResponse.getCandles()) {
                    CandleStick candleStick = new CandleStick();
                    candleStick.setInstrumentToken(NIFTY_INSTRUMENT_TOKEN);
                    candleStick.setOpenPrice(candle.getOpen());
                    candleStick.setHighPrice(candle.getHigh());
                    candleStick.setLowPrice(candle.getLow());
                    candleStick.setClosePrice(candle.getClose());

                    // Parse timestamp using shared static formatter (avoids re-creation per candle)
                    if (candle.getTimestamp() != null) {
                        try {
                            java.time.ZonedDateTime zdt = java.time.ZonedDateTime.parse(
                                    candle.getTimestamp(), KITE_TIMESTAMP_FORMATTER);
                            LocalDateTime candleTime = zdt.toLocalDateTime();
                            candleStick.setCandleStartTime(candleTime);
                            candleStick.setCandleEndTime(candleTime.plusMinutes(5));
                        } catch (Exception e) {
                            logger.warn("Failed to parse candle timestamp: {}", candle.getTimestamp());
                        }
                    }

                    historicalCandles.add(candleStick);
                }

                logger.info("Converted {} candles from Kite historical data API", historicalCandles.size());
            } else {
                String errorMsg = historicalDataResponse != null ? historicalDataResponse.getMessage() : "null response";
                logger.error("Failed to fetch historical data: {}", errorMsg);
                return Collections.emptyList();
            }

            // If we don't have enough candles, log warning but continue
            if (historicalCandles.size() < REQUIRED_CANDLES) {
                logger.warn("Only got {} candles, expected {} for 3 hours. Continuing with available data.",
                        historicalCandles.size(), REQUIRED_CANDLES);
            }

            // Sort candles by time (oldest first)
            historicalCandles.sort(Comparator.comparing(CandleStick::getCandleStartTime));

            logger.info("Using {} candles for prediction analysis", historicalCandles.size());

            // Get latest IndexLTP data
            List<IndexLTP> indexLTPList = indexLTPRepository.findLast5000IndexDataByAppJobConfigNum(appJobConfigNum);
            IndexLTP latestIndexLTP = indexLTPList.isEmpty() ? null : indexLTPList.getFirst();

            // Get option chain data (MiniDelta)
            List<MiniDelta> miniDeltaList = miniDeltaRepository.findByAppJobConfigNumOrderByIdAsc(appJobConfigNum);

            // Get latest tick data - CRITICAL for accurate predictions
            Tick latestTick = null;
            Double liveNiftyPrice = null;

            if (kiteTickerProvider != null) {
                // Try to get from tickerMapForJob first
                if (kiteTickerProvider.tickerMapForJob != null &&
                    kiteTickerProvider.tickerMapForJob.containsKey(NIFTY_INSTRUMENT_TOKEN)) {
                    latestTick = kiteTickerProvider.tickerMapForJob.get(NIFTY_INSTRUMENT_TOKEN);
                    if (latestTick != null) {
                        liveNiftyPrice = latestTick.getLastTradedPrice();
                        logger.info("Live tick data from tickerMapForJob - NIFTY LTP: {}", liveNiftyPrice);
                    }
                }

                // Fallback to niftyLastPrice field if tick not available
                if (liveNiftyPrice == null && kiteTickerProvider.niftyLastPrice != null
                        && kiteTickerProvider.niftyLastPrice > 0) {
                    liveNiftyPrice = kiteTickerProvider.niftyLastPrice;
                    logger.info("Live price from niftyLastPrice field - NIFTY LTP: {}", liveNiftyPrice);
                }

                if (liveNiftyPrice == null) {
                    logger.warn("No live NIFTY price available from ticker. tickerMapForJob keys: {}",
                            kiteTickerProvider.tickerMapForJob != null ?
                                kiteTickerProvider.tickerMapForJob.size() : "null");
                }
            } else {
                logger.warn("kiteTickerProvider is null - live tick data not available");
            }

            // Step 2: Calculate technical indicators
            TechnicalIndicators indicators = calculateTechnicalIndicators(historicalCandles);
            logger.info("Technical Indicators - EMA9: {}, EMA21: {}, RSI: {}, ATR: {}",
                    indicators.ema9, indicators.ema21, indicators.rsi, indicators.atr);

            // Step 3: Calculate current and previous day high/low levels
            DayHighLowLevels dayLevels = calculateDayHighLowLevels(historicalCandles, latestIndexLTP);
            logger.info("Day Levels - Current Day High: {}, Low: {}, Previous Day High: {}, Low: {}",
                    dayLevels.currentDayHigh, dayLevels.currentDayLow,
                    dayLevels.previousDayHigh, dayLevels.previousDayLow);

            // Step 4: Analyze ATM strike price behavior (CE and PE LTP)
            // CRITICAL: Priority should be live tick data > liveNiftyPrice > IndexLTP > fallback
            // Live tick data is the most accurate current price
            double currentNiftyPrice;
            double tickPrice = latestTick != null ? latestTick.getLastTradedPrice() : 0;
            double indexLTPPrice = latestIndexLTP != null && latestIndexLTP.getIndexLTP() != null
                    ? latestIndexLTP.getIndexLTP() : 0;

            if (tickPrice > 0) {
                currentNiftyPrice = tickPrice;
                logger.info("Using live tick price for analysis: {}", currentNiftyPrice);
            } else if (liveNiftyPrice != null && liveNiftyPrice > 0) {
                currentNiftyPrice = liveNiftyPrice;
                logger.info("Using liveNiftyPrice for analysis: {}", currentNiftyPrice);
            } else if (indexLTPPrice > 0) {
                currentNiftyPrice = indexLTPPrice;
                logger.info("Using IndexLTP for analysis: {}", currentNiftyPrice);
            } else {
                currentNiftyPrice = 26220; // Updated fallback to current approximate Nifty level
                logger.warn("Using fallback price for analysis: {}", currentNiftyPrice);
            }

            // Log all price sources for debugging
            logger.info("Price sources for analysis - Tick: {}, LiveNiftyPrice: {}, IndexLTP: {}, Using: {}",
                    tickPrice, liveNiftyPrice, indexLTPPrice, currentNiftyPrice);

            ATMStrikeAnalysis atmAnalysis = analyzeATMStrikeBehavior(currentNiftyPrice, miniDeltaList);
            logger.info("ATM Analysis - Strike: {}, CE LTP: {}, PE LTP: {}, CE-PE Diff: {}, Bias: {}",
                    atmAnalysis.atmStrike, atmAnalysis.atmCELTP, atmAnalysis.atmPELTP,
                    atmAnalysis.cePeDifference, atmAnalysis.atmBias);

            // Step 5: Analyze VIX, Gamma Exposure (GEX), and Theta decay
            GreeksAnalysis greeksAnalysis = analyzeGreeks(miniDeltaList, atmAnalysis, currentNiftyPrice);
            logger.info("Greeks Analysis - VIX: {}, NetGEX: {}, GEX Bias: {}, Theta Bias: {}, Overall: {}",
                    greeksAnalysis.vixValue, greeksAnalysis.netGEX, greeksAnalysis.gexBias,
                    greeksAnalysis.thetaBias, greeksAnalysis.overallGreeksBias);

            // Step 6: Analyze option chain for market bias (enhanced with ATM, Greeks data)
            OptionChainAnalysis ocAnalysis = analyzeOptionChain(miniDeltaList, latestIndexLTP, atmAnalysis, dayLevels, greeksAnalysis);
            logger.info("Option Chain Analysis - PCR: {}, MaxPain: {}, Bias: {}",
                    ocAnalysis.pcr, ocAnalysis.maxPainStrike, ocAnalysis.bias);

            // Step 6.5: Analyze Smart Money Concepts (Price Action, Order Blocks, FVGs)
            SmartMoneyAnalysis smcAnalysis = analyzeSmartMoneyConcepts(historicalCandles, currentNiftyPrice);
            logger.info("SMC Analysis - Bias: {}, Confidence: {}%, Trade: {} - {}",
                    smcAnalysis.smcBias, smcAnalysis.smcConfidence,
                    smcAnalysis.tradeSuggestion, smcAnalysis.tradeSuggestionReason);

            // Step 6.6: Analyze Technical Levels (Channel Patterns & Fibonacci Retracements)
            TechnicalLevelsAnalysis techLevelsAnalysis = analyzeTechnicalLevels(
                    historicalCandles, currentNiftyPrice, smcAnalysis, dayLevels, ocAnalysis);
            logger.info("Technical Levels - Channel: {}, Fib Nearest: {}, Support: {}, Resistance: {}, Bias: {}",
                    techLevelsAnalysis.channel.channelType,
                    techLevelsAnalysis.fibonacci.nearestFibLevel,
                    techLevelsAnalysis.strongestSupport,
                    techLevelsAnalysis.strongestResistance,
                    techLevelsAnalysis.overallBias);

            // Step 6.7: Generate Trade Setup with Entry, Target, Stop-Loss
            double atr = indicators.atr > 0 ? indicators.atr : 15.0;
            TradeSetup tradeSetup = generateTradeSetup(currentNiftyPrice, smcAnalysis, techLevelsAnalysis,
                    ocAnalysis, greeksAnalysis, atmAnalysis, atr);
            smcAnalysis.tradeSetup = tradeSetup;

            if (tradeSetup.isValid) {
                logger.info("=== TRADE SETUP ===");
                logger.info("Direction: {} | Confidence: {}%", tradeSetup.tradeDirection, tradeSetup.confidence);
                logger.info("Entry: {} ({}) - {}", tradeSetup.entryPrice, tradeSetup.entryType, tradeSetup.entryReason);
                logger.info("Stop-Loss: {} - {}", tradeSetup.stopLoss, tradeSetup.stopLossReason);
                logger.info("Targets: T1={}, T2={}, T3={} - {}",
                        tradeSetup.target1, tradeSetup.target2, tradeSetup.target3, tradeSetup.targetReason);
                logger.info("Risk: {} pts | Reward: {} pts | RR Ratio: {}",
                        tradeSetup.riskPoints, tradeSetup.rewardPoints1, tradeSetup.riskRewardRatio1);
                logger.info("Option: {} {} ({})", tradeSetup.optionStrategy, tradeSetup.suggestedStrike, tradeSetup.suggestedOptionType);
                logger.info("Setup Type: {} | Valid Until: {}", tradeSetup.setupType, tradeSetup.validUntil);
                logger.info("==================");
            } else {
                logger.info("No valid trade setup: {}", tradeSetup.invalidReason);
            }

            // Step 7: Determine overall market trend (enhanced with day levels, ATM, Greeks, SMC, and Tech Levels)
            String marketTrend = determineMarketTrend(indicators, ocAnalysis, dayLevels, atmAnalysis, greeksAnalysis, currentNiftyPrice);

            // Adjust trend based on SMC if high confidence
            if (smcAnalysis.smcConfidence >= 70) {
                if (!smcAnalysis.smcBias.equals(marketTrend) && !smcAnalysis.smcBias.equals(TREND_NEUTRAL)) {
                    logger.info("SMC bias ({}) differs from market trend ({}). SMC confidence: {}%",
                            smcAnalysis.smcBias, marketTrend, smcAnalysis.smcConfidence);
                    if (smcAnalysis.smcConfidence >= 80) {
                        marketTrend = smcAnalysis.smcBias;
                        logger.info("Overriding market trend to {} based on high SMC confidence", marketTrend);
                    }
                }
            }

            // Also consider tech levels bias
            if (techLevelsAnalysis.analysisConfidence >= 70 &&
                !techLevelsAnalysis.overallBias.equals(TREND_NEUTRAL) &&
                !techLevelsAnalysis.overallBias.equals(marketTrend)) {
                logger.info("Tech Levels bias ({}) with confidence {}%",
                        techLevelsAnalysis.overallBias, techLevelsAnalysis.analysisConfidence);
            }

            logger.info("Final Market Trend: {}", marketTrend);

            // Step 7.5: Save Trade Setup to database (after market trend is determined)
            TradeSetupEntity tradeSetupEntity = saveTradeSetup(tradeSetup, currentNiftyPrice, marketTrend,
                    smcAnalysis, techLevelsAnalysis, ocAnalysis);
            if (tradeSetupEntity != null) {
                latestTradeSetupCache.set(tradeSetupEntity);  // Thread-safe cache update
                logger.info("Trade setup saved with ID: {}", tradeSetupEntity.getId());
            }

            // Step 8: Calculate expected volatility (enhanced with VIX data)
            Double expectedVolatility = calculateExpectedVolatility(historicalCandles, latestIndexLTP, greeksAnalysis);
            logger.info("Expected Volatility: {} points", expectedVolatility);

            // Step 9: Generate 5 predicted candles (enhanced with all analysis including SMC and Tech Levels)
            // Pass both latestTick and liveNiftyPrice for robust price source handling
            List<PredictedCandleStick> predictions = generatePredictions(
                    historicalCandles, indicators, ocAnalysis, dayLevels, atmAnalysis, greeksAnalysis,
                    smcAnalysis, techLevelsAnalysis, marketTrend, expectedVolatility, latestTick, liveNiftyPrice);

            // Step 10: Save predictions
            predictions = (List<PredictedCandleStick>) predictedCandleRepository.saveAll(predictions);
            latestPredictions = new CopyOnWriteArrayList<>(predictions);

            logger.info("Generated and saved {} predictions", predictions.size());
            return predictions;

        } catch (Exception e) {
            logger.error("Error during candle prediction: {}", e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    @Override
    public List<PredictedCandleStick> getLatestPredictions() {
        if (latestPredictions.isEmpty()) {
            List<PredictedCandleStick> fromDb = predictedCandleRepository.findLatestPredictionsForInstrument(NIFTY_INSTRUMENT_TOKEN);
            // Discard stale prior-day predictions loaded from DB after a server restart
            if (!fromDb.isEmpty()) {
                LocalDateTime generatedAt = fromDb.get(0).getPredictionGeneratedAt();
                if (generatedAt != null && generatedAt.toLocalDate().equals(LocalDate.now())) {
                    latestPredictions = fromDb;
                }
            }
        }
        return latestPredictions;
    }

    @Override
    public int verifyPastPredictions() {
        int verifiedCount = 0;
        LocalDateTime now = LocalDateTime.now();

        List<PredictedCandleStick> unverifiedPredictions = predictedCandleRepository.findUnverifiedPredictions(now);
        logger.info("Found {} unverified predictions to process", unverifiedPredictions.size());

        // Resolve IndexLTP fallback once outside the loop to avoid repeated DB calls
        Double indexLTPFallbackPrice = null;
        if (kiteTickerProvider != null && kiteTickerProvider.tickerMapForJob != null
                && kiteTickerProvider.tickerMapForJob.containsKey(NIFTY_INSTRUMENT_TOKEN)) {
            indexLTPFallbackPrice = kiteTickerProvider.tickerMapForJob.get(NIFTY_INSTRUMENT_TOKEN).getLastTradedPrice();
        } else {
            List<IndexLTP> latestLTPList = indexLTPRepository.findLatestIndexDataByAppJobConfigNum(1);
            if (latestLTPList != null && !latestLTPList.isEmpty() && latestLTPList.get(0).getIndexLTP() != null) {
                indexLTPFallbackPrice = Double.valueOf(latestLTPList.get(0).getIndexLTP());
            }
        }

        List<PredictedCandleStick> toSave = new ArrayList<>();

        for (PredictedCandleStick prediction : unverifiedPredictions) {
            CandleStick actualCandle = findActualCandle(prediction.getCandleStartTime());
            Double actualClosePrice = null;

            if (actualCandle != null) {
                actualClosePrice = actualCandle.getClosePrice();
            } else if (prediction.getCandleEndTime().isBefore(now) &&
                       prediction.getCandleEndTime().isAfter(now.minusMinutes(30))) {
                // Fallback: use pre-resolved live price within a reasonable recency window
                actualClosePrice = indexLTPFallbackPrice;
                if (actualClosePrice != null) {
                    logger.debug("Using fallback price {} for verification at {}",
                            actualClosePrice, prediction.getCandleStartTime());
                }
            }

            if (actualClosePrice != null) {
                prediction.setActualClosePrice(actualClosePrice);
                prediction.setVerified(true);

                // Calculate accuracy based on close price prediction
                double predictionError = Math.abs(prediction.getClosePrice() - actualClosePrice);
                double candleRange = actualCandle != null ?
                        Math.abs(actualCandle.getHighPrice() - actualCandle.getLowPrice()) : 15.0;

                double accuracy = 100.0 - ((predictionError / Math.max(candleRange, 1.0)) * 100.0);
                accuracy = Math.max(0.0, Math.min(100.0, accuracy));

                boolean predictedUp = prediction.getClosePrice() > prediction.getOpenPrice();
                // Use actual candle's open to determine actual direction, not predicted open
                boolean actualUp = actualCandle != null
                        ? actualClosePrice > actualCandle.getOpenPrice()
                        : actualClosePrice > prediction.getOpenPrice();
                if (predictedUp == actualUp) {
                    accuracy = Math.min(100.0, accuracy + 10.0);
                }

                prediction.setPredictionAccuracy(accuracy);
                toSave.add(prediction);
                verifiedCount++;

                logger.debug("Verified prediction for {}: Predicted Close={}, Actual Close={}, Accuracy={}%",
                        prediction.getCandleStartTime(), prediction.getClosePrice(),
                        actualClosePrice, accuracy);
            }
        }

        // Batch-save all verified predictions in a single DB call
        if (!toSave.isEmpty()) {
            predictedCandleRepository.saveAll(toSave);
        }

        // Increment verification runs counter (always increment when called manually)
        totalVerificationRuns.incrementAndGet();
        lastVerificationTime = LocalDateTime.now();

        logger.info("Verified {} predictions", verifiedCount);
        return verifiedCount;
    }

    @Override
    public Double getSessionAccuracy() {
        LocalDateTime sessionStart = LocalDateTime.now().with(MARKET_OPEN);
        Double accuracy = predictedCandleRepository.getAverageAccuracyForInstrument(
                NIFTY_INSTRUMENT_TOKEN, sessionStart);
        return accuracy != null ? accuracy : 0.0;
    }

    @Override
    public String analyzeMarketBias(List<MiniDelta> miniDeltaList, IndexLTP indexLTP) {
        // Calculate current price for ATM analysis - prioritize live tick data
        double currentPrice;
        if (kiteTickerProvider != null && kiteTickerProvider.tickerMapForJob != null
                && kiteTickerProvider.tickerMapForJob.containsKey(NIFTY_INSTRUMENT_TOKEN)) {
            currentPrice = kiteTickerProvider.tickerMapForJob.get(NIFTY_INSTRUMENT_TOKEN).getLastTradedPrice();
        } else if (indexLTP != null && indexLTP.getIndexLTP() != null) {
            currentPrice = indexLTP.getIndexLTP();
        } else {
            currentPrice = 26220; // Updated fallback
        }

        // Create basic ATM and day level analysis
        ATMStrikeAnalysis atmAnalysis = analyzeATMStrikeBehavior(currentPrice, miniDeltaList);
        DayHighLowLevels dayLevels = calculateDayHighLowLevels(Collections.emptyList(), indexLTP);
        GreeksAnalysis greeksAnalysis = analyzeGreeks(miniDeltaList, atmAnalysis, currentPrice);

        OptionChainAnalysis analysis = analyzeOptionChain(miniDeltaList, indexLTP, atmAnalysis, dayLevels, greeksAnalysis);
        return analysis.bias;
    }

    @Override
    public Double calculateExpectedVolatility(List<CandleStick> recentCandles, IndexLTP indexLTP) {
        return calculateExpectedVolatility(recentCandles, indexLTP, null);
    }

    /**
     * Calculate expected volatility enhanced with VIX and Greeks data
     */
    private Double calculateExpectedVolatility(List<CandleStick> recentCandles, IndexLTP indexLTP,
                                               GreeksAnalysis greeksAnalysis) {
        if (recentCandles == null || recentCandles.isEmpty()) {
            return 15.0; // Default volatility in points for NIFTY
        }

        // Calculate ATR-based volatility
        double atr = calculateATR(recentCandles, Math.min(ATR_PERIOD, recentCandles.size()));

        // Adjust volatility based on time of day (higher at open/close)
        LocalTime now = LocalTime.now();
        double timeMultiplier = 1.0;
        if (now.isBefore(MARKET_OPEN.plusMinutes(30))) {
            timeMultiplier = 1.5; // Opening volatility boost
        } else if (now.isAfter(MARKET_CLOSE.minusMinutes(30))) {
            timeMultiplier = 1.3; // Closing volatility boost
        } else if (now.isAfter(LocalTime.of(13, 0)) && now.isBefore(LocalTime.of(14, 0))) {
            timeMultiplier = 0.8; // Lunch hour lull
        }

        // Adjust based on distance from max pain
        if (indexLTP != null && indexLTP.getMaxPainSP() != null) {
            double distanceFromMaxPain = Math.abs(indexLTP.getIndexLTP() - indexLTP.getMaxPainSP());
            if (distanceFromMaxPain > 100) {
                timeMultiplier *= 1.2; // Higher volatility far from max pain
            }
        }

        // VIX-based volatility adjustment
        if (greeksAnalysis != null && greeksAnalysis.vixValue > 0) {
            // VIX is annualized volatility, convert to expected 1-minute move
            // Formula: Daily move = VIX / sqrt(252), Minute move = Daily / sqrt(375)
            double vixDailyMove = greeksAnalysis.vixValue / Math.sqrt(252);
            double vixMinuteMove = vixDailyMove / Math.sqrt(375); // 375 1-minute candles in a day

            // VIX-based expected points (assuming NIFTY at ~26220)
            double spotPrice = indexLTP != null && indexLTP.getIndexLTP() != null
                    ? indexLTP.getIndexLTP() : 26220;
            double vixBasedVolatility = spotPrice * vixMinuteMove / 100;

            // Blend ATR and VIX-based volatility (60% ATR, 40% VIX)
            atr = (atr * 0.6) + (vixBasedVolatility * 0.4);

            // Additional adjustments based on VIX conditions
            if (greeksAnalysis.isHighVix) {
                timeMultiplier *= 1.3; // High VIX = higher volatility
            } else if (greeksAnalysis.isLowVix) {
                timeMultiplier *= 0.9; // Low VIX = lower volatility
            }

            if (greeksAnalysis.isVixSpike) {
                timeMultiplier *= 1.5; // VIX spike = significantly higher volatility
            }
        }

        // GEX-based volatility adjustment
        if (greeksAnalysis != null) {
            if (!greeksAnalysis.isPositiveGEX) {
                // Negative GEX = trending market = higher volatility
                timeMultiplier *= 1.2;
            } else if (greeksAnalysis.netGEX > 1000000) {
                // Very high positive GEX = mean reversion = lower volatility
                timeMultiplier *= 0.85;
            }

            // Near expiry volatility adjustment
            if (greeksAnalysis.isExpiryDay) {
                timeMultiplier *= 1.4; // Expiry day is volatile
            } else if (greeksAnalysis.isNearExpiry) {
                timeMultiplier *= 1.2;
            }
        }

        return atr * timeMultiplier;
    }

    @Override
    public String getTradeRecommendation() {
        List<PredictedCandleStick> predictions = getLatestPredictions();
        if (predictions.isEmpty()) {
            return TREND_NEUTRAL;
        }

        // Analyze the overall trend of predicted candles
        int bullishCount = 0;
        int bearishCount = 0;

        for (PredictedCandleStick candle : predictions) {
            if (candle.getClosePrice() > candle.getOpenPrice()) {
                bullishCount++;
            } else if (candle.getClosePrice() < candle.getOpenPrice()) {
                bearishCount++;
            }
        }

        // Consider confidence scores
        double avgConfidence = predictions.stream()
                .mapToDouble(p -> p.getConfidenceScore() != null ? p.getConfidenceScore() : 50.0)
                .average()
                .orElse(50.0);

        if (avgConfidence < 40) {
            return TREND_NEUTRAL; // Low confidence, no clear recommendation
        }

        if (bullishCount >= 4) {
            return TREND_BULLISH;
        } else if (bearishCount >= 4) {
            return TREND_BEARISH;
        } else if (bullishCount >= 3 && avgConfidence > 60) {
            return TREND_BULLISH;
        } else if (bearishCount >= 3 && avgConfidence > 60) {
            return TREND_BEARISH;
        }

        return TREND_NEUTRAL;
    }

    @Override
    public int cleanupOldPredictions(int daysToRetain) {
        LocalDateTime cutoffDate = LocalDateTime.now().minusDays(daysToRetain);
        long countBefore = predictedCandleRepository.countByInstrumentToken(NIFTY_INSTRUMENT_TOKEN);
        predictedCandleRepository.deleteByInstrumentTokenAndPredictionGeneratedAtBefore(
                NIFTY_INSTRUMENT_TOKEN, cutoffDate);
        long countAfter = predictedCandleRepository.countByInstrumentToken(NIFTY_INSTRUMENT_TOKEN);
        int deleted = (int) (countBefore - countAfter);
        logger.info("Cleaned up {} old predictions older than {} days", deleted, daysToRetain);
        return deleted;
    }

    // ===================== Public Ticker Subscription =====================
    @Override
    public void subscribeTokenForJob(List<Long> tokens) {
        try {
            if (tokens == null || tokens.isEmpty()) return;
            if (kiteTickerProvider == null) {
                logger.warn("Ticker provider not initialized, attempting to start ticker");
                startKiteTicker();
            }
            if (kiteTickerProvider != null) {
                kiteTickerProvider.subscribeTokenForJob(new ArrayList<>(tokens));
                logger.info("Requested subscription for {} tokens", tokens.size());
            } else {
                logger.warn("Unable to subscribe tokens - kiteTickerProvider still null");
            }
        } catch (Exception e) {
            logger.error("Error subscribing tokens for job: {}", e.getMessage(), e);
        }
    }

    // ===================== Private Helper Methods =====================

    /**
     * Calculate all technical indicators from candle data
     */
    private TechnicalIndicators calculateTechnicalIndicators(List<CandleStick> candles) {
        TechnicalIndicators indicators = new TechnicalIndicators();

        if (candles.isEmpty()) {
            return indicators;
        }

        // Extract close prices
        List<Double> closePrices = candles.stream()
                .map(CandleStick::getClosePrice)
                .filter(Objects::nonNull)
                .toList();

        if (closePrices.isEmpty()) {
            return indicators;
        }

        // Calculate EMAs
        indicators.ema9 = calculateEMA(closePrices, EMA_SHORT_PERIOD);
        indicators.ema21 = calculateEMA(closePrices, EMA_LONG_PERIOD);
 
        // Calculate RSI
        indicators.rsi = calculateRSI(closePrices, RSI_PERIOD);

        // Calculate ATR
        indicators.atr = calculateATR(candles, Math.min(ATR_PERIOD, candles.size()));

        // Calculate VWAP (using close * volume approximation since we don't have volume)
        indicators.vwap = calculateApproximateVWAP(candles);

        // Determine EMA crossover state
        if (indicators.ema9 > indicators.ema21) {
            indicators.emaCrossover = "BULLISH_CROSSOVER";
        } else if (indicators.ema9 < indicators.ema21) {
            indicators.emaCrossover = "BEARISH_CROSSOVER";
        } else {
            indicators.emaCrossover = "NEUTRAL";
        }

        // Calculate momentum (rate of change)
        if (closePrices.size() >= 5) {
            double recentClose = closePrices.getLast();
            double fiveMinAgo = closePrices.get(closePrices.size() - 5);
            indicators.momentum = ((recentClose - fiveMinAgo) / fiveMinAgo) * 100;
        }

        // Identify support and resistance from recent price action
        indicators.swingHigh = candles.stream()
                .map(CandleStick::getHighPrice)
                .filter(Objects::nonNull)
                .mapToDouble(Double::doubleValue)
                .max()
                .orElse(0);
        indicators.swingLow = candles.stream()
                .map(CandleStick::getLowPrice)
                .filter(Objects::nonNull)
                .mapToDouble(Double::doubleValue)
                .min()
                .orElse(0);

        return indicators;
    }

    /**
     * Core EMA computation — returns all EMA values starting from index (period-1).
     * Index 0 of the result = seed SMA for prices[0..period-1].
     * Returns empty list if prices.size() < period.
     */
    private List<Double> computeEMAValues(List<Double> closes, int period) {
        if (closes == null || closes.size() < period || period <= 0) {
            return List.of();
        }
        int n = closes.size();
        double sum = 0.0;
        for (int i = 0; i < period; i++) sum += closes.get(i);
        double ema = sum / period;

        final double multiplier = 2.0 / (period + 1);
        List<Double> result = new ArrayList<>(n - period + 1);
        result.add(ema);
        for (int i = period; i < n; i++) {
            ema = Math.fma(multiplier, closes.get(i) - ema, ema);
            result.add(ema);
        }

        return Collections.unmodifiableList(result);
    }

    /**
     * Calculate Exponential Moving Average (final value only)
     */
    private double calculateEMA(List<Double> prices, int period) {
        if (prices == null || prices.isEmpty() || period <= 0) return 0;
        int p = Math.min(period, prices.size());
        double sum = 0.0;
        for (int i = 0; i < p; i++) sum += prices.get(i);
        double ema = sum / p;
        final double multiplier = 2.0 / (p + 1);
        for (int i = p; i < prices.size(); i++) {
            ema = Math.fma(multiplier, prices.get(i) - ema, ema);
        }
        return Math.round(ema * 100.0) / 100.0;
    }

    /**
     * Calculate Relative Strength Index
     */
    /**
     * Wilder's smoothed RSI — matches TradingView/standard platforms.
     * Seeds with SMA of first 'period' gains/losses, then applies
     * exponential smoothing: avgGain = (prevAvgGain*(period-1) + currGain) / period
     */
    private double calculateRSI(List<Double> prices, int period) {
        if (prices == null || prices.size() < period + 1 || period <= 0) {
            return 50; // neutral — not enough data
        }

        // Seed: SMA of first 'period' changes
        double avgGain = 0, avgLoss = 0;
        for (int i = 1; i <= period; i++) {
            double change = prices.get(i) - prices.get(i - 1);
            if (change > 0) avgGain += change;
            else avgLoss += Math.abs(change);
        }
        avgGain /= period;
        avgLoss /= period;

        // Wilder's smoothing for remaining prices
        for (int i = period + 1; i < prices.size(); i++) {
            double change = prices.get(i) - prices.get(i - 1);
            double gain = Math.max(change, 0.0);
            double loss = Math.max(-change, 0.0);
            avgGain = (avgGain * (period - 1) + gain) / period;
            avgLoss = (avgLoss * (period - 1) + loss) / period;
        }

        if (avgLoss == 0) return 100;
        double rs = avgGain / avgLoss;
        return 100 - (100 / (1 + rs));
    }

    /**
     * Calculate Average True Range
     */
    private double calculateATR(List<CandleStick> candles, int period) {
        if (candles.size() < 2 || period <= 0) {
            return 10; // Default ATR
        }

        List<Double> trueRanges = new ArrayList<>();

        for (int i = 1; i < candles.size(); i++) {
            CandleStick current = candles.get(i);
            CandleStick previous = candles.get(i - 1);

            if (current.getHighPrice() == null || current.getLowPrice() == null ||
                    previous.getClosePrice() == null) {
                continue;
            }

            double highLow = current.getHighPrice() - current.getLowPrice();
            double highClose = Math.abs(current.getHighPrice() - previous.getClosePrice());
            double lowClose = Math.abs(current.getLowPrice() - previous.getClosePrice());

            double tr = Math.max(highLow, Math.max(highClose, lowClose));
            trueRanges.add(tr);
        }

        if (trueRanges.isEmpty()) {
            return 10;
        }

        int effectivePeriod = Math.min(period, trueRanges.size());
        return trueRanges.subList(trueRanges.size() - effectivePeriod, trueRanges.size())
                .stream()
                .mapToDouble(Double::doubleValue)
                .average()
                .orElse(10);
    }

    /**
     * Calculate approximate VWAP (Volume Weighted Average Price)
     * Since we don't have volume, we use a time-weighted average
     */
    private double calculateApproximateVWAP(List<CandleStick> candles) {
        if (candles.isEmpty()) {
            return 0;
        }

        // VWAP resets at the start of each trading day (9:15 IST).
        // Accumulate only today's candles so multi-day data doesn't corrupt the value.
        LocalDate today = LocalDate.now();
        double sum = 0;
        int count = 0;

        for (CandleStick candle : candles) {
            if (candle.getHighPrice() == null || candle.getLowPrice() == null || candle.getClosePrice() == null) {
                continue;
            }
            // Skip candles from previous days
            if (candle.getCandleStartTime() != null && !candle.getCandleStartTime().toLocalDate().equals(today)) {
                continue;
            }
            double typicalPrice = (candle.getHighPrice() + candle.getLowPrice() + candle.getClosePrice()) / 3;
            sum += typicalPrice;
            count++;
        }

        return count > 0 ? sum / count : 0;
    }

    /**
     * Calculate current and previous day high/low levels
     * Uses candle data and IndexLTP for comprehensive day level analysis
     */
    private DayHighLowLevels calculateDayHighLowLevels(List<CandleStick> historicalCandles, IndexLTP indexLTP) {
        DayHighLowLevels levels = new DayHighLowLevels();

        LocalDate today = LocalDate.now();
        LocalDate previousDay = today.minusDays(1);

        // Skip weekends for previous day
        if (previousDay.getDayOfWeek().getValue() == 7) { // Sunday
            previousDay = previousDay.minusDays(2);
        } else if (previousDay.getDayOfWeek().getValue() == 6) { // Saturday
            previousDay = previousDay.minusDays(1);
        }

        // Get current day high/low from IndexLTP if available
        if (indexLTP != null) {
            if (indexLTP.getDayHigh() != null) {
                try {
                    levels.currentDayHigh = Double.parseDouble(indexLTP.getDayHigh());
                } catch (NumberFormatException e) {
                    logger.warn("Could not parse day high: {}", indexLTP.getDayHigh());
                }
            }
            if (indexLTP.getDayLow() != null) {
                try {
                    levels.currentDayLow = Double.parseDouble(indexLTP.getDayLow());
                } catch (NumberFormatException e) {
                    logger.warn("Could not parse day low: {}", indexLTP.getDayLow());
                }
            }
        }

        // Calculate from historical candles if not available from IndexLTP
        if (levels.currentDayHigh == 0 || levels.currentDayLow == 0) {
            List<CandleStick> todayCandles = historicalCandles.stream()
                    .filter(c -> c.getCandleStartTime() != null &&
                            c.getCandleStartTime().toLocalDate().equals(today))
                    .toList();

            if (!todayCandles.isEmpty()) {
                levels.currentDayHigh = todayCandles.stream()
                        .filter(c -> c.getHighPrice() != null)
                        .mapToDouble(CandleStick::getHighPrice)
                        .max().orElse(0);
                levels.currentDayLow = todayCandles.stream()
                        .filter(c -> c.getLowPrice() != null)
                        .mapToDouble(CandleStick::getLowPrice)
                        .min().orElse(0);
            }
        }

        // Get previous day data from historical candles
        final LocalDate prevDay = previousDay;
        List<CandleStick> previousDayCandles = historicalCandles.stream()
                .filter(c -> c.getCandleStartTime() != null &&
                        c.getCandleStartTime().toLocalDate().equals(prevDay))
                .toList();

        if (!previousDayCandles.isEmpty()) {
            levels.previousDayHigh = previousDayCandles.stream()
                    .filter(c -> c.getHighPrice() != null)
                    .mapToDouble(CandleStick::getHighPrice)
                    .max().orElse(0);
            levels.previousDayLow = previousDayCandles.stream()
                    .filter(c -> c.getLowPrice() != null)
                    .mapToDouble(CandleStick::getLowPrice)
                    .min().orElse(0);

            // Get previous day open (first candle) and close (last candle)
            previousDayCandles.stream()
                    .filter(c -> c.getOpenPrice() != null)
                    .min(Comparator.comparing(CandleStick::getCandleStartTime))
                    .ifPresent(c -> levels.previousDayOpen = c.getOpenPrice());
            previousDayCandles.stream()
                    .filter(c -> c.getClosePrice() != null)
                    .max(Comparator.comparing(CandleStick::getCandleStartTime))
                    .ifPresent(c -> levels.previousDayClose = c.getClosePrice());
        }

        // If still no previous day data, try to fetch from DB
        if (levels.previousDayHigh == 0) {
            LocalDateTime prevDayStart = previousDay.atTime(MARKET_OPEN);
            LocalDateTime prevDayEnd = previousDay.atTime(MARKET_CLOSE);
            List<CandleStick> dbPrevDayCandles = candleStickRepository.findCandlesInRange(
                    NIFTY_INSTRUMENT_TOKEN, prevDayStart, prevDayEnd);

            if (!dbPrevDayCandles.isEmpty()) {
                levels.previousDayHigh = dbPrevDayCandles.stream()
                        .filter(c -> c.getHighPrice() != null)
                        .mapToDouble(CandleStick::getHighPrice)
                        .max().orElse(0);
                levels.previousDayLow = dbPrevDayCandles.stream()
                        .filter(c -> c.getLowPrice() != null)
                        .mapToDouble(CandleStick::getLowPrice)
                        .min().orElse(0);
                dbPrevDayCandles.stream()
                        .filter(c -> c.getOpenPrice() != null)
                        .min(Comparator.comparing(CandleStick::getCandleStartTime))
                        .ifPresent(c -> levels.previousDayOpen = c.getOpenPrice());
                dbPrevDayCandles.stream()
                        .filter(c -> c.getClosePrice() != null)
                        .max(Comparator.comparing(CandleStick::getCandleStartTime))
                        .ifPresent(c -> levels.previousDayClose = c.getClosePrice());
            }
        }

        // Calculate pivot points using previous day data
        if (levels.previousDayHigh > 0 && levels.previousDayLow > 0 && levels.previousDayClose > 0) {
            levels.pivotPoint = (levels.previousDayHigh + levels.previousDayLow + levels.previousDayClose) / 3;
            levels.r1 = (2 * levels.pivotPoint) - levels.previousDayLow;
            levels.s1 = (2 * levels.pivotPoint) - levels.previousDayHigh;
            levels.r2 = levels.pivotPoint + (levels.previousDayHigh - levels.previousDayLow);
            levels.s2 = levels.pivotPoint - (levels.previousDayHigh - levels.previousDayLow);
        }

        // Determine current price position relative to previous day
        double currentPrice = indexLTP != null && indexLTP.getIndexLTP() != null
                ? indexLTP.getIndexLTP() : levels.currentDayHigh;

        if (levels.previousDayHigh > 0) {
            levels.isAbovePreviousDayHigh = currentPrice > levels.previousDayHigh;
            levels.isBelowPreviousDayLow = currentPrice < levels.previousDayLow;
            levels.isInPreviousDayRange = !levels.isAbovePreviousDayHigh && !levels.isBelowPreviousDayLow;

            // Determine day level bias
            if (levels.isAbovePreviousDayHigh) {
                levels.dayLevelBias = TREND_BULLISH;
            } else if (levels.isBelowPreviousDayLow) {
                levels.dayLevelBias = TREND_BEARISH;
            } else {
                // Within range - check if closer to high or low
                double midPoint = (levels.previousDayHigh + levels.previousDayLow) / 2;
                levels.dayLevelBias = currentPrice > midPoint ? TREND_BULLISH : TREND_BEARISH;
            }
        }

        logger.debug("Day Levels calculated - PDH: {}, PDL: {}, PDC: {}, Pivot: {}, R1: {}, S1: {}",
                levels.previousDayHigh, levels.previousDayLow, levels.previousDayClose,
                levels.pivotPoint, levels.r1, levels.s1);

        return levels;
    }

    /**
     * Analyze ATM strike price behavior for CE and PE
     * This provides insight into option premium behavior and market sentiment
     */
    private ATMStrikeAnalysis analyzeATMStrikeBehavior(double currentNiftyPrice, List<MiniDelta> miniDeltaList) {
        ATMStrikeAnalysis analysis = new ATMStrikeAnalysis();

        // Calculate ATM strike (round to nearest strike gap)
        analysis.atmStrike = (int) (Math.round(currentNiftyPrice / NIFTY_STRIKE_GAP) * NIFTY_STRIKE_GAP);

        // Find ATM strike data from MiniDelta list
        String atmStrikeStr = String.valueOf(analysis.atmStrike);
        MiniDelta atmDelta = null;

        if (miniDeltaList != null) {
            atmDelta = miniDeltaList.stream()
                    .filter(d -> atmStrikeStr.equals(d.getStrikePrice()))
                    .findFirst()
                    .orElse(null);
        }

        if (atmDelta != null) {
            // Get OI data from MiniDelta
            analysis.atmCEOI = atmDelta.getCallOI() != null ? atmDelta.getCallOI() : 0;
            analysis.atmPEOI = atmDelta.getPutOI() != null ? atmDelta.getPutOI() : 0;
            analysis.atmCEOIChange = atmDelta.getCallOIChange() != null ? atmDelta.getCallOIChange() : 0;
            analysis.atmPEOIChange = atmDelta.getPutOIChange() != null ? atmDelta.getPutOIChange() : 0;
            analysis.atmPCR = analysis.atmCEOI > 0 ? analysis.atmPEOI / analysis.atmCEOI : 1.0;
        }

        // Get ATM CE and PE LTP from ticker if available
        if (kiteTickerProvider != null && kiteTickerProvider.tickerMapForJob != null) {
            // Find ATM CE and PE instrument tokens and get their LTPs
            List<InstrumentEntity> atmInstruments = instrumentRepository.findInstrumentFromStrikePrice(atmStrikeStr);

            if (atmInstruments != null && !atmInstruments.isEmpty()) {
                for (InstrumentEntity inst : atmInstruments) {
                    if (inst.getInstrument() != null) {
                        Long instToken = inst.getInstrument().getInstrument_token();
                        Tick tick = kiteTickerProvider.tickerMapForJob.get(instToken);

                        if (tick != null) {
                            String instType = inst.getInstrument().getInstrument_type();
                            if ("CE".equals(instType)) {
                                analysis.atmCELTP = tick.getLastTradedPrice();
                            } else if ("PE".equals(instType)) {
                                analysis.atmPELTP = tick.getLastTradedPrice();
                            }
                        }
                    }
                }
            } else {
                logger.warn("No ATM instruments found for strike: {}", atmStrikeStr);
            }
        }

        // Calculate derived metrics
        if (analysis.atmCELTP > 0 && analysis.atmPELTP > 0) {
            analysis.cePeDifference = analysis.atmCELTP - analysis.atmPELTP;
            analysis.cePeRatio = analysis.atmCELTP / analysis.atmPELTP;

            // Synthetic future = Spot + ATM CE - ATM PE
            analysis.syntheticFuture = currentNiftyPrice + analysis.cePeDifference;

            // IV Skew indicator: If CE is more expensive than PE, market expects upside
            // Positive skew = bullish, Negative skew = bearish
            analysis.ivSkew = (analysis.cePeRatio - 1.0) * 100;
        }

        // Determine ATM bias based on multiple factors
        analysis.atmBias = determineATMBias(analysis, currentNiftyPrice);

        logger.debug("ATM Analysis - Strike: {}, CE LTP: {}, PE LTP: {}, Diff: {}, Synthetic Future: {}, Bias: {}",
                analysis.atmStrike, analysis.atmCELTP, analysis.atmPELTP,
                analysis.cePeDifference, analysis.syntheticFuture, analysis.atmBias);

        return analysis;
    }

    /**
     * Determine bias from ATM strike analysis
     */
    private String determineATMBias(ATMStrikeAnalysis analysis, double currentPrice) {
        int bullishSignals = 0;
        int bearishSignals = 0;

        // CE-PE Difference Analysis
        // If CE > PE (positive difference), suggests bullish bias
        // If PE > CE (negative difference), suggests bearish bias
        if (analysis.cePeDifference > 5) {
            bullishSignals++;
        } else if (analysis.cePeDifference < -5) {
            bearishSignals++;
        }

        // Synthetic Future Analysis
        // If synthetic future > current price, market expects upside
        if (analysis.syntheticFuture > currentPrice + 10) {
            bullishSignals++;
        } else if (analysis.syntheticFuture < currentPrice - 10) {
            bearishSignals++;
        }

        // ATM PCR Analysis
        // High ATM PCR (>1.2) = more puts = bullish (hedging/writing)
        // Low ATM PCR (<0.8) = more calls = bearish
        if (analysis.atmPCR > 1.2) {
            bullishSignals++;
        } else if (analysis.atmPCR < 0.8) {
            bearishSignals++;
        }

        // ATM OI Change Analysis
        // Put OI increasing at ATM while Call OI decreasing = Bullish
        if (analysis.atmPEOIChange > 0 && analysis.atmCEOIChange <= 0) {
            bullishSignals++;
        } else if (analysis.atmCEOIChange > 0 && analysis.atmPEOIChange <= 0) {
            bearishSignals++;
        }

        // IV Skew Analysis
        if (analysis.ivSkew > 5) {
            bullishSignals++;
        } else if (analysis.ivSkew < -5) {
            bearishSignals++;
        }

        if (bullishSignals >= 3) {
            return TREND_BULLISH;
        } else if (bearishSignals >= 3) {
            return TREND_BEARISH;
        } else if (bullishSignals > bearishSignals) {
            return TREND_BULLISH;
        } else if (bearishSignals > bullishSignals) {
            return TREND_BEARISH;
        }

        return TREND_NEUTRAL;
    }

    /**
     * Analyze VIX, Gamma Exposure (GEX), and Theta decay for option chain prediction
     * These Greeks provide critical insights into market volatility and option dynamics
     */
    private GreeksAnalysis analyzeGreeks(List<MiniDelta> miniDeltaList, ATMStrikeAnalysis atmAnalysis, double currentPrice) {
        GreeksAnalysis analysis = new GreeksAnalysis();

        // Step 1: Analyze India VIX
        analyzeVIX(analysis);

        // Step 2: Calculate Gamma Exposure (GEX)
        calculateGammaExposure(analysis, miniDeltaList, atmAnalysis, currentPrice);

        // Step 3: Analyze Theta Decay
        analyzeTheta(analysis, miniDeltaList, atmAnalysis);

        // Step 4: Determine overall Greeks bias
        determineOverallGreeksBias(analysis);

        return analysis;
    }

    /**
     * Analyze India VIX (Volatility Index)
     * VIX indicates market fear/greed and expected volatility
     */
    private void analyzeVIX(GreeksAnalysis analysis) {


        // Get VIX data from ticker

        if (kiteTickerProvider != null && kiteTickerProvider.tickerMapForJob != null) {
            Tick vixTick = kiteTickerProvider.tickerMapForJob.get(INDIA_VIX_INSTRUMENT_TOKEN);

            if (vixTick != null) {
                logger.info("India VIX Token: {} ", vixTick.getLastTradedPrice());
                analysis.vixValue = vixTick.getLastTradedPrice();

                // Calculate VIX change (using open as reference for intraday)
                if (vixTick.getOpenPrice() > 0) {
                    analysis.vixChange = analysis.vixValue - vixTick.getOpenPrice();
                    analysis.vixPercentChange = (analysis.vixChange / vixTick.getOpenPrice()) * 100;
                }

                logger.debug("VIX Data - Value: {}, Change: {}, %Change: {}",
                        analysis.vixValue, analysis.vixChange, analysis.vixPercentChange);
            }
        }

        // If no VIX data available, use default moderate value
        if (analysis.vixValue == 0) {
            analysis.vixValue = 9.4; // Default moderate VIX
            logger.debug("Using default VIX value: {}", analysis.vixValue);
        }

        // Classify VIX levels
        analysis.isHighVix = analysis.vixValue > 20;
        analysis.isLowVix = analysis.vixValue < 12;
        analysis.isVixSpike = analysis.vixPercentChange > 10;

        // Determine VIX trend
        if (analysis.vixPercentChange > 5) {
            analysis.vixTrend = "RISING";
        } else if (analysis.vixPercentChange < -5) {
            analysis.vixTrend = "FALLING";
        } else {
            analysis.vixTrend = "STABLE";
        }

        logger.debug("VIX Analysis - High: {}, Low: {}, Spike: {}, Trend: {}",
                analysis.isHighVix, analysis.isLowVix, analysis.isVixSpike, analysis.vixTrend);
    }

    /**
     * Calculate Gamma Exposure (GEX) across the option chain
     * GEX helps predict market maker hedging behavior
     * Positive GEX = Mean reversion, Negative GEX = Trending/Volatile
     */
    private void calculateGammaExposure(GreeksAnalysis analysis, List<MiniDelta> miniDeltaList,
                                        ATMStrikeAnalysis atmAnalysis, double currentPrice) {
        if (miniDeltaList == null || miniDeltaList.isEmpty()) {
            return;
        }

        double totalCallGEX = 0;
        double totalPutGEX = 0;
        double maxGamma = 0;
        int maxGammaStrike = 0;

        for (MiniDelta delta : miniDeltaList) {
            if (delta.getStrikePrice() == null || "Total".equals(delta.getStrikePrice())) {
                continue;
            }

            try {
                int strike = Integer.parseInt(delta.getStrikePrice());
                double callOI = delta.getCallOI() != null ? delta.getCallOI() : 0;
                double putOI = delta.getPutOI() != null ? delta.getPutOI() : 0;

                // Calculate approximate gamma using OI and distance from current price
                // Gamma is highest at ATM and decreases as we move away
                double distanceFromATM = Math.abs(strike - currentPrice);
                double gammaMultiplier = Math.max(0.1, 1.0 - (distanceFromATM / (NIFTY_STRIKE_GAP * 10)));

                // GEX = Gamma * OI * 100 * Spot^2 * 0.01 (simplified approximation)
                // For market maker: Call GEX is positive, Put GEX is negative
                double callGamma = callOI * gammaMultiplier * currentPrice * 0.0001;
                double putGamma = putOI * gammaMultiplier * currentPrice * 0.0001;

                // Market makers are typically short calls (so they buy to hedge on up move)
                // and short puts (so they sell to hedge on down move)
                totalCallGEX += callGamma;  // Positive (buy on up)
                totalPutGEX -= putGamma;    // Negative (sell on down)

                // Track strike with maximum gamma
                double strikeGamma = callGamma + Math.abs(putGamma);
                if (strikeGamma > maxGamma) {
                    maxGamma = strikeGamma;
                    maxGammaStrike = strike;
                }

            } catch (NumberFormatException e) {
                // Skip non-numeric strike prices
            }
        }

        analysis.callGEX = totalCallGEX;
        analysis.putGEX = totalPutGEX;
        analysis.netGEX = totalCallGEX + totalPutGEX;
        analysis.gexRatio = totalPutGEX != 0 ? Math.abs(totalCallGEX / totalPutGEX) : 1.0;
        analysis.highGammaStrike = maxGammaStrike;
        analysis.isPositiveGEX = analysis.netGEX > 0;

        // Find GEX flip level (where net GEX changes sign)
        // This is typically near ATM when market is balanced
        analysis.gexFlipLevel = atmAnalysis != null ? atmAnalysis.atmStrike : (int) currentPrice;

        // Determine GEX bias
        // Positive GEX: Market makers will sell rallies and buy dips (mean reversion)
        // Negative GEX: Market makers will buy rallies and sell dips (momentum/trending)
        if (analysis.isPositiveGEX) {
            // High positive GEX suggests range-bound, mean-reverting market
            if (analysis.netGEX > 1000000) {
                analysis.gexBias = TREND_NEUTRAL; // Strong mean reversion
            } else {
                // Moderate positive GEX - slight bias based on ratio
                analysis.gexBias = analysis.gexRatio > 1.2 ? TREND_BEARISH :
                                   analysis.gexRatio < 0.8 ? TREND_BULLISH : TREND_NEUTRAL;
            }
        } else {
            // Negative GEX suggests trending/volatile market
            // Direction depends on where price is relative to high gamma strike
            if (currentPrice > analysis.highGammaStrike) {
                analysis.gexBias = TREND_BULLISH; // Momentum up
            } else {
                analysis.gexBias = TREND_BEARISH; // Momentum down
            }
        }

        logger.debug("GEX Analysis - Net: {}, Call: {}, Put: {}, Ratio: {}, Positive: {}, Bias: {}",
                analysis.netGEX, analysis.callGEX, analysis.putGEX,
                analysis.gexRatio, analysis.isPositiveGEX, analysis.gexBias);
    }

    /**
     * Analyze Theta decay across the option chain
     * Theta is time decay - premium lost per day
     */
    private void analyzeTheta(GreeksAnalysis analysis, List<MiniDelta> miniDeltaList, ATMStrikeAnalysis atmAnalysis) {
        // Calculate days to expiry
        LocalDate today = LocalDate.now();
        LocalTime now = LocalTime.now();

        // In Indian market, weekly expiry is on Tuesday
        // Calculate days to next Tuesday expiry
        int dayOfWeek = today.getDayOfWeek().getValue(); // Monday=1, Tuesday=2, Wednesday=3, ..., Sunday=7

        int daysUntilTuesday;
        if (dayOfWeek == 2) { // Today is Tuesday
            // If market is still open, it's expiry today (0 days)
            // If market has closed, next expiry is next Tuesday (7 days)
            daysUntilTuesday = now.isBefore(MARKET_CLOSE) ? 0 : 7;
        } else {
            // Calculate days to next Tuesday: (target_day - current_day + 7) % 7
            daysUntilTuesday = (2 - dayOfWeek + 7) % 7;
            // Edge case: if result is 0 (shouldn't happen since we handled Tuesday above), set to 7
            if (daysUntilTuesday == 0) {
                daysUntilTuesday = 7;
            }
        }

        analysis.daysToExpiry = daysUntilTuesday;
        analysis.isExpiryDay = daysUntilTuesday == 0;
        analysis.isNearExpiry = daysUntilTuesday <= 2;

        // Estimate theta based on premium and time to expiry
        // Theta accelerates as expiry approaches (gamma-theta relationship)
        double atmPremium = 0;
        if (atmAnalysis != null) {
            atmPremium = (atmAnalysis.atmCELTP + atmAnalysis.atmPELTP) / 2;
        }

        // Approximate theta calculation
        // ATM options lose approximately: Premium / sqrt(DaysToExpiry) per day
        if (analysis.daysToExpiry > 0 && atmPremium > 0) {
            analysis.atmTheta = atmPremium / Math.sqrt(analysis.daysToExpiry);
            analysis.thetaDecayRate = analysis.atmTheta / 6.25; // Per hour (6.25 trading hours)
        } else if (analysis.isExpiryDay) {
            // On expiry day, theta is extremely high
            analysis.atmTheta = atmPremium * 0.5; // Loses significant value
            analysis.thetaDecayRate = analysis.atmTheta / 6.25;
        }

        // Calculate total theta across chain (approximation)
        double totalCallTheta = 0;
        double totalPutTheta = 0;

        if (miniDeltaList != null) {
            for (MiniDelta delta : miniDeltaList) {
                if (delta.getStrikePrice() == null || "Total".equals(delta.getStrikePrice())) {
                    continue;
                }

                double callOI = delta.getCallOI() != null ? delta.getCallOI() : 0;
                double putOI = delta.getPutOI() != null ? delta.getPutOI() : 0;

                // Approximate theta contribution based on OI
                double thetaFactor = analysis.daysToExpiry > 0 ?
                        1.0 / Math.sqrt(analysis.daysToExpiry) : 2.0;

                totalCallTheta += callOI * thetaFactor * 0.01;
                totalPutTheta += putOI * thetaFactor * 0.01;
            }
        }

        analysis.callTheta = totalCallTheta;
        analysis.putTheta = totalPutTheta;
        analysis.totalTheta = totalCallTheta + totalPutTheta;

        // Determine theta bias
        // Near expiry, option sellers benefit, buyers suffer
        // High theta favors option sellers (typically bullish bias as fear decreases)
        if (analysis.isExpiryDay) {
            // On expiry day, market tends to move towards max pain
            analysis.thetaBias = TREND_NEUTRAL; // Mean reversion to max pain
        } else if (analysis.isNearExpiry) {
            // Near expiry, premium decay accelerates
            // If call theta > put theta, more call premium to decay (bearish pressure eases)
            analysis.thetaBias = totalCallTheta > totalPutTheta ? TREND_BULLISH : TREND_BEARISH;
        } else {
            analysis.thetaBias = TREND_NEUTRAL;
        }

        logger.debug("Theta Analysis - DTE: {}, ExpiryDay: {}, ATM Theta: {}, Decay/Hr: {}, Bias: {}",
                analysis.daysToExpiry, analysis.isExpiryDay, analysis.atmTheta,
                analysis.thetaDecayRate, analysis.thetaBias);
    }

    /**
     * Determine overall bias from all Greeks analysis
     */
    private void determineOverallGreeksBias(GreeksAnalysis analysis) {
        int bullishSignals = 0;
        int bearishSignals = 0;
        double confidenceBoost = 0;

        // VIX Analysis
        // Low VIX = complacency = bullish (can continue up)
        // High VIX = fear = can be contrarian bullish (mean reversion)
        // VIX spike = immediate bearish
        if (analysis.isVixSpike) {
            bearishSignals += 2; // Strong bearish signal
        } else if ("FALLING".equals(analysis.vixTrend)) {
            bullishSignals++;
            confidenceBoost += 5;
        } else if ("RISING".equals(analysis.vixTrend)) {
            bearishSignals++;
        }

        if (analysis.isLowVix) {
            bullishSignals++;
        } else if (analysis.isHighVix) {
            // High VIX can mean fear selling is done
            // Contrarian bullish if not spiking
            if (!analysis.isVixSpike) {
                bullishSignals++;
            }
        }

        // GEX Analysis
        if (TREND_BULLISH.equals(analysis.gexBias)) {
            bullishSignals++;
        } else if (TREND_BEARISH.equals(analysis.gexBias)) {
            bearishSignals++;
        }

        // Positive GEX adds confidence to neutral/mean reversion
        if (analysis.isPositiveGEX) {
            confidenceBoost += 10;
        } else {
            // Negative GEX increases volatility expectation
            confidenceBoost -= 5;
        }

        // Theta Analysis
        if (TREND_BULLISH.equals(analysis.thetaBias)) {
            bullishSignals++;
        } else if (TREND_BEARISH.equals(analysis.thetaBias)) {
            bearishSignals++;
        }

        // Near expiry increases confidence in mean reversion
        if (analysis.isExpiryDay || analysis.isNearExpiry) {
            confidenceBoost += 5;
        }

        // Determine overall bias
        if (bullishSignals >= bearishSignals + 2) {
            analysis.overallGreeksBias = TREND_BULLISH;
        } else if (bearishSignals >= bullishSignals + 2) {
            analysis.overallGreeksBias = TREND_BEARISH;
        } else {
            analysis.overallGreeksBias = TREND_NEUTRAL;
        }

        // Calculate confidence
        analysis.greeksConfidence = Math.max(20, Math.min(90, 50 + confidenceBoost));

        logger.debug("Overall Greeks Bias: {}, Confidence: {}",
                analysis.overallGreeksBias, analysis.greeksConfidence);
    }

    /**
     * Analyze option chain data for market bias (enhanced with ATM, day levels, and Greeks)
     */
    private OptionChainAnalysis analyzeOptionChain(List<MiniDelta> miniDeltaList, IndexLTP indexLTP,
                                                    ATMStrikeAnalysis atmAnalysis, DayHighLowLevels dayLevels,
                                                    GreeksAnalysis greeksAnalysis) {
        OptionChainAnalysis analysis = new OptionChainAnalysis();

        if (miniDeltaList == null || miniDeltaList.isEmpty()) {
            analysis.bias = TREND_NEUTRAL;
            return analysis;
        }

        // Calculate aggregate PCR
        double totalCallOI = 0;
        double totalPutOI = 0;
        double totalCallOIChange = 0;
        double totalPutOIChange = 0;

        for (MiniDelta delta : miniDeltaList) {
            if (delta.getCallOI() != null) totalCallOI += delta.getCallOI();
            if (delta.getPutOI() != null) totalPutOI += delta.getPutOI();
            if (delta.getCallOIChange() != null) totalCallOIChange += delta.getCallOIChange();
            if (delta.getPutOIChange() != null) totalPutOIChange += delta.getPutOIChange();
        }

        analysis.pcr = totalCallOI > 0 ? totalPutOI / totalCallOI : 1.0;
        analysis.callOIChange = totalCallOIChange;
        analysis.putOIChange = totalPutOIChange;

        // Get max pain from IndexLTP if available
        if (indexLTP != null) {
            analysis.maxPainStrike = indexLTP.getMaxPainSP();
            analysis.support = parseDoubleSafe(indexLTP.getSupport());
            analysis.resistance = parseDoubleSafe(indexLTP.getResistance());
            analysis.currentLTP = indexLTP.getIndexLTP() != null ? indexLTP.getIndexLTP().doubleValue() : null;

            // Add day high/low from IndexLTP
            Double dayHigh = parseDoubleSafe(indexLTP.getDayHigh());
            Double dayLow = parseDoubleSafe(indexLTP.getDayLow());
            analysis.currentDayHigh = dayHigh != null ? dayHigh : dayLevels.currentDayHigh;
            analysis.currentDayLow = dayLow != null ? dayLow : dayLevels.currentDayLow;
        }

        // Store analysis references
        analysis.atmAnalysis = atmAnalysis;
        analysis.dayLevels = dayLevels;
        analysis.greeksAnalysis = greeksAnalysis;

        // Determine bias based on option chain (enhanced with Greeks)
        analysis.bias = determineOptionChainBias(analysis, atmAnalysis, dayLevels, greeksAnalysis);

        return analysis;
    }

    /**
     * Determine market bias from option chain analysis (enhanced with ATM, day levels, and Greeks)
     */
    private String determineOptionChainBias(OptionChainAnalysis analysis, ATMStrikeAnalysis atmAnalysis,
                                            DayHighLowLevels dayLevels, GreeksAnalysis greeksAnalysis) {
        int bullishSignals = 0;
        int bearishSignals = 0;

        // PCR Analysis
        // PCR > 1.2 typically bullish (more puts being sold/bought as hedge)
        // PCR < 0.8 typically bearish
        if (analysis.pcr > 1.2) {
            bullishSignals++;
        } else if (analysis.pcr < 0.8) {
            bearishSignals++;
        }

        // OI Change Analysis
        // Put OI increasing with Call OI decreasing = Bullish
        // Call OI increasing with Put OI decreasing = Bearish
        if (analysis.putOIChange > 0 && analysis.callOIChange < 0) {
            bullishSignals++;
        } else if (analysis.callOIChange > 0 && analysis.putOIChange < 0) {
            bearishSignals++;
        }

        // Max Pain Analysis
        // If price below max pain, tendency to move up
        // If price above max pain, tendency to move down
        if (analysis.maxPainStrike != null && analysis.currentLTP != null) {
            double distanceFromMaxPain = analysis.currentLTP - analysis.maxPainStrike;
            if (distanceFromMaxPain < -30) {
                bullishSignals++; // Price below max pain, likely to rise
            } else if (distanceFromMaxPain > 30) {
                bearishSignals++; // Price above max pain, likely to fall
            }
        }

        // Support/Resistance Analysis
        if (analysis.support != null && analysis.resistance != null && analysis.currentLTP != null) {
            double range = analysis.resistance - analysis.support;
            double positionInRange = (analysis.currentLTP - analysis.support) / range;

            if (positionInRange < 0.3) {
                bullishSignals++; // Near support, likely to bounce
            } else if (positionInRange > 0.7) {
                bearishSignals++; // Near resistance, likely to face rejection
            }
        }

        // ATM Strike Analysis Integration
        if (atmAnalysis != null) {
            if (TREND_BULLISH.equals(atmAnalysis.atmBias)) {
                bullishSignals += 2; // ATM analysis carries more weight
            } else if (TREND_BEARISH.equals(atmAnalysis.atmBias)) {
                bearishSignals += 2;
            }

            // CE-PE difference indicates market expectation
            if (atmAnalysis.cePeDifference > 10) {
                bullishSignals++;
            } else if (atmAnalysis.cePeDifference < -10) {
                bearishSignals++;
            }
        }

        // Day Level Analysis Integration
        if (dayLevels != null) {
            if (TREND_BULLISH.equals(dayLevels.dayLevelBias)) {
                bullishSignals++;
            } else if (TREND_BEARISH.equals(dayLevels.dayLevelBias)) {
                bearishSignals++;
            }

            // Previous day breakout signals
            if (dayLevels.isAbovePreviousDayHigh) {
                bullishSignals += 2; // Strong bullish signal
            } else if (dayLevels.isBelowPreviousDayLow) {
                bearishSignals += 2; // Strong bearish signal
            }

            // Pivot point analysis
            if (analysis.currentLTP != null && dayLevels.pivotPoint > 0) {
                if (analysis.currentLTP > dayLevels.r1) {
                    bullishSignals++; // Above R1, bullish momentum
                } else if (analysis.currentLTP < dayLevels.s1) {
                    bearishSignals++; // Below S1, bearish momentum
                }
            }
        }

        // Greeks Analysis Integration (VIX, GEX, Theta)
        if (greeksAnalysis != null) {
            // VIX Analysis
            if (greeksAnalysis.isVixSpike) {
                bearishSignals += 2; // VIX spike is strongly bearish
            } else if (greeksAnalysis.isLowVix && "FALLING".equals(greeksAnalysis.vixTrend)) {
                bullishSignals++; // Low and falling VIX is bullish
            } else if (greeksAnalysis.isHighVix && "RISING".equals(greeksAnalysis.vixTrend)) {
                bearishSignals++; // High and rising VIX is bearish
            }

            // GEX Analysis
            if (TREND_BULLISH.equals(greeksAnalysis.gexBias)) {
                bullishSignals++;
            } else if (TREND_BEARISH.equals(greeksAnalysis.gexBias)) {
                bearishSignals++;
            }

            // Negative GEX environment - price can trend more easily; amplify the emerging bias
            if (!greeksAnalysis.isPositiveGEX) {
                // Derive preliminary bias from votes accumulated so far (analysis.bias not set yet)
                String prelimBias = bullishSignals > bearishSignals ? TREND_BULLISH
                        : bearishSignals > bullishSignals ? TREND_BEARISH
                        : TREND_NEUTRAL;
                if (TREND_BULLISH.equals(prelimBias)) {
                    bullishSignals++;
                } else if (TREND_BEARISH.equals(prelimBias)) {
                    bearishSignals++;
                }
            }

            // Theta Analysis - especially important near expiry
            if (TREND_BULLISH.equals(greeksAnalysis.thetaBias)) {
                bullishSignals++;
            } else if (TREND_BEARISH.equals(greeksAnalysis.thetaBias)) {
                bearishSignals++;
            }

            // On expiry day, max pain pull is stronger
            if (greeksAnalysis.isExpiryDay && analysis.maxPainStrike != null && analysis.currentLTP != null) {
                double distToMaxPain = analysis.currentLTP - analysis.maxPainStrike;
                if (distToMaxPain < -20) {
                    bullishSignals += 2; // Strong pull up to max pain
                } else if (distToMaxPain > 20) {
                    bearishSignals += 2; // Strong pull down to max pain
                }
            }

            // Overall Greeks bias
            if (TREND_BULLISH.equals(greeksAnalysis.overallGreeksBias)) {
                bullishSignals++;
            } else if (TREND_BEARISH.equals(greeksAnalysis.overallGreeksBias)) {
                bearishSignals++;
            }
        }

        // Determine overall bias with weighted scoring
        if (bullishSignals >= 6) {
            return TREND_BULLISH;
        } else if (bearishSignals >= 6) {
            return TREND_BEARISH;
        } else if (bullishSignals > bearishSignals + 2) {
            return TREND_BULLISH;
        } else if (bearishSignals > bullishSignals + 2) {
            return TREND_BEARISH;
        }

        return TREND_NEUTRAL;
    }

    /**
     * Determine overall market trend combining technical, option chain, day levels, ATM, and Greeks analysis
     */
    private String determineMarketTrend(TechnicalIndicators indicators, OptionChainAnalysis ocAnalysis,
                                        DayHighLowLevels dayLevels, ATMStrikeAnalysis atmAnalysis,
                                        GreeksAnalysis greeksAnalysis, double currentPrice) {
        int bullishScore = 0;
        int bearishScore = 0;

        // Technical Analysis Signals
        // EMA Crossover
        if ("BULLISH_CROSSOVER".equals(indicators.emaCrossover)) {
            bullishScore += 2;
        } else if ("BEARISH_CROSSOVER".equals(indicators.emaCrossover)) {
            bearishScore += 2;
        }

        // RSI Analysis
        if (indicators.rsi < 30) {
            bullishScore++; // Oversold
        } else if (indicators.rsi > 70) {
            bearishScore++; // Overbought
        } else if (indicators.rsi > 50 && indicators.rsi < 70) {
            bullishScore++; // Bullish momentum
        } else if (indicators.rsi < 50 && indicators.rsi > 30) {
            bearishScore++; // Bearish momentum
        }

        // Momentum
        if (indicators.momentum > 0.1) {
            bullishScore++;
        } else if (indicators.momentum < -0.1) {
            bearishScore++;
        }

        // Option Chain Bias
        if (TREND_BULLISH.equals(ocAnalysis.bias)) {
            bullishScore += 2;
        } else if (TREND_BEARISH.equals(ocAnalysis.bias)) {
            bearishScore += 2;
        }

        // Day Level Analysis
        if (dayLevels != null) {
            // Previous day high/low breakout (strong signals)
            if (dayLevels.isAbovePreviousDayHigh) {
                bullishScore += 3; // Very strong bullish signal
            } else if (dayLevels.isBelowPreviousDayLow) {
                bearishScore += 3; // Very strong bearish signal
            }

            // Current day range analysis
            if (dayLevels.currentDayHigh > 0 && dayLevels.currentDayLow > 0) {
                double dayRange = dayLevels.currentDayHigh - dayLevels.currentDayLow;
                double positionInDayRange = (currentPrice - dayLevels.currentDayLow) / Math.max(dayRange, 1);

                // If price near day high, momentum is bullish
                if (positionInDayRange > 0.8) {
                    bullishScore++;
                } else if (positionInDayRange < 0.2) {
                    bearishScore++;
                }
            }

            // Pivot point analysis
            if (dayLevels.pivotPoint > 0) {
                if (currentPrice > dayLevels.pivotPoint) {
                    bullishScore++;
                } else {
                    bearishScore++;
                }

                // R1/S1 levels
                if (currentPrice > dayLevels.r1) {
                    bullishScore++;
                } else if (currentPrice < dayLevels.s1) {
                    bearishScore++;
                }
            }
        }

        // ATM Strike Analysis
        if (atmAnalysis != null) {
            if (TREND_BULLISH.equals(atmAnalysis.atmBias)) {
                bullishScore += 2;
            } else if (TREND_BEARISH.equals(atmAnalysis.atmBias)) {
                bearishScore += 2;
            }

            // Synthetic future indicates market expectation
            if (atmAnalysis.syntheticFuture > currentPrice + 15) {
                bullishScore++;
            } else if (atmAnalysis.syntheticFuture < currentPrice - 15) {
                bearishScore++;
            }
        }

        // Greeks Analysis (VIX, GEX, Theta)
        if (greeksAnalysis != null) {
            // VIX-based trend adjustment
            if (greeksAnalysis.isVixSpike) {
                bearishScore += 3; // VIX spike strongly bearish
            } else if ("FALLING".equals(greeksAnalysis.vixTrend) && greeksAnalysis.isLowVix) {
                bullishScore += 2; // Falling low VIX is bullish
            } else if ("RISING".equals(greeksAnalysis.vixTrend) && greeksAnalysis.isHighVix) {
                bearishScore += 2; // Rising high VIX is bearish
            }

            // GEX-based trend adjustment
            if (TREND_BULLISH.equals(greeksAnalysis.gexBias)) {
                bullishScore += 2;
            } else if (TREND_BEARISH.equals(greeksAnalysis.gexBias)) {
                bearishScore += 2;
            }

            // In positive GEX environment, mean reversion is more likely
            // In negative GEX environment, trends persist
            if (!greeksAnalysis.isPositiveGEX) {
                // Negative GEX amplifies existing trend
                if (bullishScore > bearishScore) {
                    bullishScore++;
                } else if (bearishScore > bullishScore) {
                    bearishScore++;
                }
            }

            // Theta impact - near expiry behavior
            if (greeksAnalysis.isExpiryDay) {
                // On expiry day, price tends to gravitate towards max pain
                if (ocAnalysis.maxPainStrike != null) {
                    if (currentPrice < ocAnalysis.maxPainStrike - 20) {
                        bullishScore += 2;
                    } else if (currentPrice > ocAnalysis.maxPainStrike + 20) {
                        bearishScore += 2;
                    }
                }
            }

            // Overall Greeks bias
            if (TREND_BULLISH.equals(greeksAnalysis.overallGreeksBias)) {
                bullishScore++;
            } else if (TREND_BEARISH.equals(greeksAnalysis.overallGreeksBias)) {
                bearishScore++;
            }
        }

        // Determine trend with enhanced scoring
        if (bullishScore >= bearishScore + 4) {
            return TREND_BULLISH;
        } else if (bearishScore >= bullishScore + 4) {
            return TREND_BEARISH;
        }

        return TREND_NEUTRAL;
    }

    /**
     * Generate 5 predicted candles based on all analysis (enhanced with day levels, ATM, Greeks, SMC, and Tech Levels)
     */
    private List<PredictedCandleStick> generatePredictions(List<CandleStick> historicalCandles,
                                                           TechnicalIndicators indicators,
                                                           OptionChainAnalysis ocAnalysis,
                                                           DayHighLowLevels dayLevels,
                                                           ATMStrikeAnalysis atmAnalysis,
                                                           GreeksAnalysis greeksAnalysis,
                                                           SmartMoneyAnalysis smcAnalysis,
                                                           TechnicalLevelsAnalysis techLevels,
                                                           String marketTrend,
                                                           Double expectedVolatility,
                                                           Tick latestTick,
                                                           Double liveNiftyPrice) {
        List<PredictedCandleStick> predictions = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now().withSecond(0).withNano(0);

        // CRITICAL: Use live price as the primary source for current price
        // This ensures predictions are anchored to the actual current market price
        if (historicalCandles.isEmpty()) {
            logger.error("No historical candles available for prediction base price");
            return Collections.emptyList();
        }
        CandleStick lastCandle = historicalCandles.getLast();
        double lastCandleClose = lastCandle.getClosePrice() != null ? lastCandle.getClosePrice() : 0;
        double liveTickPrice = latestTick != null ? latestTick.getLastTradedPrice() : 0;
        double livePrice = liveNiftyPrice != null && liveNiftyPrice > 0 ? liveNiftyPrice : 0;

        // Priority: Live tick price > liveNiftyPrice > Last candle close > Fallback value
        double currentPrice;
        if (liveTickPrice > 0) {
            currentPrice = liveTickPrice;
            logger.info("Prediction base price from LIVE TICK: {}", currentPrice);
        } else if (livePrice > 0) {
            currentPrice = livePrice;
            logger.info("Prediction base price from LIVE NIFTY PRICE: {}", currentPrice);
        } else if (lastCandleClose > 0) {
            currentPrice = lastCandleClose;
            logger.info("Prediction base price from LAST CANDLE CLOSE: {}", currentPrice);
        } else {
            currentPrice = 26220; // Updated fallback to current approximate Nifty level
            logger.warn("Prediction base price using FALLBACK: {}", currentPrice);
        }

        // Always log all price sources for debugging
        logger.info("Price sources - LiveTick: {}, LiveNiftyPrice: {}, LastCandleClose: {}, Using: {}",
                liveTickPrice, livePrice, lastCandleClose, currentPrice);

        // Log if there's a significant gap between live tick and last candle
        if ((liveTickPrice > 0 || livePrice > 0) && lastCandleClose > 0) {
            double liveVal = liveTickPrice > 0 ? liveTickPrice : livePrice;
            double priceGap = Math.abs(liveVal - lastCandleClose);
            if (priceGap > 10) {
                logger.info("Price gap detected: Live={}, Last candle close={}, Gap={} points",
                        liveVal, lastCandleClose, priceGap);
            }
        }


        // Base move per candle: expectedVolatility is the ATR of the 5-minute candles being predicted.
        // It is already a per-candle measure, so use it directly (no division).
        double baseMovePerCandle = expectedVolatility;
        double trendBias = getTrendBias(marketTrend);

        // Adjust trend bias based on SMC analysis
        if (smcAnalysis != null && smcAnalysis.smcConfidence >= 60) {
            if (TREND_BULLISH.equals(smcAnalysis.smcBias)) {
                trendBias = Math.max(trendBias, 0.4);
                if (smcAnalysis.smcConfidence >= 75) {
                    trendBias = Math.max(trendBias, 0.7);
                }
            } else if (TREND_BEARISH.equals(smcAnalysis.smcBias)) {
                trendBias = Math.min(trendBias, -0.4);
                if (smcAnalysis.smcConfidence >= 75) {
                    trendBias = Math.min(trendBias, -0.7);
                }
            }
        }

        // Adjust trend bias based on Greeks
        if (greeksAnalysis != null) {
            // VIX spike increases bearish bias
            if (greeksAnalysis.isVixSpike) {
                trendBias = Math.min(trendBias - 0.3, -0.4);
            }

            // Positive GEX reduces trend bias (mean reversion)
            if (greeksAnalysis.isPositiveGEX && greeksAnalysis.netGEX > 500000) {
                trendBias *= 0.7; // Dampen trend in high positive GEX
            }

            // Negative GEX amplifies trend bias
            if (!greeksAnalysis.isPositiveGEX) {
                trendBias *= 1.2; // Amplify trend in negative GEX
            }
        }

        // Adjust trend bias based on Technical Levels (Channel and Fibonacci)
        if (techLevels != null) {
            // Channel pattern influence
            if (techLevels.channel != null && techLevels.channel.channelStrength >= 50) {
                if (techLevels.channel.isNearLowerBoundary && !techLevels.channel.isPriceBreakingDown) {
                    trendBias = Math.max(trendBias, 0.3); // Expect bounce from channel support
                } else if (techLevels.channel.isNearUpperBoundary && !techLevels.channel.isPriceBreakingUp) {
                    trendBias = Math.min(trendBias, -0.3); // Expect rejection from channel resistance
                } else if (techLevels.channel.isPriceBreakingUp) {
                    trendBias = Math.max(trendBias, 0.6); // Breakout bullish
                } else if (techLevels.channel.isPriceBreakingDown) {
                    trendBias = Math.min(trendBias, -0.6); // Breakdown bearish
                }
            }

            // Fibonacci level influence
            if (techLevels.fibonacci != null && techLevels.fibonacci.isAtKeyFibLevel) {
                if (TREND_BULLISH.equals(techLevels.fibonacci.fibBias)) {
                    trendBias = Math.max(trendBias, 0.4);
                } else if (TREND_BEARISH.equals(techLevels.fibonacci.fibBias)) {
                    trendBias = Math.min(trendBias, -0.4);
                }
            }
        }

        // Support and resistance levels (enhanced with day levels, SMC, and tech levels)
        double support = ocAnalysis.support != null ? ocAnalysis.support : indicators.swingLow;
        double resistance = ocAnalysis.resistance != null ? ocAnalysis.resistance : indicators.swingHigh;
        Integer maxPain = ocAnalysis.maxPainStrike;

        // Enhance support/resistance with SMC order blocks
        if (smcAnalysis != null) {
            if (smcAnalysis.nearestBullishOB > 0 && smcAnalysis.nearestBullishOB > support) {
                support = smcAnalysis.nearestBullishOB; // OB is stronger support
            }
            if (smcAnalysis.nearestBearishOB > 0 && smcAnalysis.nearestBearishOB < resistance) {
                resistance = smcAnalysis.nearestBearishOB; // OB is stronger resistance
            }
        }

        // Enhance with tech levels (channel boundaries and Fib levels)
        if (techLevels != null) {
            if (techLevels.strongestSupport > 0 && techLevels.strongestSupport > support) {
                support = techLevels.strongestSupport;
            }
            if (techLevels.strongestResistance > 0 && techLevels.strongestResistance < resistance) {
                resistance = techLevels.strongestResistance;
            }
        }

        // Incorporate previous day levels as additional support/resistance
        double pdh = dayLevels != null ? dayLevels.previousDayHigh : 0;
        double pdl = dayLevels != null ? dayLevels.previousDayLow : 0;
        double pivotPoint = dayLevels != null ? dayLevels.pivotPoint : 0;

        // ATM derived levels
        double syntheticFuture = atmAnalysis != null ? atmAnalysis.syntheticFuture : currentPrice;

        // High gamma strike acts as magnet
        double highGammaStrike = greeksAnalysis != null ? greeksAnalysis.highGammaStrike : 0;

        // SMC-derived levels
        double nearestFVGTarget = smcAnalysis != null ? smcAnalysis.nearestFVGTarget : 0;

        // Channel and Fibonacci derived levels
        double channelUpper = techLevels != null && techLevels.channel != null ? techLevels.channel.upperChannelLine : 0;
        double channelLower = techLevels != null && techLevels.channel != null ? techLevels.channel.lowerChannelLine : 0;
        double fib382 = techLevels != null && techLevels.fibonacci != null ? techLevels.fibonacci.level_382 : 0;
        double fib618 = techLevels != null && techLevels.fibonacci != null ? techLevels.fibonacci.level_618 : 0;

        for (int i = 1; i <= 5; i++) {
            PredictedCandleStick prediction = new PredictedCandleStick();
            prediction.setInstrumentToken(NIFTY_INSTRUMENT_TOKEN);
            prediction.setPredictionGeneratedAt(now);
            prediction.setPredictionSequence(i);
            prediction.setTrendDirection(marketTrend);
            prediction.setPredictionBasis(BASIS_COMBINED);
            prediction.setVerified(false);
            prediction.setPredictedVolatility(expectedVolatility);
            prediction.setSupportLevel(support);
            prediction.setResistanceLevel(resistance);
            prediction.setMaxPainStrike(maxPain);
            prediction.setPcrAtPrediction(ocAnalysis.pcr);

            // Calculate candle times — predictions are 5-minute candles
            LocalDateTime candleStart = now.plusMinutes((long) i * 5);
            // Skip predictions that fall at or after market close (15:30)
            LocalDateTime marketClose = candleStart.toLocalDate().atTime(15, 30);
            if (!candleStart.isBefore(marketClose)) {
                break;
            }
            prediction.setCandleStartTime(candleStart);
            prediction.setCandleEndTime(candleStart.plusMinutes(5).minusSeconds(1));

            // Generate predicted OHLC
            double open = currentPrice;
            prediction.setOpenPrice(round(open));

            // Calculate movement with randomness and trend bias
            double moveAmount = baseMovePerCandle * (0.5 + Math.random()); // Randomize movement

            // Apply support/resistance constraints
            double predictedMove = trendBias * moveAmount;

            // Enhanced constraint using previous day levels
            if (pdh > 0 && open + predictedMove > pdh && !dayLevels.isAbovePreviousDayHigh) {
                // Previous day high acts as resistance if not already broken
                predictedMove = Math.min(predictedMove, (pdh - open) * 0.7);
            }
            if (pdl > 0 && open + predictedMove < pdl && !dayLevels.isBelowPreviousDayLow) {
                // Previous day low acts as support if not already broken
                predictedMove = Math.max(predictedMove, (pdl - open) * 0.7);
            }

            // Pivot point constraint
            if (pivotPoint > 0) {
                if (open < pivotPoint && open + predictedMove > pivotPoint) {
                    // Pivot acts as resistance when approaching from below
                    predictedMove *= 0.8;
                } else if (open > pivotPoint && open + predictedMove < pivotPoint) {
                    // Pivot acts as support when approaching from above
                    predictedMove *= 0.8;
                }
            }

            // Synthetic future pull - price tends to move toward synthetic future
            if (syntheticFuture > 0) {
                double pullToSynthetic = (syntheticFuture - open) * 0.03; // 3% pull
                predictedMove += pullToSynthetic;
            }

            // Check if approaching resistance (limit upward movement)
            if (open + predictedMove > resistance && trendBias > 0) {
                predictedMove = Math.min(predictedMove, (resistance - open) * 0.5);
            }

            // Check if approaching support (limit downward movement)
            if (open + predictedMove < support && trendBias < 0) {
                predictedMove = Math.max(predictedMove, (support - open) * 0.5);
            }

            // Max pain gravitational pull
            if (maxPain != null) {
                double distanceFromMaxPain = open - maxPain;
                double maxPainPull = -distanceFromMaxPain * 0.02; // 2% pull toward max pain

                // Stronger pull on expiry day
                if (greeksAnalysis != null && greeksAnalysis.isExpiryDay) {
                    maxPainPull *= 2.0; // Double the pull on expiry
                } else if (greeksAnalysis != null && greeksAnalysis.isNearExpiry) {
                    maxPainPull *= 1.5;
                }

                predictedMove += maxPainPull;
            }

            // High gamma strike acts as magnet in positive GEX environment
            if (highGammaStrike > 0 && greeksAnalysis != null && greeksAnalysis.isPositiveGEX) {
                double distanceFromGammaStrike = open - highGammaStrike;
                double gammaPull = -distanceFromGammaStrike * 0.015; // 1.5% pull toward high gamma strike
                predictedMove += gammaPull;
            }

            // SMC: Fair Value Gap filling tendency
            // Price tends to return to fill FVGs - this is a strong edge in price action trading
            if (nearestFVGTarget > 0) {
                double distanceToFVG = nearestFVGTarget - open;
                double fvgPull = distanceToFVG * 0.04; // 4% pull toward FVG (stronger than other pulls)

                // FVGs get filled more aggressively in ranging markets
                if (TREND_NEUTRAL.equals(marketTrend)) {
                    fvgPull *= 1.5;
                }

                // Limit FVG pull if it conflicts strongly with trend
                if ((distanceToFVG > 0 && trendBias < -0.4) || (distanceToFVG < 0 && trendBias > 0.4)) {
                    fvgPull *= 0.5; // Reduce pull when against strong trend
                }

                predictedMove += fvgPull;
            }

            // SMC: Order Block influence
            // Price tends to respect order blocks as strong support/resistance
            if (smcAnalysis != null) {
                // If price is approaching a bullish OB from above, expect bounce
                if (smcAnalysis.nearestBullishOB > 0) {
                    double distanceToOB = open - smcAnalysis.nearestBullishOB;
                    if (distanceToOB > 0 && distanceToOB < 30 && predictedMove < 0) {
                        // About to enter bullish OB zone - reduce downward movement
                        predictedMove *= 0.5;
                        // Add bounce effect
                        if (distanceToOB < 15) {
                            predictedMove = Math.max(predictedMove, baseMovePerCandle * 0.3);
                        }
                    }
                }

                // If price is approaching a bearish OB from below, expect rejection
                if (smcAnalysis.nearestBearishOB > 0) {
                    double distanceToOB = smcAnalysis.nearestBearishOB - open;
                    if (distanceToOB > 0 && distanceToOB < 30 && predictedMove > 0) {
                        // About to enter bearish OB zone - reduce upward movement
                        predictedMove *= 0.5;
                        // Add rejection effect
                        if (distanceToOB < 15) {
                            predictedMove = Math.min(predictedMove, -baseMovePerCandle * 0.3);
                        }
                    }
                }

                // If in an order block, expect strong reaction
                if (smcAnalysis.isInOrderBlock) {
                    if (TREND_BULLISH.equals(smcAnalysis.smcBias)) {
                        predictedMove = Math.max(predictedMove, baseMovePerCandle * 0.5);
                    } else if (TREND_BEARISH.equals(smcAnalysis.smcBias)) {
                        predictedMove = Math.min(predictedMove, -baseMovePerCandle * 0.5);
                    }
                }
            }

            // SMC: Price Action pattern influence
            if (smcAnalysis != null && smcAnalysis.priceAction != null) {
                PriceActionAnalysis pa = smcAnalysis.priceAction;

                // Strong bullish patterns increase upward momentum
                if (pa.hasThreeWhiteSoldiers || pa.hasBullishEngulfing) {
                    if (predictedMove > 0) {
                        predictedMove *= 1.3;
                    } else {
                        predictedMove *= 0.7; // Resist downward move
                    }
                }

                // Strong bearish patterns increase downward momentum
                if (pa.hasThreeBlackCrows || pa.hasBearishEngulfing) {
                    if (predictedMove < 0) {
                        predictedMove *= 1.3;
                    } else {
                        predictedMove *= 0.7; // Resist upward move
                    }
                }

                // Momentum continuation
                if (pa.consecutiveBullishCandles >= 4 && !pa.isMomentumDecreasing) {
                    predictedMove = Math.max(predictedMove, baseMovePerCandle * 0.4);
                }
                if (pa.consecutiveBearishCandles >= 4 && !pa.isMomentumDecreasing) {
                    predictedMove = Math.min(predictedMove, -baseMovePerCandle * 0.4);
                }
            }

            // Channel pattern constraints
            if (channelUpper > 0 && channelLower > 0) {
                // If approaching channel upper boundary, limit upward movement
                if (open + predictedMove > channelUpper - 5 && predictedMove > 0) {
                    // Expect rejection or breakout
                    if (techLevels != null && techLevels.channel != null && !techLevels.channel.isPriceBreakingUp) {
                        predictedMove = Math.min(predictedMove, (channelUpper - open) * 0.6);
                    }
                }
                // If approaching channel lower boundary, limit downward movement
                if (open + predictedMove < channelLower + 5 && predictedMove < 0) {
                    // Expect bounce or breakdown
                    if (techLevels != null && techLevels.channel != null && !techLevels.channel.isPriceBreakingDown) {
                        predictedMove = Math.max(predictedMove, (channelLower - open) * 0.6);
                    }
                }
            }

            // Fibonacci level constraints - price often reacts at Fib levels
            if (fib382 > 0 && fib618 > 0) {
                // Check if moving toward 38.2% level
                if ((open < fib382 && open + predictedMove > fib382 - 3) ||
                    (open > fib382 && open + predictedMove < fib382 + 3)) {
                    // Dampen move as price approaches 38.2% Fib
                    predictedMove *= 0.8;
                }
                // Check if moving toward 61.8% level (golden ratio - stronger)
                if ((open < fib618 && open + predictedMove > fib618 - 3) ||
                    (open > fib618 && open + predictedMove < fib618 + 3)) {
                    // Stronger dampening at 61.8% Fib
                    predictedMove *= 0.7;
                }
            }

            double close = open + predictedMove;

            // Calculate high and low based on volatility
            double candleVolatility = baseMovePerCandle * (0.3 + Math.random() * 0.4);
            double high, low;

            if (close > open) {
                // Bullish candle
                high = Math.max(open, close) + candleVolatility * 0.3;
                low = open - candleVolatility * 0.2;
            } else {
                // Bearish candle
                high = open + candleVolatility * 0.2;
                low = Math.min(open, close) - candleVolatility * 0.3;
            }

            prediction.setHighPrice(round(high));
            prediction.setLowPrice(round(low));
            prediction.setClosePrice(round(close));

            // Calculate confidence score (including SMC)
            double confidence = calculateConfidence(indicators, ocAnalysis, smcAnalysis, i);
            prediction.setConfidenceScore(confidence);

            predictions.add(prediction);

            // Update current price for next candle
            currentPrice = close;
        }

        return predictions;
    }

    /**
     * Get trend bias multiplier
     */
    private double getTrendBias(String trend) {
        return switch (trend) {
            case TREND_BULLISH -> 0.6;
            case TREND_BEARISH -> -0.6;
            default -> 0.0;
        };
    }

    /**
     * Calculate confidence score for a prediction (enhanced with SMC)
     */
    private double calculateConfidence(TechnicalIndicators indicators, OptionChainAnalysis ocAnalysis,
                                        SmartMoneyAnalysis smcAnalysis, int sequence) {
        double baseConfidence = 60;

        // Higher confidence for first predictions, lower for later ones
        double sequencePenalty = (sequence - 1) * 5;
        baseConfidence -= sequencePenalty;

        // Boost confidence if EMA alignment is strong
        if ("BULLISH_CROSSOVER".equals(indicators.emaCrossover) || "BEARISH_CROSSOVER".equals(indicators.emaCrossover)) {
            baseConfidence += 10;
        }

        // Boost if RSI is in extreme zones (clearer signals)
        if (indicators.rsi < 25 || indicators.rsi > 75) {
            baseConfidence += 8;
        }

        // Boost if PCR is in extreme zones
        if (ocAnalysis.pcr > 1.5 || ocAnalysis.pcr < 0.6) {
            baseConfidence += 7;
        }

        // Penalty for conflicting signals
        if (indicators.momentum > 0 && "BEARISH_CROSSOVER".equals(indicators.emaCrossover)) {
            baseConfidence -= 10;
        }
        if (indicators.momentum < 0 && "BULLISH_CROSSOVER".equals(indicators.emaCrossover)) {
            baseConfidence -= 10;
        }

        // SMC-based confidence adjustments
        if (smcAnalysis != null) {
            // Boost for high SMC confidence
            if (smcAnalysis.smcConfidence >= 75) {
                baseConfidence += 12;
            } else if (smcAnalysis.smcConfidence >= 60) {
                baseConfidence += 6;
            }

            // Boost when in order block with matching bias
            if (smcAnalysis.isInOrderBlock) {
                baseConfidence += 8;
            }

            // Boost for nearby FVG targets
            if (smcAnalysis.nearestFVGTarget > 0) {
                baseConfidence += 5;
            }

            // Boost for strong price action patterns
            if (smcAnalysis.priceAction != null) {
                PriceActionAnalysis pa = smcAnalysis.priceAction;
                if (pa.patternStrength >= 75) {
                    baseConfidence += 10;
                } else if (pa.patternStrength >= 50) {
                    baseConfidence += 5;
                }

                // Strong market structure
                if (pa.isTrendingUp || pa.isTrendingDown) {
                    baseConfidence += 5;
                }

                // Penalty for consolidation (less predictable)
                if (pa.isConsolidating) {
                    baseConfidence -= 8;
                }
            }

            // Penalty if SMC and technical bias conflict
            if (smcAnalysis.smcBias.equals(TREND_BULLISH) && indicators.momentum < -5) {
                baseConfidence -= 10;
            }
            if (smcAnalysis.smcBias.equals(TREND_BEARISH) && indicators.momentum > 5) {
                baseConfidence -= 10;
            }

            // Boost for valid trade setup with good RR ratio
            if (smcAnalysis.tradeSetup != null && smcAnalysis.tradeSetup.isValid) {
                if (smcAnalysis.tradeSetup.riskRewardRatio1 >= 2.0) {
                    baseConfidence += 10;
                } else if (smcAnalysis.tradeSetup.riskRewardRatio1 >= 1.5) {
                    baseConfidence += 5;
                }

                // Boost for high confidence setups
                if (smcAnalysis.tradeSetup.confidence >= 75) {
                    baseConfidence += 8;
                }
            }
        }

        return Math.max(20, Math.min(95, baseConfidence));
    }

    /**
     * Find actual candle for a given time for verification
     */
    private CandleStick findActualCandle(LocalDateTime candleStartTime) {
        List<CandleStick> candles = candleStickRepository.findCandlesInRange(
                NIFTY_INSTRUMENT_TOKEN, candleStartTime, candleStartTime.plusMinutes(1));
        return candles.isEmpty() ? null : candles.getFirst();
    }

    // ===================== Trade Setup Persistence =====================

    /**
     * Save trade setup to database
     */
    private TradeSetupEntity saveTradeSetup(TradeSetup tradeSetup, double currentPrice, String marketTrend,
                                             SmartMoneyAnalysis smcAnalysis, TechnicalLevelsAnalysis techLevels,
                                             OptionChainAnalysis ocAnalysis) {
        try {
            TradeSetupEntity entity = new TradeSetupEntity();

            // Basic trade info
            entity.setInstrumentToken(NIFTY_INSTRUMENT_TOKEN);
            entity.setTradeDirection(tradeSetup.tradeDirection);
            entity.setSetupType(tradeSetup.setupType);
            entity.setConfidence(tradeSetup.confidence);

            // Entry details
            entity.setEntryPrice(tradeSetup.entryPrice);
            entity.setEntryType(tradeSetup.entryType);
            entity.setEntryReason(tradeSetup.entryReason);

            // Target details
            entity.setTarget1(tradeSetup.target1);
            entity.setTarget2(tradeSetup.target2);
            entity.setTarget3(tradeSetup.target3);
            entity.setTargetReason(tradeSetup.targetReason);

            // Stop-loss details
            entity.setStopLoss(tradeSetup.stopLoss);
            entity.setTrailingStopDistance(tradeSetup.trailingStopDistance);
            entity.setStopLossReason(tradeSetup.stopLossReason);

            // Risk-Reward analysis
            entity.setRiskPoints(tradeSetup.riskPoints);
            entity.setRewardPoints1(tradeSetup.rewardPoints1);
            entity.setRiskRewardRatio1(tradeSetup.riskRewardRatio1);
            entity.setRiskRewardRatio2(tradeSetup.riskRewardRatio2);

            // Option trading specific
            entity.setSuggestedOptionType(tradeSetup.suggestedOptionType);
            entity.setSuggestedStrike(tradeSetup.suggestedStrike);
            entity.setOptionStrategy(tradeSetup.optionStrategy);

            // Market context
            entity.setCurrentPrice(currentPrice);
            entity.setMarketTrend(marketTrend);
            entity.setSmcBias(smcAnalysis != null ? smcAnalysis.smcBias : null);
            entity.setSmcConfidence(smcAnalysis != null ? smcAnalysis.smcConfidence : null);
            entity.setChannelType(techLevels != null && techLevels.channel != null ? techLevels.channel.channelType : null);
            entity.setNearestFibLevel(techLevels != null && techLevels.fibonacci != null ? techLevels.fibonacci.nearestFibLevel : null);
            entity.setPcrValue(ocAnalysis != null ? ocAnalysis.pcr : null);
            entity.setMaxPainStrike(ocAnalysis != null ? ocAnalysis.maxPainStrike : null);

            // Validity
            entity.setCreatedAt(LocalDateTime.now());
            entity.setValidUntil(tradeSetup.validUntil);
            entity.setIsValid(tradeSetup.isValid);
            entity.setInvalidReason(tradeSetup.invalidReason);

            // Initial tracking values
            entity.setIsExecuted(false);
            entity.setIsClosed(false);

            return tradeSetupRepository.save(entity);
        } catch (Exception e) {
            logger.error("Error saving trade setup: {}", e.getMessage(), e);
            return null;
        }
    }

    @Override
    public Optional<TradeSetupEntity> getLatestTradeSetup() {
        // First check cache (thread-safe access)
        TradeSetupEntity cachedSetup = latestTradeSetupCache.get();
        if (cachedSetup != null &&
            cachedSetup.getValidUntil() != null &&
            cachedSetup.getValidUntil().isAfter(LocalDateTime.now())) {
            return Optional.of(cachedSetup);
        }

        // Fetch from database
        Optional<TradeSetupEntity> latestSetup = tradeSetupRepository.findLatestValidSetup(
                NIFTY_INSTRUMENT_TOKEN, LocalDateTime.now());

        latestSetup.ifPresent(setup -> latestTradeSetupCache.set(setup));  // Thread-safe cache update

        // If no valid setup, return the most recent one (even if expired)
        if (latestSetup.isEmpty()) {
            return tradeSetupRepository.findLatestSetup(NIFTY_INSTRUMENT_TOKEN);
        }

        return latestSetup;
    }

    @Override
    public List<TradeSetupEntity> getTodayTradeSetups() {
        return tradeSetupRepository.findTodaySetups(NIFTY_INSTRUMENT_TOKEN);
    }

    @Override
    public Map<String, Object> getTradeSetupPerformance() {
        Map<String, Object> performance = new HashMap<>();

        LocalDateTime startOfWeek = LocalDateTime.now().minusDays(7);

        // Today's stats
        List<TradeSetupEntity> todaySetups = getTodayTradeSetups();
        performance.put("todaySetupsCount", todaySetups.size());
        performance.put("todayValidSetups", todaySetups.stream().filter(TradeSetupEntity::getIsValid).count());

        // Win rate (last 7 days)
        Double winRate = tradeSetupRepository.getWinRate(NIFTY_INSTRUMENT_TOKEN, startOfWeek);
        performance.put("weeklyWinRate", winRate != null ? winRate : 0.0);

        // Total P/L (last 7 days)
        Double totalPL = tradeSetupRepository.getTotalProfitLoss(NIFTY_INSTRUMENT_TOKEN, startOfWeek);
        performance.put("weeklyProfitLoss", totalPL != null ? totalPL : 0.0);

        // Closed setups count
        List<TradeSetupEntity> closedSetups = tradeSetupRepository.findClosedSetups(NIFTY_INSTRUMENT_TOKEN, startOfWeek);
        performance.put("weeklyClosedSetups", closedSetups.size());

        // Average RR ratio
        double avgRR = todaySetups.stream()
                .filter(s -> s.getRiskRewardRatio1() != null && s.getRiskRewardRatio1() > 0)
                .mapToDouble(TradeSetupEntity::getRiskRewardRatio1)
                .average()
                .orElse(0.0);
        performance.put("averageRiskReward", avgRR);

        return performance;
    }

    // ===================== Automated Prediction Job Methods =====================

    /**
     * Check if current time is within market hours (9:15 AM to 3:30 PM IST)
     * and it's a trading day (not weekend)
     */
    private boolean isWithinMarketHours() {
        LocalTime now = LocalTime.now();
        LocalDate today = LocalDate.now();
        DayOfWeek dayOfWeek = today.getDayOfWeek();

        // Skip weekends
        if (dayOfWeek == DayOfWeek.SATURDAY || dayOfWeek == DayOfWeek.SUNDAY) {
            return false;
        }

        // Check market hours
        return !now.isBefore(MARKET_OPEN) && !now.isAfter(MARKET_CLOSE);
    }

    /**
     * Check if ticker is connected and live data is available
     */
    private boolean isTickerReady() {
        return isTickerConnected.get() && kiteTickerProvider != null;
    }

    @Override
    public void executePredictionJob() {
        // Check market hours first
        if (!isWithinMarketHours()) {
            logger.debug("Prediction job skipped - outside market hours or weekend");
            return;
        }

        // Check if ticker is connected
        if (!isTickerReady()) {
            logger.debug("Prediction job skipped - ticker not connected. Attempting to connect...");
            if (!startKiteTicker()) {
                logger.warn("Failed to connect ticker, prediction job cannot run");
                return;
            }
        }

        if (!predictionJobActive.get()) {
            logger.debug("Prediction job is not active, skipping execution");
            return;
        }

        try {
            logger.info("Executing prediction job at {}", LocalDateTime.now());


            // Generate predictions using default job config (1 for NIFTY)
            LocalDateTime startTime = LocalDateTime.now().minusHours(2);
            LocalDateTime endTime = LocalDateTime.now().withNano(0).minusMinutes(1);

            List<PredictedCandleStick> predictions = predictNextFiveCandles(1, startTime, endTime);

            if (!predictions.isEmpty()) {
                lastPredictionTime = LocalDateTime.now();
                totalPredictionJobRuns.incrementAndGet();
                logger.info("Prediction job completed successfully. Generated {} predictions", predictions.size());
            } else {
                logger.warn("Prediction job completed but no predictions were generated");
            }

        } catch (Exception e) {
            logger.error("Error during prediction job execution: {}", e.getMessage(), e);
        }
    }

    @Override
    public PredictionDeviation verifyAndCalculateDeviation() {
        return verifyAndCalculateDeviation(false);
    }

    /**
     * Verify and calculate deviation with optional force flag.
     * When force=true, skips ticker connection check and uses wider time window.
     */
    public PredictionDeviation verifyAndCalculateDeviation(boolean forceCalculation) {
        LocalTime now = LocalTime.now();
        LocalDate today = LocalDate.now();
        DayOfWeek dayOfWeek = today.getDayOfWeek();

        // Skip weekends (unless force calculation is requested)
        if (!forceCalculation && (dayOfWeek == DayOfWeek.SATURDAY || dayOfWeek == DayOfWeek.SUNDAY)) {
            logger.debug("Verification skipped - weekend");
            return null;
        }

        // Only run during and after market hours for verification (unless force)
        if (!forceCalculation && now.isBefore(MARKET_OPEN)) {
            logger.debug("Verification skipped - before market hours");
            return null;
        }

        // Check if ticker is connected (skip check when force calculation is requested)
        if (!forceCalculation && !isTickerReady()) {
            logger.debug("Verification skipped - ticker not connected");
            return null;
        }

        try {
            logger.info("Starting verification and deviation calculation at {} (force={})", LocalDateTime.now(), forceCalculation);

            // First, verify past predictions
            int verifiedCount = verifyPastPredictions();
            logger.info("Verified {} predictions", verifiedCount);

            // Calculate deviation statistics - use wider time window for force calculation
            int lookbackMinutes = forceCalculation ? 120 : 15; // 2 hours for force, 15 min otherwise
            LocalDateTime verificationPeriodStart = LocalDateTime.now().minusMinutes(lookbackMinutes);
            LocalDateTime verificationPeriodEnd = LocalDateTime.now();

            List<PredictedCandleStick> recentVerified = getRecentlyVerifiedPredictions(
                    verificationPeriodStart, verificationPeriodEnd);

            // If no recent verified predictions, try to get from today's session
            if (recentVerified.isEmpty() && forceCalculation) {
                logger.info("No verified predictions in last {} minutes, trying today's session", lookbackMinutes);
                LocalDateTime todayStart = LocalDateTime.of(today, MARKET_OPEN);
                recentVerified = getRecentlyVerifiedPredictions(todayStart, verificationPeriodEnd);
            }

            if (recentVerified.isEmpty()) {
                logger.info("No verified predictions found for deviation calculation");
                return null;
            }

            // Create deviation record
            PredictionDeviation deviation = calculateDeviationStatistics(recentVerified);

            if (deviation != null) {
                // Add market context
                enrichDeviationWithMarketContext(deviation);

                // Calculate correction factors
                calculateCorrectionFactors(deviation);

                // Save to database
                predictionDeviationRepository.save(deviation);

                lastVerificationTime = LocalDateTime.now();
                totalVerificationRuns.incrementAndGet();

                logger.info("Deviation statistics saved - Batch: {}, Avg Deviation: {}%, Direction Accuracy: {}%",
                        deviation.getBatchId(),
                        deviation.getAvgCloseDeviationPercent(),
                        deviation.getDirectionAccuracyPercent());
            }

            return deviation;

        } catch (Exception e) {
            logger.error("Error during verification and deviation calculation: {}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * Get recently verified predictions for deviation calculation
     */
    private List<PredictedCandleStick> getRecentlyVerifiedPredictions(
            LocalDateTime startTime, LocalDateTime endTime) {

        List<PredictedCandleStick> allRecent = predictedCandleRepository.findPredictionsInTimeRange(
                NIFTY_INSTRUMENT_TOKEN, startTime.minusMinutes(30), endTime);

        return allRecent.stream()
                .filter(p -> p.getVerified() != null && p.getVerified())
                .filter(p -> p.getActualClosePrice() != null)
                .filter(p -> p.getPredictionAccuracy() != null)
                .toList();
    }

    /**
     * Calculate comprehensive deviation statistics from verified predictions
     */
    private PredictionDeviation calculateDeviationStatistics(List<PredictedCandleStick> verifiedPredictions) {
        if (verifiedPredictions == null || verifiedPredictions.isEmpty()) {
            return null;
        }

        PredictionDeviation deviation = new PredictionDeviation();
        deviation.setInstrumentToken(NIFTY_INSTRUMENT_TOKEN);
        deviation.setVerificationTime(LocalDateTime.now());
        deviation.setTradingDate(LocalDate.now());
        deviation.setBatchId(LocalDateTime.now().format(BATCH_ID_FORMATTER));
        deviation.setPredictionsVerified(verifiedPredictions.size());

        // Calculate close price deviations
        List<Double> closeDeviations = new ArrayList<>();
        List<Double> closeDeviationsPercent = new ArrayList<>();
        double totalBias = 0;

        int bullishPredictions = 0, bearishPredictions = 0, neutralPredictions = 0;
        int correctBullish = 0, correctBearish = 0;

        // Sequence-wise deviation tracking
        Map<Integer, List<Double>> sequenceDeviations = new HashMap<>();
        for (int i = 1; i <= 5; i++) {
            sequenceDeviations.put(i, new ArrayList<>());
        }

        // Volatility tracking
        double totalPredictedVolatility = 0;
        double totalActualVolatility = 0;
        int volatilityCount = 0;

        for (PredictedCandleStick pred : verifiedPredictions) {
            double predictedClose = pred.getClosePrice();
            double actualClose = pred.getActualClosePrice();

            // Absolute deviation
            double absDeviation = Math.abs(predictedClose - actualClose);
            closeDeviations.add(absDeviation);

            // Percentage deviation (relative to actual price)
            double percentDeviation = (absDeviation / actualClose) * 100;
            closeDeviationsPercent.add(percentDeviation);

            // Bias calculation (positive = over-predicted, negative = under-predicted)
            double bias = predictedClose - actualClose;
            totalBias += bias;

            // Direction accuracy
            String trendDir = pred.getTrendDirection();
            if (TREND_BULLISH.equals(trendDir)) {
                bullishPredictions++;
                boolean actualUp = actualClose > pred.getOpenPrice();
                if (actualUp) correctBullish++;
            } else if (TREND_BEARISH.equals(trendDir)) {
                bearishPredictions++;
                boolean actualDown = actualClose < pred.getOpenPrice();
                if (actualDown) correctBearish++;
            } else {
                neutralPredictions++;
            }

            // Sequence-wise tracking
            Integer seq = pred.getPredictionSequence();
            if (seq != null && seq >= 1 && seq <= 5) {
                sequenceDeviations.get(seq).add(absDeviation);
            }

            // Volatility tracking — use actual close range from high/low stored on the prediction
            // (avoids an extra DB query per prediction for CandleStick lookup)
            if (pred.getPredictedVolatility() != null && pred.getPredictedVolatility() > 0) {
                totalPredictedVolatility += pred.getPredictedVolatility();

                if (pred.getHighPrice() != null && pred.getLowPrice() != null) {
                    double actualVol = Math.abs(pred.getHighPrice() - pred.getLowPrice());
                    totalActualVolatility += actualVol;
                    volatilityCount++;
                }
            }
        }

        // Set close deviation statistics
        deviation.setAvgCloseDeviation(closeDeviations.stream().mapToDouble(d -> d).average().orElse(0));
        deviation.setAvgCloseDeviationPercent(closeDeviationsPercent.stream().mapToDouble(d -> d).average().orElse(0));
        deviation.setMaxCloseDeviation(closeDeviations.stream().mapToDouble(d -> d).max().orElse(0));
        deviation.setMinCloseDeviation(closeDeviations.stream().mapToDouble(d -> d).min().orElse(0));

        // Calculate standard deviation
        double mean = deviation.getAvgCloseDeviation();
        double variance = closeDeviations.stream()
                .mapToDouble(d -> Math.pow(d - mean, 2))
                .average()
                .orElse(0);
        deviation.setCloseDeviationStd(Math.sqrt(variance));

        // Direction accuracy
        int totalDirectional = bullishPredictions + bearishPredictions;
        int correctDirectional = correctBullish + correctBearish;
        deviation.setDirectionAccuracyPercent(
                totalDirectional > 0 ? (correctDirectional * 100.0 / totalDirectional) : 0);
        deviation.setBullishPredictions(bullishPredictions);
        deviation.setBearishPredictions(bearishPredictions);
        deviation.setNeutralPredictions(neutralPredictions);
        deviation.setCorrectBullish(correctBullish);
        deviation.setCorrectBearish(correctBearish);

        // Systematic bias
        double systematicBias = totalBias / verifiedPredictions.size();
        deviation.setSystematicBias(systematicBias);
        if (systematicBias > 5) {
            deviation.setBiasDirection("OVER_PREDICT");
        } else if (systematicBias < -5) {
            deviation.setBiasDirection("UNDER_PREDICT");
        } else {
            deviation.setBiasDirection("NEUTRAL");
        }

        // Volatility ratio
        if (volatilityCount > 0) {
            deviation.setPredictedAvgVolatility(totalPredictedVolatility / volatilityCount);
            deviation.setActualAvgVolatility(totalActualVolatility / volatilityCount);
            if (totalPredictedVolatility > 0) {
                deviation.setVolatilityUnderestimateRatio(totalActualVolatility / totalPredictedVolatility);
            }
        }

        // Sequence-wise deviations
        deviation.setSeq1AvgDeviation(calculateAverage(sequenceDeviations.get(1)));
        deviation.setSeq2AvgDeviation(calculateAverage(sequenceDeviations.get(2)));
        deviation.setSeq3AvgDeviation(calculateAverage(sequenceDeviations.get(3)));
        deviation.setSeq4AvgDeviation(calculateAverage(sequenceDeviations.get(4)));
        deviation.setSeq5AvgDeviation(calculateAverage(sequenceDeviations.get(5)));

        return deviation;
    }

    /**
     * Safely parse a String to Double; returns null on null input or parse error.
     */
    private Double parseDoubleSafe(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            logger.warn("Could not parse double value: '{}'", value);
            return null;
        }
    }

    /**
     * Calculate average from a list of doubles
     */
    private Double calculateAverage(List<Double> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        return values.stream().mapToDouble(d -> d).average().orElse(0);
    }

    /**
     * Enrich deviation record with market context
     */
    private void enrichDeviationWithMarketContext(PredictionDeviation deviation) {
        LocalTime now = LocalTime.now();
        LocalDate today = LocalDate.now();

        // Market hour (1-7 for each hour of trading)
        int marketHour = (int) ((now.toSecondOfDay() - MARKET_OPEN.toSecondOfDay()) / 3600) + 1;
        deviation.setMarketHour(Math.max(1, Math.min(7, marketHour)));

        deviation.setIsOpeningHour(now.isBefore(MARKET_OPEN.plusHours(1)));
        deviation.setIsClosingHour(now.isAfter(MARKET_CLOSE.minusHours(1)));

        // Day of week
        deviation.setDayOfWeek(today.getDayOfWeek().getValue());

        // Check if expiry day (Tuesday for weekly expiry)
        deviation.setIsExpiryDay(today.getDayOfWeek() == DayOfWeek.TUESDAY);

        // Get PCR and VIX from latest IndexLTP
        try {
            List<IndexLTP> indexLTPList = indexLTPRepository.findLast5000IndexDataByAppJobConfigNum(1);
            if (!indexLTPList.isEmpty()) {
                IndexLTP latest = indexLTPList.getFirst();
                deviation.setAvgPcr(latest.getMeanStrikePCR());

                // Try to get VIX value
                if (kiteTickerProvider != null && kiteTickerProvider.tickerMapForJob != null) {
                    Tick vixTick = kiteTickerProvider.tickerMapForJob.get(INDIA_VIX_INSTRUMENT_TOKEN);
                    if (vixTick != null) {
                        deviation.setAvgVix(vixTick.getLastTradedPrice());
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Could not fetch market context: {}", e.getMessage());
        }

        // Determine market trend from recent predictions
        if (!latestPredictions.isEmpty()) {
            String trend = latestPredictions.getFirst().getTrendDirection();
            deviation.setMarketTrend(trend);
        }
    }

    /**
     * Calculate correction factors based on deviation history
     */
    private void calculateCorrectionFactors(PredictionDeviation deviation) {
        try {
            // Get recent deviations for analysis
            LocalDateTime startTime = LocalDateTime.now().minusDays(7);
            List<PredictionDeviation> recentDeviations = predictionDeviationRepository
                    .findRecentDeviations(NIFTY_INSTRUMENT_TOKEN, startTime);

            if (recentDeviations.isEmpty()) {
                // First deviation record - no correction needed
                deviation.setCumulativeSessions(1);
                deviation.setCumulativeAvgDeviation(deviation.getAvgCloseDeviation());
                deviation.setCumulativeAccuracy(deviation.getDirectionAccuracyPercent());
                deviation.setSuggestedCloseCorrection(0.0);
                deviation.setSuggestedVolatilityCorrection(1.0);
                return;
            }

            // Calculate cumulative statistics
            int totalSessions = recentDeviations.size() + 1;
            double totalDeviation = recentDeviations.stream()
                    .filter(d -> d.getAvgCloseDeviation() != null)
                    .mapToDouble(PredictionDeviation::getAvgCloseDeviation)
                    .sum() + deviation.getAvgCloseDeviation();
            double totalAccuracy = recentDeviations.stream()
                    .filter(d -> d.getDirectionAccuracyPercent() != null)
                    .mapToDouble(PredictionDeviation::getDirectionAccuracyPercent)
                    .sum() + deviation.getDirectionAccuracyPercent();

            deviation.setCumulativeSessions(totalSessions);
            deviation.setCumulativeAvgDeviation(totalDeviation / totalSessions);
            deviation.setCumulativeAccuracy(totalAccuracy / totalSessions);

            // Calculate suggested corrections
            // If we're consistently over/under predicting, suggest a correction
            Double avgBias = predictionDeviationRepository.getAverageSystematicBias(
                    NIFTY_INSTRUMENT_TOKEN, startTime);
            if (avgBias != null) {
                // Suggest correction opposite to the bias (if bias is +10, correct by -10)
                deviation.setSuggestedCloseCorrection(-avgBias);
            }

            // Volatility correction based on underestimate ratio
            Double avgVolRatio = predictionDeviationRepository.getAverageVolatilityRatio(
                    NIFTY_INSTRUMENT_TOKEN, startTime);
            if (avgVolRatio != null && avgVolRatio > 0) {
                deviation.setSuggestedVolatilityCorrection(avgVolRatio);
            } else {
                deviation.setSuggestedVolatilityCorrection(1.0);
            }

        } catch (Exception e) {
            logger.warn("Error calculating correction factors: {}", e.getMessage());
            deviation.setSuggestedCloseCorrection(0.0);
            deviation.setSuggestedVolatilityCorrection(1.0);
        }
    }

    @Override
    public Optional<PredictionDeviation> getLatestDeviation() {
        return predictionDeviationRepository.findLatestDeviation(NIFTY_INSTRUMENT_TOKEN);
    }

    @Override
    public List<PredictionDeviation> getTodayDeviations() {
        return predictionDeviationRepository.findByTradingDateOrderByVerificationTimeAsc(LocalDate.now());
    }

    @Override
    public Map<String, Double> getCorrectionFactors() {
        Map<String, Double> corrections = new HashMap<>();

        try {
            LocalDateTime startTime = LocalDateTime.now().minusDays(7);

            // Get average systematic bias for close price correction
            Double avgBias = predictionDeviationRepository.getAverageSystematicBias(
                    NIFTY_INSTRUMENT_TOKEN, startTime);
            corrections.put("closeCorrection", avgBias != null ? -avgBias : 0.0);

            // Get average volatility underestimate ratio
            Double volatilityRatio = predictionDeviationRepository.getAverageVolatilityRatio(
                    NIFTY_INSTRUMENT_TOKEN, startTime);
            corrections.put("volatilityMultiplier", volatilityRatio != null ? volatilityRatio : 1.0);

            // Get time-based correction for current market hour
            LocalTime now = LocalTime.now();
            int currentHour = (int) ((now.toSecondOfDay() - MARKET_OPEN.toSecondOfDay()) / 3600) + 1;
            currentHour = Math.max(1, Math.min(7, currentHour));

            List<PredictionDeviation> hourDeviations = predictionDeviationRepository.findByMarketHour(
                    NIFTY_INSTRUMENT_TOKEN, currentHour, startTime);

            if (!hourDeviations.isEmpty()) {
                double hourlyBias = hourDeviations.stream()
                        .filter(d -> d.getSystematicBias() != null)
                        .mapToDouble(PredictionDeviation::getSystematicBias)
                        .average()
                        .orElse(0);
                corrections.put("hourlyCorrection", -hourlyBias);
            } else {
                corrections.put("hourlyCorrection", 0.0);
            }

            // Get expiry day correction if applicable
            if (LocalDate.now().getDayOfWeek() == DayOfWeek.TUESDAY) {
                List<PredictionDeviation> expiryDeviations = predictionDeviationRepository
                        .findExpiryDayDeviations(NIFTY_INSTRUMENT_TOKEN, startTime);

                if (!expiryDeviations.isEmpty()) {
                    double expiryBias = expiryDeviations.stream()
                            .filter(d -> d.getSystematicBias() != null)
                            .mapToDouble(PredictionDeviation::getSystematicBias)
                            .average()
                            .orElse(0);
                    corrections.put("expiryDayCorrection", -expiryBias);
                }
            }

            // Get sequence-wise correction (for 5th candle which typically has highest deviation)
            Object[] seqDeviations = predictionDeviationRepository.getSequenceWiseDeviations(
                    NIFTY_INSTRUMENT_TOKEN, startTime);

            if (seqDeviations != null && seqDeviations.length > 0 && seqDeviations[0] != null) {
                // The result is a single row with 5 values
                Object[] row = (Object[]) seqDeviations[0];
                for (int i = 0; i < 5 && i < row.length; i++) {
                    if (row[i] != null) {
                        corrections.put("seq" + (i + 1) + "Correction",
                                ((Number) row[i]).doubleValue() * 0.5); // Apply 50% of deviation as correction
                    }
                }
            }

            // Get direction accuracy for confidence adjustment
            Double directionAccuracy = predictionDeviationRepository.getAverageDirectionAccuracy(
                    NIFTY_INSTRUMENT_TOKEN, startTime);
            corrections.put("directionAccuracy", directionAccuracy != null ? directionAccuracy : 50.0);

        } catch (Exception e) {
            logger.error("Error fetching correction factors: {}", e.getMessage(), e);
            corrections.put("closeCorrection", 0.0);
            corrections.put("volatilityMultiplier", 1.0);
            corrections.put("hourlyCorrection", 0.0);
        }

        return corrections;
    }

    @Override
    public boolean isPredictionJobActive() {
        return predictionJobActive.get();
    }

    @Override
    public void startPredictionJob() {
        // Check if within market hours before starting
        if (!isWithinMarketHours()) {
            logger.warn("Cannot start prediction job - outside market hours or weekend");
            return;
        }

        // Ensure ticker is connected before starting the job
        if (!isTickerReady()) {
            logger.info("Starting ticker connection for prediction job...");
            if (!startKiteTicker()) {
                logger.error("Failed to start ticker connection. Prediction job will not be fully functional.");
            }
        }

        predictionJobActive.set(true);
        logger.info("Prediction job started - Ticker connected: {}", isTickerConnected.get());
    }

    @Override
    public void stopPredictionJob() {
        predictionJobActive.set(false);
        logger.info("Prediction job stopped");
    }

    @Override
    public Map<String, Object> getPredictionJobStats() {
        Map<String, Object> stats = new HashMap<>();

        stats.put("isActive", predictionJobActive.get());
        stats.put("isTickerConnected", isTickerConnected.get());
        stats.put("isWithinMarketHours", isWithinMarketHours());
        stats.put("totalPredictionRuns", totalPredictionJobRuns.get());
        stats.put("totalVerificationRuns", totalVerificationRuns.get());
        stats.put("lastPredictionTime", lastPredictionTime);
        stats.put("lastVerificationTime", lastVerificationTime);

        // Today's deviations summary
        List<PredictionDeviation> todayDeviations = getTodayDeviations();
        stats.put("todayVerificationCount", todayDeviations.size());

        if (!todayDeviations.isEmpty()) {
            double avgDeviation = todayDeviations.stream()
                    .filter(d -> d.getAvgCloseDeviationPercent() != null)
                    .mapToDouble(PredictionDeviation::getAvgCloseDeviationPercent)
                    .average()
                    .orElse(0);
            stats.put("todayAvgDeviationPercent", avgDeviation);

            double avgAccuracy = todayDeviations.stream()
                    .filter(d -> d.getDirectionAccuracyPercent() != null)
                    .mapToDouble(PredictionDeviation::getDirectionAccuracyPercent)
                    .average()
                    .orElse(0);
            stats.put("todayDirectionAccuracyPercent", avgAccuracy);

            // Get latest deviation stats
            PredictionDeviation latest = todayDeviations.getLast();
            stats.put("latestBatchId", latest.getBatchId());
            stats.put("latestAvgDeviation", latest.getAvgCloseDeviation());
            stats.put("latestSystematicBias", latest.getSystematicBias());
            stats.put("latestBiasDirection", latest.getBiasDirection());
        }

        // Session accuracy
        Double sessionAccuracy = getSessionAccuracy();
        stats.put("sessionAccuracy", sessionAccuracy);

        // Correction factors
        Map<String, Double> corrections = getCorrectionFactors();
        stats.put("correctionFactors", corrections);

        return stats;
    }

    @Override
    public Map<String, Object> getLiveTickData() {
        Map<String, Object> tickData = buildDefaultTickData();
        populatePreviousDayData(tickData);
        try {
            // Snapshot the volatile field once. kiteTickerProvider can be set to null
            // by startKiteTicker() on a concurrent thread, so a null-check on the field
            // does not guarantee it stays non-null for subsequent reads in this method.
            KiteTickerProvider ticker = this.kiteTickerProvider;
            if (ticker == null) {
                logger.warn("KiteTickerProvider is null — attempting to start");
                startKiteTicker();
                ticker = this.kiteTickerProvider; // re-read after start
                if (ticker == null) {
                    tickData.put("error", "Ticker provider not initialized");
                    return tickData;
                }
            }
            int tickerMapSize = ticker.tickerMapForJob.size();
            tickData.put("tickerMapSize", tickerMapSize);
            tickData.put("tokenLTP", buildTokenLtpMap(ticker));
            double niftyLTP = populateNiftyAndVix(tickData, ticker);
            if (niftyLTP > 0) populateAtmOptions(tickData, niftyLTP, ticker);
            tickData.put("isLive", tickerMapSize > 0);
            tickData.put("timestamp", LocalDateTime.now().toString());
        } catch (Exception e) {
            logger.error("Error fetching live tick data: {}", e.getMessage(), e);
            tickData.put("error", e.getMessage());
        }
        return tickData;
    }

    private Map<String, Object> buildDefaultTickData() {
        Map<String, Object> tickData = new HashMap<>();
        tickData.put("niftyLTP", 0.0);
        tickData.put("niftyChange", 0.0);
        tickData.put("niftyChangePercent", 0.0);
        tickData.put("niftyOpen", 0.0);
        tickData.put("niftyHigh", 0.0);
        tickData.put("niftyLow", 0.0);
        tickData.put("previousDayHigh", 0.0);
        tickData.put("previousDayLow", 0.0);
        tickData.put("previousDayOpen", 0.0);
        tickData.put("previousDayClose", 0.0);
        tickData.put("vixValue", 0.0);
        tickData.put("atmStrike", 0);
        tickData.put("atmCELTP", 0.0);
        tickData.put("atmPELTP", 0.0);
        tickData.put("atmCEChange", 0.0);
        tickData.put("atmPEChange", 0.0);
        tickData.put("cePeDiff", 0.0);
        tickData.put("straddlePremium", 0.0);
        tickData.put("syntheticFuture", 0.0);
        tickData.put("sentiment", "NEUTRAL");
        tickData.put("timestamp", LocalDateTime.now().toString());
        tickData.put("isLive", false);
        tickData.put("tickerMapSize", 0);
        return tickData;
    }

    private void populatePreviousDayData(Map<String, Object> tickData) {
        try {
            com.trading.kalyani.KTManager.model.PreviousDayHighLowResponse prevDayData =
                instrumentService.getPreviousDayHighLow(String.valueOf(NIFTY_INSTRUMENT_TOKEN));
            if (prevDayData != null && prevDayData.isSuccess()) {
                tickData.put("previousDayHigh", prevDayData.getHigh());
                tickData.put("previousDayLow", prevDayData.getLow());
                tickData.put("previousDayOpen", prevDayData.getOpen());
                tickData.put("previousDayClose", prevDayData.getClose());
                logger.debug("Previous day NIFTY: High={}, Low={}", prevDayData.getHigh(), prevDayData.getLow());
            } else {
                logger.warn("Failed to fetch previous day data: {}",
                    prevDayData != null ? prevDayData.getMessage() : "null response");
            }
        } catch (Exception e) {
            logger.error("Error fetching previous day high/low: {}", e.getMessage());
        }
    }

    private Map<Long, Double> buildTokenLtpMap(KiteTickerProvider ticker) {
        Map<Long, Double> tokenLTP = new HashMap<>();
        for (Map.Entry<Long, com.zerodhatech.models.Tick> e : ticker.tickerMapForJob.entrySet()) {
            if (e.getValue() != null) {
                tokenLTP.put(e.getKey(), e.getValue().getLastTradedPrice());
            }
        }
        logger.debug("tokenLTP snapshot: {} entries", tokenLTP.size());
        return tokenLTP;
    }

    /** Populates NIFTY and VIX fields into tickData. Returns the resolved niftyLTP (0 if unavailable). */
    private double populateNiftyAndVix(Map<String, Object> tickData, KiteTickerProvider ticker) {
        double niftyLTP = 0.0;
        Tick niftyTick = ticker.tickerMapForJob.get(NIFTY_INSTRUMENT_TOKEN);
        if (niftyTick != null) {
            niftyLTP = niftyTick.getLastTradedPrice();
            double prevClose = niftyLTP - niftyTick.getChange();
            tickData.put("niftyLTP", niftyLTP);
            tickData.put("niftyChange", niftyTick.getChange());
            tickData.put("niftyChangePercent", prevClose > 0 ? (niftyTick.getChange() / prevClose) * 100 : 0.0);
            tickData.put("niftyOpen", niftyTick.getOpenPrice());
            tickData.put("niftyHigh", niftyTick.getHighPrice());
            tickData.put("niftyLow", niftyTick.getLowPrice());
            logger.debug("NIFTY LTP: {}", niftyLTP);
        } else if (ticker.niftyLastPrice != null && ticker.niftyLastPrice > 0) {
            niftyLTP = ticker.niftyLastPrice;
            tickData.put("niftyLTP", niftyLTP);
            logger.debug("NIFTY LTP from fallback field: {}", niftyLTP);
        }

        Tick vixTick = ticker.tickerMapForJob.get(INDIA_VIX_INSTRUMENT_TOKEN);
        if (vixTick != null) {
            tickData.put("vixValue", vixTick.getLastTradedPrice());
        } else if (ticker.vixLastPrice != null && ticker.vixLastPrice > 0) {
            tickData.put("vixValue", ticker.vixLastPrice);
        }

        return niftyLTP;
    }

    /** Resolves ATM CE/PE instruments, subscribes any unsubscribed tokens, and populates derived option fields. */
    private void populateAtmOptions(Map<String, Object> tickData, double niftyLTP,
                                     com.trading.kalyani.KTManager.utilities.KiteTickerProvider ticker) {
        int atmStrike = (int) (Math.round(niftyLTP / NIFTY_STRIKE_GAP) * NIFTY_STRIKE_GAP);
        tickData.put("atmStrike", atmStrike);
        logger.debug("ATM Strike: {} (NIFTY LTP: {})", atmStrike, niftyLTP);

        double atmCELTP = 0.0, atmPELTP = 0.0, atmCEChange = 0.0, atmPEChange = 0.0;
        Long atmCEToken = null, atmPEToken = null;
        String atmCESymbol = null, atmPESymbol = null;

        List<InstrumentEntity> options = instrumentRepository
                .findNearestExpiryInstrumentFromStrikePrice(String.valueOf(atmStrike));

        if (options == null || options.isEmpty()) {
            logger.warn("No NIFTY options found for ATM strike {} (NIFTY LTP={}). " +
                    "Instruments table may need refresh for current expiry.", atmStrike, niftyLTP);
        } else {
            for (InstrumentEntity inst : options) {
                if (inst.getInstrument() == null) continue;
                String strikeStr = inst.getInstrument().getStrike();
                if (strikeStr == null) continue;
                int instrumentStrike;
                try {
                    instrumentStrike = (int) Double.parseDouble(strikeStr);
                } catch (NumberFormatException e) {
                    continue;
                }
                if (instrumentStrike != atmStrike) continue;

                Long token = inst.getInstrument().getInstrument_token();
                String instType = inst.getInstrument().getInstrument_type();
                String symbol = inst.getInstrument().getTradingsymbol();

                Tick tick = ticker.tickerMapForJob.get(token);
                if (tick == null) {
                    // Subscribe — tick will arrive on next incoming WebSocket frame
                    ArrayList<Long> tokens = new ArrayList<>();
                    tokens.add(token);
                    ticker.subscribeTokenForJob(tokens);
                    logger.info("ATM option {} ({}) not yet subscribed — subscribed now, LTP available on next tick",
                            symbol, token);
                }

                double ltp = tick != null ? tick.getLastTradedPrice() : 0.0;
                double change = tick != null ? tick.getChange() : 0.0;

                if ("CE".equals(instType)) {
                    atmCELTP = ltp; atmCEChange = change; atmCEToken = token; atmCESymbol = symbol;
                } else if ("PE".equals(instType)) {
                    atmPELTP = ltp; atmPEChange = change; atmPEToken = token; atmPESymbol = symbol;
                }
                if (atmCEToken != null && atmPEToken != null) break;
            }
        }

        logger.debug("ATM CE: {} token={} LTP={} | PE: {} token={} LTP={}",
                atmCESymbol, atmCEToken, atmCELTP, atmPESymbol, atmPEToken, atmPELTP);

        tickData.put("atmCELTP", atmCELTP);
        tickData.put("atmPELTP", atmPELTP);
        tickData.put("atmCEChange", atmCEChange);
        tickData.put("atmPEChange", atmPEChange);
        tickData.put("atmCEToken", atmCEToken);
        tickData.put("atmPEToken", atmPEToken);
        tickData.put("atmCESymbol", atmCESymbol);
        tickData.put("atmPESymbol", atmPESymbol);

        double cePeDiff = atmCELTP - atmPELTP;
        tickData.put("cePeDiff", cePeDiff);
        tickData.put("straddlePremium", atmCELTP + atmPELTP);
        tickData.put("syntheticFuture", niftyLTP + cePeDiff);
        tickData.put("sentiment", cePeDiff > 10 ? "BULLISH" : cePeDiff < -10 ? "BEARISH" : "NEUTRAL");
    }

    @Override
    public Map<String, Object> getRollingChartData() {
        Map<String, Object> chartData = new HashMap<>();

        try {
            LocalDateTime now = LocalDateTime.now();
            // Rolling 30-minute window: 25 minutes of historical + 5 minutes of predictions
            LocalDateTime windowStart = now.minusMinutes(25);
            LocalDateTime windowEnd = now.plusMinutes(5);

            // Build actual candle series using Kite Connect Historical Data API
            List<Object[]> actualSeries = new ArrayList<>();

            try {
                // Fetch 5-minute candles for the past 25 minutes to match prediction interval
                HistoricalDataRequest histRequest = HistoricalDataRequest.builder()
                        .instrumentToken(String.valueOf(NIFTY_INSTRUMENT_TOKEN))
                        .fromDate(windowStart)
                        .toDate(now)
                        .interval("5minute")  // 5-minute candles to match predictions
                        .continuous(false)
                        .oi(false)
                        .build();

                HistoricalDataResponse histResponse = instrumentService.getHistoricalData(histRequest);

                if (histResponse != null && histResponse.isSuccess() && histResponse.getCandles() != null && !histResponse.getCandles().isEmpty()) {
                    logger.info("Rolling Chart: Fetched {} candles from Kite Historical API",
                            histResponse.getCandleCount());

                    for (HistoricalDataResponse.HistoricalCandle candle : histResponse.getCandles()) {
                        if (candle.getTimestamp() != null && !candle.getTimestamp().isEmpty()) {
                            try {
                                // Try multiple timestamp formats
                                long timestamp = parseTimestampToEpochMillis(candle.getTimestamp());
                                actualSeries.add(new Object[]{timestamp, candle.getClose()});
                            } catch (Exception parseEx) {
                                logger.debug("Failed to parse timestamp: {}", candle.getTimestamp());
                            }
                        }
                    }
                    logger.info("Rolling Chart: Parsed {} actual candle points from Historical API", actualSeries.size());
                } else {
                    logger.warn("Rolling Chart: Historical API returned no data. Response: {}",
                            histResponse != null ? histResponse.getMessage() : "null");
                }
            } catch (Exception e) {
                logger.warn("Rolling Chart: Error fetching historical data from Kite API: {}", e.getMessage());
            }

            // Fallback: If no data from Kite API, try CandleStickRepository
            if (actualSeries.isEmpty()) {
                List<CandleStick> actualCandles = candleStickRepository.findCandlesInRange(
                        NIFTY_INSTRUMENT_TOKEN, windowStart, now);

                logger.info("Rolling Chart: Fallback - Found {} candles from CandleStickRepository",
                        actualCandles != null ? actualCandles.size() : 0);

                if (actualCandles != null && !actualCandles.isEmpty()) {
                    for (CandleStick candle : actualCandles) {
                        if (candle.getCandleStartTime() != null && candle.getClosePrice() != null) {
                            long timestamp = candle.getCandleStartTime()
                                    .atZone(java.time.ZoneId.of("Asia/Kolkata"))
                                    .toInstant()
                                    .toEpochMilli();
                            actualSeries.add(new Object[]{timestamp, candle.getClosePrice()});
                        }
                    }
                }
            }

            // Final fallback: Try IndexLTP if still no data
            if (actualSeries.isEmpty()) {
                List<IndexLTP> indexLTPList = indexLTPRepository.findLatestIndexDataByAppJobConfigNum(1);
                if (indexLTPList != null && !indexLTPList.isEmpty()) {
                    java.util.Collections.reverse(indexLTPList);

                    for (IndexLTP ltp : indexLTPList) {
                        if (ltp.getIndexLTP() != null && ltp.getIndexTS() != null) {
                            LocalDateTime ltpTime = ltp.getIndexTS();
                            if (ltpTime.isAfter(windowStart) && ltpTime.isBefore(now)) {
                                long timestamp = ltpTime
                                        .atZone(java.time.ZoneId.of("Asia/Kolkata"))
                                        .toInstant()
                                        .toEpochMilli();
                                actualSeries.add(new Object[]{timestamp, Double.valueOf(ltp.getIndexLTP())});
                            }
                        }
                    }
                    logger.info("Rolling Chart: Using IndexLTP fallback, {} data points", actualSeries.size());
                }
            }

            // Get predictions for the entire 30-minute window
            List<PredictedCandleStick> allPredictions = predictedCandleRepository.findPredictionsInTimeRange(
                    NIFTY_INSTRUMENT_TOKEN, windowStart, windowEnd);

            logger.info("Rolling Chart: Found {} predictions in time range",
                    allPredictions != null ? allPredictions.size() : 0);

            // Sort predictions by time, then by id (descending) to get latest prediction first for each time
            if (allPredictions != null && !allPredictions.isEmpty()) {
                allPredictions.sort((a, b) -> {
                    int timeCompare = a.getCandleStartTime().compareTo(b.getCandleStartTime());
                    if (timeCompare != 0) return timeCompare;
                    // For same timestamp, prefer higher ID (more recent prediction)
                    return Long.compare(b.getId(), a.getId());
                });
            } else {
                allPredictions = new ArrayList<>();
            }

            // Build predicted candle series - deduplicate by timestamp (keep only one per candle start time)
            List<Object[]> predictedSeries = new ArrayList<>();
            Map<Long, Object[]> predictedByTimestamp = new LinkedHashMap<>(); // Preserves insertion order
            Map<Long, Object[]> actualByTimestamp = new LinkedHashMap<>();

            for (PredictedCandleStick pred : allPredictions) {
                if (pred.getCandleStartTime() != null && pred.getClosePrice() != null) {
                    long timestamp = pred.getCandleStartTime()
                            .atZone(java.time.ZoneId.of("Asia/Kolkata"))
                            .toInstant()
                            .toEpochMilli();

                    // Only add if we haven't seen this timestamp yet (first one is the latest due to sorting)
                    if (!predictedByTimestamp.containsKey(timestamp)) {
                        predictedByTimestamp.put(timestamp, new Object[]{timestamp, pred.getClosePrice()});
                    }

                    // For actual series, use verified predictions' actualClosePrice
                    if (!actualByTimestamp.containsKey(timestamp) &&
                            Boolean.TRUE.equals(pred.getVerified()) &&
                            pred.getActualClosePrice() != null && pred.getActualClosePrice() > 0) {
                        actualByTimestamp.put(timestamp, new Object[]{timestamp, pred.getActualClosePrice()});
                    }
                }
            }

            // Convert maps to lists
            predictedSeries.addAll(predictedByTimestamp.values());

            // If actualSeries from historical API is empty, use verified predictions
            if (actualSeries.isEmpty()) {
                actualSeries.addAll(actualByTimestamp.values());
                if (!actualSeries.isEmpty()) {
                    logger.info("Rolling Chart: Using verified predictions' actualClosePrice, {} data points", actualSeries.size());
                }
            }

            logger.info("Rolling Chart: {} actual points, {} predicted points",
                    actualSeries.size(), predictedSeries.size());

            // Calculate window timestamps for frontend
            long windowStartMs = windowStart.atZone(java.time.ZoneId.of("Asia/Kolkata"))
                    .toInstant().toEpochMilli();
            long windowEndMs = windowEnd.atZone(java.time.ZoneId.of("Asia/Kolkata"))
                    .toInstant().toEpochMilli();
            long nowMs = now.atZone(java.time.ZoneId.of("Asia/Kolkata"))
                    .toInstant().toEpochMilli();

            chartData.put("actualSeries", actualSeries);
            chartData.put("predictedSeries", predictedSeries);
            chartData.put("windowStart", windowStartMs);
            chartData.put("windowEnd", windowEndMs);
            chartData.put("currentTime", nowMs);
            chartData.put("actualCount", actualSeries.size());
            chartData.put("predictedCount", predictedSeries.size());
            chartData.put("lastUpdated", LocalDateTime.now().toString());

        } catch (Exception e) {
            logger.error("Error getting rolling chart data: {}", e.getMessage(), e);
            chartData.put("error", e.getMessage());
            chartData.put("actualSeries", new ArrayList<>());
            chartData.put("predictedSeries", new ArrayList<>());
        }

        return chartData;
    }

    /**
     * Parse timestamp string to epoch milliseconds.
     * Handles multiple timestamp formats from Kite API.
     */
    private long parseTimestampToEpochMillis(String timestamp) {
        // Format 0: Custom format with +0530 offset
        try {
            DateTimeFormatter customFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss+0530");
            LocalDateTime ldt = LocalDateTime.parse(timestamp, customFormatter);
            return ldt.atZone(java.time.ZoneId.of("Asia/Kolkata")).toInstant().toEpochMilli();
        } catch (Exception ignored) {}

        // Fix non-standard offset format
        timestamp = timestamp.replace("+0530", "+05:30");

        // Try different formats
        java.time.ZoneId istZone = java.time.ZoneId.of("Asia/Kolkata");

        // Format 1: ISO_LOCAL_DATE_TIME (e.g., "2025-12-26T13:35:00")
        try {
            LocalDateTime ldt = LocalDateTime.parse(timestamp, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            return ldt.atZone(istZone).toInstant().toEpochMilli();
        } catch (Exception ignored) {}

        // Format 2: ISO_DATE_TIME with offset (e.g., "2025-12-26T13:35:00+05:30")
        try {
            java.time.ZonedDateTime zdt = java.time.ZonedDateTime.parse(timestamp, DateTimeFormatter.ISO_DATE_TIME);
            return zdt.toInstant().toEpochMilli();
        } catch (Exception ignored) {}

        // Format 3: Custom format "yyyy-MM-dd HH:mm:ss" (common Kite format)
        try {
            DateTimeFormatter customFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime ldt = LocalDateTime.parse(timestamp, customFormatter);
            return ldt.atZone(istZone).toInstant().toEpochMilli();
        } catch (Exception ignored) {}

        // Format 4: Just date "yyyy-MM-dd"
        try {
            LocalDate ld = LocalDate.parse(timestamp, DateTimeFormatter.ISO_LOCAL_DATE);
            return ld.atStartOfDay(istZone).toInstant().toEpochMilli();
        } catch (Exception ignored) {}

        // If all parsing fails, throw exception
        throw new RuntimeException("Unable to parse timestamp: " + timestamp);
    }

    // Minimum candles: 21 (EMA21 warmup) + 1 (previous candle for crossover detection)
    private static final int EMA_MIN_CANDLES = 22;

    @Override
    public Map<String, Object> getEMAChartData() {
        Map<String, Object> chartData = new HashMap<>();
        try {
            LocalDateTime sessionStart = LocalDateTime.now().with(MARKET_OPEN);

            List<CandleStick> candles = candleStickRepository.findCandlesInRange(
                    NIFTY_INSTRUMENT_TOKEN, sessionStart, LocalDateTime.now());

            int rawCount = candles != null ? candles.size() : 0;
            logger.info("EMA Chart: {} candles loaded for today's session", rawCount);

            if (rawCount < EMA_MIN_CANDLES) {
                return emaNeutralResponse(chartData, rawCount,
                        "Waiting for enough 1-min candles (need " + EMA_MIN_CANDLES + ", have " + rawCount + ").");
            }

            candles.sort(Comparator.comparing(CandleStick::getCandleStartTime));
            candles = candles.stream()
                    .filter(c -> c.getClosePrice() != null)
                    .collect(Collectors.toList());

            if (candles.size() < EMA_MIN_CANDLES) {
                logger.warn("EMA Chart: Only {} valid-close candles after filter (need {})", candles.size(), EMA_MIN_CANDLES);
                return emaNeutralResponse(chartData, candles.size(), null);
            }

            List<Double> closePrices = candles.stream().map(CandleStick::getClosePrice).toList();
            List<Object[]> ema9Series  = calculateEMASeries(candles, closePrices, 9);
            List<Object[]> ema21Series = calculateEMASeries(candles, closePrices, 21);

            List<Object[]> priceSeries = candles.stream()
                    .filter(c -> c.getCandleStartTime() != null)
                    .map(c -> new Object[]{
                            c.getCandleStartTime().atZone(IST_ZONE_ID).toInstant().toEpochMilli(),
                            c.getClosePrice()})
                    .collect(Collectors.toList());

            chartData.put("ema9", ema9Series);
            chartData.put("ema21", ema21Series);
            chartData.put("priceSeries", priceSeries);
            chartData.put("dataPoints", candles.size());
            chartData.put("dataSource", "CandleStick");
            chartData.put("lastUpdated", LocalDateTime.now().toString());

            populateEMATradeSignals(chartData, ema9Series, ema21Series);

        } catch (Exception e) {
            logger.error("Error calculating EMA chart data: {}", e.getMessage(), e);
            chartData.put("error", e.getMessage());
            chartData.put("ema9", new ArrayList<>());
            chartData.put("ema21", new ArrayList<>());
            chartData.put("priceSeries", new ArrayList<>());
        }
        return chartData;
    }

    private Map<String, Object> emaNeutralResponse(Map<String, Object> chartData, int dataPoints, String message) {
        chartData.put("ema9", new ArrayList<>());
        chartData.put("ema21", new ArrayList<>());
        chartData.put("priceSeries", new ArrayList<>());
        chartData.put("dataPoints", dataPoints);
        chartData.put("tradeSignal", "NEUTRAL");
        chartData.put("signalStrength", "WEAK");
        if (message != null) chartData.put("message", message);
        return chartData;
    }

    private void populateEMATradeSignals(Map<String, Object> chartData,
            List<Object[]> ema9Series, List<Object[]> ema21Series) {

        Double ema9Current  = emaSeriesValue(ema9Series, 0);
        Double ema21Current = emaSeriesValue(ema21Series, 0);

        if (ema9Current == null || ema21Current == null) return;

        Double ema9Previous  = emaSeriesValue(ema9Series, 1);
        Double ema21Previous = emaSeriesValue(ema21Series, 1);

        chartData.put("currentEMA9",  ema9Current);
        chartData.put("currentEMA21", ema21Current);
        chartData.put("emaSpread9_21", Math.round((ema9Current - ema21Current) * 100.0) / 100.0);

        String trend = ema9Current > ema21Current ? "BULLISH" : "BEARISH";
        chartData.put("emaAlignment", trend);
        chartData.put("emaTrend", trend);

        boolean hasPrev        = ema9Previous != null && ema21Previous != null;
        boolean bullishCrossover = hasPrev && ema9Previous <= ema21Previous && ema9Current > ema21Current;
        boolean bearishCrossover = hasPrev && ema9Previous >= ema21Previous && ema9Current < ema21Current;

        String tradeSignal;
        String signalReason;

        if (bullishCrossover) {
            tradeSignal  = "BUY";
            signalReason = "9 EMA crossed above 21 EMA — bullish crossover.";
            logger.info("EMA Crossover BUY: prev {}/{}, curr {}/{}", ema9Previous, ema21Previous, ema9Current, ema21Current);
        } else if (bearishCrossover) {
            tradeSignal  = "SELL";
            signalReason = "9 EMA crossed below 21 EMA — bearish crossover.";
            logger.info("EMA Crossover SELL: prev {}/{}, curr {}/{}", ema9Previous, ema21Previous, ema9Current, ema21Current);
        } else {
            tradeSignal  = "NEUTRAL";
            signalReason = "No crossover — waiting for 9 EMA / 21 EMA cross.";
        }

        chartData.put("tradeSignal",   tradeSignal);
        chartData.put("signalStrength", bullishCrossover || bearishCrossover ? "STRONG" : "WEAK");
        chartData.put("signalReason",  signalReason);
    }

    /** Returns the EMA value at [size - 1 - offsetFromEnd] or null if unavailable. */
    private Double emaSeriesValue(List<Object[]> series, int offsetFromEnd) {
        int idx = series.size() - 1 - offsetFromEnd;
        if (idx < 0) return null;
        Object val = series.get(idx)[1];
        return val instanceof Number ? ((Number) val).doubleValue() : null;
    }

    /**
     * Calculate EMA series for charting (returns array of [timestamp, value] pairs)
     */
    private List<Object[]> calculateEMASeries(List<CandleStick> candles, List<Double> prices, int period) {
        List<Object[]> emaSeries = new ArrayList<>();

        if (prices.isEmpty() || period <= 0 || candles.isEmpty()) {
            return emaSeries;
        }

        List<CandleStick> validCandles = new ArrayList<>();
        List<Double> validPrices = new ArrayList<>();
        for (int i = 0; i < candles.size() && i < prices.size(); i++) {
            if (prices.get(i) != null && candles.get(i).getCandleStartTime() != null) {
                validCandles.add(candles.get(i));
                validPrices.add(prices.get(i));
            }
        }

        List<Double> emaValues = computeEMAValues(validPrices, period);
        if (emaValues.isEmpty()) {
            return emaSeries;
        }

        int offset = period - 1; // emaValues[0] aligns to validCandles[period-1]
        for (int i = 0; i < emaValues.size(); i++) {
            long timestamp = validCandles.get(offset + i).getCandleStartTime()
                    .atZone(IST_ZONE_ID)
                    .toInstant()
                    .toEpochMilli();
            emaSeries.add(new Object[]{timestamp, Math.round(emaValues.get(i) * 100.0) / 100.0});
        }

        return emaSeries;
    }

    /**
     * Calculate EMA series from price and timestamp lists (for IndexLTP data)
     */
    // ===================== Smart Money Concepts (SMC) Analysis =====================

    /**
     * Comprehensive Smart Money Concepts analysis including Price Action, Order Blocks, and FVGs
     */
    private SmartMoneyAnalysis analyzeSmartMoneyConcepts(List<CandleStick> historicalCandles, double currentPrice) {
        SmartMoneyAnalysis smc = new SmartMoneyAnalysis();

        if (historicalCandles == null || historicalCandles.size() < 10) {
            logger.warn("Insufficient candles for SMC analysis. Need at least 10, got: {}",
                    historicalCandles != null ? historicalCandles.size() : 0);
            return smc;
        }

        // Perform individual analyses
        smc.priceAction = analyzePriceAction(historicalCandles);
        detectOrderBlocks(historicalCandles, smc, currentPrice);
        detectFairValueGaps(historicalCandles, smc, currentPrice);

        // Calculate nearest key levels
        calculateNearestLevels(smc, currentPrice);

        // Determine SMC bias and trade suggestion
        determineSMCBias(smc, currentPrice);

        logger.info("SMC Analysis - Bias: {}, Confidence: {}%, Suggestion: {} - {}",
                smc.smcBias, smc.smcConfidence, smc.tradeSuggestion, smc.tradeSuggestionReason);

        return smc;
    }

    /**
     * Analyze price action including market structure and candlestick patterns
     */
    private PriceActionAnalysis analyzePriceAction(List<CandleStick> candles) {
        PriceActionAnalysis pa = new PriceActionAnalysis();

        if (candles.size() < 5) {
            return pa;
        }

        // Analyze market structure (Higher Highs, Higher Lows, etc.)
        analyzeMarketStructure(candles, pa);

        // Detect candlestick patterns
        detectCandlestickPatterns(candles, pa);

        // Analyze momentum
        analyzeMomentum(candles, pa);

        // Analyze body/wick ratios
        analyzeBodyWickRatios(candles, pa);

        // Determine price action bias
        determinePriceActionBias(pa);

        logger.debug("Price Action - Structure: {}, Pattern: {}, Momentum: {}, Bias: {}",
                pa.marketStructure, pa.dominantPattern, pa.recentMomentum, pa.priceActionBias);

        return pa;
    }

    /**
     * Analyze market structure for Higher Highs/Lows or Lower Highs/Lows
     */
    private void analyzeMarketStructure(List<CandleStick> candles, PriceActionAnalysis pa) {
        // Find swing highs and lows using a 3-candle pivot detection
        List<Double> swingHighs = new ArrayList<>();
        List<Double> swingLows = new ArrayList<>();

        for (int i = 2; i < candles.size() - 2; i++) {
            CandleStick current = candles.get(i);
            CandleStick prev1 = candles.get(i - 1);
            CandleStick prev2 = candles.get(i - 2);
            CandleStick next1 = candles.get(i + 1);
            CandleStick next2 = candles.get(i + 2);

            if (current.getHighPrice() == null || current.getLowPrice() == null) continue;

            // Swing High: current high is higher than surrounding candles
            boolean isSwingHigh = current.getHighPrice() > prev1.getHighPrice() &&
                    current.getHighPrice() > prev2.getHighPrice() &&
                    current.getHighPrice() > next1.getHighPrice() &&
                    current.getHighPrice() > next2.getHighPrice();

            // Swing Low: current low is lower than surrounding candles
            boolean isSwingLow = current.getLowPrice() < prev1.getLowPrice() &&
                    current.getLowPrice() < prev2.getLowPrice() &&
                    current.getLowPrice() < next1.getLowPrice() &&
                    current.getLowPrice() < next2.getLowPrice();

            if (isSwingHigh) {
                swingHighs.add(current.getHighPrice());
            }
            if (isSwingLow) {
                swingLows.add(current.getLowPrice());
            }
        }

        // Analyze swing highs for HH/LH pattern
        if (swingHighs.size() >= 2) {
            pa.lastSwingHigh = swingHighs.get(swingHighs.size() - 1);
            for (int i = 1; i < swingHighs.size(); i++) {
                if (swingHighs.get(i) > swingHighs.get(i - 1)) {
                    pa.higherHighs++;
                } else {
                    pa.lowerHighs++;
                }
            }
        }

        // Analyze swing lows for HL/LL pattern
        if (swingLows.size() >= 2) {
            pa.lastSwingLow = swingLows.get(swingLows.size() - 1);
            for (int i = 1; i < swingLows.size(); i++) {
                if (swingLows.get(i) > swingLows.get(i - 1)) {
                    pa.higherLows++;
                } else {
                    pa.lowerLows++;
                }
            }
        }

        // Determine market structure
        if (pa.higherHighs >= 2 && pa.higherLows >= 2) {
            pa.marketStructure = TREND_BULLISH;
            pa.isTrendingUp = true;
        } else if (pa.lowerHighs >= 2 && pa.lowerLows >= 2) {
            pa.marketStructure = TREND_BEARISH;
            pa.isTrendingDown = true;
        } else if (Math.abs(pa.higherHighs - pa.lowerHighs) <= 1 && Math.abs(pa.higherLows - pa.lowerLows) <= 1) {
            pa.marketStructure = TREND_NEUTRAL;
            pa.isConsolidating = true;
        }
    }

    /**
     * Detect candlestick patterns in recent candles
     */
    private void detectCandlestickPatterns(List<CandleStick> candles, PriceActionAnalysis pa) {
        int size = candles.size();
        if (size < 3) return;

        CandleStick last = candles.get(size - 1);
        CandleStick prev = candles.get(size - 2);
        CandleStick prev2 = candles.get(size - 3);

        if (last.getOpenPrice() == null || last.getClosePrice() == null ||
                last.getHighPrice() == null || last.getLowPrice() == null) return;

        double lastBody = Math.abs(last.getClosePrice() - last.getOpenPrice());
        double lastRange = last.getHighPrice() - last.getLowPrice();
        double prevBody = Math.abs(prev.getClosePrice() - prev.getOpenPrice());

        boolean lastIsBullish = last.getClosePrice() > last.getOpenPrice();
        boolean prevIsBullish = prev.getClosePrice() > prev.getOpenPrice();

        // Engulfing patterns
        if (lastIsBullish && !prevIsBullish &&
                last.getOpenPrice() < prev.getClosePrice() &&
                last.getClosePrice() > prev.getOpenPrice() &&
                lastBody > prevBody * 1.2) {
            pa.hasBullishEngulfing = true;
            pa.dominantPattern = "BULLISH_ENGULFING";
            pa.patternStrength = 75;
        } else if (!lastIsBullish && prevIsBullish &&
                last.getOpenPrice() > prev.getClosePrice() &&
                last.getClosePrice() < prev.getOpenPrice() &&
                lastBody > prevBody * 1.2) {
            pa.hasBearishEngulfing = true;
            pa.dominantPattern = "BEARISH_ENGULFING";
            pa.patternStrength = 75;
        }

        // Doji pattern (small body relative to range)
        if (lastRange > 0 && lastBody / lastRange < 0.1) {
            pa.hasDoji = true;
            if (pa.dominantPattern.equals("NONE")) {
                pa.dominantPattern = "DOJI";
                pa.patternStrength = 50;
            }
        }

        // Hammer pattern (bullish reversal)
        double lowerWick = Math.min(last.getOpenPrice(), last.getClosePrice()) - last.getLowPrice();
        double upperWick = last.getHighPrice() - Math.max(last.getOpenPrice(), last.getClosePrice());
        if (lowerWick > lastBody * 2 && upperWick < lastBody * 0.5 && !pa.isTrendingUp) {
            pa.hasHammer = true;
            if (pa.dominantPattern.equals("NONE") || pa.patternStrength < 65) {
                pa.dominantPattern = "HAMMER";
                pa.patternStrength = 65;
            }
        }

        // Shooting star pattern (bearish reversal)
        if (upperWick > lastBody * 2 && lowerWick < lastBody * 0.5 && !pa.isTrendingDown) {
            pa.hasShootingStar = true;
            if (pa.dominantPattern.equals("NONE") || pa.patternStrength < 65) {
                pa.dominantPattern = "SHOOTING_STAR";
                pa.patternStrength = 65;
            }
        }

        // Pin bar patterns
        if (lowerWick > lastBody * 2.5 && lastIsBullish) {
            pa.hasBullishPinBar = true;
            if (pa.patternStrength < 70) {
                pa.dominantPattern = "BULLISH_PIN_BAR";
                pa.patternStrength = 70;
            }
        } else if (upperWick > lastBody * 2.5 && !lastIsBullish) {
            pa.hasBearishPinBar = true;
            if (pa.patternStrength < 70) {
                pa.dominantPattern = "BEARISH_PIN_BAR";
                pa.patternStrength = 70;
            }
        }

        // Three White Soldiers / Three Black Crows
        if (size >= 3) {
            boolean prev2IsBullish = prev2.getClosePrice() > prev2.getOpenPrice();
            double prev2Body = Math.abs(prev2.getClosePrice() - prev2.getOpenPrice());

            // Three White Soldiers (strong bullish)
            if (lastIsBullish && prevIsBullish && prev2IsBullish &&
                    lastBody > prev2Body * 0.8 && prevBody > prev2Body * 0.8 &&
                    last.getClosePrice() > prev.getClosePrice() && prev.getClosePrice() > prev2.getClosePrice()) {
                pa.hasThreeWhiteSoldiers = true;
                pa.dominantPattern = "THREE_WHITE_SOLDIERS";
                pa.patternStrength = 85;
            }

            // Three Black Crows (strong bearish)
            if (!lastIsBullish && !prevIsBullish && !prev2IsBullish &&
                    lastBody > prev2Body * 0.8 && prevBody > prev2Body * 0.8 &&
                    last.getClosePrice() < prev.getClosePrice() && prev.getClosePrice() < prev2.getClosePrice()) {
                pa.hasThreeBlackCrows = true;
                pa.dominantPattern = "THREE_BLACK_CROWS";
                pa.patternStrength = 85;
            }

            // Morning Star (bullish reversal): bearish candle → small star → bullish candle
            // Candle sequence: prev2 (bearish), prev (star/small body), last (bullish)
            double prev2Mid = (prev2.getOpenPrice() + prev2.getClosePrice()) / 2;
            boolean prev2IsBearish = !prev2IsBullish;
            boolean prevIsSmallBody = prevBody < prev2Body * 0.3; // star has small body
            if (prev2IsBearish && prevIsSmallBody && lastIsBullish
                    && last.getClosePrice() > prev2Mid               // closes above midpoint of first candle
                    && lastBody > prev2Body * 0.5) {                 // last candle has meaningful body
                pa.hasMorningStar = true;
                if (pa.patternStrength < 80) {
                    pa.dominantPattern = "MORNING_STAR";
                    pa.patternStrength = 80;
                }
            }

            // Evening Star (bearish reversal): bullish candle → small star → bearish candle
            if (prev2IsBullish && prevIsSmallBody && !lastIsBullish
                    && last.getClosePrice() < prev2Mid               // closes below midpoint of first candle
                    && lastBody > prev2Body * 0.5) {
                pa.hasEveningStar = true;
                if (pa.patternStrength < 80) {
                    pa.dominantPattern = "EVENING_STAR";
                    pa.patternStrength = 80;
                }
            }
        }
    }

    /**
     * Analyze momentum from recent candles
     */
    private void analyzeMomentum(List<CandleStick> candles, PriceActionAnalysis pa) {
        int recentCount = Math.min(10, candles.size());
        List<CandleStick> recentCandles = candles.subList(candles.size() - recentCount, candles.size());

        double momentumSum = 0;
        int consecutiveBullish = 0;
        int consecutiveBearish = 0;
        int currentStreak = 0;
        Boolean lastWasBullish = null;

        for (CandleStick candle : recentCandles) {
            if (candle.getClosePrice() == null || candle.getOpenPrice() == null) continue;

            double candleMove = candle.getClosePrice() - candle.getOpenPrice();
            momentumSum += candleMove;

            boolean isBullish = candleMove > 0;
            if (lastWasBullish == null) {
                currentStreak = 1;
            } else if (isBullish == lastWasBullish) {
                currentStreak++;
            } else {
                currentStreak = 1;
            }

            if (isBullish) {
                consecutiveBullish = currentStreak;
                consecutiveBearish = 0;
            } else {
                consecutiveBearish = currentStreak;
                consecutiveBullish = 0;
            }

            lastWasBullish = isBullish;
        }

        pa.recentMomentum = momentumSum;
        pa.consecutiveBullishCandles = consecutiveBullish;
        pa.consecutiveBearishCandles = consecutiveBearish;

        // Calculate momentum strength (0-100)
        double avgMove = momentumSum / recentCount;
        pa.momentumStrength = Math.min(100, Math.abs(avgMove) * 10);

        // Determine if momentum is increasing or decreasing
        if (recentCount >= 5) {
            double firstHalfMomentum = 0;
            double secondHalfMomentum = 0;
            int half = recentCount / 2;

            for (int i = 0; i < half; i++) {
                CandleStick c = recentCandles.get(i);
                if (c.getClosePrice() != null && c.getOpenPrice() != null) {
                    firstHalfMomentum += c.getClosePrice() - c.getOpenPrice();
                }
            }
            for (int i = half; i < recentCount; i++) {
                CandleStick c = recentCandles.get(i);
                if (c.getClosePrice() != null && c.getOpenPrice() != null) {
                    secondHalfMomentum += c.getClosePrice() - c.getOpenPrice();
                }
            }

            pa.isMomentumIncreasing = Math.abs(secondHalfMomentum) > Math.abs(firstHalfMomentum);
            pa.isMomentumDecreasing = Math.abs(secondHalfMomentum) < Math.abs(firstHalfMomentum) * 0.7;
        }
    }

    /**
     * Analyze body and wick ratios
     */
    private void analyzeBodyWickRatios(List<CandleStick> candles, PriceActionAnalysis pa) {
        int recentCount = Math.min(20, candles.size());
        List<CandleStick> recentCandles = candles.subList(candles.size() - recentCount, candles.size());

        double totalBody = 0;
        double totalUpperWick = 0;
        double totalLowerWick = 0;
        int count = 0;
        int longUpperWickCount = 0;
        int longLowerWickCount = 0;

        for (CandleStick candle : recentCandles) {
            if (candle.getOpenPrice() == null || candle.getClosePrice() == null ||
                    candle.getHighPrice() == null || candle.getLowPrice() == null) continue;

            double body = Math.abs(candle.getClosePrice() - candle.getOpenPrice());
            double upperWick = candle.getHighPrice() - Math.max(candle.getOpenPrice(), candle.getClosePrice());
            double lowerWick = Math.min(candle.getOpenPrice(), candle.getClosePrice()) - candle.getLowPrice();

            totalBody += body;
            totalUpperWick += upperWick;
            totalLowerWick += lowerWick;
            count++;

            if (body > 0 && upperWick > body * 1.5) longUpperWickCount++;
            if (body > 0 && lowerWick > body * 1.5) longLowerWickCount++;
        }

        if (count > 0) {
            pa.averageBodySize = totalBody / count;
            pa.averageWickSize = (totalUpperWick + totalLowerWick) / (count * 2);
            pa.bodyToWickRatio = pa.averageWickSize > 0 ? pa.averageBodySize / pa.averageWickSize : 2.0;
            pa.hasLongUpperWicks = longUpperWickCount > count * 0.3;
            pa.hasLongLowerWicks = longLowerWickCount > count * 0.3;
        }
    }

    /**
     * Determine overall price action bias
     */
    private void determinePriceActionBias(PriceActionAnalysis pa) {
        int bullishScore = 0;
        int bearishScore = 0;

        // Market structure (highest weight)
        if (pa.isTrendingUp) bullishScore += 3;
        if (pa.isTrendingDown) bearishScore += 3;

        // Candlestick patterns
        if (pa.hasBullishEngulfing || pa.hasHammer || pa.hasBullishPinBar || pa.hasThreeWhiteSoldiers) {
            bullishScore += 2;
        }
        if (pa.hasBearishEngulfing || pa.hasShootingStar || pa.hasBearishPinBar || pa.hasThreeBlackCrows) {
            bearishScore += 2;
        }

        // Momentum
        if (pa.recentMomentum > 0 && pa.consecutiveBullishCandles >= 3) bullishScore += 2;
        if (pa.recentMomentum < 0 && pa.consecutiveBearishCandles >= 3) bearishScore += 2;
        if (pa.isMomentumIncreasing && pa.recentMomentum > 0) bullishScore++;
        if (pa.isMomentumIncreasing && pa.recentMomentum < 0) bearishScore++;

        // Wick analysis (rejection signals)
        if (pa.hasLongUpperWicks) bearishScore++;  // Rejection from highs
        if (pa.hasLongLowerWicks) bullishScore++;  // Rejection from lows

        // Strong body-to-wick ratio indicates conviction
        if (pa.bodyToWickRatio > 2.0 && pa.recentMomentum > 0) bullishScore++;
        if (pa.bodyToWickRatio > 2.0 && pa.recentMomentum < 0) bearishScore++;

        // Determine bias
        if (bullishScore >= bearishScore + 3) {
            pa.priceActionBias = TREND_BULLISH;
            pa.priceActionConfidence = Math.min(90, 50 + (bullishScore - bearishScore) * 8);
        } else if (bearishScore >= bullishScore + 3) {
            pa.priceActionBias = TREND_BEARISH;
            pa.priceActionConfidence = Math.min(90, 50 + (bearishScore - bullishScore) * 8);
        } else {
            pa.priceActionBias = TREND_NEUTRAL;
            pa.priceActionConfidence = 50;
        }
    }

    /**
     * Detect Order Blocks in historical candles
     * Order blocks form when price makes a strong move after a consolidation/reversal
     */
    private void detectOrderBlocks(List<CandleStick> candles, SmartMoneyAnalysis smc, double currentPrice) {
        if (candles.size() < 5) return;

        for (int i = 3; i < candles.size() - 1; i++) {
            CandleStick current = candles.get(i);
            CandleStick next = candles.get(i + 1);

            if (current.getOpenPrice() == null || current.getClosePrice() == null ||
                    current.getHighPrice() == null || current.getLowPrice() == null ||
                    next.getOpenPrice() == null || next.getClosePrice() == null) continue;

            double currentBody = Math.abs(current.getClosePrice() - current.getOpenPrice());
            double nextBody = Math.abs(next.getClosePrice() - next.getOpenPrice());
            boolean currentIsBullish = current.getClosePrice() > current.getOpenPrice();
            boolean nextIsBullish = next.getClosePrice() > next.getOpenPrice();

            // Bullish Order Block: Bearish candle followed by strong bullish candle
            // The bearish candle becomes the order block (demand zone)
            if (!currentIsBullish && nextIsBullish && nextBody > currentBody * 1.5) {
                // Check if next candle closes above current candle's high (displacement)
                if (next.getClosePrice() > current.getHighPrice()) {
                    OrderBlock ob = new OrderBlock(true, current.getHighPrice(), current.getLowPrice(),
                            current.getCandleStartTime());
                    ob.strength = nextBody > currentBody * 2.5 ? 3 : (nextBody > currentBody * 2 ? 2 : 1);

                    // Check if OB is still valid (not broken)
                    boolean isBroken = false;
                    for (int j = i + 2; j < candles.size(); j++) {
                        CandleStick checkCandle = candles.get(j);
                        if (checkCandle.getLowPrice() != null && checkCandle.getLowPrice() < ob.bottomPrice) {
                            isBroken = true;
                            break;
                        }
                        if (checkCandle.getLowPrice() != null && ob.containsPrice(checkCandle.getLowPrice())) {
                            ob.isMitigated = true;
                            ob.touchCount++;
                        }
                    }

                    ob.isBroken = isBroken;
                    if (!isBroken && ob.bottomPrice < currentPrice) {
                        smc.bullishOrderBlocks.add(ob);
                    }
                }
            }

            // Bearish Order Block: Bullish candle followed by strong bearish candle
            // The bullish candle becomes the order block (supply zone)
            if (currentIsBullish && !nextIsBullish && nextBody > currentBody * 1.5) {
                // Check if next candle closes below current candle's low (displacement)
                if (next.getClosePrice() < current.getLowPrice()) {
                    OrderBlock ob = new OrderBlock(false, current.getHighPrice(), current.getLowPrice(),
                            current.getCandleStartTime());
                    ob.strength = nextBody > currentBody * 2.5 ? 3 : (nextBody > currentBody * 2 ? 2 : 1);

                    // Check if OB is still valid (not broken)
                    boolean isBroken = false;
                    for (int j = i + 2; j < candles.size(); j++) {
                        CandleStick checkCandle = candles.get(j);
                        if (checkCandle.getHighPrice() != null && checkCandle.getHighPrice() > ob.topPrice) {
                            isBroken = true;
                            break;
                        }
                        if (checkCandle.getHighPrice() != null && ob.containsPrice(checkCandle.getHighPrice())) {
                            ob.isMitigated = true;
                            ob.touchCount++;
                        }
                    }

                    ob.isBroken = isBroken;
                    if (!isBroken && ob.topPrice > currentPrice) {
                        smc.bearishOrderBlocks.add(ob);
                    }
                }
            }
        }

        // Sort order blocks by proximity to current price
        smc.bullishOrderBlocks.sort((a, b) -> Double.compare(b.topPrice, a.topPrice)); // Nearest first (descending)
        smc.bearishOrderBlocks.sort((a, b) -> Double.compare(a.bottomPrice, b.bottomPrice)); // Nearest first (ascending)

        logger.debug("Detected {} bullish and {} bearish order blocks",
                smc.bullishOrderBlocks.size(), smc.bearishOrderBlocks.size());
    }

    /**
     * Detect Fair Value Gaps (FVGs) in historical candles
     * FVG forms when candle 1's high is below candle 3's low (bullish) or vice versa
     */
    private void detectFairValueGaps(List<CandleStick> candles, SmartMoneyAnalysis smc, double currentPrice) {
        if (candles.size() < 3) return;

        for (int i = 0; i < candles.size() - 2; i++) {
            CandleStick candle1 = candles.get(i);
            CandleStick candle2 = candles.get(i + 1);
            CandleStick candle3 = candles.get(i + 2);

            if (candle1.getHighPrice() == null || candle1.getLowPrice() == null ||
                    candle3.getHighPrice() == null || candle3.getLowPrice() == null) continue;

            // Bullish FVG: Candle 1's high is below Candle 3's low (gap up)
            if (candle1.getHighPrice() < candle3.getLowPrice()) {
                FairValueGap fvg = new FairValueGap(true, candle3.getLowPrice(), candle1.getHighPrice(),
                        candle2.getCandleStartTime());
                fvg.candlesSinceCreation = candles.size() - i - 2;

                // Check if FVG has been filled
                for (int j = i + 3; j < candles.size(); j++) {
                    CandleStick checkCandle = candles.get(j);
                    if (checkCandle.getLowPrice() != null) {
                        if (checkCandle.getLowPrice() <= fvg.bottomPrice) {
                            fvg.isFilled = true;
                            fvg.fillPercentage = 100;
                            break;
                        } else if (checkCandle.getLowPrice() < fvg.topPrice) {
                            fvg.isPartiallyFilled = true;
                            double filled = fvg.topPrice - checkCandle.getLowPrice();
                            fvg.fillPercentage = Math.max(fvg.fillPercentage, (filled / fvg.gapSize) * 100);
                        }
                    }
                }

                // Only keep unfilled or partially filled FVGs that are relevant
                if (!fvg.isFilled && fvg.gapSize >= 5 && fvg.topPrice <= currentPrice) {
                    smc.bullishFVGs.add(fvg);
                }
            }

            // Bearish FVG: Candle 1's low is above Candle 3's high (gap down)
            if (candle1.getLowPrice() > candle3.getHighPrice()) {
                FairValueGap fvg = new FairValueGap(false, candle1.getLowPrice(), candle3.getHighPrice(),
                        candle2.getCandleStartTime());
                fvg.candlesSinceCreation = candles.size() - i - 2;

                // Check if FVG has been filled
                for (int j = i + 3; j < candles.size(); j++) {
                    CandleStick checkCandle = candles.get(j);
                    if (checkCandle.getHighPrice() != null) {
                        if (checkCandle.getHighPrice() >= fvg.topPrice) {
                            fvg.isFilled = true;
                            fvg.fillPercentage = 100;
                            break;
                        } else if (checkCandle.getHighPrice() > fvg.bottomPrice) {
                            fvg.isPartiallyFilled = true;
                            double filled = checkCandle.getHighPrice() - fvg.bottomPrice;
                            fvg.fillPercentage = Math.max(fvg.fillPercentage, (filled / fvg.gapSize) * 100);
                        }
                    }
                }

                // Only keep unfilled or partially filled FVGs that are relevant
                if (!fvg.isFilled && fvg.gapSize >= 5 && fvg.bottomPrice >= currentPrice) {
                    smc.bearishFVGs.add(fvg);
                }
            }
        }

        // Sort FVGs by proximity to current price
        smc.bullishFVGs.sort((a, b) -> Double.compare(b.topPrice, a.topPrice)); // Nearest first
        smc.bearishFVGs.sort((a, b) -> Double.compare(a.bottomPrice, b.bottomPrice)); // Nearest first

        smc.unfilledFVGCount = smc.bullishFVGs.size() + smc.bearishFVGs.size();
        smc.hasUnfilledFVGBelow = !smc.bullishFVGs.isEmpty();
        smc.hasUnfilledFVGAbove = !smc.bearishFVGs.isEmpty();

        logger.debug("Detected {} bullish and {} bearish unfilled FVGs",
                smc.bullishFVGs.size(), smc.bearishFVGs.size());
    }

    /**
     * Calculate nearest key levels from order blocks and FVGs
     */
    private void calculateNearestLevels(SmartMoneyAnalysis smc, double currentPrice) {
        // Nearest bullish OB (support/demand zone below price)
        if (!smc.bullishOrderBlocks.isEmpty()) {
            OrderBlock nearestBullishOB = smc.bullishOrderBlocks.stream()
                    .filter(ob -> ob.topPrice < currentPrice)
                    .max((a, b) -> Double.compare(a.topPrice, b.topPrice))
                    .orElse(null);
            if (nearestBullishOB != null) {
                smc.nearestBullishOB = nearestBullishOB.midPrice;
            }
        }

        // Nearest bearish OB (resistance/supply zone above price)
        if (!smc.bearishOrderBlocks.isEmpty()) {
            OrderBlock nearestBearishOB = smc.bearishOrderBlocks.stream()
                    .filter(ob -> ob.bottomPrice > currentPrice)
                    .min((a, b) -> Double.compare(a.bottomPrice, b.bottomPrice))
                    .orElse(null);
            if (nearestBearishOB != null) {
                smc.nearestBearishOB = nearestBearishOB.midPrice;
            }
        }

        // Nearest unfilled bullish FVG (below current price - potential pullback target)
        if (!smc.bullishFVGs.isEmpty()) {
            FairValueGap nearestBullishFVG = smc.bullishFVGs.stream()
                    .filter(fvg -> !fvg.isFilled && fvg.topPrice < currentPrice)
                    .max((a, b) -> Double.compare(a.topPrice, b.topPrice))
                    .orElse(null);
            if (nearestBullishFVG != null) {
                smc.nearestUnfilledBullishFVG = nearestBullishFVG.getMidPoint();
            }
        }

        // Nearest unfilled bearish FVG (above current price - potential rally target)
        if (!smc.bearishFVGs.isEmpty()) {
            FairValueGap nearestBearishFVG = smc.bearishFVGs.stream()
                    .filter(fvg -> !fvg.isFilled && fvg.bottomPrice > currentPrice)
                    .min((a, b) -> Double.compare(a.bottomPrice, b.bottomPrice))
                    .orElse(null);
            if (nearestBearishFVG != null) {
                smc.nearestUnfilledBearishFVG = nearestBearishFVG.getMidPoint();
            }
        }

        // Determine nearest FVG target (price tends to fill FVGs)
        double distanceToBullishFVG = smc.nearestUnfilledBullishFVG > 0 ?
                currentPrice - smc.nearestUnfilledBullishFVG : Double.MAX_VALUE;
        double distanceToBearishFVG = smc.nearestUnfilledBearishFVG > 0 ?
                smc.nearestUnfilledBearishFVG - currentPrice : Double.MAX_VALUE;

        if (distanceToBullishFVG < distanceToBearishFVG && distanceToBullishFVG < 50) {
            smc.nearestFVGTarget = smc.nearestUnfilledBullishFVG;
        } else if (distanceToBearishFVG < 50) {
            smc.nearestFVGTarget = smc.nearestUnfilledBearishFVG;
        }

        // Check if price is currently in an order block
        for (OrderBlock ob : smc.bullishOrderBlocks) {
            if (ob.containsPrice(currentPrice)) {
                smc.isInOrderBlock = true;
                break;
            }
        }
        if (!smc.isInOrderBlock) {
            for (OrderBlock ob : smc.bearishOrderBlocks) {
                if (ob.containsPrice(currentPrice)) {
                    smc.isInOrderBlock = true;
                    break;
                }
            }
        }

        // Check if price is near an order block (within 20 points)
        smc.isNearOrderBlock = (smc.nearestBullishOB > 0 && currentPrice - smc.nearestBullishOB < 20) ||
                (smc.nearestBearishOB > 0 && smc.nearestBearishOB - currentPrice < 20);
    }

    /**
     * Determine SMC bias and generate trade suggestion
     */
    private void determineSMCBias(SmartMoneyAnalysis smc, double currentPrice) {
        int bullishScore = 0;
        int bearishScore = 0;

        PriceActionAnalysis pa = smc.priceAction;

        // Price Action contribution (high weight)
        if (pa != null) {
            if (TREND_BULLISH.equals(pa.priceActionBias)) {
                bullishScore += 3;
            } else if (TREND_BEARISH.equals(pa.priceActionBias)) {
                bearishScore += 3;
            }

            // Strong candlestick patterns
            if (pa.hasBullishEngulfing || pa.hasThreeWhiteSoldiers || pa.hasMorningStar) {
                bullishScore += 2;
            }
            if (pa.hasBearishEngulfing || pa.hasThreeBlackCrows || pa.hasEveningStar) {
                bearishScore += 2;
            }

            // Momentum
            if (pa.consecutiveBullishCandles >= 3) bullishScore++;
            if (pa.consecutiveBearishCandles >= 3) bearishScore++;
        }

        // Order Block contribution
        if (smc.isInOrderBlock) {
            // Check which type of OB price is in
            for (OrderBlock ob : smc.bullishOrderBlocks) {
                if (ob.containsPrice(currentPrice)) {
                    bullishScore += ob.strength;
                    break;
                }
            }
            for (OrderBlock ob : smc.bearishOrderBlocks) {
                if (ob.containsPrice(currentPrice)) {
                    bearishScore += ob.strength;
                    break;
                }
            }
        }

        // Nearby order blocks as support/resistance
        if (smc.nearestBullishOB > 0 && currentPrice - smc.nearestBullishOB < 30) {
            bullishScore++;  // Near support
        }
        if (smc.nearestBearishOB > 0 && smc.nearestBearishOB - currentPrice < 30) {
            bearishScore++;  // Near resistance
        }

        // FVG contribution (price tends to fill gaps)
        if (smc.hasUnfilledFVGBelow && smc.nearestUnfilledBullishFVG > 0) {
            double distance = currentPrice - smc.nearestUnfilledBullishFVG;
            if (distance < 30) {
                bearishScore++;  // Likely to pull back to fill FVG
            }
        }
        if (smc.hasUnfilledFVGAbove && smc.nearestUnfilledBearishFVG > 0) {
            double distance = smc.nearestUnfilledBearishFVG - currentPrice;
            if (distance < 30) {
                bullishScore++;  // Likely to rally to fill FVG
            }
        }

        // Determine bias
        if (bullishScore >= bearishScore + 3) {
            smc.smcBias = TREND_BULLISH;
            smc.smcConfidence = Math.min(90, 50 + (bullishScore - bearishScore) * 6);
        } else if (bearishScore >= bullishScore + 3) {
            smc.smcBias = TREND_BEARISH;
            smc.smcConfidence = Math.min(90, 50 + (bearishScore - bullishScore) * 6);
        } else {
            smc.smcBias = TREND_NEUTRAL;
            smc.smcConfidence = 50;
        }

        // Generate trade suggestion
        generateTradeSuggestion(smc, currentPrice, bullishScore, bearishScore);
    }

    /**
     * Generate specific trade suggestion based on SMC analysis
     */
    private void generateTradeSuggestion(SmartMoneyAnalysis smc, double currentPrice,
                                          int bullishScore, int bearishScore) {
        PriceActionAnalysis pa = smc.priceAction;

        // High confidence bullish setup
        if (smc.isInOrderBlock && smc.smcBias.equals(TREND_BULLISH)) {
            for (OrderBlock ob : smc.bullishOrderBlocks) {
                if (ob.containsPrice(currentPrice) && ob.strength >= 2) {
                    smc.tradeSuggestion = "BUY";
                    smc.tradeSuggestionReason = String.format(
                            "Price in strong bullish order block (%.2f-%.2f). Buy on confirmation. SL below %.2f",
                            ob.bottomPrice, ob.topPrice, ob.bottomPrice);
                    return;
                }
            }
        }

        // High confidence bearish setup
        if (smc.isInOrderBlock && smc.smcBias.equals(TREND_BEARISH)) {
            for (OrderBlock ob : smc.bearishOrderBlocks) {
                if (ob.containsPrice(currentPrice) && ob.strength >= 2) {
                    smc.tradeSuggestion = "SELL";
                    smc.tradeSuggestionReason = String.format(
                            "Price in strong bearish order block (%.2f-%.2f). Sell on confirmation. SL above %.2f",
                            ob.bottomPrice, ob.topPrice, ob.topPrice);
                    return;
                }
            }
        }

        // FVG fill setup
        if (smc.nearestFVGTarget > 0) {
            double distanceToTarget = Math.abs(currentPrice - smc.nearestFVGTarget);
            if (distanceToTarget < 40) {
                if (smc.nearestFVGTarget < currentPrice) {
                    smc.tradeSuggestion = "SELL";
                    smc.tradeSuggestionReason = String.format(
                            "Unfilled FVG below at %.2f (%.1f pts away). Expect pullback to fill gap.",
                            smc.nearestFVGTarget, distanceToTarget);
                } else {
                    smc.tradeSuggestion = "BUY";
                    smc.tradeSuggestionReason = String.format(
                            "Unfilled FVG above at %.2f (%.1f pts away). Expect rally to fill gap.",
                            smc.nearestFVGTarget, distanceToTarget);
                }
                return;
            }
        }

        // Trend following setup
        if (pa != null && pa.isTrendingUp && bullishScore > bearishScore + 2) {
            smc.tradeSuggestion = "BUY";
            smc.tradeSuggestionReason = String.format(
                    "Bullish market structure with %d HH and %d HL. Trend is up.",
                    pa.higherHighs, pa.higherLows);
            return;
        }

        if (pa != null && pa.isTrendingDown && bearishScore > bullishScore + 2) {
            smc.tradeSuggestion = "SELL";
            smc.tradeSuggestionReason = String.format(
                    "Bearish market structure with %d LH and %d LL. Trend is down.",
                    pa.lowerHighs, pa.lowerLows);
            return;
        }

        // Pattern-based setup
        if (pa != null) {
            if (pa.hasBullishEngulfing || pa.hasHammer || pa.hasThreeWhiteSoldiers) {
                smc.tradeSuggestion = "BUY";
                smc.tradeSuggestionReason = String.format("Bullish %s pattern detected.", pa.dominantPattern);
                return;
            }
            if (pa.hasBearishEngulfing || pa.hasShootingStar || pa.hasThreeBlackCrows) {
                smc.tradeSuggestion = "SELL";
                smc.tradeSuggestionReason = String.format("Bearish %s pattern detected.", pa.dominantPattern);
                return;
            }
        }

        // Default - wait for clearer setup
        smc.tradeSuggestion = "WAIT";
        smc.tradeSuggestionReason = "No high-probability setup. Wait for price to reach order block or form clear pattern.";
    }

    // ===================== Channel Pattern Analysis =====================

    /**
     * Detect channel patterns in price action
     * Identifies ascending, descending, and horizontal (range) channels
     */
    private ChannelPattern detectChannelPattern(List<CandleStick> candles, double currentPrice) {
        ChannelPattern channel = new ChannelPattern();

        if (candles == null || candles.size() < 20) {
            return channel;
        }

        // Find swing highs and lows for channel detection
        List<double[]> swingHighs = new ArrayList<>();  // [index, price]
        List<double[]> swingLows = new ArrayList<>();

        for (int i = 3; i < candles.size() - 3; i++) {
            CandleStick c = candles.get(i);
            if (c.getHighPrice() == null || c.getLowPrice() == null) continue;

            boolean isSwingHigh = true;
            boolean isSwingLow = true;

            for (int j = 1; j <= 3; j++) {
                CandleStick prev = candles.get(i - j);
                CandleStick next = candles.get(i + j);

                if (prev.getHighPrice() == null || next.getHighPrice() == null) {
                    isSwingHigh = false;
                    isSwingLow = false;
                    break;
                }

                if (c.getHighPrice() <= prev.getHighPrice() || c.getHighPrice() <= next.getHighPrice()) {
                    isSwingHigh = false;
                }
                if (c.getLowPrice() >= prev.getLowPrice() || c.getLowPrice() >= next.getLowPrice()) {
                    isSwingLow = false;
                }
            }

            if (isSwingHigh) {
                swingHighs.add(new double[]{i, c.getHighPrice()});
            }
            if (isSwingLow) {
                swingLows.add(new double[]{i, c.getLowPrice()});
            }
        }

        if (swingHighs.size() < 2 || swingLows.size() < 2) {
            return channel;
        }

        // Calculate trend lines using linear regression on swing points
        double[] upperLine = calculateTrendLine(swingHighs);  // [slope, intercept]
        double[] lowerLine = calculateTrendLine(swingLows);

        if (upperLine == null || lowerLine == null) {
            return channel;
        }

        double upperSlope = upperLine[0];
        double lowerSlope = lowerLine[0];
        double avgSlope = (upperSlope + lowerSlope) / 2;

        // Determine channel type based on slopes
        double slopeThreshold = 0.05; // Threshold for horizontal channel

        if (Math.abs(avgSlope) < slopeThreshold) {
            channel.channelType = "HORIZONTAL";
        } else if (avgSlope > 0 && upperSlope > 0 && lowerSlope > 0) {
            channel.channelType = "ASCENDING";
        } else if (avgSlope < 0 && upperSlope < 0 && lowerSlope < 0) {
            channel.channelType = "DESCENDING";
        } else {
            channel.channelType = "CONVERGING"; // Could be wedge/triangle
        }

        channel.channelSlope = avgSlope;

        // Calculate current channel boundaries
        int lastIndex = candles.size() - 1;
        channel.upperChannelLine = upperLine[0] * lastIndex + upperLine[1];
        channel.lowerChannelLine = lowerLine[0] * lastIndex + lowerLine[1];
        channel.channelWidth = channel.upperChannelLine - channel.lowerChannelLine;

        // Calculate position in channel
        if (channel.channelWidth > 0) {
            channel.currentPositionInChannel = (currentPrice - channel.lowerChannelLine) / channel.channelWidth;
            channel.isNearUpperBoundary = channel.currentPositionInChannel > 0.85;
            channel.isNearLowerBoundary = channel.currentPositionInChannel < 0.15;
        }

        // Check for breakout
        channel.isPriceBreakingUp = currentPrice > channel.upperChannelLine;
        channel.isPriceBreakingDown = currentPrice < channel.lowerChannelLine;

        // Calculate channel strength based on touch count and parallel nature
        channel.channelTouches = swingHighs.size() + swingLows.size();
        double parallelScore = 1.0 - Math.min(1.0, Math.abs(upperSlope - lowerSlope) / Math.max(0.01, Math.abs(avgSlope)));
        channel.channelStrength = Math.min(100, channel.channelTouches * 10 + parallelScore * 50);

        // Projected targets
        channel.projectedUpperTarget = channel.upperChannelLine + channel.channelWidth * 0.5;
        channel.projectedLowerTarget = channel.lowerChannelLine - channel.channelWidth * 0.5;

        // Determine bias
        if (channel.isPriceBreakingUp) {
            channel.channelBias = TREND_BULLISH;
        } else if (channel.isPriceBreakingDown) {
            channel.channelBias = TREND_BEARISH;
        } else if ("ASCENDING".equals(channel.channelType)) {
            channel.channelBias = channel.isNearLowerBoundary ? TREND_BULLISH :
                                  (channel.isNearUpperBoundary ? TREND_NEUTRAL : TREND_BULLISH);
        } else if ("DESCENDING".equals(channel.channelType)) {
            channel.channelBias = channel.isNearUpperBoundary ? TREND_BEARISH :
                                  (channel.isNearLowerBoundary ? TREND_NEUTRAL : TREND_BEARISH);
        } else if ("HORIZONTAL".equals(channel.channelType)) {
            channel.channelBias = channel.isNearLowerBoundary ? TREND_BULLISH :
                                  (channel.isNearUpperBoundary ? TREND_BEARISH : TREND_NEUTRAL);
        }

        logger.debug("Channel Pattern - Type: {}, Width: {}, Position: {}, Bias: {}",
                channel.channelType, channel.channelWidth, channel.currentPositionInChannel, channel.channelBias);

        return channel;
    }

    /**
     * Calculate trend line using simple linear regression
     * Returns [slope, intercept]
     */
    private double[] calculateTrendLine(List<double[]> points) {
        if (points.size() < 2) return null;

        double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
        int n = points.size();

        for (double[] point : points) {
            sumX += point[0];
            sumY += point[1];
            sumXY += point[0] * point[1];
            sumX2 += point[0] * point[0];
        }

        double denominator = n * sumX2 - sumX * sumX;
        if (Math.abs(denominator) < 0.0001) return null;

        double slope = (n * sumXY - sumX * sumY) / denominator;
        double intercept = (sumY - slope * sumX) / n;

        return new double[]{slope, intercept};
    }

    // ===================== Fibonacci Retracement Analysis =====================

    /**
     * Calculate Fibonacci retracement and extension levels
     */
    private FibonacciLevels calculateFibonacciLevels(List<CandleStick> candles, double currentPrice) {
        FibonacciLevels fib = new FibonacciLevels();

        if (candles == null || candles.size() < 20) {
            return fib;
        }

        // Find significant swing high and low
        double swingHigh = 0;
        double swingLow = Double.MAX_VALUE;
        int swingHighIndex = 0;
        int swingLowIndex = 0;

        // Look for swing points in recent candles (last 60 candles = 1 hour)
        int lookback = Math.min(60, candles.size());
        int startIndex = candles.size() - lookback;

        for (int i = startIndex; i < candles.size(); i++) {
            CandleStick c = candles.get(i);
            if (c.getHighPrice() == null || c.getLowPrice() == null) continue;

            if (c.getHighPrice() > swingHigh) {
                swingHigh = c.getHighPrice();
                swingHighIndex = i;
            }
            if (c.getLowPrice() < swingLow) {
                swingLow = c.getLowPrice();
                swingLowIndex = i;
            }
        }

        if (swingHigh == 0 || swingLow == Double.MAX_VALUE) {
            return fib;
        }

        fib.swingHigh = swingHigh;
        fib.swingLow = swingLow;
        fib.currentPrice = currentPrice;

        // Determine trend direction (which swing point came first)
        fib.isUptrend = swingLowIndex < swingHighIndex;

        double range = swingHigh - swingLow;

        if (fib.isUptrend) {
            // Uptrend: Retracement from high going down
            fib.level_0 = swingHigh;
            fib.level_236 = swingHigh - range * 0.236;
            fib.level_382 = swingHigh - range * 0.382;
            fib.level_500 = swingHigh - range * 0.500;
            fib.level_618 = swingHigh - range * 0.618;
            fib.level_786 = swingHigh - range * 0.786;
            fib.level_100 = swingLow;

            // Extension levels (below swing low for uptrend retracement)
            fib.ext_1272 = swingHigh - range * 1.272;
            fib.ext_1618 = swingHigh - range * 1.618;
            fib.ext_2000 = swingHigh - range * 2.000;
            fib.ext_2618 = swingHigh - range * 2.618;
        } else {
            // Downtrend: Retracement from low going up
            fib.level_0 = swingLow;
            fib.level_236 = swingLow + range * 0.236;
            fib.level_382 = swingLow + range * 0.382;
            fib.level_500 = swingLow + range * 0.500;
            fib.level_618 = swingLow + range * 0.618;
            fib.level_786 = swingLow + range * 0.786;
            fib.level_100 = swingHigh;

            // Extension levels (above swing high for downtrend retracement)
            fib.ext_1272 = swingLow + range * 1.272;
            fib.ext_1618 = swingLow + range * 1.618;
            fib.ext_2000 = swingLow + range * 2.000;
            fib.ext_2618 = swingLow + range * 2.618;
        }

        // Find nearest Fibonacci level
        double[] fibLevels = {fib.level_0, fib.level_236, fib.level_382, fib.level_500,
                              fib.level_618, fib.level_786, fib.level_100};
        String[] fibNames = {"0%", "23.6%", "38.2%", "50%", "61.8%", "78.6%", "100%"};

        double minDistance = Double.MAX_VALUE;
        for (int i = 0; i < fibLevels.length; i++) {
            double distance = Math.abs(currentPrice - fibLevels[i]);
            if (distance < minDistance) {
                minDistance = distance;
                fib.nearestFibLevel = fibNames[i];
                fib.distanceToNearestFib = distance;
            }
        }

        // Check if at key Fibonacci level (within 0.2% of range)
        double tolerance = range * 0.002;
        fib.isAtKeyFibLevel = minDistance <= tolerance;

        // Determine Fibonacci bias
        if (fib.isUptrend) {
            // In uptrend, expect bounces at fib levels
            if (currentPrice <= fib.level_618 && currentPrice >= fib.level_786) {
                fib.fibBias = TREND_BULLISH; // Golden zone for buying
            } else if (currentPrice <= fib.level_382 && currentPrice >= fib.level_500) {
                fib.fibBias = TREND_BULLISH; // Shallow retracement, bullish
            } else if (currentPrice < fib.level_100) {
                fib.fibBias = TREND_BEARISH; // Broke below swing low
            } else {
                fib.fibBias = TREND_NEUTRAL;
            }
        } else {
            // In downtrend, expect rejections at fib levels
            if (currentPrice >= fib.level_618 && currentPrice <= fib.level_786) {
                fib.fibBias = TREND_BEARISH; // Golden zone for selling
            } else if (currentPrice >= fib.level_382 && currentPrice <= fib.level_500) {
                fib.fibBias = TREND_BEARISH; // Shallow retracement, bearish
            } else if (currentPrice > fib.level_100) {
                fib.fibBias = TREND_BULLISH; // Broke above swing high
            } else {
                fib.fibBias = TREND_NEUTRAL;
            }
        }

        // Identify confluence zones (where multiple levels are close)
        identifyConfluenceZones(fib);

        logger.debug("Fibonacci - Swing H: {}, L: {}, Trend: {}, Nearest: {} ({}pts away), Bias: {}",
                fib.swingHigh, fib.swingLow, fib.isUptrend ? "UP" : "DOWN",
                fib.nearestFibLevel, fib.distanceToNearestFib, fib.fibBias);

        return fib;
    }

    /**
     * Identify confluence zones where multiple support/resistance levels align
     */
    private void identifyConfluenceZones(FibonacciLevels fib) {
        // For now, key confluence is 50% and 61.8% zone (golden pocket)
        double goldenPocketMid = (fib.level_500 + fib.level_618) / 2;
        fib.confluenceZones.add(goldenPocketMid);

        // 38.2% is also important
        fib.confluenceZones.add(fib.level_382);
    }

    // ===================== Technical Levels Analysis =====================

    /**
     * Comprehensive technical levels analysis combining channel and Fibonacci
     */
    private TechnicalLevelsAnalysis analyzeTechnicalLevels(List<CandleStick> candles, double currentPrice,
                                                           SmartMoneyAnalysis smcAnalysis,
                                                           DayHighLowLevels dayLevels,
                                                           OptionChainAnalysis ocAnalysis) {
        TechnicalLevelsAnalysis analysis = new TechnicalLevelsAnalysis();

        // Detect channel pattern
        analysis.channel = detectChannelPattern(candles, currentPrice);

        // Calculate Fibonacci levels
        analysis.fibonacci = calculateFibonacciLevels(candles, currentPrice);

        // Collect all support levels
        if (analysis.channel.lowerChannelLine > 0) {
            analysis.allSupportLevels.add(analysis.channel.lowerChannelLine);
        }
        if (analysis.fibonacci.level_382 > 0 && analysis.fibonacci.level_382 < currentPrice) {
            analysis.allSupportLevels.add(analysis.fibonacci.level_382);
        }
        if (analysis.fibonacci.level_500 > 0 && analysis.fibonacci.level_500 < currentPrice) {
            analysis.allSupportLevels.add(analysis.fibonacci.level_500);
        }
        if (analysis.fibonacci.level_618 > 0 && analysis.fibonacci.level_618 < currentPrice) {
            analysis.allSupportLevels.add(analysis.fibonacci.level_618);
        }
        if (smcAnalysis != null && smcAnalysis.nearestBullishOB > 0) {
            analysis.allSupportLevels.add(smcAnalysis.nearestBullishOB);
        }
        if (dayLevels != null && dayLevels.s1 > 0) {
            analysis.allSupportLevels.add(dayLevels.s1);
        }
        if (ocAnalysis != null && ocAnalysis.support != null) {
            analysis.allSupportLevels.add(ocAnalysis.support);
        }

        // Collect all resistance levels
        if (analysis.channel.upperChannelLine > 0) {
            analysis.allResistanceLevels.add(analysis.channel.upperChannelLine);
        }
        if (analysis.fibonacci.level_382 > 0 && analysis.fibonacci.level_382 > currentPrice) {
            analysis.allResistanceLevels.add(analysis.fibonacci.level_382);
        }
        if (analysis.fibonacci.level_500 > 0 && analysis.fibonacci.level_500 > currentPrice) {
            analysis.allResistanceLevels.add(analysis.fibonacci.level_500);
        }
        if (analysis.fibonacci.level_618 > 0 && analysis.fibonacci.level_618 > currentPrice) {
            analysis.allResistanceLevels.add(analysis.fibonacci.level_618);
        }
        if (smcAnalysis != null && smcAnalysis.nearestBearishOB > 0) {
            analysis.allResistanceLevels.add(smcAnalysis.nearestBearishOB);
        }
        if (dayLevels != null && dayLevels.r1 > 0) {
            analysis.allResistanceLevels.add(dayLevels.r1);
        }
        if (ocAnalysis != null && ocAnalysis.resistance != null) {
            analysis.allResistanceLevels.add(ocAnalysis.resistance);
        }

        // Find strongest (nearest) support and resistance
        analysis.strongestSupport = analysis.allSupportLevels.stream()
                .filter(s -> s < currentPrice)
                .max(Double::compareTo)
                .orElse(0.0);

        analysis.strongestResistance = analysis.allResistanceLevels.stream()
                .filter(r -> r > currentPrice)
                .min(Double::compareTo)
                .orElse(0.0);

        // Determine overall bias
        int bullishScore = 0;
        int bearishScore = 0;

        if (TREND_BULLISH.equals(analysis.channel.channelBias)) bullishScore += 2;
        if (TREND_BEARISH.equals(analysis.channel.channelBias)) bearishScore += 2;
        if (TREND_BULLISH.equals(analysis.fibonacci.fibBias)) bullishScore += 2;
        if (TREND_BEARISH.equals(analysis.fibonacci.fibBias)) bearishScore += 2;

        if (bullishScore > bearishScore) {
            analysis.overallBias = TREND_BULLISH;
        } else if (bearishScore > bullishScore) {
            analysis.overallBias = TREND_BEARISH;
        } else {
            analysis.overallBias = TREND_NEUTRAL;
        }

        analysis.analysisConfidence = 50 + Math.abs(bullishScore - bearishScore) * 10;

        return analysis;
    }

    // ===================== Trade Setup Generation =====================

    /**
     * Generate complete trade setup with entry, targets, and stop-loss
     */
    private TradeSetup generateTradeSetup(double currentPrice,
                                           SmartMoneyAnalysis smcAnalysis,
                                           TechnicalLevelsAnalysis techLevels,
                                           OptionChainAnalysis ocAnalysis,
                                           GreeksAnalysis greeksAnalysis,
                                           ATMStrikeAnalysis atmAnalysis,
                                           double atr) {
        TradeSetup setup = new TradeSetup();
        setup.validUntil = LocalDateTime.now().plusMinutes(15); // Valid for 15 minutes

        // Determine trade direction based on confluence of signals
        int bullishSignals = 0;
        int bearishSignals = 0;

        // SMC signals
        if (smcAnalysis != null) {
            if (TREND_BULLISH.equals(smcAnalysis.smcBias)) bullishSignals += 2;
            if (TREND_BEARISH.equals(smcAnalysis.smcBias)) bearishSignals += 2;
            if (smcAnalysis.isInOrderBlock) {
                for (OrderBlock ob : smcAnalysis.bullishOrderBlocks) {
                    if (ob.containsPrice(currentPrice)) {
                        bullishSignals += 3; // Strong bullish signal
                        break;
                    }
                }
                for (OrderBlock ob : smcAnalysis.bearishOrderBlocks) {
                    if (ob.containsPrice(currentPrice)) {
                        bearishSignals += 3; // Strong bearish signal
                        break;
                    }
                }
            }
        }

        // Technical levels signals
        if (techLevels != null) {
            if (TREND_BULLISH.equals(techLevels.overallBias)) bullishSignals++;
            if (TREND_BEARISH.equals(techLevels.overallBias)) bearishSignals++;

            // Channel signals
            if (techLevels.channel != null) {
                if (techLevels.channel.isNearLowerBoundary && !techLevels.channel.isPriceBreakingDown) {
                    bullishSignals += 2; // Bounce from channel support
                }
                if (techLevels.channel.isNearUpperBoundary && !techLevels.channel.isPriceBreakingUp) {
                    bearishSignals += 2; // Rejection from channel resistance
                }
                if (techLevels.channel.isPriceBreakingUp) bullishSignals += 2;
                if (techLevels.channel.isPriceBreakingDown) bearishSignals += 2;
            }

            // Fibonacci signals
            if (techLevels.fibonacci != null && techLevels.fibonacci.isAtKeyFibLevel) {
                if (TREND_BULLISH.equals(techLevels.fibonacci.fibBias)) bullishSignals += 2;
                if (TREND_BEARISH.equals(techLevels.fibonacci.fibBias)) bearishSignals += 2;
            }
        }

        // Option chain signals
        if (ocAnalysis != null) {
            if (TREND_BULLISH.equals(ocAnalysis.bias)) bullishSignals++;
            if (TREND_BEARISH.equals(ocAnalysis.bias)) bearishSignals++;
        }

        // Greeks signals
        if (greeksAnalysis != null) {
            if (TREND_BULLISH.equals(greeksAnalysis.overallGreeksBias)) bullishSignals++;
            if (TREND_BEARISH.equals(greeksAnalysis.overallGreeksBias)) bearishSignals++;
        }

        // Determine direction and confidence
        if (bullishSignals >= bearishSignals + 3) {
            setup.tradeDirection = "BUY";
            setup.confidence = Math.min(90, 50 + (bullishSignals - bearishSignals) * 5);
        } else if (bearishSignals >= bullishSignals + 3) {
            setup.tradeDirection = "SELL";
            setup.confidence = Math.min(90, 50 + (bearishSignals - bullishSignals) * 5);
        } else {
            setup.tradeDirection = "NONE";
            setup.isValid = false;
            setup.invalidReason = "No clear directional bias. Bullish: " + bullishSignals + ", Bearish: " + bearishSignals;
            return setup;
        }

        // Calculate entry, targets, and stop-loss based on direction
        if ("BUY".equals(setup.tradeDirection)) {
            generateBuySetup(setup, currentPrice, smcAnalysis, techLevels, ocAnalysis, atmAnalysis, atr);
        } else {
            generateSellSetup(setup, currentPrice, smcAnalysis, techLevels, ocAnalysis, atmAnalysis, atr);
        }

        // Calculate risk-reward
        if (setup.stopLoss > 0 && setup.entryPrice > 0) {
            setup.riskPoints = Math.abs(setup.entryPrice - setup.stopLoss);
            setup.rewardPoints1 = Math.abs(setup.target1 - setup.entryPrice);
            setup.riskRewardRatio1 = setup.riskPoints > 0 ? setup.rewardPoints1 / setup.riskPoints : 0;
            setup.riskRewardRatio2 = setup.riskPoints > 0 ? Math.abs(setup.target2 - setup.entryPrice) / setup.riskPoints : 0;

            // Reject if risk-reward is too low
            if (setup.riskRewardRatio1 < 1.0) {
                setup.isValid = false;
                setup.invalidReason = String.format("Risk-Reward ratio too low: %.2f", setup.riskRewardRatio1);
            }
        }

        logger.info("Trade Setup Generated: {}", setup);

        return setup;
    }

    /**
     * Generate BUY trade setup details
     */
    private void generateBuySetup(TradeSetup setup, double currentPrice,
                                   SmartMoneyAnalysis smcAnalysis,
                                   TechnicalLevelsAnalysis techLevels,
                                   OptionChainAnalysis ocAnalysis,
                                   ATMStrikeAnalysis atmAnalysis,
                                   double atr) {
        // Entry price - slightly below current for limit order
        setup.entryPrice = round(currentPrice - atr * 0.1);
        setup.entryType = "LIMIT";

        // Determine entry reason
        StringBuilder entryReason = new StringBuilder("BUY signal: ");
        if (smcAnalysis != null && smcAnalysis.isInOrderBlock) {
            entryReason.append("In bullish order block. ");
            setup.setupType = "ORDER_BLOCK";
        } else if (techLevels != null && techLevels.channel.isNearLowerBoundary) {
            entryReason.append("Near channel support at ").append(round(techLevels.channel.lowerChannelLine)).append(". ");
            setup.setupType = "CHANNEL_BOUNCE";
        } else if (techLevels != null && techLevels.fibonacci.isAtKeyFibLevel) {
            entryReason.append("At ").append(techLevels.fibonacci.nearestFibLevel).append(" Fib level. ");
            setup.setupType = "FIB_RETRACEMENT";
        } else {
            entryReason.append("Multiple bullish confluences. ");
            setup.setupType = "CONFLUENCE";
        }
        setup.entryReason = entryReason.toString();

        // Stop-loss - below nearest strong support or order block
        double slLevel = currentPrice - atr * 1.5; // Default: 1.5x ATR

        if (techLevels != null && techLevels.strongestSupport > 0) {
            slLevel = Math.min(slLevel, techLevels.strongestSupport - atr * 0.3);
        }
        if (smcAnalysis != null && smcAnalysis.nearestBullishOB > 0) {
            // SL below order block
            for (OrderBlock ob : smcAnalysis.bullishOrderBlocks) {
                if (ob.topPrice < currentPrice) {
                    slLevel = Math.max(slLevel, ob.bottomPrice - atr * 0.2);
                    break;
                }
            }
        }
        setup.stopLoss = round(slLevel);
        setup.stopLossReason = "Below nearest support/order block";
        setup.trailingStopDistance = round(atr * 0.8);

        // Targets
        // Target 1: Nearest resistance or 1:1 RR
        double target1 = currentPrice + (currentPrice - slLevel); // 1:1 RR
        if (techLevels != null && techLevels.strongestResistance > 0) {
            target1 = Math.min(target1, techLevels.strongestResistance);
        }
        setup.target1 = round(target1);

        // Target 2: Next resistance or 2:1 RR
        double target2 = currentPrice + (currentPrice - slLevel) * 2; // 2:1 RR
        if (techLevels != null && techLevels.channel.upperChannelLine > 0) {
            target2 = Math.max(target1, techLevels.channel.upperChannelLine);
        }
        if (techLevels != null && techLevels.fibonacci.ext_1272 > currentPrice) {
            target2 = Math.max(target2, techLevels.fibonacci.ext_1272);
        }
        setup.target2 = round(target2);

        // Target 3: Extension target
        double target3 = currentPrice + (currentPrice - slLevel) * 3; // 3:1 RR
        if (techLevels != null && techLevels.fibonacci.ext_1618 > currentPrice) {
            target3 = Math.max(target3, techLevels.fibonacci.ext_1618);
        }
        setup.target3 = round(target3);

        setup.targetReason = String.format("T1: 1:1 RR/Nearest resistance, T2: Channel upper/Fib 127.2%%, T3: Fib 161.8%%");

        // Option suggestion
        if (atmAnalysis != null) {
            setup.suggestedOptionType = "CE";
            setup.suggestedStrike = atmAnalysis.atmStrike;
            setup.optionStrategy = "BUY_CE";
        }
    }

    /**
     * Generate SELL trade setup details
     */
    private void generateSellSetup(TradeSetup setup, double currentPrice,
                                    SmartMoneyAnalysis smcAnalysis,
                                    TechnicalLevelsAnalysis techLevels,
                                    OptionChainAnalysis ocAnalysis,
                                    ATMStrikeAnalysis atmAnalysis,
                                    double atr) {
        // Entry price - slightly above current for limit order
        setup.entryPrice = round(currentPrice + atr * 0.1);
        setup.entryType = "LIMIT";

        // Determine entry reason
        StringBuilder entryReason = new StringBuilder("SELL signal: ");
        if (smcAnalysis != null && smcAnalysis.isInOrderBlock) {
            entryReason.append("In bearish order block. ");
            setup.setupType = "ORDER_BLOCK";
        } else if (techLevels != null && techLevels.channel.isNearUpperBoundary) {
            entryReason.append("Near channel resistance at ").append(round(techLevels.channel.upperChannelLine)).append(". ");
            setup.setupType = "CHANNEL_REJECTION";
        } else if (techLevels != null && techLevels.fibonacci.isAtKeyFibLevel) {
            entryReason.append("At ").append(techLevels.fibonacci.nearestFibLevel).append(" Fib level. ");
            setup.setupType = "FIB_RETRACEMENT";
        } else {
            entryReason.append("Multiple bearish confluences. ");
            setup.setupType = "CONFLUENCE";
        }
        setup.entryReason = entryReason.toString();

        // Stop-loss - above nearest strong resistance or order block
        double slLevel = currentPrice + atr * 1.5; // Default: 1.5x ATR

        if (techLevels != null && techLevels.strongestResistance > 0) {
            slLevel = Math.max(slLevel, techLevels.strongestResistance + atr * 0.3);
        }
        if (smcAnalysis != null && smcAnalysis.nearestBearishOB > 0) {
            // SL above order block
            for (OrderBlock ob : smcAnalysis.bearishOrderBlocks) {
                if (ob.bottomPrice > currentPrice) {
                    slLevel = Math.min(slLevel, ob.topPrice + atr * 0.2);
                    break;
                }
            }
        }
        setup.stopLoss = round(slLevel);
        setup.stopLossReason = "Above nearest resistance/order block";
        setup.trailingStopDistance = round(atr * 0.8);

        // Targets
        // Target 1: Nearest support or 1:1 RR
        double target1 = currentPrice - (slLevel - currentPrice); // 1:1 RR
        if (techLevels != null && techLevels.strongestSupport > 0) {
            target1 = Math.max(target1, techLevels.strongestSupport);
        }
        setup.target1 = round(target1);

        // Target 2: Next support or 2:1 RR
        double target2 = currentPrice - (slLevel - currentPrice) * 2; // 2:1 RR
        if (techLevels != null && techLevels.channel.lowerChannelLine > 0) {
            target2 = Math.min(target2, techLevels.channel.lowerChannelLine);
        }
        if (techLevels != null && techLevels.fibonacci.ext_1272 < currentPrice) {
            target2 = Math.min(target2, techLevels.fibonacci.ext_1272);
        }
        setup.target2 = round(target2);

        // Target 3: Extension target
        double target3 = currentPrice - (slLevel - currentPrice) * 3; // 3:1 RR
        if (techLevels != null && techLevels.fibonacci.ext_1618 < currentPrice) {
            target3 = Math.min(target3, techLevels.fibonacci.ext_1618);
        }
        setup.target3 = round(target3);

        setup.targetReason = String.format("T1: 1:1 RR/Nearest support, T2: Channel lower/Fib 127.2%%, T3: Fib 161.8%%");

        // Option suggestion
        if (atmAnalysis != null) {
            setup.suggestedOptionType = "PE";
            setup.suggestedStrike = atmAnalysis.atmStrike;
            setup.optionStrategy = "BUY_PE";
        }
    }

    /**
     * Round to 2 decimal places
     */
    private double round(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    // ===================== Inner Classes =====================

    @Override
    public Double getLTPForToken(Long token) {
        if (token == null) return null;
        if (kiteTickerProvider == null) {
            logger.debug("kiteTickerProvider is null in getLTPForToken");
            return null;
        }
        try {
            Tick tick = kiteTickerProvider.tickerMapForJob.get(token);
            if (tick != null) {
                double ltp = tick.getLastTradedPrice();
                if (ltp > 0) return ltp;
            }
            return null;
        } catch (Exception e) {
            logger.warn("getLTPForToken error for token {}: {}", token, e.getMessage());
            return null;
        }
    }

    @Override
    public Long getTickTimestampForToken(Long token) {
        if (token == null) return null;
        if (kiteTickerProvider == null) return null;
        try {
            Tick tick = kiteTickerProvider.tickerMapForJob.get(token);
            if (tick != null) {
                Date ts = tick.getTickTimestamp();
                if (ts != null) return ts.toInstant().toEpochMilli();
            }
            return null;
        } catch (Exception e) {
            logger.warn("getTickTimestampForToken error for token {}: {}", token, e.getMessage());
            return null;
        }
    }

    /**
     * Container for technical indicators
     */
    private static class TechnicalIndicators {
        double ema9 = 0;
        double ema21 = 0;
        double rsi = 50;
        double atr = 10;
        double vwap = 0;
        double momentum = 0;
        double swingHigh = 0;
        double swingLow = 0;
        String emaCrossover = "NEUTRAL";
    }

    /**
     * Container for option chain analysis results
     */
    private static class OptionChainAnalysis {
        double pcr = 1.0;
        double callOIChange = 0;
        double putOIChange = 0;
        Integer maxPainStrike = null;
        Double support = null;
        Double resistance = null;
        Double currentLTP = null;
        Double currentDayHigh = null;
        Double currentDayLow = null;
        String bias = TREND_NEUTRAL;
        ATMStrikeAnalysis atmAnalysis = null;
        DayHighLowLevels dayLevels = null;
        GreeksAnalysis greeksAnalysis = null;  // VIX, GEX, Theta analysis
    }

    /**
     * Container for ATM strike price analysis
     * Analyzes CE and PE behavior at the At-The-Money strike
     */
    private static class ATMStrikeAnalysis {
        int atmStrike = 0;                    // ATM strike price
        double atmCELTP = 0;                  // ATM Call LTP
        double atmPELTP = 0;                  // ATM Put LTP
        double cePeDifference = 0;            // CE - PE difference (positive = CE expensive)
        double cePeRatio = 1.0;               // CE/PE ratio
        double atmCEOI = 0;                   // ATM Call Open Interest
        double atmPEOI = 0;                   // ATM Put Open Interest
        double atmCEOIChange = 0;             // ATM Call OI Change
        double atmPEOIChange = 0;             // ATM Put OI Change
        double atmPCR = 1.0;                  // ATM level PCR
        String atmBias = TREND_NEUTRAL;       // Bias based on ATM analysis
        double syntheticFuture = 0;           // Synthetic future price (Spot + CE - PE)
        double ivSkew = 0;                    // Implied volatility skew indicator
    }

    /**
     * Container for current and previous day high/low levels
     */
    private static class DayHighLowLevels {
        double currentDayHigh = 0;
        double currentDayLow = 0;
        double previousDayHigh = 0;
        double previousDayLow = 0;
        double previousDayClose = 0;
        double previousDayOpen = 0;
        boolean isAbovePreviousDayHigh = false;
        boolean isBelowPreviousDayLow = false;
        boolean isInPreviousDayRange = false;
        double pivotPoint = 0;                // Classic pivot point
        double r1 = 0;                        // Resistance 1
        double r2 = 0;                        // Resistance 2
        double s1 = 0;                        // Support 1
        double s2 = 0;                        // Support 2
        String dayLevelBias = TREND_NEUTRAL;
    }

    /**
     * Container for VIX, Gamma Exposure (GEX), and Theta decay analysis
     * Critical for understanding market volatility and option dynamics
     */
    private static class GreeksAnalysis {
        // VIX (India Volatility Index) Analysis
        double vixValue = 0;                  // Current India VIX value
        double vixChange = 0;                 // VIX change from previous close
        double vixPercentChange = 0;          // VIX percentage change
        String vixTrend = TREND_NEUTRAL;      // VIX trend: RISING, FALLING, STABLE
        boolean isHighVix = false;            // VIX > 20 considered high
        boolean isLowVix = false;             // VIX < 12 considered low
        boolean isVixSpike = false;           // Sudden VIX increase > 10%

        // Gamma Exposure (GEX) Analysis
        double netGEX = 0;                    // Net Gamma Exposure (Call GEX - Put GEX)
        double callGEX = 0;                   // Total Call Gamma Exposure
        double putGEX = 0;                    // Total Put Gamma Exposure
        double gexRatio = 1.0;                // Call GEX / Put GEX ratio
        int gexFlipLevel = 0;                 // Price level where GEX flips sign
        boolean isPositiveGEX = true;         // Positive GEX = mean reversion, Negative = trending
        String gexBias = TREND_NEUTRAL;       // Bias based on GEX analysis
        double highGammaStrike = 0;           // Strike with highest gamma concentration

        // Theta Decay Analysis
        double totalTheta = 0;                // Total theta exposure across option chain
        double callTheta = 0;                 // Total Call theta
        double putTheta = 0;                  // Total Put theta
        double atmTheta = 0;                  // ATM theta (most impacted by time decay)
        double thetaDecayRate = 0;            // Rate of theta decay per hour
        double daysToExpiry = 0;              // Days to nearest expiry
        boolean isExpiryDay = false;          // Is today expiry day?
        boolean isNearExpiry = false;         // Less than 2 days to expiry
        String thetaBias = TREND_NEUTRAL;     // Bias based on theta analysis

        // Combined Greeks Bias
        String overallGreeksBias = TREND_NEUTRAL;
        double greeksConfidence = 50.0;       // Confidence based on Greeks alignment
    }

    /**
     * Container for Price Action Analysis
     * Includes candlestick patterns, market structure (HH/HL/LH/LL), and trend analysis
     */
    private static class PriceActionAnalysis {
        // Market Structure
        String marketStructure = TREND_NEUTRAL;    // BULLISH (HH+HL), BEARISH (LH+LL), RANGING
        int higherHighs = 0;                        // Count of higher highs
        int higherLows = 0;                         // Count of higher lows
        int lowerHighs = 0;                         // Count of lower highs
        int lowerLows = 0;                          // Count of lower lows
        double lastSwingHigh = 0;                   // Last swing high price
        double lastSwingLow = 0;                    // Last swing low price
        boolean isTrendingUp = false;
        boolean isTrendingDown = false;
        boolean isConsolidating = false;

        // Candlestick Patterns
        String dominantPattern = "NONE";           // ENGULFING_BULLISH, ENGULFING_BEARISH, DOJI, etc.
        double patternStrength = 0;                 // 0-100 pattern reliability
        boolean hasBullishEngulfing = false;
        boolean hasBearishEngulfing = false;
        boolean hasMorningStar = false;
        boolean hasEveningStar = false;
        boolean hasHammer = false;
        boolean hasShootingStar = false;
        boolean hasDoji = false;
        boolean hasThreeWhiteSoldiers = false;
        boolean hasThreeBlackCrows = false;
        boolean hasBullishPinBar = false;
        boolean hasBearishPinBar = false;

        // Momentum Analysis
        double recentMomentum = 0;                  // Positive = bullish momentum, Negative = bearish
        double momentumStrength = 0;                // 0-100 strength indicator
        boolean isMomentumIncreasing = false;
        boolean isMomentumDecreasing = false;
        int consecutiveBullishCandles = 0;
        int consecutiveBearishCandles = 0;

        // Body/Wick Analysis
        double averageBodySize = 0;
        double averageWickSize = 0;
        double bodyToWickRatio = 0;                 // High = strong moves, Low = indecision
        boolean hasLongUpperWicks = false;          // Rejection from highs
        boolean hasLongLowerWicks = false;          // Rejection from lows

        // Price Action Bias
        String priceActionBias = TREND_NEUTRAL;
        double priceActionConfidence = 50.0;
    }

    /**
     * Container for Order Block detection
     * Order blocks are areas where institutional traders placed large orders
     */
    private static class OrderBlock {
        boolean isBullish;                          // True = Bullish OB (demand), False = Bearish OB (supply)
        double topPrice;                            // Top of order block zone
        double bottomPrice;                         // Bottom of order block zone
        double midPrice;                            // Middle of order block
        LocalDateTime createdAt;                    // When the OB was formed
        int strength;                               // 1-3 (1=weak, 2=medium, 3=strong)
        boolean isMitigated;                        // Has price returned to this OB?
        boolean isBroken;                           // Has price broken through?
        int touchCount;                             // How many times price has touched this OB
        double volumeIndicator;                     // Relative volume at OB formation

        public OrderBlock(boolean isBullish, double top, double bottom, LocalDateTime created) {
            this.isBullish = isBullish;
            this.topPrice = top;
            this.bottomPrice = bottom;
            this.midPrice = (top + bottom) / 2;
            this.createdAt = created;
            this.strength = 1;
            this.isMitigated = false;
            this.isBroken = false;
            this.touchCount = 0;
            this.volumeIndicator = 1.0;
        }

        public boolean containsPrice(double price) {
            return price >= bottomPrice && price <= topPrice;
        }
    }

    /**
     * Container for Fair Value Gap (FVG) detection
     * FVGs are imbalances in price that tend to get filled
     */
    private static class FairValueGap {
        boolean isBullish;                          // True = Bullish FVG (gap up), False = Bearish FVG (gap down)
        double topPrice;                            // Top of the gap
        double bottomPrice;                         // Bottom of the gap
        double gapSize;                             // Size of the gap in points
        LocalDateTime createdAt;                    // When the FVG was created
        boolean isFilled;                           // Has the gap been filled?
        boolean isPartiallyFilled;                  // Has price entered the gap?
        double fillPercentage;                      // How much of the gap has been filled (0-100)
        int candlesSinceCreation;                   // Number of candles since FVG was created

        public FairValueGap(boolean isBullish, double top, double bottom, LocalDateTime created) {
            this.isBullish = isBullish;
            this.topPrice = top;
            this.bottomPrice = bottom;
            this.gapSize = Math.abs(top - bottom);
            this.createdAt = created;
            this.isFilled = false;
            this.isPartiallyFilled = false;
            this.fillPercentage = 0;
            this.candlesSinceCreation = 0;
        }

        public double getMidPoint() {
            return (topPrice + bottomPrice) / 2;
        }
    }

    /**
     * Container for combined Smart Money Concepts (SMC) analysis
     */
    private static class SmartMoneyAnalysis {
        PriceActionAnalysis priceAction;
        List<OrderBlock> bullishOrderBlocks = new ArrayList<>();
        List<OrderBlock> bearishOrderBlocks = new ArrayList<>();
        List<FairValueGap> bullishFVGs = new ArrayList<>();
        List<FairValueGap> bearishFVGs = new ArrayList<>();

        // Key levels
        double nearestBullishOB = 0;                // Nearest bullish order block below current price
        double nearestBearishOB = 0;                // Nearest bearish order block above current price
        double nearestUnfilledBullishFVG = 0;       // Nearest unfilled bullish FVG
        double nearestUnfilledBearishFVG = 0;       // Nearest unfilled bearish FVG
        double nearestFVGTarget = 0;                // Most likely FVG target for price

        // Analysis results
        boolean isInOrderBlock = false;
        boolean isNearOrderBlock = false;
        boolean hasUnfilledFVGAbove = false;
        boolean hasUnfilledFVGBelow = false;
        int unfilledFVGCount = 0;

        // Smart Money Bias
        String smcBias = TREND_NEUTRAL;
        double smcConfidence = 50.0;
        String tradeSuggestion = "WAIT";           // BUY, SELL, WAIT
        String tradeSuggestionReason = "";

        // Enhanced trade setup with entry, target, stoploss
        TradeSetup tradeSetup = null;
    }

    /**
     * Container for Channel Pattern detection
     * Identifies ascending, descending, and horizontal channels
     */
    private static class ChannelPattern {
        String channelType = "NONE";              // ASCENDING, DESCENDING, HORIZONTAL, NONE
        double upperChannelLine = 0;              // Upper channel boundary
        double lowerChannelLine = 0;              // Lower channel boundary
        double channelWidth = 0;                  // Width of the channel
        double channelSlope = 0;                  // Slope of the channel (positive = ascending)
        double currentPositionInChannel = 0;      // 0 = at lower, 1 = at upper, 0.5 = middle
        boolean isNearUpperBoundary = false;
        boolean isNearLowerBoundary = false;
        boolean isPriceBreakingUp = false;
        boolean isPriceBreakingDown = false;
        int channelTouches = 0;                   // Number of times price touched boundaries
        double channelStrength = 0;               // 0-100 reliability score
        String channelBias = TREND_NEUTRAL;

        // Projected levels
        double projectedUpperTarget = 0;          // Next upper channel target
        double projectedLowerTarget = 0;          // Next lower channel target
    }

    /**
     * Container for Fibonacci Retracement analysis
     * Calculates key Fibonacci levels from swing high to swing low
     */
    private static class FibonacciLevels {
        double swingHigh = 0;                     // Recent swing high
        double swingLow = 0;                      // Recent swing low
        boolean isUptrend = false;                // True if retracing from high to low

        // Key Fibonacci retracement levels
        double level_0 = 0;                       // 0% - Start of move
        double level_236 = 0;                     // 23.6% retracement
        double level_382 = 0;                     // 38.2% retracement (golden ratio)
        double level_500 = 0;                     // 50% retracement
        double level_618 = 0;                     // 61.8% retracement (golden ratio)
        double level_786 = 0;                     // 78.6% retracement
        double level_100 = 0;                     // 100% - Full retracement

        // Fibonacci extension levels (for targets)
        double ext_1272 = 0;                      // 127.2% extension
        double ext_1618 = 0;                      // 161.8% extension (golden ratio)
        double ext_2000 = 0;                      // 200% extension
        double ext_2618 = 0;                      // 261.8% extension

        // Current position analysis
        double currentPrice = 0;
        String nearestFibLevel = "";              // Which fib level is price nearest to
        double distanceToNearestFib = 0;
        boolean isAtKeyFibLevel = false;          // Within 0.2% of a key level
        String fibBias = TREND_NEUTRAL;

        // Confluence zones (where multiple fibs align)
        List<Double> confluenceZones = new ArrayList<>();
    }

    /**
     * Container for complete Trade Setup with entry, target, and stop-loss
     */
    private static class TradeSetup {
        String tradeDirection = "NONE";           // BUY, SELL, NONE
        String setupType = "";                    // e.g., "FIB_RETRACEMENT", "CHANNEL_BOUNCE", "ORDER_BLOCK"
        double confidence = 0;                    // 0-100 confidence score

        // Entry details
        double entryPrice = 0;                    // Suggested entry price
        String entryType = "LIMIT";               // LIMIT, MARKET, STOP
        String entryReason = "";                  // Why this entry

        // Target details
        double target1 = 0;                       // First target (conservative)
        double target2 = 0;                       // Second target (moderate)
        double target3 = 0;                       // Third target (aggressive)
        String targetReason = "";                 // Why these targets

        // Stop-loss details
        double stopLoss = 0;                      // Stop-loss price
        double trailingStopDistance = 0;          // Points for trailing stop
        String stopLossReason = "";               // Why this stop-loss

        // Risk-Reward analysis
        double riskPoints = 0;                    // Entry - StopLoss
        double rewardPoints1 = 0;                 // Target1 - Entry
        double riskRewardRatio1 = 0;              // Reward1 / Risk
        double riskRewardRatio2 = 0;              // Reward2 / Risk

        // Option trading specific
        String suggestedOptionType = "";          // CE, PE
        int suggestedStrike = 0;                  // Suggested strike price
        String optionStrategy = "";               // e.g., "BUY_CE", "BUY_PE", "SELL_CE", "SELL_PE"

        // Validity
        LocalDateTime validUntil = null;          // Setup validity time
        boolean isValid = true;
        String invalidReason = "";

        @Override
        public String toString() {
            if ("NONE".equals(tradeDirection)) {
                return "No trade setup - " + invalidReason;
            }
            return String.format("%s | Entry: %.2f | SL: %.2f | T1: %.2f | T2: %.2f | RR: %.2f | %s",
                    tradeDirection, entryPrice, stopLoss, target1, target2, riskRewardRatio1, setupType);
        }
    }

    /**
     * Container for combined Technical Levels analysis
     */
    private static class TechnicalLevelsAnalysis {
        ChannelPattern channel;
        FibonacciLevels fibonacci;
        TradeSetup tradeSetup;

        // Combined key levels
        List<Double> allSupportLevels = new ArrayList<>();
        List<Double> allResistanceLevels = new ArrayList<>();
        double strongestSupport = 0;
        double strongestResistance = 0;

        String overallBias = TREND_NEUTRAL;
        double analysisConfidence = 50.0;
    }
}

