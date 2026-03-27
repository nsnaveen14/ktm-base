package com.trading.kalyani.KTManager.utilities;

import com.trading.kalyani.KTManager.entity.CandleStick;
import com.trading.kalyani.KTManager.repository.CandleStickRepository;
import com.trading.kalyani.KTManager.service.InternalOrderBlockService;
import com.trading.kalyani.KTManager.service.IOBAutoTradeService;
import com.zerodhatech.kiteconnect.kitehttp.exceptions.KiteException;
import com.zerodhatech.models.Tick;
import com.zerodhatech.ticker.KiteTicker;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;
import static com.trading.kalyani.KTManager.utilities.DateUtilities.convertToLocalDateTime;

public class KiteTickerProvider extends KiteTicker {

    private final CandleStickRepository candleStickRepository;

    @Setter
    private InternalOrderBlockService iobService;

    @Setter
    private IOBAutoTradeService iobAutoTradeService;

    public final List<Long> tickerTokens = Collections.synchronizedList(new ArrayList<>());

    public Map<Long, Tick> tickerMapForJob = new ConcurrentHashMap<>();

    public Double niftyLastPrice;

    public Double vixLastPrice;

    public LocalDateTime lastNifty50TS;

    private static final Logger logger = LogManager.getLogger(KiteTickerProvider.class);

    private static final int RECONNECT_MAX_RETRIES  = 10;
    private static final int RECONNECT_INTERVAL_SEC = 30;

    public Map<String, CandleStick> candleStickMap = new ConcurrentHashMap<>();

    CandleStick lastCandleStick = new CandleStick();

    public List<CandleStick> candleSticks = new ArrayList<>();

    private CountDownLatch connectionLatch;

    private volatile boolean connectionReady = false;

    public KiteTickerProvider(String accessToken, String apiKey, CandleStickRepository candleStickRepository) {
        super(accessToken, apiKey);
        this.candleStickRepository = candleStickRepository;
        connectionLatch = new CountDownLatch(1);
    }

    public boolean startTickerConnection() {
        logger.debug("Starting ticker connection, provider instance: {}", super.hashCode());
        connectionLatch = new CountDownLatch(1);
        connectionReady = false;

        try {
            configureReconnection();
            registerOnConnectedListener();
            registerTickListener();
            super.connect();

            logger.info("Waiting for WebSocket connection to be established...");
            boolean connected = connectionLatch.await(30, TimeUnit.SECONDS);
            if (!connected) {
                logger.error("Timeout waiting for WebSocket connection");
                return false;
            }

            boolean isConnected = super.isConnectionOpen() && connectionReady;
            logger.info("Connection status — isConnectionOpen: {}, connectionReady: {}", super.isConnectionOpen(), connectionReady);
            return isConnected;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while waiting for connection", e);
            return false;
        }
    }

    private void configureReconnection() {
        super.setTryReconnection(true);
        try {
            super.setMaximumRetries(RECONNECT_MAX_RETRIES);
            super.setMaximumRetryInterval(RECONNECT_INTERVAL_SEC);
        } catch (KiteException e) {
            logger.error("Error configuring reconnection: {}", e.getMessage(), e);
        }
    }

    private void registerOnConnectedListener() {
        super.setOnConnectedListener(() -> {
            try {
                logger.info("WebSocket connected — ready for subscriptions");
                connectionReady = true;
                connectionLatch.countDown();
            } catch (Exception e) {
                logger.error("Error in onConnected callback", e);
            }
        });
    }

    private void registerTickListener() {
        super.setOnTickerArrivalListener(ticks -> {
            if (ticks == null || ticks.isEmpty()) return;
            logger.debug("Received {} ticks", ticks.size());
            for (Tick tick : ticks) {
                try {
                    processTick(tick);
                } catch (Exception e) {
                    logger.error("Error processing tick for token {}: {}", tick.getInstrumentToken(), e.getMessage(), e);
                }
            }
        });
    }

    private void processTick(Tick tick) {
        tickerMapForJob.put(tick.getInstrumentToken(), tick);

        Long instrumentToken = tick.getInstrumentToken();
        Double lastPrice = tick.getLastTradedPrice();
        LocalDateTime tickTime = tick.getTickTimestamp() != null
                ? convertToLocalDateTime(tick.getTickTimestamp())
                : LocalDateTime.now(); // fallback: use server time when exchange timestamp absent

        if (instrumentToken.equals(NIFTY_INSTRUMENT_TOKEN)) {
            processCandleForNifty50(tick, NIFTY_INSTRUMENT_TOKEN);
            niftyLastPrice = lastPrice;
            processIOBAutoTrade(instrumentToken, lastPrice, tickTime);
        }
        if (instrumentToken.equals(INDIA_VIX_INSTRUMENT_TOKEN)) {
            vixLastPrice = lastPrice;
            logger.debug("VIX updated: {}", vixLastPrice);
        }
    }

    public void disconnectTicker() {
        super.unsubscribe(new ArrayList<>(tickerTokens));
        tickerTokens.clear();
        connectionReady = false;
        super.setOnDisconnectedListener(() -> logger.info("Ticker disconnected successfully"));
        super.disconnect();
    }

    public void subscribeTokenForJob(ArrayList<Long> inputTokens) {
        try {
            if (inputTokens == null || inputTokens.isEmpty()) return;

            if (!connectionReady) {
                logger.info("Waiting for connection to be ready before subscribing tokens: {}", inputTokens);
                boolean ready = connectionLatch.await(30, TimeUnit.SECONDS);
                if (!ready) {
                    logger.error("Timeout waiting for connection to be ready - cannot subscribe tokens: {}", inputTokens);
                    return;
                }
            }

            if (!super.isConnectionOpen()) {
                logger.warn("Ticker connection not open - attempting to reconnect before subscribing tokens: {}", inputTokens);
                try {
                    super.connect();
                    Thread.sleep(2000);
                    if (!super.isConnectionOpen()) {
                        logger.error("Reconnect attempted but connection still not open — cannot subscribe tokens: {}", inputTokens);
                        return;
                    }
                } catch (Exception e) {
                    logger.error("Failed to reconnect ticker before subscribing tokens: {}", e.getMessage());
                    return;
                }
            }

            ArrayList<Long> newTokens = inputTokens.stream()
                    .filter(t -> !tickerTokens.contains(t))
                    .collect(Collectors.toCollection(ArrayList::new));
            if (newTokens.isEmpty()) {
                logger.debug("All tokens already subscribed: {}", inputTokens);
                return;
            }

            logger.info("Subscribing to new tokens: {}", newTokens);
            super.subscribe(newTokens);
            super.setMode(newTokens, KiteTicker.modeFull);
            tickerTokens.addAll(newTokens);

            logger.info("subscribeTokenForJob completed for tokens: {}. Total subscribed tokens: {}", newTokens, tickerTokens.size());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while waiting to subscribe tokens: {}", inputTokens, e);
        } catch (Exception e) {
            logger.error("Error in subscribeTokenForJob for tokens {}: {}", inputTokens, e.getMessage(), e);
        }
    }

    public void unsubscribeTokensForDeltaJob(ArrayList<Long> inputTokens) {
        if (inputTokens == null || inputTokens.isEmpty()) return;
        super.unsubscribe(inputTokens);
        tickerTokens.removeAll(inputTokens);
    }

    public void unsubscribeTokensForSnapshotJob(ArrayList<Long> inputTokens) {
        if (inputTokens == null || inputTokens.isEmpty()) return;
        super.unsubscribe(inputTokens);
        tickerTokens.removeAll(inputTokens);
    }

    public void getNiftyLastPrice() {
        ArrayList<Long> niftyToken = new ArrayList<>();
        niftyToken.add(NIFTY_INSTRUMENT_TOKEN);
        super.subscribe(niftyToken);
        super.setMode(niftyToken, KiteTicker.modeFull);
    }

    public void processCandleForNifty50(Tick tick, Long instrumentToken) {
        if (!instrumentToken.equals(tick.getInstrumentToken())) return;

        LocalDateTime tickTime = convertToLocalDateTime(tick.getTickTimestamp());
        LocalDateTime minuteStart = tickTime.withSecond(0).withNano(0);
        String minuteKey = instrumentToken + "_" + minuteStart;
        double price = tick.getLastTradedPrice();

        if (!candleStickMap.containsKey(minuteKey)) {
            // Close and evict the previous candle
            if (lastCandleStick.getCandleStartTime() != null) {
                lastCandleStick = candleStickRepository.save(lastCandleStick);
                candleSticks.add(lastCandleStick);
                String prevKey = instrumentToken + "_" + lastCandleStick.getCandleStartTime();
                candleStickMap.remove(prevKey);
                logger.info("Closed and saved candle: {}", lastCandleStick);
            }

            CandleStick candleStick = new CandleStick();
            candleStick.setInstrumentToken(instrumentToken);
            candleStick.setOpenPrice(price);
            candleStick.setHighPrice(price);
            candleStick.setLowPrice(price);
            candleStick.setCandleStartTime(minuteStart);
            candleStick = candleStickRepository.save(candleStick);
            candleStickMap.put(minuteKey, candleStick);
            lastCandleStick = candleStick;
            logger.info("Opened new candle: {}", candleStick);
        } else {
            CandleStick candleStick = candleStickMap.get(minuteKey);
            candleStick.setClosePrice(price);
            if (price > candleStick.getHighPrice()) candleStick.setHighPrice(price);
            if (price < candleStick.getLowPrice())  candleStick.setLowPrice(price);
            candleStick.setCandleEndTime(tickTime);
            lastCandleStick = candleStick;
        }
    }

    private void processIOBAutoTrade(Long instrumentToken, Double currentPrice, LocalDateTime tickTime) {
        if (currentPrice == null) return;
        try {
            if (iobAutoTradeService != null) {
                iobAutoTradeService.processPriceTick(instrumentToken, currentPrice, tickTime);
            }
            if (iobService != null) {
                iobService.checkMitigation(instrumentToken, currentPrice);
                iobService.checkTargetHits(instrumentToken, currentPrice);
            }
        } catch (Exception e) {
            logger.error("Error processing IOB auto-trade tick for token {}: {}", instrumentToken, e.getMessage(), e);
        }
    }

    public boolean isMarketOpen() {
        ZoneId zone = ZoneId.of("Asia/Kolkata");
        LocalTime time = ZonedDateTime.now(zone).toLocalTime();
        return !time.isBefore(LocalTime.of(9, 0)) && !time.isAfter(LocalTime.of(15, 30));
    }
}
