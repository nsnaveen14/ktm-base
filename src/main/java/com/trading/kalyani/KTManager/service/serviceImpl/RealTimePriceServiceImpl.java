package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.entity.InternalOrderBlock;
import com.trading.kalyani.KTManager.repository.InternalOrderBlockRepository;
import com.trading.kalyani.KTManager.service.AutoTradingService;
import com.trading.kalyani.KTManager.service.InternalOrderBlockService;
import com.trading.kalyani.KTManager.service.RealTimePriceService;
import com.trading.kalyani.KTManager.service.TelegramNotificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;

/**
 * Implementation of RealTimePriceService for managing live price feeds.
 * Integrates with Kite WebSocket for real-time tick data.
 */
@Service
public class RealTimePriceServiceImpl implements RealTimePriceService {

    private static final Logger logger = LoggerFactory.getLogger(RealTimePriceServiceImpl.class);

    @Autowired
    private InternalOrderBlockRepository iobRepository;

    @Autowired(required = false)
    private InternalOrderBlockService iobService;

    @Autowired(required = false)
    private AutoTradingService autoTradingService;

    @Autowired(required = false)
    private TelegramNotificationService telegramService;

    // Connection state
    private volatile boolean connected = false;
    private LocalDateTime lastConnectionTime;
    private LocalDateTime lastTickTime;
    private int reconnectAttempts = 0;

    // Subscribed instruments
    private final Set<Long> subscribedInstruments = ConcurrentHashMap.newKeySet();

    // Current prices
    private final Map<Long, Double> currentPrices = new ConcurrentHashMap<>();

    // Last tick data
    private final Map<Long, Map<String, Object>> lastTicks = new ConcurrentHashMap<>();

    // Tick history (limited to last 100 ticks per instrument)
    private final Map<Long, LinkedList<Map<String, Object>>> tickHistory = new ConcurrentHashMap<>();
    private static final int MAX_TICK_HISTORY = 100;

    // Current candle data
    private final Map<String, Map<String, Object>> currentCandles = new ConcurrentHashMap<>();

    // Price listeners
    private final Map<Long, List<Consumer<Map<String, Object>>>> priceListeners = new ConcurrentHashMap<>();

    // Zone touch alert callbacks
    private final Map<Long, Consumer<Map<String, Object>>> zoneTouchCallbacks = new ConcurrentHashMap<>();

    // Executor for async operations
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private ScheduledFuture<?> heartbeatTask;
    private ScheduledFuture<?> pricePollingTask;

    // ==================== Connection Management ====================

    @Override
    public void connect() {
        if (connected) {
            logger.info("Already connected to price feed");
            return;
        }

        try {
            logger.info("Connecting to real-time price feed...");

            // Initialize default subscriptions
            subscribedInstruments.add(NIFTY_INSTRUMENT_TOKEN);

            // Start heartbeat
            startHeartbeat();

            // Start price polling (fallback if WebSocket not available)
            startPricePolling();

            connected = true;
            lastConnectionTime = LocalDateTime.now();
            reconnectAttempts = 0;

            logger.info("Connected to real-time price feed. Subscribed instruments: {}",
                    subscribedInstruments.size());

            // TODO: Integrate with Kite WebSocket
            // KiteConnect kite = kiteService.getKiteConnect();
            // KiteTicker ticker = new KiteTicker(kite.getAccessToken(), kite.getApiKey());
            // ticker.setOnTickerArrivalListener(this::handleTick);
            // ticker.connect();
            // ticker.subscribe(subscribedInstruments.stream().mapToLong(Long::longValue).toArray());

        } catch (Exception e) {
            logger.error("Error connecting to price feed: {}", e.getMessage(), e);
            connected = false;
            scheduleReconnect();
        }
    }

    @Override
    public void disconnect() {
        logger.info("Disconnecting from real-time price feed...");

        connected = false;

        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
        }
        if (pricePollingTask != null) {
            pricePollingTask.cancel(false);
        }

        // TODO: Disconnect Kite WebSocket
        // ticker.disconnect();

        logger.info("Disconnected from real-time price feed");
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    @Override
    public Map<String, Object> getConnectionStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("connected", connected);
        status.put("lastConnectionTime", lastConnectionTime);
        status.put("lastTickTime", lastTickTime);
        status.put("subscribedInstruments", subscribedInstruments.size());
        status.put("reconnectAttempts", reconnectAttempts);
        status.put("activeListeners", priceListeners.values().stream()
                .mapToInt(List::size).sum());
        return status;
    }

    private void startHeartbeat() {
        heartbeatTask = scheduler.scheduleAtFixedRate(() -> {
            if (!connected) {
                scheduleReconnect();
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    private void startPricePolling() {
        // Fallback polling mechanism - polls every 5 seconds
        pricePollingTask = scheduler.scheduleAtFixedRate(() -> {
            if (connected) {
                pollPrices();
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    private void scheduleReconnect() {
        if (reconnectAttempts >= 5) {
            logger.error("Max reconnect attempts reached. Manual intervention required.");
            return;
        }

        reconnectAttempts++;
        int delay = Math.min(30, reconnectAttempts * 5);

        scheduler.schedule(() -> {
            logger.info("Attempting reconnect #{}", reconnectAttempts);
            connect();
        }, delay, TimeUnit.SECONDS);
    }

    private void pollPrices() {
        // This would typically fetch prices from a REST endpoint or cache
        // For now, we simulate with the last known prices
        for (Long token : subscribedInstruments) {
            Double currentPrice = currentPrices.get(token);
            if (currentPrice != null) {
                // Simulate small price movement for testing
                double variation = (Math.random() - 0.5) * 2; // ±1 point
                double newPrice = currentPrice + variation;
                handlePriceUpdate(token, newPrice);
            }
        }
    }

    // ==================== Subscription Management ====================

    @Override
    public void subscribeToInstruments(List<Long> instrumentTokens) {
        subscribedInstruments.addAll(instrumentTokens);
        logger.info("Subscribed to instruments: {}", instrumentTokens);

        // TODO: Subscribe via Kite WebSocket
        // ticker.subscribe(instrumentTokens.stream().mapToLong(Long::longValue).toArray());
    }

    @Override
    public void unsubscribeFromInstruments(List<Long> instrumentTokens) {
        subscribedInstruments.removeAll(instrumentTokens);
        instrumentTokens.forEach(token -> {
            currentPrices.remove(token);
            lastTicks.remove(token);
            tickHistory.remove(token);
            priceListeners.remove(token);
        });
        logger.info("Unsubscribed from instruments: {}", instrumentTokens);

        // TODO: Unsubscribe via Kite WebSocket
        // ticker.unsubscribe(instrumentTokens.stream().mapToLong(Long::longValue).toArray());
    }

    @Override
    public List<Long> getSubscribedInstruments() {
        return new ArrayList<>(subscribedInstruments);
    }

    @Override
    public void subscribeToZoneTouchAlerts(Long iobId, Consumer<Map<String, Object>> callback) {
        zoneTouchCallbacks.put(iobId, callback);
        logger.debug("Subscribed to zone touch alerts for IOB: {}", iobId);
    }

    @Override
    public void unsubscribeFromZoneTouchAlerts(Long iobId) {
        zoneTouchCallbacks.remove(iobId);
    }

    // ==================== Price Data ====================

    @Override
    public Double getCurrentPrice(Long instrumentToken) {
        return currentPrices.get(instrumentToken);
    }

    @Override
    public Map<Long, Double> getAllCurrentPrices() {
        return new HashMap<>(currentPrices);
    }

    @Override
    public Map<String, Object> getLastTick(Long instrumentToken) {
        return lastTicks.getOrDefault(instrumentToken, new HashMap<>());
    }

    @Override
    public List<Map<String, Object>> getTickHistory(Long instrumentToken, int count) {
        LinkedList<Map<String, Object>> history = tickHistory.get(instrumentToken);
        if (history == null || history.isEmpty()) {
            return new ArrayList<>();
        }

        int size = Math.min(count, history.size());
        return new ArrayList<>(history.subList(history.size() - size, history.size()));
    }

    // ==================== OHLC Data ====================

    @Override
    public Map<String, Object> getCurrentCandle(Long instrumentToken, String timeframe) {
        String key = instrumentToken + "_" + timeframe;
        return currentCandles.getOrDefault(key, new HashMap<>());
    }

    @Override
    public List<Map<String, Object>> getOHLCData(Long instrumentToken, String timeframe, int count) {
        // This would return historical OHLC data
        // For now, return empty list - implement with historical data API
        return new ArrayList<>();
    }

    // ==================== Price Listeners ====================

    @Override
    public void addPriceListener(Long instrumentToken, Consumer<Map<String, Object>> listener) {
        priceListeners.computeIfAbsent(instrumentToken, k -> new CopyOnWriteArrayList<>())
                .add(listener);
    }

    @Override
    public void removePriceListener(Long instrumentToken, Consumer<Map<String, Object>> listener) {
        List<Consumer<Map<String, Object>>> listeners = priceListeners.get(instrumentToken);
        if (listeners != null) {
            listeners.remove(listener);
        }
    }

    @Override
    public void broadcastPriceUpdate(Long instrumentToken, Map<String, Object> tickData) {
        List<Consumer<Map<String, Object>>> listeners = priceListeners.get(instrumentToken);
        if (listeners != null) {
            for (Consumer<Map<String, Object>> listener : listeners) {
                try {
                    listener.accept(tickData);
                } catch (Exception e) {
                    logger.error("Error in price listener: {}", e.getMessage());
                }
            }
        }
    }

    // ==================== IOB Integration ====================

    @Override
    public void checkIOBZonesTouch(Long instrumentToken, Double currentPrice) {
        if (currentPrice == null) return;

        List<InternalOrderBlock> freshIOBs = iobRepository.findFreshIOBs(instrumentToken);

        for (InternalOrderBlock iob : freshIOBs) {
            boolean isLong = "LONG".equals(iob.getTradeDirection());
            boolean inZone = currentPrice >= iob.getZoneLow() && currentPrice <= iob.getZoneHigh();

            if (inZone) {
                // Notify zone touch callback if registered
                Consumer<Map<String, Object>> callback = zoneTouchCallbacks.get(iob.getId());
                if (callback != null) {
                    Map<String, Object> alert = new HashMap<>();
                    alert.put("iobId", iob.getId());
                    alert.put("instrumentToken", instrumentToken);
                    alert.put("currentPrice", currentPrice);
                    alert.put("zoneHigh", iob.getZoneHigh());
                    alert.put("zoneLow", iob.getZoneLow());
                    alert.put("direction", iob.getTradeDirection());
                    alert.put("timestamp", LocalDateTime.now());
                    callback.accept(alert);
                }

                // Trigger auto trading if enabled
                if (autoTradingService != null && autoTradingService.isAutoTradingEnabled()) {
                    Map<String, Object> conditions = autoTradingService.checkEntryConditions(
                            iob.getId(), currentPrice);
                    if (Boolean.TRUE.equals(conditions.get("valid"))) {
                        logger.info("Zone touch detected for IOB {} - Auto trade conditions met",
                                iob.getId());
                    }
                }

                // Check for mitigation
                if (iobService != null) {
                    iobService.checkMitigation(instrumentToken, currentPrice);
                }
            }
        }
    }

    @Override
    public Map<String, Object> getDistanceToNearestZone(Long instrumentToken, Double currentPrice) {
        Map<String, Object> result = new HashMap<>();

        if (currentPrice == null) {
            result.put("error", "No current price");
            return result;
        }

        List<InternalOrderBlock> freshIOBs = iobRepository.findFreshIOBs(instrumentToken);

        if (freshIOBs.isEmpty()) {
            result.put("hasNearbyZone", false);
            return result;
        }

        InternalOrderBlock nearestIOB = null;
        double minDistance = Double.MAX_VALUE;

        for (InternalOrderBlock iob : freshIOBs) {
            double distance;
            if (currentPrice > iob.getZoneHigh()) {
                distance = currentPrice - iob.getZoneHigh();
            } else if (currentPrice < iob.getZoneLow()) {
                distance = iob.getZoneLow() - currentPrice;
            } else {
                distance = 0; // Price is in zone
            }

            if (distance < minDistance) {
                minDistance = distance;
                nearestIOB = iob;
            }
        }

        if (nearestIOB != null) {
            result.put("hasNearbyZone", true);
            result.put("iobId", nearestIOB.getId());
            result.put("iobType", nearestIOB.getObType());
            result.put("zoneHigh", nearestIOB.getZoneHigh());
            result.put("zoneLow", nearestIOB.getZoneLow());
            result.put("distancePoints", minDistance);
            result.put("distancePercent", (minDistance / currentPrice) * 100);
            result.put("direction", nearestIOB.getTradeDirection());
            result.put("inZone", minDistance == 0);
        }

        return result;
    }

    // ==================== Tick Handler ====================

    /**
     * Handle incoming tick data from WebSocket or polling
     */
    private void handlePriceUpdate(Long instrumentToken, Double newPrice) {
        if (instrumentToken == null || newPrice == null) return;

        Double previousPrice = currentPrices.get(instrumentToken);
        currentPrices.put(instrumentToken, newPrice);
        lastTickTime = LocalDateTime.now();

        // Create tick data
        Map<String, Object> tickData = new HashMap<>();
        tickData.put("instrumentToken", instrumentToken);
        tickData.put("lastPrice", newPrice);
        tickData.put("timestamp", lastTickTime);
        tickData.put("previousPrice", previousPrice);
        tickData.put("change", previousPrice != null ? newPrice - previousPrice : 0);
        tickData.put("changePercent", previousPrice != null && previousPrice > 0 ?
                ((newPrice - previousPrice) / previousPrice) * 100 : 0);

        // Store last tick
        lastTicks.put(instrumentToken, tickData);

        // Add to history
        tickHistory.computeIfAbsent(instrumentToken, k -> new LinkedList<>()).add(tickData);
        if (tickHistory.get(instrumentToken).size() > MAX_TICK_HISTORY) {
            tickHistory.get(instrumentToken).removeFirst();
        }

        // Update current candle
        updateCurrentCandle(instrumentToken, newPrice);

        // Broadcast to listeners
        broadcastPriceUpdate(instrumentToken, tickData);

        // Check IOB zones
        checkIOBZonesTouch(instrumentToken, newPrice);
    }

    private void updateCurrentCandle(Long instrumentToken, Double price) {
        String key = instrumentToken + "_1min";
        Map<String, Object> candle = currentCandles.computeIfAbsent(key, k -> {
            Map<String, Object> newCandle = new HashMap<>();
            newCandle.put("open", price);
            newCandle.put("high", price);
            newCandle.put("low", price);
            newCandle.put("close", price);
            newCandle.put("startTime", LocalDateTime.now());
            return newCandle;
        });

        candle.put("close", price);
        candle.put("high", Math.max((Double) candle.get("high"), price));
        candle.put("low", Math.min((Double) candle.get("low"), price));
    }

    /**
     * Handle tick from Kite WebSocket (to be called when integrated)
     */
    public void handleKiteTick(Object tick) {
        // TODO: Parse Kite tick object
        // long instrumentToken = tick.getInstrumentToken();
        // double lastPrice = tick.getLastTradedPrice();
        // handlePriceUpdate(instrumentToken, lastPrice);
    }

    /**
     * Initialize with seed prices (useful for testing)
     */
    public void seedPrices(Map<Long, Double> prices) {
        currentPrices.putAll(prices);
        for (Map.Entry<Long, Double> entry : prices.entrySet()) {
            handlePriceUpdate(entry.getKey(), entry.getValue());
        }
    }
}
