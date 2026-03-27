package com.trading.kalyani.KTManager.service;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Service for real-time price tick integration.
 * Provides WebSocket-based live price updates and tick data management.
 */
public interface RealTimePriceService {

    // ==================== Connection Management ====================

    /**
     * Connect to the real-time price feed
     */
    void connect();

    /**
     * Disconnect from the price feed
     */
    void disconnect();

    /**
     * Check if connected
     */
    boolean isConnected();

    /**
     * Get connection status
     */
    Map<String, Object> getConnectionStatus();

    // ==================== Subscription Management ====================

    /**
     * Subscribe to price updates for instruments
     */
    void subscribeToInstruments(List<Long> instrumentTokens);

    /**
     * Unsubscribe from instruments
     */
    void unsubscribeFromInstruments(List<Long> instrumentTokens);

    /**
     * Get subscribed instruments
     */
    List<Long> getSubscribedInstruments();

    /**
     * Subscribe to IOB zone touch alerts
     */
    void subscribeToZoneTouchAlerts(Long iobId, Consumer<Map<String, Object>> callback);

    /**
     * Unsubscribe from zone touch alerts
     */
    void unsubscribeFromZoneTouchAlerts(Long iobId);

    // ==================== Price Data ====================

    /**
     * Get current price for an instrument
     */
    Double getCurrentPrice(Long instrumentToken);

    /**
     * Get all current prices
     */
    Map<Long, Double> getAllCurrentPrices();

    /**
     * Get last tick data for an instrument
     */
    Map<String, Object> getLastTick(Long instrumentToken);

    /**
     * Get tick history for an instrument (last N ticks)
     */
    List<Map<String, Object>> getTickHistory(Long instrumentToken, int count);

    // ==================== OHLC Data ====================

    /**
     * Get current candle OHLC for an instrument
     */
    Map<String, Object> getCurrentCandle(Long instrumentToken, String timeframe);

    /**
     * Get historical OHLC data
     */
    List<Map<String, Object>> getOHLCData(Long instrumentToken, String timeframe, int count);

    // ==================== Price Listeners ====================

    /**
     * Register a price update listener
     */
    void addPriceListener(Long instrumentToken, Consumer<Map<String, Object>> listener);

    /**
     * Remove a price update listener
     */
    void removePriceListener(Long instrumentToken, Consumer<Map<String, Object>> listener);

    /**
     * Broadcast price update to all listeners
     */
    void broadcastPriceUpdate(Long instrumentToken, Map<String, Object> tickData);

    // ==================== IOB Integration ====================

    /**
     * Check IOB zone touch for real-time mitigation detection
     */
    void checkIOBZonesTouch(Long instrumentToken, Double currentPrice);

    /**
     * Get distance to nearest IOB zone
     */
    Map<String, Object> getDistanceToNearestZone(Long instrumentToken, Double currentPrice);
}
