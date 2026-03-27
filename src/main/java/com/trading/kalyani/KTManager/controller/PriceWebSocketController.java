package com.trading.kalyani.KTManager.controller;

import com.trading.kalyani.KTManager.service.RealTimePriceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;

/**
 * WebSocket controller for real-time price updates.
 */
@Controller
public class PriceWebSocketController {

    private static final Logger logger = LoggerFactory.getLogger(PriceWebSocketController.class);

    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    @Autowired
    private RealTimePriceService realTimePriceService;

    /**
     * Handle subscription requests from clients
     */
    @MessageMapping("/prices/subscribe")
    @SendTo("/topic/prices/status")
    public Map<String, Object> handleSubscription(Map<String, Object> request) {
        logger.info("Price subscription request received: {}", request);

        Map<String, Object> response = new HashMap<>();
        response.put("status", "subscribed");
        response.put("timestamp", LocalDateTime.now());

        if (request.containsKey("instruments")) {
            @SuppressWarnings("unchecked")
            List<Long> instruments = (List<Long>) request.get("instruments");
            realTimePriceService.subscribeToInstruments(instruments);
            response.put("instruments", instruments);
        } else {
            response.put("instruments", realTimePriceService.getSubscribedInstruments());
        }

        return response;
    }

    /**
     * Handle unsubscription requests
     */
    @MessageMapping("/prices/unsubscribe")
    @SendTo("/topic/prices/status")
    public Map<String, Object> handleUnsubscription(Map<String, Object> request) {
        logger.info("Price unsubscription request: {}", request);

        if (request.containsKey("instruments")) {
            @SuppressWarnings("unchecked")
            List<Long> instruments = (List<Long>) request.get("instruments");
            realTimePriceService.unsubscribeFromInstruments(instruments);
        }

        Map<String, Object> response = new HashMap<>();
        response.put("status", "unsubscribed");
        response.put("timestamp", LocalDateTime.now());
        return response;
    }

    /**
     * Get current prices on demand
     */
    @MessageMapping("/prices/get-current")
    @SendTo("/topic/prices/current")
    public Map<String, Object> getCurrentPrices() {
        Map<String, Object> response = new HashMap<>();
        response.put("prices", realTimePriceService.getAllCurrentPrices());
        response.put("timestamp", LocalDateTime.now());
        return response;
    }

    /**
     * Get connection status
     */
    @MessageMapping("/prices/status")
    @SendTo("/topic/prices/status")
    public Map<String, Object> getConnectionStatus() {
        return realTimePriceService.getConnectionStatus();
    }

    /**
     * Broadcast price updates to all connected clients
     * Called every second during market hours
     */
    @Scheduled(fixedRate = 1000)
    public void broadcastPriceUpdates() {
        if (!realTimePriceService.isConnected()) {
            return;
        }

        Map<Long, Double> prices = realTimePriceService.getAllCurrentPrices();
        if (prices.isEmpty()) {
            return;
        }

        Map<String, Object> update = new HashMap<>();
        update.put("type", "PRICE_UPDATE");
        update.put("timestamp", LocalDateTime.now());

        // Format prices by instrument name
        Map<String, Object> priceData = new HashMap<>();

        Double niftyPrice = prices.get(NIFTY_INSTRUMENT_TOKEN);
        if (niftyPrice != null) {
            Map<String, Object> niftyTick = realTimePriceService.getLastTick(NIFTY_INSTRUMENT_TOKEN);
            priceData.put("NIFTY", Map.of(
                    "price", niftyPrice,
                    "change", niftyTick.getOrDefault("change", 0),
                    "changePercent", niftyTick.getOrDefault("changePercent", 0),
                    "token", NIFTY_INSTRUMENT_TOKEN
            ));
        }


        update.put("prices", priceData);

        // Broadcast to /topic/prices
        messagingTemplate.convertAndSend("/topic/prices", update);
    }

    /**
     * Broadcast IOB zone touch alerts
     */
    public void broadcastZoneTouchAlert(Map<String, Object> alert) {
        Map<String, Object> message = new HashMap<>();
        message.put("type", "ZONE_TOUCH_ALERT");
        message.put("timestamp", LocalDateTime.now());
        message.put("alert", alert);

        messagingTemplate.convertAndSend("/topic/iob/zone-touch", message);
    }

    /**
     * Broadcast IOB mitigation alerts
     */
    public void broadcastMitigationAlert(Map<String, Object> alert) {
        Map<String, Object> message = new HashMap<>();
        message.put("type", "MITIGATION_ALERT");
        message.put("timestamp", LocalDateTime.now());
        message.put("alert", alert);

        messagingTemplate.convertAndSend("/topic/iob/mitigation", message);
    }

    /**
     * Broadcast new IOB detection
     */
    public void broadcastNewIOB(Map<String, Object> iob) {
        Map<String, Object> message = new HashMap<>();
        message.put("type", "NEW_IOB");
        message.put("timestamp", LocalDateTime.now());
        message.put("iob", iob);

        messagingTemplate.convertAndSend("/topic/iob/new", message);
    }

    /**
     * Broadcast auto trade events
     */
    public void broadcastAutoTradeEvent(String eventType, Map<String, Object> data) {
        Map<String, Object> message = new HashMap<>();
        message.put("type", eventType);
        message.put("timestamp", LocalDateTime.now());
        message.put("data", data);

        messagingTemplate.convertAndSend("/topic/auto-trade", message);
    }
}
