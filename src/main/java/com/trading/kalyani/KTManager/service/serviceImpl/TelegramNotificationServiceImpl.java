package com.trading.kalyani.KTManager.service.serviceImpl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.kalyani.KTManager.config.TelegramConfig;
import com.trading.kalyani.KTManager.dto.TelegramNotificationRequest;
import com.trading.kalyani.KTManager.dto.TelegramNotificationResponse;
import com.trading.kalyani.KTManager.dto.TelegramSettings;
import com.trading.kalyani.KTManager.entity.TelegramNotification;
import com.trading.kalyani.KTManager.entity.TelegramSettingsEntity;
import com.trading.kalyani.KTManager.repository.TelegramNotificationRepository;
import com.trading.kalyani.KTManager.repository.TelegramSettingsRepository;
import com.trading.kalyani.KTManager.service.TelegramNotificationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of TelegramNotificationService.
 * Uses Telegram Bot API to send notifications via HTTP.
 */
@Service
@Slf4j
public class TelegramNotificationServiceImpl implements TelegramNotificationService {

    private final TelegramConfig config;
    private final TelegramNotificationRepository repository;
    private final TelegramSettingsRepository settingsRepository;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    // Rate limiting
    private final AtomicLong lastSendTime = new AtomicLong(0);

    // Cached settings
    private TelegramSettingsEntity cachedSettings = null;

    @Autowired
    public TelegramNotificationServiceImpl(
            TelegramConfig config,
            TelegramNotificationRepository repository,
            TelegramSettingsRepository settingsRepository) {
        this.config = config;
        this.repository = repository;
        this.settingsRepository = settingsRepository;
        this.restTemplate = new RestTemplate();
        this.objectMapper = new ObjectMapper();
    }

    // ==================== Configuration ====================

    @Override
    public boolean isConfigured() {
        return config.isConfigured();
    }

    @Override
    public boolean isCategoryEnabled(TelegramNotificationRequest.NotificationCategory category) {
        if (!isConfigured()) return false;

        TelegramConfig.NotificationCategories categories = config.getCategories();
        return switch (category) {
            case TRADE_ALERT -> categories.isTradeAlerts();
            case PREDICTION_ALERT -> categories.isPredictionAlerts();
            case DEVIATION_ALERT -> categories.isDeviationAlerts();
            case SYSTEM_ALERT -> categories.isSystemAlerts();
            case PATTERN_ALERT -> categories.isPatternAlerts();
            case MARKET_ALERT -> categories.isMarketAlerts();
            case TEST, CUSTOM -> true; // Always allow test and custom
        };
    }

    @Override
    public Map<String, Object> getConfigurationStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("enabled", config.isEnabled());
        status.put("configured", isConfigured());
        status.put("botTokenSet", config.getBotToken() != null && !config.getBotToken().isBlank());
        status.put("chatIdSet", config.getDefaultChatId() != null && !config.getDefaultChatId().isBlank());
        status.put("parseMode", config.getParseMode());

        Map<String, Boolean> categories = new HashMap<>();
        TelegramConfig.NotificationCategories cats = config.getCategories();
        categories.put("tradeAlerts", cats.isTradeAlerts());
        categories.put("predictionAlerts", cats.isPredictionAlerts());
        categories.put("deviationAlerts", cats.isDeviationAlerts());
        categories.put("systemAlerts", cats.isSystemAlerts());
        categories.put("patternAlerts", cats.isPatternAlerts());
        categories.put("marketAlerts", cats.isMarketAlerts());
        status.put("categories", categories);

        return status;
    }

    // ==================== Synchronous Notification Methods ====================

    @Override
    public TelegramNotificationResponse send(TelegramNotificationRequest request) {
        if (!isConfigured()) {
            log.warn("Telegram notifications are not configured");
            return TelegramNotificationResponse.disabled();
        }

        if (request.getCategory() != null && !isCategoryEnabled(request.getCategory())) {
            log.debug("Notification category {} is disabled", request.getCategory());
            return TelegramNotificationResponse.categoryDisabled(request.getCategory());
        }

        // Apply rate limiting
        applyRateLimit();

        // Build the message
        String formattedMessage = request.toFormattedMessage();
        String chatId = request.getChatId() != null ? request.getChatId() : config.getDefaultChatId();
        String parseMode = request.getParseMode() != null ? request.getParseMode() : config.getParseMode();
        boolean disablePreview = request.getDisableWebPagePreview() != null ?
            request.getDisableWebPagePreview() : config.isDisableWebPagePreview();
        boolean silentMode = request.getDisableNotification() != null ?
            request.getDisableNotification() : config.isDisableNotification();

        // Create notification record
        TelegramNotification notification = TelegramNotification.builder()
            .chatId(chatId)
            .category(request.getCategory() != null ? request.getCategory().name() : "CUSTOM")
            .priority(request.getPriority() != null ? request.getPriority().name() : "NORMAL")
            .title(request.getTitle())
            .message(request.getMessage())
            .formattedMessage(formattedMessage)
            .source(request.getSource())
            .eventTime(request.getEventTime())
            .retryCount(0)
            .build();

        // Send the message with retries
        TelegramNotificationResponse response = sendWithRetry(chatId, formattedMessage, parseMode,
            disablePreview, silentMode, notification);

        // Save notification record
        try {
            repository.save(notification);
        } catch (Exception e) {
            log.error("Failed to save notification record: {}", e.getMessage());
        }

        return response;
    }

    private TelegramNotificationResponse sendWithRetry(String chatId, String message, String parseMode,
            boolean disablePreview, boolean silentMode, TelegramNotification notification) {

        int maxRetries = config.getMaxRetries();
        int attempt = 0;
        Exception lastException = null;

        while (attempt < maxRetries) {
            try {
                TelegramNotificationResponse response = sendTelegramMessage(chatId, message, parseMode,
                    disablePreview, silentMode);

                if (response.isSuccess()) {
                    notification.setSuccess(true);
                    notification.setTelegramMessageId(response.getMessageId());
                    notification.setSentAt(LocalDateTime.now());
                    notification.setHttpStatus(response.getHttpStatus());
                    return response;
                } else {
                    lastException = new RuntimeException(response.getError());
                    notification.setErrorMessage(response.getError());
                    notification.setHttpStatus(response.getHttpStatus());
                }
            } catch (Exception e) {
                lastException = e;
                log.warn("Telegram send attempt {} failed: {}", attempt + 1, e.getMessage());
            }

            attempt++;
            notification.setRetryCount(attempt);

            if (attempt < maxRetries) {
                try {
                    Thread.sleep(config.getRetryDelayMs() * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        // All retries failed
        notification.setSuccess(false);
        notification.setSentAt(LocalDateTime.now());
        notification.setErrorMessage(lastException != null ? lastException.getMessage() : "Unknown error");

        return TelegramNotificationResponse.failure(
            lastException != null ? lastException.getMessage() : "Max retries exceeded",
            null,
            notification.getCategory() != null ?
                TelegramNotificationRequest.NotificationCategory.valueOf(notification.getCategory()) : null
        );
    }

    private TelegramNotificationResponse sendTelegramMessage(String chatId, String text, String parseMode,
            boolean disablePreview, boolean silentMode) {

        try {
            String url = config.getApiBaseUrl() + "/sendMessage";

            // Build request body
            Map<String, Object> body = new HashMap<>();
            body.put("chat_id", chatId);
            body.put("text", text);
            body.put("parse_mode", parseMode);
            body.put("disable_web_page_preview", disablePreview);
            body.put("disable_notification", silentMode);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(body, headers);

            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, entity, String.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                JsonNode jsonResponse = objectMapper.readTree(response.getBody());

                if (jsonResponse.has("ok") && jsonResponse.get("ok").asBoolean()) {
                    Long messageId = jsonResponse.path("result").path("message_id").asLong();
                    log.info("Telegram message sent successfully. Message ID: {}", messageId);
                    return TelegramNotificationResponse.success(messageId, chatId, null);
                } else {
                    String error = jsonResponse.path("description").asText("Unknown error");
                    log.error("Telegram API error: {}", error);
                    return TelegramNotificationResponse.failure(error, response.getStatusCode().value(), null);
                }
            } else {
                return TelegramNotificationResponse.failure(
                    "HTTP error: " + response.getStatusCode(),
                    response.getStatusCode().value(),
                    null
                );
            }
        } catch (Exception e) {
            log.error("Failed to send Telegram message: {}", e.getMessage(), e);
            return TelegramNotificationResponse.failure(e.getMessage(), null, null);
        }
    }

    private void applyRateLimit() {
        long now = System.currentTimeMillis();
        long last = lastSendTime.get();
        long elapsed = now - last;

        if (elapsed < config.getRateLimitMs()) {
            try {
                Thread.sleep(config.getRateLimitMs() - elapsed);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        lastSendTime.set(System.currentTimeMillis());
    }

    @Override
    public TelegramNotificationResponse sendMessage(String message) {
        return send(TelegramNotificationRequest.builder()
            .message(message)
            .category(TelegramNotificationRequest.NotificationCategory.CUSTOM)
            .eventTime(LocalDateTime.now())
            .build());
    }

    @Override
    public TelegramNotificationResponse sendMessage(String title, String message) {
        return send(TelegramNotificationRequest.builder()
            .title(title)
            .message(message)
            .category(TelegramNotificationRequest.NotificationCategory.CUSTOM)
            .eventTime(LocalDateTime.now())
            .build());
    }

    @Override
    public TelegramNotificationResponse sendMessage(String chatId, String title, String message) {
        return send(TelegramNotificationRequest.builder()
            .chatId(chatId)
            .title(title)
            .message(message)
            .category(TelegramNotificationRequest.NotificationCategory.CUSTOM)
            .eventTime(LocalDateTime.now())
            .build());
    }

    // ==================== Asynchronous Notification Methods ====================

    @Override
    @Async
    public CompletableFuture<TelegramNotificationResponse> sendAsync(TelegramNotificationRequest request) {
        return CompletableFuture.completedFuture(send(request));
    }

    @Override
    @Async
    public CompletableFuture<TelegramNotificationResponse> sendMessageAsync(String message) {
        return CompletableFuture.completedFuture(sendMessage(message));
    }

    @Override
    @Async
    public CompletableFuture<TelegramNotificationResponse> sendMessageAsync(String title, String message) {
        return CompletableFuture.completedFuture(sendMessage(title, message));
    }

    // ==================== Specialized Alert Methods ====================

    @Override
    public TelegramNotificationResponse sendTradeAlert(String title, String message, Map<String, Object> tradeData) {
        TelegramNotificationRequest request = TelegramNotificationRequest.builder()
            .category(TelegramNotificationRequest.NotificationCategory.TRADE_ALERT)
            .priority(TelegramNotificationRequest.NotificationPriority.HIGH)
            .title(title)
            .message(message)
            .data(tradeData)
            .source("TradeService")
            .eventTime(LocalDateTime.now())
            .build();
        return send(request);
    }

    @Override
    public TelegramNotificationResponse sendPredictionAlert(String title, String message, Map<String, Object> predictionData) {
        TelegramNotificationRequest request = TelegramNotificationRequest.builder()
            .category(TelegramNotificationRequest.NotificationCategory.PREDICTION_ALERT)
            .priority(TelegramNotificationRequest.NotificationPriority.NORMAL)
            .title(title)
            .message(message)
            .data(predictionData)
            .source("CandlePredictionService")
            .eventTime(LocalDateTime.now())
            .build();
        return send(request);
    }

    @Override
    public TelegramNotificationResponse sendDeviationAlert(String title, String message, double deviation) {
        TelegramNotificationRequest request = TelegramNotificationRequest.deviationAlert(title, message, deviation);
        request.setSource("DeviationService");
        return send(request);
    }

    @Override
    public TelegramNotificationResponse sendPatternAlert(String patternName, String direction, double price,
            Map<String, Object> patternData) {
        TelegramNotificationRequest request = TelegramNotificationRequest.builder()
            .category(TelegramNotificationRequest.NotificationCategory.PATTERN_ALERT)
            .priority(TelegramNotificationRequest.NotificationPriority.HIGH)
            .title("Pattern Detected: " + patternName)
            .message(direction + " pattern detected at ₹" + String.format("%.2f", price))
            .data(patternData != null ? patternData : Map.of("Pattern", patternName, "Direction", direction, "Price", price))
            .source("PatternService")
            .eventTime(LocalDateTime.now())
            .build();
        return send(request);
    }

    @Override
    public TelegramNotificationResponse sendSystemAlert(String title, String message) {
        return send(TelegramNotificationRequest.systemAlert(title, message));
    }

    @Override
    public TelegramNotificationResponse sendMarketAlert(String title, String message, Map<String, Object> marketData) {
        TelegramNotificationRequest request = TelegramNotificationRequest.builder()
            .category(TelegramNotificationRequest.NotificationCategory.MARKET_ALERT)
            .priority(TelegramNotificationRequest.NotificationPriority.HIGH)
            .title(title)
            .message(message)
            .data(marketData)
            .source("MarketAnalysis")
            .eventTime(LocalDateTime.now())
            .build();
        return send(request);
    }

    // ==================== Async Specialized Alert Methods ====================

    @Override
    @Async
    public CompletableFuture<TelegramNotificationResponse> sendTradeAlertAsync(String title, String message,
            Map<String, Object> tradeData) {
        return CompletableFuture.completedFuture(sendTradeAlert(title, message, tradeData));
    }

    @Override
    @Async
    public CompletableFuture<TelegramNotificationResponse> sendDeviationAlertAsync(String title, String message,
            double deviation) {
        return CompletableFuture.completedFuture(sendDeviationAlert(title, message, deviation));
    }

    @Override
    @Async
    public CompletableFuture<TelegramNotificationResponse> sendPatternAlertAsync(String patternName, String direction,
            double price, Map<String, Object> patternData) {
        return CompletableFuture.completedFuture(sendPatternAlert(patternName, direction, price, patternData));
    }

    // ==================== Notification History ====================

    @Override
    public List<TelegramNotification> getRecentNotifications() {
        return repository.findTop50ByOrderBySentAtDesc();
    }

    @Override
    public List<TelegramNotification> getTodayNotifications() {
        LocalDateTime startOfDay = LocalDate.now().atStartOfDay();
        return repository.findTodayNotifications(startOfDay);
    }

    @Override
    public List<TelegramNotification> getNotificationsByCategory(String category) {
        return repository.findByCategoryOrderBySentAtDesc(category);
    }

    @Override
    public Map<String, Object> getTodayStats() {
        LocalDateTime startOfDay = LocalDate.now().atStartOfDay();

        Map<String, Object> stats = new HashMap<>();
        stats.put("totalSent", repository.countTodaySuccessful(startOfDay) + repository.countTodayFailed(startOfDay));
        stats.put("successful", repository.countTodaySuccessful(startOfDay));
        stats.put("failed", repository.countTodayFailed(startOfDay));

        Map<String, Long> byCategory = new HashMap<>();
        for (TelegramNotificationRequest.NotificationCategory cat : TelegramNotificationRequest.NotificationCategory.values()) {
            byCategory.put(cat.name(), repository.countTodayByCategory(cat.name(), startOfDay));
        }
        stats.put("byCategory", byCategory);

        return stats;
    }

    // ==================== Testing ====================

    @Override
    public TelegramNotificationResponse sendTestNotification() {
        TelegramNotificationRequest request = TelegramNotificationRequest.builder()
            .category(TelegramNotificationRequest.NotificationCategory.TEST)
            .priority(TelegramNotificationRequest.NotificationPriority.NORMAL)
            .title("Test Notification")
            .message("This is a test message from KTManager Trading Application.\nIf you see this, Telegram notifications are working correctly!")
            .data(Map.of(
                "Application", "KTManager",
                "Test Time", LocalDateTime.now().toString(),
                "Status", "OK"
            ))
            .source("TelegramNotificationService")
            .eventTime(LocalDateTime.now())
            .build();

        return send(request);
    }

    @Override
    public Map<String, Object> validateConfiguration() {
        Map<String, Object> result = new HashMap<>();
        result.put("configured", isConfigured());

        if (!isConfigured()) {
            result.put("valid", false);
            result.put("error", "Telegram is not configured. Set telegram.enabled=true, telegram.botToken, and telegram.defaultChatId in application.yaml");
            return result;
        }

        // Try to get bot info to validate token
        try {
            String url = config.getApiBaseUrl() + "/getMe";
            ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                JsonNode json = objectMapper.readTree(response.getBody());
                if (json.has("ok") && json.get("ok").asBoolean()) {
                    JsonNode botInfo = json.get("result");
                    result.put("valid", true);
                    result.put("botUsername", botInfo.path("username").asText());
                    result.put("botFirstName", botInfo.path("first_name").asText());
                    result.put("botId", botInfo.path("id").asLong());
                } else {
                    result.put("valid", false);
                    result.put("error", json.path("description").asText("Invalid bot token"));
                }
            } else {
                result.put("valid", false);
                result.put("error", "Failed to validate bot token: HTTP " + response.getStatusCode());
            }
        } catch (Exception e) {
            result.put("valid", false);
            result.put("error", "Failed to validate configuration: " + e.getMessage());
        }

        return result;
    }

    @Override
    public Map<String, Object> getRecentChatIds() {
        Map<String, Object> result = new HashMap<>();

        if (config.getBotToken() == null || config.getBotToken().isBlank()) {
            result.put("success", false);
            result.put("error", "Bot token is not configured");
            return result;
        }

        try {
            String url = config.getApiBaseUrl() + "/getUpdates";
            ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                JsonNode json = objectMapper.readTree(response.getBody());

                if (json.has("ok") && json.get("ok").asBoolean()) {
                    JsonNode updates = json.get("result");
                    List<Map<String, Object>> chatInfoList = new ArrayList<>();
                    Set<Long> seenChatIds = new HashSet<>();

                    for (JsonNode update : updates) {
                        JsonNode message = update.path("message");
                        if (!message.isMissingNode()) {
                            JsonNode chat = message.path("chat");
                            if (!chat.isMissingNode()) {
                                long chatId = chat.path("id").asLong();
                                if (!seenChatIds.contains(chatId)) {
                                    seenChatIds.add(chatId);

                                    Map<String, Object> chatInfo = new HashMap<>();
                                    chatInfo.put("chatId", chatId);
                                    chatInfo.put("type", chat.path("type").asText());
                                    chatInfo.put("firstName", chat.path("first_name").asText(""));
                                    chatInfo.put("lastName", chat.path("last_name").asText(""));
                                    chatInfo.put("username", chat.path("username").asText(""));
                                    chatInfo.put("title", chat.path("title").asText("")); // For groups
                                    chatInfoList.add(chatInfo);
                                }
                            }
                        }
                    }

                    result.put("success", true);
                    result.put("chats", chatInfoList);
                    result.put("message", chatInfoList.isEmpty()
                        ? "No messages found. Send a message to your bot first, then try again."
                        : "Found " + chatInfoList.size() + " chat(s). Use the chatId value in your configuration.");

                    if (!chatInfoList.isEmpty()) {
                        result.put("suggestedChatId", chatInfoList.get(0).get("chatId"));
                    }
                } else {
                    result.put("success", false);
                    result.put("error", json.path("description").asText("Failed to get updates"));
                }
            } else {
                result.put("success", false);
                result.put("error", "HTTP error: " + response.getStatusCode());
            }
        } catch (Exception e) {
            result.put("success", false);
            result.put("error", "Failed to get updates: " + e.getMessage());
        }

        return result;
    }

    // ==================== Settings Management ====================

    /**
     * Get or create settings entity
     */
    private TelegramSettingsEntity getOrCreateSettingsEntity() {
        if (cachedSettings != null) {
            return cachedSettings;
        }

        Optional<TelegramSettingsEntity> existing = settingsRepository.findById(1L);
        if (existing.isPresent()) {
            cachedSettings = existing.get();
            return cachedSettings;
        }

        // Create default settings
        TelegramSettingsEntity defaults = TelegramSettingsEntity.defaults();
        cachedSettings = settingsRepository.save(defaults);
        return cachedSettings;
    }

    @Override
    public TelegramSettings getSettings() {
        TelegramSettingsEntity entity = getOrCreateSettingsEntity();
        return convertToDto(entity);
    }

    @Override
    public TelegramSettings updateSettings(TelegramSettings settings) {
        TelegramSettingsEntity entity = getOrCreateSettingsEntity();

        // Update entity from DTO
        if (settings.isEnabled()) {
            entity.setEnabled(settings.isEnabled());
        }
        if (settings.getParseMode() != null) {
            entity.setParseMode(settings.getParseMode());
        }
        entity.setDisableWebPagePreview(settings.isDisableWebPagePreview());
        entity.setDisableNotification(settings.isDisableNotification());
        if (settings.getRateLimitMs() > 0) {
            entity.setRateLimitMs(settings.getRateLimitMs());
        }

        // Trade Alerts
        if (settings.getTradeAlerts() != null) {
            TelegramSettings.TradeAlertSettings ta = settings.getTradeAlerts();
            entity.setTradeAlertsEnabled(ta.isEnabled());
            entity.setIobAlertsEnabled(ta.isIobAlerts());
            entity.setIobMitigationAlertsEnabled(ta.isIobMitigationAlerts());
            entity.setTradeSetupAlertsEnabled(ta.isTradeSetupAlerts());
            entity.setTradeDecisionAlertsEnabled(ta.isTradeDecisionAlerts());
            entity.setLiquidityZoneAlertsEnabled(ta.isLiquidityZoneAlerts());
            entity.setBrahmastraAlertsEnabled(ta.isBrahmastraAlerts());
            entity.setTradeMinConfidence(ta.getMinConfidenceThreshold());
        }

        // Prediction Alerts
        if (settings.getPredictionAlerts() != null) {
            TelegramSettings.PredictionAlertSettings pa = settings.getPredictionAlerts();
            entity.setPredictionAlertsEnabled(pa.isEnabled());
            entity.setCandlePredictionAlertsEnabled(pa.isCandlePredictionAlerts());
            entity.setTrendChangeAlertsEnabled(pa.isTrendChangeAlerts());
            entity.setTargetHitAlertsEnabled(pa.isTargetHitAlerts());
        }

        // Deviation Alerts
        if (settings.getDeviationAlerts() != null) {
            TelegramSettings.DeviationAlertSettings da = settings.getDeviationAlerts();
            entity.setDeviationAlertsEnabled(da.isEnabled());
            entity.setDeviationThresholdPoints(da.getDeviationThresholdPoints());
            entity.setLargeDeviationOnly(da.isLargeDeviationOnly());
        }

        // System Alerts
        if (settings.getSystemAlerts() != null) {
            TelegramSettings.SystemAlertSettings sa = settings.getSystemAlerts();
            entity.setSystemAlertsEnabled(sa.isEnabled());
            entity.setTickerConnectionAlertsEnabled(sa.isTickerConnectionAlerts());
            entity.setJobStatusAlertsEnabled(sa.isJobStatusAlerts());
            entity.setErrorAlertsEnabled(sa.isErrorAlerts());
        }

        // Pattern Alerts
        if (settings.getPatternAlerts() != null) {
            TelegramSettings.PatternAlertSettings pa = settings.getPatternAlerts();
            entity.setPatternAlertsEnabled(pa.isEnabled());
            entity.setBullishPatternAlertsEnabled(pa.isBullishPatternAlerts());
            entity.setBearishPatternAlertsEnabled(pa.isBearishPatternAlerts());
            entity.setPatternMinConfidence(pa.getMinConfidenceThreshold());
        }

        // Market Alerts
        if (settings.getMarketAlerts() != null) {
            TelegramSettings.MarketAlertSettings ma = settings.getMarketAlerts();
            entity.setMarketAlertsEnabled(ma.isEnabled());
            entity.setMarketOpenCloseAlertsEnabled(ma.isMarketOpenCloseAlerts());
            entity.setSignificantMoveAlertsEnabled(ma.isSignificantMoveAlerts());
            entity.setSignificantMoveThresholdPercent(ma.getSignificantMoveThresholdPercent());
        }

        cachedSettings = settingsRepository.save(entity);
        log.info("Telegram settings updated successfully");

        return convertToDto(cachedSettings);
    }

    @Override
    public boolean isAlertTypeEnabled(String category, String alertType) {
        TelegramSettingsEntity settings = getOrCreateSettingsEntity();

        if (!Boolean.TRUE.equals(settings.getEnabled())) {
            return false;
        }

        return switch (category.toUpperCase()) {
            case "TRADE_ALERT", "TRADE" -> {
                if (!Boolean.TRUE.equals(settings.getTradeAlertsEnabled())) {
                    yield false;
                }
                yield switch (alertType.toUpperCase()) {
                    case "IOB", "IOB_ALERTS" -> Boolean.TRUE.equals(settings.getIobAlertsEnabled());
                    case "IOB_MITIGATION", "IOB_MITIGATION_ALERTS" -> Boolean.TRUE.equals(settings.getIobMitigationAlertsEnabled());
                    case "IOB_ENTRY", "IOB_ENTRY_ALERTS" -> Boolean.TRUE.equals(settings.getIobMitigationAlertsEnabled()); // Reuse mitigation setting for entry
                    case "TRADE_SETUP", "TRADE_SETUP_ALERTS" -> Boolean.TRUE.equals(settings.getTradeSetupAlertsEnabled());
                    case "TRADE_DECISION", "TRADE_DECISION_ALERTS" -> Boolean.TRUE.equals(settings.getTradeDecisionAlertsEnabled());
                    case "LIQUIDITY_ZONE", "LIQUIDITY_ZONE_ALERTS" -> Boolean.TRUE.equals(settings.getLiquidityZoneAlertsEnabled());
                    case "BRAHMASTRA", "BRAHMASTRA_ALERTS" -> Boolean.TRUE.equals(settings.getBrahmastraAlertsEnabled());
                    default -> true; // Default to enabled if alert type not recognized
                };
            }
            case "PREDICTION_ALERT", "PREDICTION" -> {
                if (!Boolean.TRUE.equals(settings.getPredictionAlertsEnabled())) {
                    yield false;
                }
                yield switch (alertType.toUpperCase()) {
                    case "CANDLE_PREDICTION" -> Boolean.TRUE.equals(settings.getCandlePredictionAlertsEnabled());
                    case "TREND_CHANGE" -> Boolean.TRUE.equals(settings.getTrendChangeAlertsEnabled());
                    case "TARGET_HIT" -> Boolean.TRUE.equals(settings.getTargetHitAlertsEnabled());
                    default -> true;
                };
            }
            case "DEVIATION_ALERT", "DEVIATION" -> Boolean.TRUE.equals(settings.getDeviationAlertsEnabled());
            case "SYSTEM_ALERT", "SYSTEM" -> {
                if (!Boolean.TRUE.equals(settings.getSystemAlertsEnabled())) {
                    yield false;
                }
                yield switch (alertType.toUpperCase()) {
                    case "TICKER_CONNECTION" -> Boolean.TRUE.equals(settings.getTickerConnectionAlertsEnabled());
                    case "JOB_STATUS" -> Boolean.TRUE.equals(settings.getJobStatusAlertsEnabled());
                    case "ERROR" -> Boolean.TRUE.equals(settings.getErrorAlertsEnabled());
                    default -> true;
                };
            }
            case "PATTERN_ALERT", "PATTERN" -> {
                if (!Boolean.TRUE.equals(settings.getPatternAlertsEnabled())) {
                    yield false;
                }
                yield switch (alertType.toUpperCase()) {
                    case "BULLISH", "BULLISH_PATTERN" -> Boolean.TRUE.equals(settings.getBullishPatternAlertsEnabled());
                    case "BEARISH", "BEARISH_PATTERN" -> Boolean.TRUE.equals(settings.getBearishPatternAlertsEnabled());
                    default -> true;
                };
            }
            case "MARKET_ALERT", "MARKET" -> {
                if (!Boolean.TRUE.equals(settings.getMarketAlertsEnabled())) {
                    yield false;
                }
                yield switch (alertType.toUpperCase()) {
                    case "MARKET_OPEN_CLOSE" -> Boolean.TRUE.equals(settings.getMarketOpenCloseAlertsEnabled());
                    case "SIGNIFICANT_MOVE" -> Boolean.TRUE.equals(settings.getSignificantMoveAlertsEnabled());
                    default -> true;
                };
            }
            default -> false;
        };
    }

    @Override
    public double getTradeAlertMinConfidence() {
        TelegramSettingsEntity settings = getOrCreateSettingsEntity();
        return settings.getTradeMinConfidence() != null ? settings.getTradeMinConfidence() : 51.0;
    }

    @Override
    public double getPatternAlertMinConfidence() {
        TelegramSettingsEntity settings = getOrCreateSettingsEntity();
        return settings.getPatternMinConfidence() != null ? settings.getPatternMinConfidence() : 60.0;
    }

    @Override
    public double getDeviationThresholdPoints() {
        TelegramSettingsEntity settings = getOrCreateSettingsEntity();
        return settings.getDeviationThresholdPoints() != null ? settings.getDeviationThresholdPoints() : 10.0;
    }

    /**
     * Convert entity to DTO
     */
    private TelegramSettings convertToDto(TelegramSettingsEntity entity) {
        return TelegramSettings.builder()
            .enabled(Boolean.TRUE.equals(entity.getEnabled()))
            .botToken(maskToken(config.getBotToken()))
            .defaultChatId(config.getDefaultChatId())
            .parseMode(entity.getParseMode())
            .disableWebPagePreview(Boolean.TRUE.equals(entity.getDisableWebPagePreview()))
            .disableNotification(Boolean.TRUE.equals(entity.getDisableNotification()))
            .connectionTimeout(config.getConnectionTimeout())
            .readTimeout(config.getReadTimeout())
            .maxRetries(config.getMaxRetries())
            .retryDelayMs(config.getRetryDelayMs())
            .rateLimitMs(entity.getRateLimitMs() != null ? entity.getRateLimitMs() : 1000L)
            .tradeAlerts(TelegramSettings.TradeAlertSettings.builder()
                .enabled(Boolean.TRUE.equals(entity.getTradeAlertsEnabled()))
                .iobAlerts(Boolean.TRUE.equals(entity.getIobAlertsEnabled()))
                .iobMitigationAlerts(Boolean.TRUE.equals(entity.getIobMitigationAlertsEnabled()))
                .tradeSetupAlerts(Boolean.TRUE.equals(entity.getTradeSetupAlertsEnabled()))
                .tradeDecisionAlerts(Boolean.TRUE.equals(entity.getTradeDecisionAlertsEnabled()))
                .liquidityZoneAlerts(Boolean.TRUE.equals(entity.getLiquidityZoneAlertsEnabled()))
                .brahmastraAlerts(Boolean.TRUE.equals(entity.getBrahmastraAlertsEnabled()))
                .minConfidenceThreshold(entity.getTradeMinConfidence() != null ? entity.getTradeMinConfidence() : 51.0)
                .build())
            .predictionAlerts(TelegramSettings.PredictionAlertSettings.builder()
                .enabled(Boolean.TRUE.equals(entity.getPredictionAlertsEnabled()))
                .candlePredictionAlerts(Boolean.TRUE.equals(entity.getCandlePredictionAlertsEnabled()))
                .trendChangeAlerts(Boolean.TRUE.equals(entity.getTrendChangeAlertsEnabled()))
                .targetHitAlerts(Boolean.TRUE.equals(entity.getTargetHitAlertsEnabled()))
                .build())
            .deviationAlerts(TelegramSettings.DeviationAlertSettings.builder()
                .enabled(Boolean.TRUE.equals(entity.getDeviationAlertsEnabled()))
                .deviationThresholdPoints(entity.getDeviationThresholdPoints() != null ? entity.getDeviationThresholdPoints() : 10.0)
                .largeDeviationOnly(Boolean.TRUE.equals(entity.getLargeDeviationOnly()))
                .build())
            .systemAlerts(TelegramSettings.SystemAlertSettings.builder()
                .enabled(Boolean.TRUE.equals(entity.getSystemAlertsEnabled()))
                .tickerConnectionAlerts(Boolean.TRUE.equals(entity.getTickerConnectionAlertsEnabled()))
                .jobStatusAlerts(Boolean.TRUE.equals(entity.getJobStatusAlertsEnabled()))
                .errorAlerts(Boolean.TRUE.equals(entity.getErrorAlertsEnabled()))
                .build())
            .patternAlerts(TelegramSettings.PatternAlertSettings.builder()
                .enabled(Boolean.TRUE.equals(entity.getPatternAlertsEnabled()))
                .bullishPatternAlerts(Boolean.TRUE.equals(entity.getBullishPatternAlertsEnabled()))
                .bearishPatternAlerts(Boolean.TRUE.equals(entity.getBearishPatternAlertsEnabled()))
                .minConfidenceThreshold(entity.getPatternMinConfidence() != null ? entity.getPatternMinConfidence() : 60.0)
                .build())
            .marketAlerts(TelegramSettings.MarketAlertSettings.builder()
                .enabled(Boolean.TRUE.equals(entity.getMarketAlertsEnabled()))
                .marketOpenCloseAlerts(Boolean.TRUE.equals(entity.getMarketOpenCloseAlertsEnabled()))
                .significantMoveAlerts(Boolean.TRUE.equals(entity.getSignificantMoveAlertsEnabled()))
                .significantMoveThresholdPercent(entity.getSignificantMoveThresholdPercent() != null ? entity.getSignificantMoveThresholdPercent() : 1.0)
                .build())
            .build();
    }

    /**
     * Mask bot token for security
     */
    private String maskToken(String token) {
        if (token == null || token.length() < 10) {
            return "***";
        }
        return token.substring(0, 5) + "..." + token.substring(token.length() - 4);
    }
}
