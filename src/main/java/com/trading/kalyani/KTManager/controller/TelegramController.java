package com.trading.kalyani.KTManager.controller;

import com.trading.kalyani.KTManager.dto.TelegramNotificationRequest;
import com.trading.kalyani.KTManager.dto.TelegramNotificationResponse;
import com.trading.kalyani.KTManager.dto.TelegramSettings;
import com.trading.kalyani.KTManager.entity.TelegramNotification;
import com.trading.kalyani.KTManager.service.TelegramNotificationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST Controller for Telegram notification management.
 * Provides endpoints for sending notifications, testing configuration, and viewing history.
 */
@RestController
@RequestMapping("/api/telegram")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Telegram Notifications", description = "APIs for managing Telegram notifications")
public class TelegramController {

    private final TelegramNotificationService telegramService;

    // ==================== Configuration ====================

    @GetMapping("/status")
    @Operation(summary = "Get Telegram configuration status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        return ResponseEntity.ok(telegramService.getConfigurationStatus());
    }

    @GetMapping("/validate")
    @Operation(summary = "Validate Telegram bot configuration")
    public ResponseEntity<Map<String, Object>> validateConfiguration() {
        return ResponseEntity.ok(telegramService.validateConfiguration());
    }

    // ==================== Send Notifications ====================

    @PostMapping("/send")
    @Operation(summary = "Send a custom notification")
    public ResponseEntity<TelegramNotificationResponse> sendNotification(
            @RequestBody TelegramNotificationRequest request) {
        TelegramNotificationResponse response = telegramService.send(request);
        return response.isSuccess() ? ResponseEntity.ok(response) : ResponseEntity.badRequest().body(response);
    }

    @PostMapping("/send/simple")
    @Operation(summary = "Send a simple text message")
    public ResponseEntity<TelegramNotificationResponse> sendSimpleMessage(
            @RequestParam String message,
            @RequestParam(required = false) String title) {
        TelegramNotificationResponse response = title != null ?
            telegramService.sendMessage(title, message) :
            telegramService.sendMessage(message);
        return response.isSuccess() ? ResponseEntity.ok(response) : ResponseEntity.badRequest().body(response);
    }

    @PostMapping("/send/trade-alert")
    @Operation(summary = "Send a trade alert")
    public ResponseEntity<TelegramNotificationResponse> sendTradeAlert(
            @RequestParam String title,
            @RequestParam String message,
            @RequestBody(required = false) Map<String, Object> tradeData) {
        TelegramNotificationResponse response = telegramService.sendTradeAlert(title, message, tradeData);
        return response.isSuccess() ? ResponseEntity.ok(response) : ResponseEntity.badRequest().body(response);
    }

    @PostMapping("/send/prediction-alert")
    @Operation(summary = "Send a prediction alert")
    public ResponseEntity<TelegramNotificationResponse> sendPredictionAlert(
            @RequestParam String title,
            @RequestParam String message,
            @RequestBody(required = false) Map<String, Object> predictionData) {
        TelegramNotificationResponse response = telegramService.sendPredictionAlert(title, message, predictionData);
        return response.isSuccess() ? ResponseEntity.ok(response) : ResponseEntity.badRequest().body(response);
    }

    @PostMapping("/send/deviation-alert")
    @Operation(summary = "Send a deviation alert")
    public ResponseEntity<TelegramNotificationResponse> sendDeviationAlert(
            @RequestParam String title,
            @RequestParam String message,
            @RequestParam double deviation) {
        TelegramNotificationResponse response = telegramService.sendDeviationAlert(title, message, deviation);
        return response.isSuccess() ? ResponseEntity.ok(response) : ResponseEntity.badRequest().body(response);
    }

    @PostMapping("/send/pattern-alert")
    @Operation(summary = "Send a candlestick pattern alert")
    public ResponseEntity<TelegramNotificationResponse> sendPatternAlert(
            @RequestParam String patternName,
            @RequestParam String direction,
            @RequestParam double price,
            @RequestBody(required = false) Map<String, Object> patternData) {
        TelegramNotificationResponse response = telegramService.sendPatternAlert(patternName, direction, price, patternData);
        return response.isSuccess() ? ResponseEntity.ok(response) : ResponseEntity.badRequest().body(response);
    }

    @PostMapping("/send/system-alert")
    @Operation(summary = "Send a system alert")
    public ResponseEntity<TelegramNotificationResponse> sendSystemAlert(
            @RequestParam String title,
            @RequestParam String message) {
        TelegramNotificationResponse response = telegramService.sendSystemAlert(title, message);
        return response.isSuccess() ? ResponseEntity.ok(response) : ResponseEntity.badRequest().body(response);
    }

    @PostMapping("/send/market-alert")
    @Operation(summary = "Send a market alert")
    public ResponseEntity<TelegramNotificationResponse> sendMarketAlert(
            @RequestParam String title,
            @RequestParam String message,
            @RequestBody(required = false) Map<String, Object> marketData) {
        TelegramNotificationResponse response = telegramService.sendMarketAlert(title, message, marketData);
        return response.isSuccess() ? ResponseEntity.ok(response) : ResponseEntity.badRequest().body(response);
    }

    // ==================== Testing ====================

    @PostMapping("/test")
    @Operation(summary = "Send a test notification to verify configuration")
    public ResponseEntity<TelegramNotificationResponse> sendTestNotification() {
        TelegramNotificationResponse response = telegramService.sendTestNotification();
        return response.isSuccess() ? ResponseEntity.ok(response) : ResponseEntity.badRequest().body(response);
    }

    // ==================== History & Stats ====================

    @GetMapping("/notifications/recent")
    @Operation(summary = "Get recent notifications (last 50)")
    public ResponseEntity<List<TelegramNotification>> getRecentNotifications() {
        return ResponseEntity.ok(telegramService.getRecentNotifications());
    }

    @GetMapping("/notifications/today")
    @Operation(summary = "Get today's notifications")
    public ResponseEntity<List<TelegramNotification>> getTodayNotifications() {
        return ResponseEntity.ok(telegramService.getTodayNotifications());
    }

    @GetMapping("/notifications/category/{category}")
    @Operation(summary = "Get notifications by category")
    public ResponseEntity<List<TelegramNotification>> getNotificationsByCategory(
            @PathVariable String category) {
        return ResponseEntity.ok(telegramService.getNotificationsByCategory(category.toUpperCase()));
    }

    @GetMapping("/stats/today")
    @Operation(summary = "Get today's notification statistics")
    public ResponseEntity<Map<String, Object>> getTodayStats() {
        return ResponseEntity.ok(telegramService.getTodayStats());
    }

    @GetMapping("/get-chat-ids")
    @Operation(summary = "Get recent chat IDs from messages sent to the bot. Send a message to your bot first, then call this endpoint.")
    public ResponseEntity<Map<String, Object>> getChatIds() {
        return ResponseEntity.ok(telegramService.getRecentChatIds());
    }

    // ==================== Settings Management ====================

    @GetMapping("/settings")
    @Operation(summary = "Get current Telegram notification settings")
    public ResponseEntity<TelegramSettings> getSettings() {
        return ResponseEntity.ok(telegramService.getSettings());
    }

    @PutMapping("/settings")
    @Operation(summary = "Update Telegram notification settings")
    public ResponseEntity<TelegramSettings> updateSettings(@RequestBody TelegramSettings settings) {
        return ResponseEntity.ok(telegramService.updateSettings(settings));
    }

    @GetMapping("/settings/alert-enabled")
    @Operation(summary = "Check if a specific alert type is enabled")
    public ResponseEntity<Map<String, Object>> isAlertEnabled(
            @RequestParam String category,
            @RequestParam(defaultValue = "") String alertType) {
        boolean enabled = telegramService.isAlertTypeEnabled(category, alertType);
        return ResponseEntity.ok(Map.of(
            "category", category,
            "alertType", alertType,
            "enabled", enabled
        ));
    }
}
