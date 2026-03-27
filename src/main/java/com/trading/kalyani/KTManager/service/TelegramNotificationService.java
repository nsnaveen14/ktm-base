package com.trading.kalyani.KTManager.service;

import com.trading.kalyani.KTManager.dto.TelegramNotificationRequest;
import com.trading.kalyani.KTManager.dto.TelegramNotificationResponse;
import com.trading.kalyani.KTManager.dto.TelegramSettings;
import com.trading.kalyani.KTManager.entity.TelegramNotification;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Service interface for sending Telegram notifications.
 * Provides both synchronous and asynchronous methods for flexibility.
 */
public interface TelegramNotificationService {

    // ==================== Configuration ====================

    /**
     * Check if Telegram notifications are enabled and properly configured
     */
    boolean isConfigured();

    /**
     * Check if a specific notification category is enabled
     */
    boolean isCategoryEnabled(TelegramNotificationRequest.NotificationCategory category);

    /**
     * Get current configuration status
     */
    Map<String, Object> getConfigurationStatus();

    // ==================== Synchronous Notification Methods ====================

    /**
     * Send a notification (blocking)
     */
    TelegramNotificationResponse send(TelegramNotificationRequest request);

    /**
     * Send a simple text message
     */
    TelegramNotificationResponse sendMessage(String message);

    /**
     * Send a message with title
     */
    TelegramNotificationResponse sendMessage(String title, String message);

    /**
     * Send a message to a specific chat
     */
    TelegramNotificationResponse sendMessage(String chatId, String title, String message);

    // ==================== Asynchronous Notification Methods ====================

    /**
     * Send a notification asynchronously
     */
    CompletableFuture<TelegramNotificationResponse> sendAsync(TelegramNotificationRequest request);

    /**
     * Send a simple message asynchronously
     */
    CompletableFuture<TelegramNotificationResponse> sendMessageAsync(String message);

    /**
     * Send a message with title asynchronously
     */
    CompletableFuture<TelegramNotificationResponse> sendMessageAsync(String title, String message);

    // ==================== Specialized Alert Methods ====================

    /**
     * Send a trade alert
     */
    TelegramNotificationResponse sendTradeAlert(String title, String message, Map<String, Object> tradeData);

    /**
     * Send a prediction alert
     */
    TelegramNotificationResponse sendPredictionAlert(String title, String message, Map<String, Object> predictionData);

    /**
     * Send a deviation alert
     */
    TelegramNotificationResponse sendDeviationAlert(String title, String message, double deviation);

    /**
     * Send a pattern alert (for candlestick patterns)
     */
    TelegramNotificationResponse sendPatternAlert(String patternName, String direction, double price, Map<String, Object> patternData);

    /**
     * Send a system alert
     */
    TelegramNotificationResponse sendSystemAlert(String title, String message);

    /**
     * Send a market alert
     */
    TelegramNotificationResponse sendMarketAlert(String title, String message, Map<String, Object> marketData);

    // ==================== Async Specialized Alert Methods ====================

    /**
     * Send a trade alert asynchronously
     */
    CompletableFuture<TelegramNotificationResponse> sendTradeAlertAsync(String title, String message, Map<String, Object> tradeData);

    /**
     * Send a deviation alert asynchronously
     */
    CompletableFuture<TelegramNotificationResponse> sendDeviationAlertAsync(String title, String message, double deviation);

    /**
     * Send a pattern alert asynchronously
     */
    CompletableFuture<TelegramNotificationResponse> sendPatternAlertAsync(String patternName, String direction, double price, Map<String, Object> patternData);

    // ==================== Notification History ====================

    /**
     * Get recent notifications
     */
    List<TelegramNotification> getRecentNotifications();

    /**
     * Get today's notifications
     */
    List<TelegramNotification> getTodayNotifications();

    /**
     * Get notifications by category
     */
    List<TelegramNotification> getNotificationsByCategory(String category);

    /**
     * Get notification statistics for today
     */
    Map<String, Object> getTodayStats();

    // ==================== Testing ====================

    /**
     * Send a test notification to verify configuration
     */
    TelegramNotificationResponse sendTestNotification();

    /**
     * Validate bot token and chat ID
     */
    Map<String, Object> validateConfiguration();

    /**
     * Get recent chat IDs from messages sent to the bot.
     * Useful for finding your chat ID when setting up.
     */
    Map<String, Object> getRecentChatIds();

    // ==================== Settings Management ====================

    /**
     * Get current Telegram notification settings
     */
    TelegramSettings getSettings();

    /**
     * Update Telegram notification settings
     */
    TelegramSettings updateSettings(TelegramSettings settings);

    /**
     * Check if a specific alert type is enabled
     */
    boolean isAlertTypeEnabled(String category, String alertType);

    /**
     * Get minimum confidence threshold for trade alerts
     */
    double getTradeAlertMinConfidence();

    /**
     * Get minimum confidence threshold for pattern alerts
     */
    double getPatternAlertMinConfidence();

    /**
     * Get deviation threshold in points
     */
    double getDeviationThresholdPoints();
}
