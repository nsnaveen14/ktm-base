package com.trading.kalyani.KTManager.dto;

import lombok.*;

import java.time.LocalDateTime;

/**
 * Response DTO for Telegram notification operations.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class TelegramNotificationResponse {

    /**
     * Whether the operation was successful
     */
    private boolean success;

    /**
     * Message ID returned by Telegram (if successful)
     */
    private Long messageId;

    /**
     * Chat ID the message was sent to
     */
    private String chatId;

    /**
     * Error message if the operation failed
     */
    private String error;

    /**
     * HTTP status code from Telegram API
     */
    private Integer httpStatus;

    /**
     * Timestamp when the notification was processed
     */
    private LocalDateTime timestamp;

    /**
     * Original notification category
     */
    private TelegramNotificationRequest.NotificationCategory category;

    /**
     * Create a success response
     */
    public static TelegramNotificationResponse success(Long messageId, String chatId,
            TelegramNotificationRequest.NotificationCategory category) {
        return TelegramNotificationResponse.builder()
            .success(true)
            .messageId(messageId)
            .chatId(chatId)
            .category(category)
            .timestamp(LocalDateTime.now())
            .httpStatus(200)
            .build();
    }

    /**
     * Create a failure response
     */
    public static TelegramNotificationResponse failure(String error, Integer httpStatus,
            TelegramNotificationRequest.NotificationCategory category) {
        return TelegramNotificationResponse.builder()
            .success(false)
            .error(error)
            .httpStatus(httpStatus)
            .category(category)
            .timestamp(LocalDateTime.now())
            .build();
    }

    /**
     * Create a disabled response (when Telegram is not configured)
     */
    public static TelegramNotificationResponse disabled() {
        return TelegramNotificationResponse.builder()
            .success(false)
            .error("Telegram notifications are disabled or not configured")
            .timestamp(LocalDateTime.now())
            .build();
    }

    /**
     * Create a category disabled response
     */
    public static TelegramNotificationResponse categoryDisabled(
            TelegramNotificationRequest.NotificationCategory category) {
        return TelegramNotificationResponse.builder()
            .success(false)
            .error("Notification category '" + category + "' is disabled")
            .category(category)
            .timestamp(LocalDateTime.now())
            .build();
    }
}
