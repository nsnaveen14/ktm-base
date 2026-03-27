package com.trading.kalyani.KTManager.dto;

import lombok.*;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * DTO for sending Telegram notification requests.
 * Supports various message types and formatting options.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class TelegramNotificationRequest {

    /**
     * Notification category for filtering and tracking
     */
    private NotificationCategory category;

    /**
     * Priority level of the notification
     */
    @Builder.Default
    private NotificationPriority priority = NotificationPriority.NORMAL;

    /**
     * Message title (will be formatted as bold header)
     */
    private String title;

    /**
     * Main message content
     */
    private String message;

    /**
     * Optional chat ID override (uses default if not specified)
     */
    private String chatId;

    /**
     * Parse mode override: HTML, Markdown, or MarkdownV2
     */
    private String parseMode;

    /**
     * Disable link previews for this message
     */
    private Boolean disableWebPagePreview;

    /**
     * Send message silently
     */
    private Boolean disableNotification;

    /**
     * Additional data to include in the message
     */
    private Map<String, Object> data;

    /**
     * Source service/job that generated this notification
     */
    private String source;

    /**
     * Timestamp of the event that triggered this notification
     */
    private LocalDateTime eventTime;

    /**
     * Notification categories for filtering
     */
    public enum NotificationCategory {
        TRADE_ALERT,
        PREDICTION_ALERT,
        DEVIATION_ALERT,
        SYSTEM_ALERT,
        PATTERN_ALERT,
        MARKET_ALERT,
        TEST,
        CUSTOM
    }

    /**
     * Priority levels for notifications
     */
    public enum NotificationPriority {
        LOW,
        NORMAL,
        HIGH,
        CRITICAL
    }

    /**
     * Format the notification as an HTML message for Telegram
     */
    public String toFormattedMessage() {
        StringBuilder sb = new StringBuilder();

        // Add priority emoji
        String priorityEmoji = switch (priority) {
            case CRITICAL -> "🚨";
            case HIGH -> "⚠️";
            case NORMAL -> "📊";
            case LOW -> "ℹ️";
        };

        // Add category emoji
        String categoryEmoji = category != null ? switch (category) {
            case TRADE_ALERT -> "💹";
            case PREDICTION_ALERT -> "🔮";
            case DEVIATION_ALERT -> "📉";
            case SYSTEM_ALERT -> "🔧";
            case PATTERN_ALERT -> "🕯️";
            case MARKET_ALERT -> "📈";
            case TEST -> "🧪";
            case CUSTOM -> "📝";
        } : "📝";

        // Build header
        if (title != null && !title.isBlank()) {
            sb.append(priorityEmoji).append(" <b>").append(categoryEmoji).append(" ").append(escapeHtml(title)).append("</b>\n\n");
        }

        // Add main message
        if (message != null && !message.isBlank()) {
            sb.append(escapeHtml(message)).append("\n");
        }

        // Add data fields if present
        if (data != null && !data.isEmpty()) {
            sb.append("\n");
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                sb.append("• <b>").append(escapeHtml(entry.getKey())).append(":</b> ")
                  .append(escapeHtml(String.valueOf(entry.getValue()))).append("\n");
            }
        }

        // Add footer with source and time
        sb.append("\n<i>");
        if (source != null && !source.isBlank()) {
            sb.append("📌 ").append(escapeHtml(source));
        }
        if (eventTime != null) {
            if (source != null) sb.append(" | ");
            sb.append("🕐 ").append(eventTime.toString());
        }
        sb.append("</i>");

        return sb.toString();
    }

    /**
     * Escape HTML special characters for Telegram HTML parse mode
     */
    private String escapeHtml(String text) {
        if (text == null) return "";
        return text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;");
    }

    /**
     * Builder helper for quick trade alerts
     */
    public static TelegramNotificationRequest tradeAlert(String title, String message) {
        return TelegramNotificationRequest.builder()
            .category(NotificationCategory.TRADE_ALERT)
            .priority(NotificationPriority.HIGH)
            .title(title)
            .message(message)
            .eventTime(LocalDateTime.now())
            .build();
    }

    /**
     * Builder helper for prediction alerts
     */
    public static TelegramNotificationRequest predictionAlert(String title, String message) {
        return TelegramNotificationRequest.builder()
            .category(NotificationCategory.PREDICTION_ALERT)
            .priority(NotificationPriority.NORMAL)
            .title(title)
            .message(message)
            .eventTime(LocalDateTime.now())
            .build();
    }

    /**
     * Builder helper for deviation alerts
     */
    public static TelegramNotificationRequest deviationAlert(String title, String message, double deviation) {
        NotificationPriority priority = deviation >= 20 ? NotificationPriority.CRITICAL :
                                        deviation >= 10 ? NotificationPriority.HIGH :
                                        NotificationPriority.NORMAL;
        return TelegramNotificationRequest.builder()
            .category(NotificationCategory.DEVIATION_ALERT)
            .priority(priority)
            .title(title)
            .message(message)
            .data(Map.of("Deviation", deviation + " points"))
            .eventTime(LocalDateTime.now())
            .build();
    }

    /**
     * Builder helper for pattern alerts
     */
    public static TelegramNotificationRequest patternAlert(String patternName, String direction, double price) {
        return TelegramNotificationRequest.builder()
            .category(NotificationCategory.PATTERN_ALERT)
            .priority(NotificationPriority.HIGH)
            .title("Pattern Detected: " + patternName)
            .message(direction + " pattern detected")
            .data(Map.of("Price", price, "Direction", direction))
            .eventTime(LocalDateTime.now())
            .build();
    }

    /**
     * Builder helper for system alerts
     */
    public static TelegramNotificationRequest systemAlert(String title, String message) {
        return TelegramNotificationRequest.builder()
            .category(NotificationCategory.SYSTEM_ALERT)
            .priority(NotificationPriority.NORMAL)
            .title(title)
            .message(message)
            .eventTime(LocalDateTime.now())
            .build();
    }
}
