package com.trading.kalyani.KTManager.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration properties for Telegram Bot integration.
 *
 * To set up your Telegram Bot:
 * 1. Message @BotFather on Telegram
 * 2. Send /newbot and follow the instructions
 * 3. Copy the bot token and set it in application.yaml
 * 4. Get your chat ID by messaging @userinfobot or using the /getUpdates API
 */
@Component
@ConfigurationProperties(prefix = "telegram")
@Getter
@Setter
public class TelegramConfig {

    /**
     * Enable/disable Telegram notifications globally
     */
    private boolean enabled = false;

    /**
     * Telegram Bot API token from @BotFather
     */
    private String botToken;

    /**
     * Default chat ID to send notifications to (can be user or group chat)
     */
    private String defaultChatId;

    /**
     * Parse mode for messages: HTML, Markdown, or MarkdownV2
     */
    private String parseMode = "HTML";

    /**
     * Disable link previews in messages
     */
    private boolean disableWebPagePreview = true;

    /**
     * Send notifications silently (no sound on recipient's device)
     */
    private boolean disableNotification = false;

    /**
     * Connection timeout in milliseconds
     */
    private int connectionTimeout = 10000;

    /**
     * Read timeout in milliseconds
     */
    private int readTimeout = 10000;

    /**
     * Maximum retries for failed message sends
     */
    private int maxRetries = 3;

    /**
     * Delay between retries in milliseconds
     */
    private long retryDelayMs = 1000;

    /**
     * Rate limit: minimum interval between messages in milliseconds
     */
    private long rateLimitMs = 1000;

    /**
     * Enable/disable specific notification categories
     */
    private NotificationCategories categories = new NotificationCategories();

    @Getter
    @Setter
    public static class NotificationCategories {
        private boolean tradeAlerts = true;
        private boolean predictionAlerts = true;
        private boolean deviationAlerts = true;
        private boolean systemAlerts = true;
        private boolean patternAlerts = true;
        private boolean marketAlerts = true;
    }

    /**
     * Get the Telegram Bot API base URL
     */
    public String getApiBaseUrl() {
        return "https://api.telegram.org/bot" + botToken;
    }

    /**
     * Check if the configuration is valid
     */
    public boolean isConfigured() {
        return enabled && botToken != null && !botToken.isBlank()
               && defaultChatId != null && !defaultChatId.isBlank();
    }
}
