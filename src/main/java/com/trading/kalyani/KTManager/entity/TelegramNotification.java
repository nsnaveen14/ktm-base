package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

/**
 * Entity to track sent Telegram notifications for auditing and analytics.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "telegram_notification", indexes = {
    @Index(name = "idx_telegram_notification_category", columnList = "category"),
    @Index(name = "idx_telegram_notification_sent_at", columnList = "sent_at"),
    @Index(name = "idx_telegram_notification_success", columnList = "success")
})
public class TelegramNotification {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "telegram_notification_seq")
    @SequenceGenerator(name = "telegram_notification_seq", sequenceName = "telegram_notification_seq", allocationSize = 1)
    private Long id;

    @Column(name = "message_id")
    private Long telegramMessageId;

    @Column(name = "chat_id")
    private String chatId;

    @Column(name = "category")
    private String category;

    @Column(name = "priority")
    private String priority;

    @Column(name = "title")
    private String title;

    @Column(name = "message", length = 4096)
    private String message;

    @Column(name = "formatted_message", length = 4096)
    private String formattedMessage;

    @Column(name = "source")
    private String source;

    @Column(name = "success")
    private boolean success;

    @Column(name = "error_message", length = 1024)
    private String errorMessage;

    @Column(name = "http_status")
    private Integer httpStatus;

    @Column(name = "retry_count")
    private int retryCount;

    @Column(name = "sent_at")
    private LocalDateTime sentAt;

    @Column(name = "event_time")
    private LocalDateTime eventTime;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
    }
}
