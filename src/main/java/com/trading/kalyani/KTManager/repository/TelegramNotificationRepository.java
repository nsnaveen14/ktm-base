package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.TelegramNotification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Repository for Telegram notification records.
 */
@Repository
public interface TelegramNotificationRepository extends JpaRepository<TelegramNotification, Long> {

    /**
     * Find all notifications by category
     */
    List<TelegramNotification> findByCategoryOrderBySentAtDesc(String category);

    /**
     * Find all notifications sent today
     */
    @Query("SELECT t FROM TelegramNotification t WHERE t.sentAt >= :startOfDay ORDER BY t.sentAt DESC")
    List<TelegramNotification> findTodayNotifications(@Param("startOfDay") LocalDateTime startOfDay);

    /**
     * Find notifications by category sent today
     */
    @Query("SELECT t FROM TelegramNotification t WHERE t.category = :category AND t.sentAt >= :startOfDay ORDER BY t.sentAt DESC")
    List<TelegramNotification> findTodayNotificationsByCategory(
        @Param("category") String category,
        @Param("startOfDay") LocalDateTime startOfDay
    );

    /**
     * Find failed notifications for retry
     */
    List<TelegramNotification> findBySuccessFalseAndRetryCountLessThanOrderByCreatedAtAsc(int maxRetries);

    /**
     * Count notifications by category today
     */
    @Query("SELECT COUNT(t) FROM TelegramNotification t WHERE t.category = :category AND t.sentAt >= :startOfDay")
    long countTodayByCategory(@Param("category") String category, @Param("startOfDay") LocalDateTime startOfDay);

    /**
     * Count successful notifications today
     */
    @Query("SELECT COUNT(t) FROM TelegramNotification t WHERE t.success = true AND t.sentAt >= :startOfDay")
    long countTodaySuccessful(@Param("startOfDay") LocalDateTime startOfDay);

    /**
     * Count failed notifications today
     */
    @Query("SELECT COUNT(t) FROM TelegramNotification t WHERE t.success = false AND t.sentAt >= :startOfDay")
    long countTodayFailed(@Param("startOfDay") LocalDateTime startOfDay);

    /**
     * Find recent notifications (last N)
     */
    List<TelegramNotification> findTop50ByOrderBySentAtDesc();

    /**
     * Find notifications within a date range
     */
    @Query("SELECT t FROM TelegramNotification t WHERE t.sentAt BETWEEN :start AND :end ORDER BY t.sentAt DESC")
    List<TelegramNotification> findByDateRange(
        @Param("start") LocalDateTime start,
        @Param("end") LocalDateTime end
    );

    /**
     * Delete old notifications (older than specified date)
     */
    void deleteByCreatedAtBefore(LocalDateTime before);
}
