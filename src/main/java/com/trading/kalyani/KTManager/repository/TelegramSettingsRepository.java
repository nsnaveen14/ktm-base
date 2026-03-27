package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.TelegramSettingsEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Repository for Telegram settings.
 */
@Repository
public interface TelegramSettingsRepository extends JpaRepository<TelegramSettingsEntity, Long> {
    // Single-row pattern - always use id=1
}
