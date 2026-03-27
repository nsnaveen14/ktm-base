package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.AutoTradingConfig;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface AutoTradingConfigRepository extends JpaRepository<AutoTradingConfig, Long> {

    Optional<AutoTradingConfig> findByConfigName(String configName);

    default AutoTradingConfig getDefaultConfig() {
        return findByConfigName("DEFAULT").orElse(null);
    }
}
