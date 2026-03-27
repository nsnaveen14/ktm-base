package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.TradeSetupEntity;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Repository for TradeSetupEntity.
 * Provides methods to save and retrieve trade setups.
 */
@Repository
public interface TradeSetupRepository extends CrudRepository<TradeSetupEntity, Long> {

    /**
     * Find the latest valid trade setup for an instrument.
     */
    @Query(value = "SELECT * FROM trade_setup WHERE instrument_token = :instrumentToken " +
            "AND is_valid = true AND valid_until > :currentTime " +
            "ORDER BY created_at DESC LIMIT 1", nativeQuery = true)
    Optional<TradeSetupEntity> findLatestValidSetup(@Param("instrumentToken") Long instrumentToken,
                                                     @Param("currentTime") LocalDateTime currentTime);

    /**
     * Find the most recent trade setup for an instrument (regardless of validity).
     */
    @Query(value = "SELECT * FROM trade_setup WHERE instrument_token = :instrumentToken " +
            "ORDER BY created_at DESC LIMIT 1", nativeQuery = true)
    Optional<TradeSetupEntity> findLatestSetup(@Param("instrumentToken") Long instrumentToken);

    /**
     * Find all active (not closed) trade setups.
     */
    @Query(value = "SELECT * FROM trade_setup WHERE is_executed = true AND is_closed = false " +
            "ORDER BY execution_time DESC", nativeQuery = true)
    List<TradeSetupEntity> findActiveSetups();

    /**
     * Find trade setups created within a time range.
     */
    @Query(value = "SELECT * FROM trade_setup WHERE instrument_token = :instrumentToken " +
            "AND created_at >= :startTime AND created_at <= :endTime " +
            "ORDER BY created_at DESC", nativeQuery = true)
    List<TradeSetupEntity> findSetupsInTimeRange(@Param("instrumentToken") Long instrumentToken,
                                                  @Param("startTime") LocalDateTime startTime,
                                                  @Param("endTime") LocalDateTime endTime);

    /**
     * Find all setups for today.
     */
    @Query(value = "SELECT * FROM trade_setup WHERE instrument_token = :instrumentToken " +
            "AND DATE(created_at) = CURRENT_DATE ORDER BY created_at DESC", nativeQuery = true)
    List<TradeSetupEntity> findTodaySetups(@Param("instrumentToken") Long instrumentToken);

    /**
     * Find setups by trade direction.
     */
    List<TradeSetupEntity> findByInstrumentTokenAndTradeDirectionOrderByCreatedAtDesc(
            Long instrumentToken, String tradeDirection);

    /**
     * Find setups by setup type.
     */
    List<TradeSetupEntity> findByInstrumentTokenAndSetupTypeOrderByCreatedAtDesc(
            Long instrumentToken, String setupType);

    /**
     * Find closed setups for performance analysis.
     */
    @Query(value = "SELECT * FROM trade_setup WHERE instrument_token = :instrumentToken " +
            "AND is_closed = true AND created_at >= :startTime " +
            "ORDER BY exit_time DESC", nativeQuery = true)
    List<TradeSetupEntity> findClosedSetups(@Param("instrumentToken") Long instrumentToken,
                                             @Param("startTime") LocalDateTime startTime);

    /**
     * Get win rate for closed setups.
     */
    @Query(value = "SELECT COUNT(*) FILTER (WHERE profit_loss > 0) * 100.0 / NULLIF(COUNT(*), 0) " +
            "FROM trade_setup WHERE instrument_token = :instrumentToken " +
            "AND is_closed = true AND created_at >= :startTime", nativeQuery = true)
    Double getWinRate(@Param("instrumentToken") Long instrumentToken,
                      @Param("startTime") LocalDateTime startTime);

    /**
     * Get total profit/loss for closed setups.
     */
    @Query(value = "SELECT COALESCE(SUM(profit_loss), 0) FROM trade_setup " +
            "WHERE instrument_token = :instrumentToken AND is_closed = true " +
            "AND created_at >= :startTime", nativeQuery = true)
    Double getTotalProfitLoss(@Param("instrumentToken") Long instrumentToken,
                               @Param("startTime") LocalDateTime startTime);

    /**
     * Delete old setups.
     */
    void deleteByCreatedAtBefore(LocalDateTime before);

    /**
     * Find expired but not closed setups.
     */
    @Query(value = "SELECT * FROM trade_setup WHERE valid_until < :currentTime " +
            "AND is_closed = false ORDER BY valid_until DESC", nativeQuery = true)
    List<TradeSetupEntity> findExpiredSetups(@Param("currentTime") LocalDateTime currentTime);
}

