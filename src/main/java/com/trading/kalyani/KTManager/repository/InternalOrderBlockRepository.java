package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.InternalOrderBlock;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface InternalOrderBlockRepository extends JpaRepository<InternalOrderBlock, Long> {

    /**
     * Find latest IOB for a specific instrument and timeframe
     */
    Optional<InternalOrderBlock> findFirstByInstrumentTokenAndTimeframeOrderByDetectionTimestampDesc(
            Long instrumentToken, String timeframe);

    /**x.0
     * Find all fresh (unmitigated) IOBs for an instrument
     */
    @Query("SELECT iob FROM InternalOrderBlock iob WHERE iob.instrumentToken = :instrumentToken " +
            "AND iob.status = 'FRESH' ORDER BY iob.detectionTimestamp DESC")
    List<InternalOrderBlock> findFreshIOBs(@Param("instrumentToken") Long instrumentToken);

    /**
     * Find fresh IOBs by type (BULLISH_IOB or BEARISH_IOB)
     */
    @Query("SELECT iob FROM InternalOrderBlock iob WHERE iob.instrumentToken = :instrumentToken " +
            "AND iob.obType = :obType AND iob.status = 'FRESH' ORDER BY iob.detectionTimestamp DESC")
    List<InternalOrderBlock> findFreshIOBsByType(
            @Param("instrumentToken") Long instrumentToken,
            @Param("obType") String obType);

    /**
     * Find IOBs detected today
     */
    @Query("SELECT iob FROM InternalOrderBlock iob WHERE iob.instrumentToken = :instrumentToken " +
            "AND iob.detectionTimestamp >= :startOfDay ORDER BY iob.detectionTimestamp DESC")
    List<InternalOrderBlock> findTodaysIOBs(
            @Param("instrumentToken") Long instrumentToken,
            @Param("startOfDay") LocalDateTime startOfDay);

    /**
     * Find valid IOBs for trading
     */
    @Query("SELECT iob FROM InternalOrderBlock iob WHERE iob.instrumentToken = :instrumentToken " +
            "AND iob.isValid = true AND iob.status = 'FRESH' " +
            "AND iob.tradeTaken = false ORDER BY iob.signalConfidence DESC")
    List<InternalOrderBlock> findValidTradableIOBs(@Param("instrumentToken") Long instrumentToken);

    /**
     * Find IOBs within price range (for mitigation check)
     * An IOB is considered mitigated when the current price enters the IOB zone (between zoneLow and zoneHigh)
     * This applies to both BULLISH_IOB and BEARISH_IOB types
     */
    @Query("SELECT iob FROM InternalOrderBlock iob WHERE iob.instrumentToken = :instrumentToken " +
            "AND iob.status = 'FRESH' " +
            "AND :currentPrice >= iob.zoneLow AND :currentPrice <= iob.zoneHigh")
    List<InternalOrderBlock> findIOBsAtPrice(
            @Param("instrumentToken") Long instrumentToken,
            @Param("currentPrice") Double currentPrice);

    /**
     * Find IOBs by timeframe today
     */
    @Query("SELECT iob FROM InternalOrderBlock iob WHERE iob.instrumentToken = :instrumentToken " +
            "AND iob.timeframe = :timeframe AND iob.detectionTimestamp >= :startOfDay " +
            "ORDER BY iob.detectionTimestamp DESC")
    List<InternalOrderBlock> findTodaysIOBsByTimeframe(
            @Param("instrumentToken") Long instrumentToken,
            @Param("timeframe") String timeframe,
            @Param("startOfDay") LocalDateTime startOfDay);

    /**
     * Count fresh IOBs by type
     */
    @Query("SELECT COUNT(iob) FROM InternalOrderBlock iob WHERE iob.instrumentToken = :instrumentToken " +
            "AND iob.obType = :obType AND iob.status = 'FRESH'")
    Long countFreshIOBsByType(
            @Param("instrumentToken") Long instrumentToken,
            @Param("obType") String obType);

    /**
     * Find all IOBs for multiple instruments
     */
    @Query("SELECT iob FROM InternalOrderBlock iob WHERE iob.instrumentToken IN :instrumentTokens " +
            "AND iob.status = 'FRESH' ORDER BY iob.signalConfidence DESC")
    List<InternalOrderBlock> findFreshIOBsForInstruments(
            @Param("instrumentTokens") List<Long> instrumentTokens);

    /**
     * Update status of expired IOBs
     */
    @Modifying(clearAutomatically = true)
    @Transactional
    @Query("UPDATE InternalOrderBlock iob SET iob.status = 'EXPIRED' " +
            "WHERE iob.status = 'FRESH' AND iob.detectionTimestamp < :cutoffTime")
    void expireOldIOBs(@Param("cutoffTime") LocalDateTime cutoffTime);

    /**
     * Find IOB by unique signature (for deduplication)
     */
    Optional<InternalOrderBlock> findByIobSignature(String iobSignature);

    /**
     * Check if IOB exists by signature
     */
    boolean existsByIobSignature(String iobSignature);

    /**
     * Find IOBs by instrument, type and candle time (for deduplication)
     */
    @Query("SELECT iob FROM InternalOrderBlock iob WHERE iob.instrumentToken = :instrumentToken " +
            "AND iob.obType = :obType AND iob.obCandleTime = :obCandleTime")
    List<InternalOrderBlock> findByInstrumentTypeAndCandleTime(
            @Param("instrumentToken") Long instrumentToken,
            @Param("obType") String obType,
            @Param("obCandleTime") LocalDateTime obCandleTime);

    /**
     * Find all IOBs (any status) for an instrument for comprehensive duplicate check
     */
    @Query("SELECT iob FROM InternalOrderBlock iob WHERE iob.instrumentToken = :instrumentToken " +
            "AND iob.detectionTimestamp >= :since ORDER BY iob.detectionTimestamp DESC")
    List<InternalOrderBlock> findAllIOBsSince(
            @Param("instrumentToken") Long instrumentToken,
            @Param("since") LocalDateTime since);

    /**
     * Find active IOBs - those that are still being tracked (not completed)
     * Active means: FRESH, MITIGATED, or TRADED but NOT (target3AlertSent=true OR status='STOPPED' OR status='COMPLETED')
     */
    @Query("SELECT iob FROM InternalOrderBlock iob WHERE iob.instrumentToken = :instrumentToken " +
            "AND iob.detectionTimestamp >= :startOfDay " +
            "AND (iob.target3AlertSent IS NULL OR iob.target3AlertSent = false) " +
            "AND iob.status NOT IN ('STOPPED', 'COMPLETED', 'EXPIRED') " +
            "ORDER BY iob.obCandleTime DESC")
    List<InternalOrderBlock> findActiveIOBs(
            @Param("instrumentToken") Long instrumentToken,
            @Param("startOfDay") LocalDateTime startOfDay);

    /**
     * Find all active IOBs regardless of detection date - for IOBs that persist across days
     * Active means: FRESH or TRADED only (excludes MITIGATED, STOPPED, COMPLETED, EXPIRED)
     */
    @Query("SELECT iob FROM InternalOrderBlock iob WHERE iob.instrumentToken = :instrumentToken " +
            "AND (iob.target3AlertSent IS NULL OR iob.target3AlertSent = false) " +
            "AND iob.status NOT IN ('MITIGATED', 'STOPPED', 'COMPLETED', 'EXPIRED') " +
            "ORDER BY iob.obCandleTime DESC")
    List<InternalOrderBlock> findAllActiveIOBs(
            @Param("instrumentToken") Long instrumentToken);

    /**
     * Find mitigated IOBs - those that have been touched by price but not yet completed
     */
    @Query("SELECT iob FROM InternalOrderBlock iob WHERE iob.instrumentToken = :instrumentToken " +
            "AND iob.status = 'MITIGATED' " +
            "AND (iob.target3AlertSent IS NULL OR iob.target3AlertSent = false) " +
            "ORDER BY iob.obCandleTime DESC")
    List<InternalOrderBlock> findMitigatedIOBs(
            @Param("instrumentToken") Long instrumentToken);

    /**
     * Find completed IOBs - those where target 3 is hit or stop loss is hit
     */
    @Query("SELECT iob FROM InternalOrderBlock iob WHERE iob.instrumentToken = :instrumentToken " +
            "AND iob.detectionTimestamp >= :startOfDay " +
            "AND (iob.target3AlertSent = true OR iob.status IN ('STOPPED', 'COMPLETED')) " +
            "ORDER BY iob.obCandleTime DESC")
    List<InternalOrderBlock> findCompletedIOBs(
            @Param("instrumentToken") Long instrumentToken,
            @Param("startOfDay") LocalDateTime startOfDay);

    /**
     * Find completed IOBs within a date range - for showing recently completed IOBs
     */
    @Query("SELECT iob FROM InternalOrderBlock iob WHERE iob.instrumentToken = :instrumentToken " +
            "AND iob.detectionTimestamp >= :since " +
            "AND (iob.target3AlertSent = true OR iob.status IN ('STOPPED', 'COMPLETED')) " +
            "ORDER BY iob.obCandleTime DESC")
    List<InternalOrderBlock> findCompletedIOBsSince(
            @Param("instrumentToken") Long instrumentToken,
            @Param("since") LocalDateTime since);

    /**
     * Find all IOBs for today (both active and completed)
     */
    @Query("SELECT iob FROM InternalOrderBlock iob WHERE iob.instrumentToken = :instrumentToken " +
            "AND iob.detectionTimestamp >= :startOfDay " +
            "AND iob.status NOT IN ('EXPIRED') " +
            "ORDER BY iob.obCandleTime DESC")
    List<InternalOrderBlock> findAllTodaysIOBsExcludingExpired(
            @Param("instrumentToken") Long instrumentToken,
            @Param("startOfDay") LocalDateTime startOfDay);

    /**
     * Find all IOBs that need target/SL monitoring: MITIGATED or TRADED, regardless of detection date.
     * Excludes IOBs where target 3 has already been reached (fully completed).
     */
    @Query("SELECT iob FROM InternalOrderBlock iob WHERE iob.instrumentToken = :instrumentToken " +
            "AND iob.status IN ('MITIGATED', 'TRADED') " +
            "AND (iob.target3AlertSent IS NULL OR iob.target3AlertSent = false) " +
            "ORDER BY iob.obCandleTime DESC")
    List<InternalOrderBlock> findIOBsNeedingTargetMonitoring(
            @Param("instrumentToken") Long instrumentToken);
}
