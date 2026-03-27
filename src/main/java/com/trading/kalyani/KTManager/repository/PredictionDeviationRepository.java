package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.PredictionDeviation;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface PredictionDeviationRepository extends CrudRepository<PredictionDeviation, Long> {

    /**
     * Find all deviations for a specific trading date
     */
    List<PredictionDeviation> findByTradingDateOrderByVerificationTimeAsc(LocalDate tradingDate);

    /**
     * Find the latest deviation record
     */
    @Query(value = "SELECT * FROM prediction_deviation WHERE instrument_token = :instrumentToken " +
            "ORDER BY verification_time DESC LIMIT 1", nativeQuery = true)
    Optional<PredictionDeviation> findLatestDeviation(@Param("instrumentToken") Long instrumentToken);

    /**
     * Find recent deviations for calculating running averages
     */
    @Query(value = "SELECT * FROM prediction_deviation WHERE instrument_token = :instrumentToken " +
            "AND verification_time >= :startTime ORDER BY verification_time DESC", nativeQuery = true)
    List<PredictionDeviation> findRecentDeviations(@Param("instrumentToken") Long instrumentToken,
                                                    @Param("startTime") LocalDateTime startTime);

    /**
     * Find deviations for specific market hour (for time-based correction)
     */
    @Query(value = "SELECT * FROM prediction_deviation WHERE instrument_token = :instrumentToken " +
            "AND market_hour = :marketHour AND verification_time >= :startTime " +
            "ORDER BY verification_time DESC", nativeQuery = true)
    List<PredictionDeviation> findByMarketHour(@Param("instrumentToken") Long instrumentToken,
                                                @Param("marketHour") Integer marketHour,
                                                @Param("startTime") LocalDateTime startTime);

    /**
     * Find deviations for expiry days only (for expiry-specific correction)
     */
    @Query(value = "SELECT * FROM prediction_deviation WHERE instrument_token = :instrumentToken " +
            "AND is_expiry_day = true AND verification_time >= :startTime " +
            "ORDER BY verification_time DESC", nativeQuery = true)
    List<PredictionDeviation> findExpiryDayDeviations(@Param("instrumentToken") Long instrumentToken,
                                                       @Param("startTime") LocalDateTime startTime);

    /**
     * Get average deviation for last N sessions
     */
    @Query(value = "SELECT AVG(avg_close_deviation) FROM prediction_deviation " +
            "WHERE instrument_token = :instrumentToken ORDER BY verification_time DESC LIMIT :limit", nativeQuery = true)
    Double getAverageDeviation(@Param("instrumentToken") Long instrumentToken, @Param("limit") int limit);

    /**
     * Get average systematic bias for recent sessions
     */
    @Query(value = "SELECT AVG(systematic_bias) FROM prediction_deviation " +
            "WHERE instrument_token = :instrumentToken AND verification_time >= :startTime", nativeQuery = true)
    Double getAverageSystematicBias(@Param("instrumentToken") Long instrumentToken,
                                    @Param("startTime") LocalDateTime startTime);

    /**
     * Get sequence-wise deviation averages for optimization
     */
    @Query(value = "SELECT " +
            "AVG(seq1_avg_deviation) as seq1, " +
            "AVG(seq2_avg_deviation) as seq2, " +
            "AVG(seq3_avg_deviation) as seq3, " +
            "AVG(seq4_avg_deviation) as seq4, " +
            "AVG(seq5_avg_deviation) as seq5 " +
            "FROM prediction_deviation WHERE instrument_token = :instrumentToken " +
            "AND verification_time >= :startTime", nativeQuery = true)
    Object[] getSequenceWiseDeviations(@Param("instrumentToken") Long instrumentToken,
                                       @Param("startTime") LocalDateTime startTime);

    /**
     * Find deviations by day of week for pattern analysis
     */
    @Query(value = "SELECT * FROM prediction_deviation WHERE instrument_token = :instrumentToken " +
            "AND day_of_week = :dayOfWeek AND verification_time >= :startTime " +
            "ORDER BY verification_time DESC", nativeQuery = true)
    List<PredictionDeviation> findByDayOfWeek(@Param("instrumentToken") Long instrumentToken,
                                               @Param("dayOfWeek") Integer dayOfWeek,
                                               @Param("startTime") LocalDateTime startTime);

    /**
     * Get volatility underestimate ratio average
     */
    @Query(value = "SELECT AVG(volatility_underestimate_ratio) FROM prediction_deviation " +
            "WHERE instrument_token = :instrumentToken AND verification_time >= :startTime", nativeQuery = true)
    Double getAverageVolatilityRatio(@Param("instrumentToken") Long instrumentToken,
                                     @Param("startTime") LocalDateTime startTime);

    /**
     * Delete old deviation records for cleanup
     */
    void deleteByVerificationTimeBefore(LocalDateTime before);

    /**
     * Count deviations for today
     */
    @Query(value = "SELECT COUNT(*) FROM prediction_deviation " +
            "WHERE instrument_token = :instrumentToken AND trading_date = :tradingDate", nativeQuery = true)
    Integer countTodayDeviations(@Param("instrumentToken") Long instrumentToken,
                                 @Param("tradingDate") LocalDate tradingDate);

    /**
     * Get direction accuracy for recent sessions
     */
    @Query(value = "SELECT AVG(direction_accuracy_percent) FROM prediction_deviation " +
            "WHERE instrument_token = :instrumentToken AND verification_time >= :startTime", nativeQuery = true)
    Double getAverageDirectionAccuracy(@Param("instrumentToken") Long instrumentToken,
                                       @Param("startTime") LocalDateTime startTime);
}

