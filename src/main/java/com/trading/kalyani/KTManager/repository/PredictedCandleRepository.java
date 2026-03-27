package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.PredictedCandleStick;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface PredictedCandleRepository extends CrudRepository<PredictedCandleStick, Long> {

    @Query("SELECT p FROM PredictedCandleStick p WHERE p.instrumentToken = :instrumentToken " +
            "AND p.predictionGeneratedAt = (SELECT MAX(p2.predictionGeneratedAt) FROM PredictedCandleStick p2 WHERE p2.instrumentToken = :instrumentToken) " +
            "ORDER BY p.predictionSequence ASC")
    List<PredictedCandleStick> findLatestPredictionsForInstrument(@Param("instrumentToken") Long instrumentToken);

    @Query("SELECT p FROM PredictedCandleStick p WHERE p.instrumentToken = :instrumentToken " +
            "AND p.candleStartTime >= :startTime AND p.candleStartTime <= :endTime " +
            "ORDER BY p.candleStartTime ASC")
    List<PredictedCandleStick> findPredictionsInTimeRange(@Param("instrumentToken") Long instrumentToken,
                                                          @Param("startTime") LocalDateTime startTime,
                                                          @Param("endTime") LocalDateTime endTime);

    @Query("SELECT p FROM PredictedCandleStick p WHERE p.verified = false " +
            "AND p.candleEndTime < :currentTime ORDER BY p.candleStartTime ASC")
    List<PredictedCandleStick> findUnverifiedPredictions(@Param("currentTime") LocalDateTime currentTime);

    @Query("SELECT AVG(p.predictionAccuracy) FROM PredictedCandleStick p " +
            "WHERE p.instrumentToken = :instrumentToken AND p.verified = true " +
            "AND p.predictionGeneratedAt >= :startTime")
    Double getAverageAccuracyForInstrument(@Param("instrumentToken") Long instrumentToken,
                                           @Param("startTime") LocalDateTime startTime);

    @Query("SELECT p FROM PredictedCandleStick p WHERE p.instrumentToken = :instrumentToken " +
            "ORDER BY p.id DESC")
    List<PredictedCandleStick> findRecentPredictions(@Param("instrumentToken") Long instrumentToken, Pageable pageable);

    void deleteByInstrumentTokenAndPredictionGeneratedAtBefore(Long instrumentToken, LocalDateTime before);

    long countByInstrumentToken(Long instrumentToken);
}
