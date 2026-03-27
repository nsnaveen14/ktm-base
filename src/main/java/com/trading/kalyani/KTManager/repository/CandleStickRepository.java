package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.CandleStick;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface CandleStickRepository extends CrudRepository<CandleStick, Long> {

    @Query(value = "SELECT * FROM candle_stick WHERE instrument_token = :instrumentToken " +
            "AND candle_start_time between :startTime and :endTime ORDER BY candle_start_time ASC", nativeQuery = true)
    List<CandleStick> findByInstrumentTokenAndTimeRange(@Param("instrumentToken") Long instrumentToken,
                                                        @Param("startTime") LocalDateTime startTime,
                                                        @Param("endTime") LocalDateTime endTime);

    @Query(value = "SELECT * FROM candle_stick WHERE instrument_token = :instrumentToken " +
            "ORDER BY candle_start_time DESC LIMIT :limit", nativeQuery = true)
    List<CandleStick> findRecentCandles(@Param("instrumentToken") Long instrumentToken,
                                        @Param("limit") int limit);

    @Query(value = "SELECT * FROM candle_stick WHERE instrument_token = :instrumentToken " +
            "AND candle_start_time >= :startTime AND candle_start_time <= :endTime " +
            "ORDER BY candle_start_time ASC", nativeQuery = true)
    List<CandleStick> findCandlesInRange(@Param("instrumentToken") Long instrumentToken,
                                         @Param("startTime") LocalDateTime startTime,
                                         @Param("endTime") LocalDateTime endTime);

    @Query(value = "SELECT * FROM candle_stick WHERE instrument_token = :instrumentToken " +
            "ORDER BY candle_start_time DESC LIMIT 1", nativeQuery = true)
    CandleStick findLatestCandle(@Param("instrumentToken") Long instrumentToken);

    // For Brahmastra - find candles between start and end time
    List<CandleStick> findByInstrumentTokenAndCandleStartTimeBetween(
            Long instrumentToken, LocalDateTime start, LocalDateTime end);
}
