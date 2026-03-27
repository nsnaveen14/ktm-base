package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.BrahmastraSignal;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Repository for Brahmastra trading signals.
 */
@Repository
public interface BrahmastraSignalRepository extends JpaRepository<BrahmastraSignal, Long> {

    /**
     * Find signals by instrument token and time range.
     */
    List<BrahmastraSignal> findByInstrumentTokenAndSignalTimeBetweenOrderBySignalTimeDesc(
            Long instrumentToken, LocalDateTime start, LocalDateTime end);

    /**
     * Find active signals for an instrument.
     */
    List<BrahmastraSignal> findByInstrumentTokenAndStatusOrderBySignalTimeDesc(
            Long instrumentToken, String status);

    /**
     * Find signals by symbol and time range.
     */
    List<BrahmastraSignal> findBySymbolAndSignalTimeBetweenOrderBySignalTimeDesc(
            String symbol, LocalDateTime start, LocalDateTime end);

    /**
     * Find all active signals.
     */
    List<BrahmastraSignal> findByStatusOrderBySignalTimeDesc(String status);

    /**
     * Find latest signal for an instrument.
     */
    @Query(value = "SELECT * FROM brahmastra_signals s WHERE s.instrument_token = :token ORDER BY s.signal_time DESC LIMIT 1", nativeQuery = true)
    BrahmastraSignal findLatestByInstrumentToken(@Param("token") Long instrumentToken);

    /**
     * Find signals by appJobConfigNum.
     */
    List<BrahmastraSignal> findByAppJobConfigNumAndSignalTimeBetweenOrderBySignalTimeDesc(
            Integer appJobConfigNum, LocalDateTime start, LocalDateTime end);

    /**
     * Count winning trades for a symbol in date range.
     */
    @Query("SELECT COUNT(s) FROM BrahmastraSignal s WHERE s.symbol = :symbol " +
           "AND s.signalTime BETWEEN :start AND :end AND s.pnl > 0")
    Long countWinningTrades(@Param("symbol") String symbol,
                            @Param("start") LocalDateTime start,
                            @Param("end") LocalDateTime end);

    /**
     * Count total trades for a symbol in date range.
     */
    @Query("SELECT COUNT(s) FROM BrahmastraSignal s WHERE s.symbol = :symbol " +
           "AND s.signalTime BETWEEN :start AND :end AND s.status = 'CLOSED'")
    Long countTotalTrades(@Param("symbol") String symbol,
                          @Param("start") LocalDateTime start,
                          @Param("end") LocalDateTime end);

    /**
     * Get total PnL for a symbol.
     */
    @Query("SELECT COALESCE(SUM(s.pnl), 0) FROM BrahmastraSignal s WHERE s.symbol = :symbol " +
           "AND s.signalTime BETWEEN :start AND :end")
    Double getTotalPnL(@Param("symbol") String symbol,
                       @Param("start") LocalDateTime start,
                       @Param("end") LocalDateTime end);

    /**
     * Find signals for today (use with today's start and end datetime).
     */
    List<BrahmastraSignal> findBySignalTimeBetweenOrderBySignalTimeDesc(
            LocalDateTime start, LocalDateTime end);

    /**
     * Find signals by status and date range.
     */
    List<BrahmastraSignal> findByStatusAndSignalTimeBetweenOrderBySignalTimeDesc(
            String status, LocalDateTime start, LocalDateTime end);
}

