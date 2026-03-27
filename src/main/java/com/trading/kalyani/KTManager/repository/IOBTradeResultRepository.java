package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.IOBTradeResult;
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
public interface IOBTradeResultRepository extends JpaRepository<IOBTradeResult, Long> {

    /**
     * Find by IOB ID
     */
    Optional<IOBTradeResult> findByIobId(Long iobId);

    /**
     * Find by trade ID
     */
    Optional<IOBTradeResult> findByTradeId(String tradeId);

    /**
     * Find all trades for an instrument
     */
    List<IOBTradeResult> findByInstrumentTokenOrderByEntryTimeDesc(Long instrumentToken);

    /**
     * Find open trades
     */
    @Query("SELECT t FROM IOBTradeResult t WHERE t.status = 'OPEN' ORDER BY t.entryTime DESC")
    List<IOBTradeResult> findOpenTrades();

    /**
     * Find open trades for an instrument
     */
    @Query("SELECT t FROM IOBTradeResult t WHERE t.instrumentToken = :instrumentToken AND t.status = 'OPEN'")
    List<IOBTradeResult> findOpenTradesByInstrument(@Param("instrumentToken") Long instrumentToken);

    /**
     * Find closed trades within date range
     */
    @Query("SELECT t FROM IOBTradeResult t WHERE t.status = 'CLOSED' " +
            "AND t.exitTime >= :startDate AND t.exitTime <= :endDate " +
            "ORDER BY t.exitTime DESC")
    List<IOBTradeResult> findClosedTradesInRange(
            @Param("startDate") LocalDateTime startDate,
            @Param("endDate") LocalDateTime endDate);

    /**
     * Find today's trades
     */
    @Query("SELECT t FROM IOBTradeResult t WHERE t.entryTime >= :startOfDay ORDER BY t.entryTime DESC")
    List<IOBTradeResult> findTodaysTrades(@Param("startOfDay") LocalDateTime startOfDay);

    /**
     * Find winning trades
     */
    @Query("SELECT t FROM IOBTradeResult t WHERE t.isWinner = true AND t.status = 'CLOSED' " +
            "AND t.exitTime >= :startDate ORDER BY t.exitTime DESC")
    List<IOBTradeResult> findWinningTrades(@Param("startDate") LocalDateTime startDate);

    /**
     * Find losing trades
     */
    @Query("SELECT t FROM IOBTradeResult t WHERE t.isWinner = false AND t.status = 'CLOSED' " +
            "AND t.exitTime >= :startDate ORDER BY t.exitTime DESC")
    List<IOBTradeResult> findLosingTrades(@Param("startDate") LocalDateTime startDate);

    /**
     * Count winning trades in period
     */
    @Query("SELECT COUNT(t) FROM IOBTradeResult t WHERE t.isWinner = true AND t.status = 'CLOSED' " +
            "AND t.exitTime >= :startDate AND t.exitTime <= :endDate")
    Long countWinningTrades(@Param("startDate") LocalDateTime startDate, @Param("endDate") LocalDateTime endDate);

    /**
     * Count total closed trades in period
     */
    @Query("SELECT COUNT(t) FROM IOBTradeResult t WHERE t.status = 'CLOSED' " +
            "AND t.exitTime >= :startDate AND t.exitTime <= :endDate")
    Long countClosedTrades(@Param("startDate") LocalDateTime startDate, @Param("endDate") LocalDateTime endDate);

    /**
     * Get average RR achieved
     */
    @Query("SELECT AVG(t.achievedRR) FROM IOBTradeResult t WHERE t.status = 'CLOSED' " +
            "AND t.exitTime >= :startDate AND t.exitTime <= :endDate AND t.achievedRR IS NOT NULL")
    Double getAverageRR(@Param("startDate") LocalDateTime startDate, @Param("endDate") LocalDateTime endDate);

    /**
     * Get total P&L in period
     */
    @Query("SELECT SUM(t.netPnl) FROM IOBTradeResult t WHERE t.status = 'CLOSED' " +
            "AND t.exitTime >= :startDate AND t.exitTime <= :endDate AND t.netPnl IS NOT NULL")
    Double getTotalPnl(@Param("startDate") LocalDateTime startDate, @Param("endDate") LocalDateTime endDate);

    /**
     * Find trades by timeframe
     */
    @Query("SELECT t FROM IOBTradeResult t WHERE t.timeframe = :timeframe AND t.status = 'CLOSED' " +
            "ORDER BY t.exitTime DESC")
    List<IOBTradeResult> findByTimeframe(@Param("timeframe") String timeframe);

    /**
     * Find trades with FVG confluence
     */
    @Query("SELECT t FROM IOBTradeResult t WHERE t.hasFvg = true AND t.status = 'CLOSED' " +
            "ORDER BY t.exitTime DESC")
    List<IOBTradeResult> findTradesWithFVG();

    /**
     * Find trades with HTF alignment
     */
    @Query("SELECT t FROM IOBTradeResult t WHERE t.htfAligned = true AND t.status = 'CLOSED' " +
            "ORDER BY t.exitTime DESC")
    List<IOBTradeResult> findHTFAlignedTrades();

    /**
     * Get trades by trade mode
     */
    List<IOBTradeResult> findByTradeModeOrderByEntryTimeDesc(String tradeMode);

    /**
     * Find backtest trades
     */
    @Query("SELECT t FROM IOBTradeResult t WHERE t.tradeMode = 'BACKTEST' " +
            "AND t.instrumentToken = :instrumentToken ORDER BY t.entryTime ASC")
    List<IOBTradeResult> findBacktestTrades(@Param("instrumentToken") Long instrumentToken);

    /**
     * Delete all backtest trades for an instrument (more reliable than find+delete)
     */
    @Modifying(clearAutomatically = true)
    @Transactional
    @Query("DELETE FROM IOBTradeResult t WHERE t.tradeMode = 'BACKTEST' AND t.instrumentToken = :instrumentToken")
    int deleteBacktestTradesByInstrument(@Param("instrumentToken") Long instrumentToken);

    /**
     * Check if trade_id exists
     */
    boolean existsByTradeId(String tradeId);

    /**
     * Get performance by IOB type
     */
    @Query("SELECT t.iobType, COUNT(t), SUM(CASE WHEN t.isWinner = true THEN 1 ELSE 0 END), AVG(t.achievedRR) " +
            "FROM IOBTradeResult t WHERE t.status = 'CLOSED' " +
            "GROUP BY t.iobType")
    List<Object[]> getPerformanceByIOBType();

    /**
     * Get performance by timeframe
     */
    @Query("SELECT t.timeframe, COUNT(t), SUM(CASE WHEN t.isWinner = true THEN 1 ELSE 0 END), AVG(t.achievedRR) " +
            "FROM IOBTradeResult t WHERE t.status = 'CLOSED' " +
            "GROUP BY t.timeframe")
    List<Object[]> getPerformanceByTimeframe();
}
