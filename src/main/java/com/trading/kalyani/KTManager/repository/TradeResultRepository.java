package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.TradeResult;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface TradeResultRepository extends JpaRepository<TradeResult, Long> {

    /**
     * Find trade result by IOB ID
     */
    Optional<TradeResult> findByIobId(Long iobId);

    /**
     * Find trade result by trade ID
     */
    Optional<TradeResult> findByTradeId(String tradeId);

    /**
     * Find all open trades
     */
    @Query("SELECT t FROM TradeResult t WHERE t.status = 'OPEN' ORDER BY t.entryTime DESC")
    List<TradeResult> findOpenTrades();

    /**
     * Find open trades for an instrument
     */
    @Query("SELECT t FROM TradeResult t WHERE t.instrumentToken = :instrumentToken AND t.status = 'OPEN'")
    List<TradeResult> findOpenTradesByInstrument(@Param("instrumentToken") Long instrumentToken);

    /**
     * Find closed trades in date range
     */
    @Query("SELECT t FROM TradeResult t WHERE t.status = 'CLOSED' " +
            "AND t.exitTime >= :startDate AND t.exitTime <= :endDate " +
            "ORDER BY t.exitTime DESC")
    List<TradeResult> findClosedTradesInRange(
            @Param("startDate") LocalDateTime startDate,
            @Param("endDate") LocalDateTime endDate);

    /**
     * Find trades by instrument in date range
     */
    @Query("SELECT t FROM TradeResult t WHERE t.instrumentToken = :instrumentToken " +
            "AND t.status = 'CLOSED' " +
            "AND t.exitTime >= :startDate AND t.exitTime <= :endDate " +
            "ORDER BY t.exitTime DESC")
    List<TradeResult> findTradesByInstrumentInRange(
            @Param("instrumentToken") Long instrumentToken,
            @Param("startDate") LocalDateTime startDate,
            @Param("endDate") LocalDateTime endDate);

    /**
     * Find all closed trades for analysis
     */
    @Query("SELECT t FROM TradeResult t WHERE t.status = 'CLOSED' ORDER BY t.exitTime DESC")
    List<TradeResult> findAllClosedTrades();

    /**
     * Find trades by type (SIMULATED, LIVE, BACKTEST)
     */
    @Query("SELECT t FROM TradeResult t WHERE t.tradeType = :tradeType AND t.status = 'CLOSED' " +
            "ORDER BY t.exitTime DESC")
    List<TradeResult> findByTradeType(@Param("tradeType") String tradeType);

    /**
     * Find winning trades
     */
    @Query("SELECT t FROM TradeResult t WHERE t.outcome = 'WIN' AND t.status = 'CLOSED' " +
            "ORDER BY t.exitTime DESC")
    List<TradeResult> findWinningTrades();

    /**
     * Find losing trades
     */
    @Query("SELECT t FROM TradeResult t WHERE t.outcome = 'LOSS' AND t.status = 'CLOSED' " +
            "ORDER BY t.exitTime DESC")
    List<TradeResult> findLosingTrades();

    /**
     * Count trades by outcome
     */
    @Query("SELECT t.outcome, COUNT(t) FROM TradeResult t WHERE t.status = 'CLOSED' " +
            "GROUP BY t.outcome")
    List<Object[]> countByOutcome();

    /**
     * Sum P&L for closed trades
     */
    @Query("SELECT SUM(t.netPnl) FROM TradeResult t WHERE t.status = 'CLOSED'")
    Double sumNetPnl();

    /**
     * Sum P&L for date range
     */
    @Query("SELECT SUM(t.netPnl) FROM TradeResult t WHERE t.status = 'CLOSED' " +
            "AND t.exitTime >= :startDate AND t.exitTime <= :endDate")
    Double sumNetPnlInRange(
            @Param("startDate") LocalDateTime startDate,
            @Param("endDate") LocalDateTime endDate);

    /**
     * Find trades with FVG confluence
     */
    @Query("SELECT t FROM TradeResult t WHERE t.hadFvg = true AND t.status = 'CLOSED'")
    List<TradeResult> findTradesWithFVG();

    /**
     * Find trend-aligned trades
     */
    @Query("SELECT t FROM TradeResult t WHERE t.wasTrendAligned = true AND t.status = 'CLOSED'")
    List<TradeResult> findTrendAlignedTrades();

    /**
     * Find high-confidence trades (confidence > 75)
     */
    @Query("SELECT t FROM TradeResult t WHERE t.signalConfidence > 75 AND t.status = 'CLOSED'")
    List<TradeResult> findHighConfidenceTrades();

    /**
     * Find recent trades (last N)
     */
    @Query("SELECT t FROM TradeResult t WHERE t.status = 'CLOSED' ORDER BY t.exitTime DESC")
    List<TradeResult> findRecentTrades(org.springframework.data.domain.Pageable pageable);

    /**
     * Find by IOB type
     */
    @Query("SELECT t FROM TradeResult t WHERE t.iobType = :iobType AND t.status = 'CLOSED'")
    List<TradeResult> findByIOBType(@Param("iobType") String iobType);

    /**
     * Calculate average metrics
     */
    @Query("SELECT AVG(t.netPnl), AVG(t.achievedRRRatio), AVG(t.durationMinutes) " +
            "FROM TradeResult t WHERE t.status = 'CLOSED'")
    Object[] calculateAverageMetrics();

    /**
     * Find trades for backtest period
     */
    @Query("SELECT t FROM TradeResult t WHERE t.tradeType = 'BACKTEST' " +
            "AND t.entryTime >= :startDate AND t.entryTime <= :endDate " +
            "ORDER BY t.entryTime ASC")
    List<TradeResult> findBacktestTrades(
            @Param("startDate") LocalDateTime startDate,
            @Param("endDate") LocalDateTime endDate);
}
