package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.SimulatedTrade;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface SimulatedTradeRepository extends JpaRepository<SimulatedTrade, Long> {

    Optional<SimulatedTrade> findByTradeId(String tradeId);

    List<SimulatedTrade> findAllByTradeIdIn(List<String> tradeIds);

    // Find trades by status
    List<SimulatedTrade> findByStatus(String status);

    // Find all open trades
    @Query("SELECT t FROM SimulatedTrade t WHERE t.status = 'OPEN' ORDER BY t.entryTime DESC")
    List<SimulatedTrade> findOpenTrades();

    // Find trades by date range
    List<SimulatedTrade> findByTradeDateBetweenOrderByEntryTimeDesc(LocalDateTime startDate, LocalDateTime endDate);

    // Find trades in date range
    @Query("SELECT t FROM SimulatedTrade t WHERE t.tradeDate BETWEEN :startDate AND :endDate ORDER BY t.tradeDate DESC")
    List<SimulatedTrade> findByTradeDateBetween(@Param("startDate") LocalDateTime startDate,
                                                  @Param("endDate") LocalDateTime endDate);

    // Find today's trades using parameters
    @Query("SELECT t FROM SimulatedTrade t WHERE t.tradeDate >= :startOfDay AND t.tradeDate < :endOfDay ORDER BY t.entryTime DESC")
    List<SimulatedTrade> findTodaysTrades(@Param("startOfDay") LocalDateTime startOfDay, @Param("endOfDay") LocalDateTime endOfDay);

    // Find today's closed trades
    @Query("SELECT t FROM SimulatedTrade t WHERE t.tradeDate >= :startOfDay AND t.tradeDate < :endOfDay AND t.status = 'CLOSED' ORDER BY t.exitTime DESC")
    List<SimulatedTrade> findTodaysClosedTrades(@Param("startOfDay") LocalDateTime startOfDay, @Param("endOfDay") LocalDateTime endOfDay);

    // Find trades by signal source
    List<SimulatedTrade> findBySignalSource(String signalSource);

    // Find trades by option type
    List<SimulatedTrade> findByOptionType(String optionType);

    // Find profitable trades today
    @Query("SELECT t FROM SimulatedTrade t WHERE t.tradeDate >= :startOfDay AND t.tradeDate < :endOfDay AND t.isProfitable = true")
    List<SimulatedTrade> findTodaysProfitableTrades(@Param("startOfDay") LocalDateTime startOfDay, @Param("endOfDay") LocalDateTime endOfDay);

    // Calculate today's total P&L
    @Query("SELECT COALESCE(SUM(t.netPnl), 0) FROM SimulatedTrade t WHERE t.tradeDate >= :startOfDay AND t.tradeDate < :endOfDay AND t.status = 'CLOSED'")
    Double calculateTodaysTotalPnl(@Param("startOfDay") LocalDateTime startOfDay, @Param("endOfDay") LocalDateTime endOfDay);

    // Calculate today's gross P&L
    @Query("SELECT COALESCE(SUM(t.grossPnl), 0) FROM SimulatedTrade t WHERE t.tradeDate >= :startOfDay AND t.tradeDate < :endOfDay AND t.status = 'CLOSED'")
    Double calculateTodaysGrossPnl(@Param("startOfDay") LocalDateTime startOfDay, @Param("endOfDay") LocalDateTime endOfDay);

    // Count today's trades
    @Query("SELECT COUNT(t) FROM SimulatedTrade t WHERE t.tradeDate >= :startOfDay AND t.tradeDate < :endOfDay")
    Long countTodaysTrades(@Param("startOfDay") LocalDateTime startOfDay, @Param("endOfDay") LocalDateTime endOfDay);

    // Count today's winning trades
    @Query("SELECT COUNT(t) FROM SimulatedTrade t WHERE t.tradeDate >= :startOfDay AND t.tradeDate < :endOfDay AND t.isProfitable = true AND t.status = 'CLOSED'")
    Long countTodaysWinningTrades(@Param("startOfDay") LocalDateTime startOfDay, @Param("endOfDay") LocalDateTime endOfDay);

    // Find last N trades using native query for LIMIT support
    @Query(value = "SELECT * FROM simulated_trades ORDER BY entry_time DESC LIMIT :limit", nativeQuery = true)
    List<SimulatedTrade> findLastNTrades(@Param("limit") int limit);

    // Find trades by exit reason for today
    @Query("SELECT t FROM SimulatedTrade t WHERE t.exitReason = :exitReason AND t.tradeDate >= :startOfDay AND t.tradeDate < :endOfDay")
    List<SimulatedTrade> findTodaysTradesByExitReason(@Param("exitReason") String exitReason, @Param("startOfDay") LocalDateTime startOfDay, @Param("endOfDay") LocalDateTime endOfDay);

    // Check if there's an open trade for a specific strike
    @Query("SELECT t FROM SimulatedTrade t WHERE t.strikePrice = :strike AND t.optionType = :optionType AND t.status = 'OPEN'")
    Optional<SimulatedTrade> findOpenTradeByStrikeAndType(@Param("strike") Double strike,
                                                           @Param("optionType") String optionType);

    // Get weekly P&L
    @Query("SELECT COALESCE(SUM(t.netPnl), 0) FROM SimulatedTrade t WHERE t.tradeDate >= :startOfWeek AND t.status = 'CLOSED'")
    Double calculateWeeklyPnl(@Param("startOfWeek") LocalDateTime startOfWeek);

    // Get monthly P&L
    @Query("SELECT COALESCE(SUM(t.netPnl), 0) FROM SimulatedTrade t WHERE t.tradeDate >= :startOfMonth AND t.status = 'CLOSED'")
    Double calculateMonthlyPnl(@Param("startOfMonth") LocalDateTime startOfMonth);

    // Find open trade by signal source
    @Query("SELECT t FROM SimulatedTrade t WHERE t.signalSource = :signalSource AND t.status = 'OPEN' ORDER BY t.entryTime DESC")
    Optional<SimulatedTrade> findOpenTradeBySignalSource(@Param("signalSource") String signalSource);

    // Find all open trades by signal source
    @Query("SELECT t FROM SimulatedTrade t WHERE t.signalSource = :signalSource AND t.status = 'OPEN'")
    List<SimulatedTrade> findOpenTradesBySignalSource(@Param("signalSource") String signalSource);

    // Find open trade by signal source and signal type (direction)
    @Query("SELECT t FROM SimulatedTrade t WHERE t.signalSource = :signalSource AND t.signalType = :signalType AND t.status = 'OPEN' ORDER BY t.entryTime DESC")
    Optional<SimulatedTrade> findOpenTradeBySignalSourceAndType(@Param("signalSource") String signalSource, @Param("signalType") String signalType);

    // Count open trades (to check if we can open more)
    @Query("SELECT COUNT(t) FROM SimulatedTrade t WHERE t.status = 'OPEN'")
    Long countOpenTrades();

    // ============= Performance Statistics Queries =============

    // Count closed trades by signal source in date range
    @Query("SELECT COUNT(t) FROM SimulatedTrade t WHERE t.signalSource = :signalSource AND t.status = 'CLOSED' AND t.tradeDate >= :startDate AND t.tradeDate < :endDate")
    Long countClosedTradesBySignalSource(@Param("signalSource") String signalSource, @Param("startDate") LocalDateTime startDate, @Param("endDate") LocalDateTime endDate);

    // Count winning trades by signal source in date range
    @Query("SELECT COUNT(t) FROM SimulatedTrade t WHERE t.signalSource = :signalSource AND t.status = 'CLOSED' AND t.isProfitable = true AND t.tradeDate >= :startDate AND t.tradeDate < :endDate")
    Long countWinningTradesBySignalSource(@Param("signalSource") String signalSource, @Param("startDate") LocalDateTime startDate, @Param("endDate") LocalDateTime endDate);

    // Calculate total P&L by signal source in date range
    @Query("SELECT COALESCE(SUM(t.netPnl), 0) FROM SimulatedTrade t WHERE t.signalSource = :signalSource AND t.status = 'CLOSED' AND t.tradeDate >= :startDate AND t.tradeDate < :endDate")
    Double calculatePnlBySignalSource(@Param("signalSource") String signalSource, @Param("startDate") LocalDateTime startDate, @Param("endDate") LocalDateTime endDate);

    // Find closed trades by signal source in date range
    @Query("SELECT t FROM SimulatedTrade t WHERE t.signalSource = :signalSource AND t.status = 'CLOSED' AND t.tradeDate >= :startDate AND t.tradeDate < :endDate ORDER BY t.tradeDate DESC")
    List<SimulatedTrade> findClosedTradesBySignalSource(@Param("signalSource") String signalSource, @Param("startDate") LocalDateTime startDate, @Param("endDate") LocalDateTime endDate);

    // Get distinct signal sources that have trades
    @Query("SELECT DISTINCT t.signalSource FROM SimulatedTrade t WHERE t.signalSource IS NOT NULL")
    List<String> findDistinctSignalSources();

    // Count all closed trades in date range
    @Query("SELECT COUNT(t) FROM SimulatedTrade t WHERE t.status = 'CLOSED' AND t.tradeDate >= :startDate AND t.tradeDate < :endDate")
    Long countClosedTradesInRange(@Param("startDate") LocalDateTime startDate, @Param("endDate") LocalDateTime endDate);

    // Count all winning trades in date range
    @Query("SELECT COUNT(t) FROM SimulatedTrade t WHERE t.status = 'CLOSED' AND t.isProfitable = true AND t.tradeDate >= :startDate AND t.tradeDate < :endDate")
    Long countWinningTradesInRange(@Param("startDate") LocalDateTime startDate, @Param("endDate") LocalDateTime endDate);

    // Calculate total P&L in date range
    @Query("SELECT COALESCE(SUM(t.netPnl), 0) FROM SimulatedTrade t WHERE t.status = 'CLOSED' AND t.tradeDate >= :startDate AND t.tradeDate < :endDate")
    Double calculatePnlInRange(@Param("startDate") LocalDateTime startDate, @Param("endDate") LocalDateTime endDate);
}
