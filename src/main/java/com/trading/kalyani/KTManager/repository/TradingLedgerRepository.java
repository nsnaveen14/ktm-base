package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.TradingLedger;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

@Repository
public interface TradingLedgerRepository extends JpaRepository<TradingLedger, Long> {

    Optional<TradingLedger> findByTradeDate(LocalDate tradeDate);

    // Find ledger entries in date range
    List<TradingLedger> findByTradeDateBetween(LocalDate startDate, LocalDate endDate);

    // Find last N days of ledger
    @Query("SELECT l FROM TradingLedger l ORDER BY l.tradeDate DESC LIMIT :limit")
    List<TradingLedger> findLastNDays(@Param("limit") int limit);

    // Calculate total P&L for date range
    @Query("SELECT COALESCE(SUM(l.netPnl), 0) FROM TradingLedger l WHERE l.tradeDate BETWEEN :startDate AND :endDate")
    Double calculateTotalPnlForRange(@Param("startDate") LocalDate startDate,
                                      @Param("endDate") LocalDate endDate);

    // Calculate average win rate
    @Query("SELECT COALESCE(AVG(l.winRate), 0) FROM TradingLedger l WHERE l.tradeDate BETWEEN :startDate AND :endDate")
    Double calculateAvgWinRateForRange(@Param("startDate") LocalDate startDate,
                                        @Param("endDate") LocalDate endDate);

    // Get total trades for range
    @Query("SELECT COALESCE(SUM(l.totalTrades), 0) FROM TradingLedger l WHERE l.tradeDate BETWEEN :startDate AND :endDate")
    Integer calculateTotalTradesForRange(@Param("startDate") LocalDate startDate,
                                          @Param("endDate") LocalDate endDate);

    // Find profitable days
    @Query("SELECT l FROM TradingLedger l WHERE l.netPnl > 0 ORDER BY l.tradeDate DESC")
    List<TradingLedger> findProfitableDays();

    // Find losing days
    @Query("SELECT l FROM TradingLedger l WHERE l.netPnl < 0 ORDER BY l.tradeDate DESC")
    List<TradingLedger> findLosingDays();

    // Get current month ledger entries
    @Query("SELECT l FROM TradingLedger l WHERE MONTH(l.tradeDate) = MONTH(CURRENT_DATE) AND YEAR(l.tradeDate) = YEAR(CURRENT_DATE) ORDER BY l.tradeDate DESC")
    List<TradingLedger> findCurrentMonthLedger();

    // Get this week's ledger entries
    @Query("SELECT l FROM TradingLedger l WHERE l.tradeDate >= :startOfWeek ORDER BY l.tradeDate DESC")
    List<TradingLedger> findThisWeekLedger(@Param("startOfWeek") LocalDate startOfWeek);
}

