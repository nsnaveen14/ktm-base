package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.BrahmastraBacktestResult;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Repository for Brahmastra backtest results.
 */
@Repository
public interface BrahmastraBacktestResultRepository extends JpaRepository<BrahmastraBacktestResult, Long> {

    /**
     * Find backtest results by symbol.
     */
    List<BrahmastraBacktestResult> findBySymbolOrderByRunTimestampDesc(String symbol);

    /**
     * Find latest backtest result for a symbol.
     */
    @Query(value = "SELECT * FROM brahmastra_backtest_results b WHERE b.symbol = :symbol ORDER BY b.run_timestamp DESC LIMIT 1", nativeQuery = true)
    BrahmastraBacktestResult findLatestBySymbol(@Param("symbol") String symbol);

    /**
     * Find backtest results by timeframe.
     */
    List<BrahmastraBacktestResult> findByTimeframeOrderByRunTimestampDesc(String timeframe);

    /**
     * Find backtest results in date range.
     */
    List<BrahmastraBacktestResult> findByRunTimestampBetweenOrderByRunTimestampDesc(
            LocalDateTime start, LocalDateTime end);

    /**
     * Get average win rate across all backtests for a symbol.
     */
    @Query("SELECT AVG(b.winRate) FROM BrahmastraBacktestResult b WHERE b.symbol = :symbol")
    Double getAverageWinRateBySymbol(@Param("symbol") String symbol);

    /**
     * Get best performing backtest for a symbol.
     */
    @Query(value = "SELECT * FROM brahmastra_backtest_results b WHERE b.symbol = :symbol ORDER BY b.net_pnl_percent DESC LIMIT 1", nativeQuery = true)
    BrahmastraBacktestResult findBestPerformingBySymbol(@Param("symbol") String symbol);
}

