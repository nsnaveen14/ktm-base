package com.trading.kalyani.KTManager.service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Service for backtesting IOB trading strategy on historical data.
 */
public interface BacktestingService {

    /**
     * Run a backtest for a specific instrument over a date range
     */
    Map<String, Object> runBacktest(Long instrumentToken, String timeframe,
                                    LocalDateTime startDate, LocalDateTime endDate,
                                    Map<String, Object> parameters);

    /**
     * Run backtest for all indices
     */
    Map<String, Object> runBacktestAllIndices(LocalDateTime startDate, LocalDateTime endDate,
                                               Map<String, Object> parameters);

    /**
     * Get backtest results
     */
    Map<String, Object> getBacktestResults(String backtestId);

    /**
     * Get all backtest sessions
     */
    List<Map<String, Object>> getBacktestHistory();

    /**
     * Compare two backtest results
     */
    Map<String, Object> compareBacktests(String backtestId1, String backtestId2);

    /**
     * Optimize parameters based on backtest results
     */
    Map<String, Object> optimizeParameters(Long instrumentToken,
                                            LocalDateTime startDate, LocalDateTime endDate);
}
