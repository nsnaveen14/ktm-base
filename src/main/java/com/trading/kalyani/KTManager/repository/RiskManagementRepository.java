package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.RiskManagement;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Repository for Risk Management data.
 */
@Repository
public interface RiskManagementRepository extends JpaRepository<RiskManagement, Long> {

    /**
     * Find latest risk management record for an instrument
     */
    Optional<RiskManagement> findTopByInstrumentTokenOrderByAnalysisTimestampDesc(Long instrumentToken);

    /**
     * Find today's risk management record for an instrument
     */
    Optional<RiskManagement> findByInstrumentTokenAndAnalysisDate(Long instrumentToken, LocalDate analysisDate);

    /**
     * Find all risk management records for an instrument
     */
    List<RiskManagement> findByInstrumentTokenOrderByAnalysisDateDesc(Long instrumentToken);

    /**
     * Find global risk management record (instrumentToken = null)
     */
    Optional<RiskManagement> findTopByInstrumentTokenIsNullOrderByAnalysisTimestampDesc();

    /**
     * Find today's global risk management record
     */
    Optional<RiskManagement> findByInstrumentTokenIsNullAndAnalysisDate(LocalDate analysisDate);

    /**
     * Find risk records where daily limit was reached
     */
    List<RiskManagement> findByDailyLimitReachedTrueOrderByAnalysisDateDesc();

    /**
     * Find risk records where portfolio heat was exceeded
     */
    List<RiskManagement> findByPortfolioHeatExceededTrueOrderByAnalysisDateDesc();

    /**
     * Find risk records where trading is blocked
     */
    List<RiskManagement> findByTradingAllowedFalseOrderByAnalysisDateDesc();

    /**
     * Find by correlation group
     */
    List<RiskManagement> findByCorrelationGroupAndAnalysisDate(String correlationGroup, LocalDate analysisDate);

    /**
     * Get total correlated exposure for a group today
     */
    @Query("SELECT SUM(r.currentInstrumentExposure) FROM RiskManagement r " +
           "WHERE r.correlationGroup = :group AND r.analysisDate = :date")
    Double getTotalCorrelatedExposure(@Param("group") String correlationGroup, @Param("date") LocalDate date);

    /**
     * Get total open positions count today
     */
    @Query("SELECT SUM(r.openPositionsCount) FROM RiskManagement r " +
           "WHERE r.analysisDate = :date AND r.openPositionsCount IS NOT NULL")
    Integer getTotalOpenPositions(@Param("date") LocalDate date);

    /**
     * Get total portfolio heat today
     */
    @Query("SELECT SUM(r.totalOpenRisk) FROM RiskManagement r " +
           "WHERE r.analysisDate = :date AND r.totalOpenRisk IS NOT NULL")
    Double getTotalPortfolioRisk(@Param("date") LocalDate date);

    /**
     * Get daily P&L summary
     */
    @Query("SELECT SUM(r.dailyTotalPnl) FROM RiskManagement r " +
           "WHERE r.analysisDate = :date AND r.dailyTotalPnl IS NOT NULL")
    Double getTotalDailyPnl(@Param("date") LocalDate date);

    /**
     * Get average win rate across instruments
     */
    @Query("SELECT AVG(r.winRate) FROM RiskManagement r " +
           "WHERE r.winRate IS NOT NULL AND r.analysisDate >= :since")
    Double getAverageWinRate(@Param("since") LocalDate since);

    /**
     * Get maximum drawdown in period
     */
    @Query("SELECT MAX(r.maxDrawdownPercent) FROM RiskManagement r " +
           "WHERE r.analysisDate >= :since")
    Double getMaxDrawdownInPeriod(@Param("since") LocalDate since);

    /**
     * Delete old risk management records
     */
    void deleteByAnalysisDateBefore(LocalDate before);
}
