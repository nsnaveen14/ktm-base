package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.PerformanceMetrics;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

@Repository
public interface PerformanceMetricsRepository extends JpaRepository<PerformanceMetrics, Long> {

    /**
     * Find latest metrics by period type
     */
    Optional<PerformanceMetrics> findTopByPeriodTypeOrderByMetricDateDesc(String periodType);

    /**
     * Find metrics for a specific date and period type
     */
    Optional<PerformanceMetrics> findByMetricDateAndPeriodType(LocalDate metricDate, String periodType);

    /**
     * Find all metrics for a date range
     */
    @Query("SELECT p FROM PerformanceMetrics p WHERE p.periodType = :periodType " +
            "AND p.metricDate >= :startDate AND p.metricDate <= :endDate " +
            "ORDER BY p.metricDate ASC")
    List<PerformanceMetrics> findMetricsInRange(
            @Param("periodType") String periodType,
            @Param("startDate") LocalDate startDate,
            @Param("endDate") LocalDate endDate);

    /**
     * Find metrics by instrument
     */
    @Query("SELECT p FROM PerformanceMetrics p WHERE p.instrumentToken = :instrumentToken " +
            "AND p.periodType = :periodType ORDER BY p.metricDate DESC")
    List<PerformanceMetrics> findByInstrumentAndPeriod(
            @Param("instrumentToken") Long instrumentToken,
            @Param("periodType") String periodType);

    /**
     * Find latest all-time metrics
     */
    @Query("SELECT p FROM PerformanceMetrics p WHERE p.periodType = 'ALL_TIME' " +
            "AND p.instrumentToken IS NULL ORDER BY p.calculationTimestamp DESC")
    Optional<PerformanceMetrics> findLatestAllTimeMetrics();

    /**
     * Find daily metrics for last N days
     */
    @Query("SELECT p FROM PerformanceMetrics p WHERE p.periodType = 'DAILY' " +
            "AND p.instrumentToken IS NULL ORDER BY p.metricDate DESC")
    List<PerformanceMetrics> findRecentDailyMetrics(org.springframework.data.domain.Pageable pageable);

    /**
     * Find weekly metrics
     */
    @Query("SELECT p FROM PerformanceMetrics p WHERE p.periodType = 'WEEKLY' " +
            "ORDER BY p.metricDate DESC")
    List<PerformanceMetrics> findWeeklyMetrics();

    /**
     * Find monthly metrics
     */
    @Query("SELECT p FROM PerformanceMetrics p WHERE p.periodType = 'MONTHLY' " +
            "ORDER BY p.metricDate DESC")
    List<PerformanceMetrics> findMonthlyMetrics();
}
