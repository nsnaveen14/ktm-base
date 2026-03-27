package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.LiquidityZoneAnalysis;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface LiquidityZoneAnalysisRepository extends JpaRepository<LiquidityZoneAnalysis, Long> {

    /**
     * Find latest analysis for a specific instrument and timeframe
     */
    Optional<LiquidityZoneAnalysis> findFirstByInstrumentTokenAndTimeframeOrderByAnalysisTimestampDesc(
            Long instrumentToken, String timeframe);

    /**
     * Find all analyses for an instrument today
     */
    @Query("SELECT l FROM LiquidityZoneAnalysis l WHERE l.instrumentToken = :instrumentToken " +
            "AND l.analysisTimestamp >= :startOfDay ORDER BY l.analysisTimestamp DESC")
    List<LiquidityZoneAnalysis> findTodaysAnalyses(
            @Param("instrumentToken") Long instrumentToken,
            @Param("startOfDay") LocalDateTime startOfDay);

    /**
     * Find analyses by instrument and timeframe for today
     */
    @Query("SELECT l FROM LiquidityZoneAnalysis l WHERE l.instrumentToken = :instrumentToken " +
            "AND l.timeframe = :timeframe AND l.analysisTimestamp >= :startOfDay " +
            "ORDER BY l.analysisTimestamp DESC")
    List<LiquidityZoneAnalysis> findTodaysAnalysesByTimeframe(
            @Param("instrumentToken") Long instrumentToken,
            @Param("timeframe") String timeframe,
            @Param("startOfDay") LocalDateTime startOfDay);

    /**
     * Find valid trade setups for an instrument today
     */
    @Query("SELECT l FROM LiquidityZoneAnalysis l WHERE l.instrumentToken = :instrumentToken " +
            "AND l.isValidSetup = true AND l.analysisTimestamp >= :startOfDay " +
            "ORDER BY l.analysisTimestamp DESC")
    List<LiquidityZoneAnalysis> findValidSetupsToday(
            @Param("instrumentToken") Long instrumentToken,
            @Param("startOfDay") LocalDateTime startOfDay);

    /**
     * Find latest valid setup for instrument and timeframe
     */
    @Query("SELECT l FROM LiquidityZoneAnalysis l WHERE l.instrumentToken = :instrumentToken " +
            "AND l.timeframe = :timeframe AND l.isValidSetup = true " +
            "ORDER BY l.analysisTimestamp DESC")
    Optional<LiquidityZoneAnalysis> findLatestValidSetup(
            @Param("instrumentToken") Long instrumentToken,
            @Param("timeframe") String timeframe);

    /**
     * Find all analyses for multiple instruments today
     */
    @Query("SELECT l FROM LiquidityZoneAnalysis l WHERE l.instrumentToken IN :instrumentTokens " +
            "AND l.analysisTimestamp >= :startOfDay ORDER BY l.analysisTimestamp DESC")
    List<LiquidityZoneAnalysis> findTodaysAnalysesForInstruments(
            @Param("instrumentTokens") List<Long> instrumentTokens,
            @Param("startOfDay") LocalDateTime startOfDay);

    /**
     * Count liquidity grabs today
     */
    @Query("SELECT COUNT(l) FROM LiquidityZoneAnalysis l WHERE l.instrumentToken = :instrumentToken " +
            "AND (l.buySideGrabbed = true OR l.sellSideGrabbed = true) " +
            "AND l.analysisTimestamp >= :startOfDay")
    Long countLiquidityGrabsToday(
            @Param("instrumentToken") Long instrumentToken,
            @Param("startOfDay") LocalDateTime startOfDay);
}

