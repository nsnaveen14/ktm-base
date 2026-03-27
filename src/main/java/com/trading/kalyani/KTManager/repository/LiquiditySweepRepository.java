package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.LiquiditySweepAnalysis;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface LiquiditySweepRepository extends JpaRepository<LiquiditySweepAnalysis, Long> {

    /**
     * Find the latest analysis for a given job config
     */
    @Query("SELECT l FROM LiquiditySweepAnalysis l WHERE l.appJobConfigNum = :appJobConfigNum ORDER BY l.analysisTimestamp DESC LIMIT 1")
    Optional<LiquiditySweepAnalysis> findLatestByAppJobConfigNum(@Param("appJobConfigNum") Integer appJobConfigNum);

    /**
     * Find analyses with valid trade setups
     */
    @Query("SELECT l FROM LiquiditySweepAnalysis l WHERE l.appJobConfigNum = :appJobConfigNum AND l.isValidSetup = true ORDER BY l.analysisTimestamp DESC")
    List<LiquiditySweepAnalysis> findValidSetupsByAppJobConfigNum(@Param("appJobConfigNum") Integer appJobConfigNum);

    /**
     * Find latest valid setup for a job config
     */
    @Query("SELECT l FROM LiquiditySweepAnalysis l WHERE l.appJobConfigNum = :appJobConfigNum AND l.isValidSetup = true ORDER BY l.analysisTimestamp DESC LIMIT 1")
    Optional<LiquiditySweepAnalysis> findLatestValidSetupByAppJobConfigNum(@Param("appJobConfigNum") Integer appJobConfigNum);

    /**
     * Find analyses by signal type
     */
    @Query("SELECT l FROM LiquiditySweepAnalysis l WHERE l.appJobConfigNum = :appJobConfigNum AND l.signalType = :signalType ORDER BY l.analysisTimestamp DESC")
    List<LiquiditySweepAnalysis> findBySignalType(@Param("appJobConfigNum") Integer appJobConfigNum, @Param("signalType") String signalType);

    /**
     * Find analyses with whale activity
     */
    @Query("SELECT l FROM LiquiditySweepAnalysis l WHERE l.appJobConfigNum = :appJobConfigNum AND l.hasWhaleActivity = true ORDER BY l.analysisTimestamp DESC")
    List<LiquiditySweepAnalysis> findWithWhaleActivity(@Param("appJobConfigNum") Integer appJobConfigNum);

    /**
     * Find today's analyses for a job config
     */
    @Query("SELECT l FROM LiquiditySweepAnalysis l WHERE l.appJobConfigNum = :appJobConfigNum AND l.analysisTimestamp >= :startOfDay ORDER BY l.analysisTimestamp DESC")
    List<LiquiditySweepAnalysis> findTodaysAnalyses(@Param("appJobConfigNum") Integer appJobConfigNum, @Param("startOfDay") LocalDateTime startOfDay);

    /**
     * Find analyses within a time range
     */
    @Query("SELECT l FROM LiquiditySweepAnalysis l WHERE l.appJobConfigNum = :appJobConfigNum AND l.analysisTimestamp BETWEEN :startTime AND :endTime ORDER BY l.analysisTimestamp DESC")
    List<LiquiditySweepAnalysis> findByTimeRange(@Param("appJobConfigNum") Integer appJobConfigNum,
                                                   @Param("startTime") LocalDateTime startTime,
                                                   @Param("endTime") LocalDateTime endTime);

    /**
     * Count valid setups today
     */
    @Query("SELECT COUNT(l) FROM LiquiditySweepAnalysis l WHERE l.appJobConfigNum = :appJobConfigNum AND l.isValidSetup = true AND l.analysisTimestamp >= :startOfDay")
    Long countValidSetupsToday(@Param("appJobConfigNum") Integer appJobConfigNum, @Param("startOfDay") LocalDateTime startOfDay);

    /**
     * Find latest sweep events
     */
    @Query("SELECT l FROM LiquiditySweepAnalysis l WHERE l.appJobConfigNum = :appJobConfigNum AND (l.bslSwept = true OR l.sslSwept = true) ORDER BY l.analysisTimestamp DESC")
    List<LiquiditySweepAnalysis> findRecentSweepEvents(@Param("appJobConfigNum") Integer appJobConfigNum);
}

