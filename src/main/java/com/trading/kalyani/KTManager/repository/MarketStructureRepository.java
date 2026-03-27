package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.MarketStructure;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Repository for Market Structure Analysis data.
 */
@Repository
public interface MarketStructureRepository extends JpaRepository<MarketStructure, Long> {

    /**
     * Find latest market structure for an instrument and timeframe
     */
    Optional<MarketStructure> findTopByInstrumentTokenAndTimeframeOrderByAnalysisTimestampDesc(
            Long instrumentToken, String timeframe);

    /**
     * Find all market structures for an instrument
     */
    List<MarketStructure> findByInstrumentTokenOrderByAnalysisTimestampDesc(Long instrumentToken);

    /**
     * Find market structures by timeframe
     */
    List<MarketStructure> findByInstrumentTokenAndTimeframeOrderByAnalysisTimestampDesc(
            Long instrumentToken, String timeframe);

    /**
     * Find market structures with CHoCH detected
     */
    List<MarketStructure> findByInstrumentTokenAndChochDetectedTrueOrderByChochTimestampDesc(
            Long instrumentToken);

    /**
     * Find market structures with CHoCH detected for a specific timeframe
     */
    List<MarketStructure> findByInstrumentTokenAndTimeframeAndChochDetectedTrueOrderByChochTimestampDesc(
            Long instrumentToken, String timeframe);

    /**
     * Find recent market structures within a time range
     */
    List<MarketStructure> findByInstrumentTokenAndAnalysisTimestampAfterOrderByAnalysisTimestampDesc(
            Long instrumentToken, LocalDateTime after);

    /**
     * Find market structures by trend direction
     */
    List<MarketStructure> findByInstrumentTokenAndTrendDirectionOrderByAnalysisTimestampDesc(
            Long instrumentToken, String trendDirection);

    /**
     * Find market structures by market phase
     */
    List<MarketStructure> findByInstrumentTokenAndMarketPhaseOrderByAnalysisTimestampDesc(
            Long instrumentToken, String marketPhase);

    /**
     * Find structures with specific overall bias
     */
    List<MarketStructure> findByInstrumentTokenAndOverallBiasOrderByAnalysisTimestampDesc(
            Long instrumentToken, String overallBias);

    /**
     * Count CHoCH events in a time period
     */
    @Query("SELECT COUNT(m) FROM MarketStructure m WHERE m.instrumentToken = :token " +
           "AND m.chochDetected = true AND m.analysisTimestamp >= :since")
    Long countChochEventsSince(@Param("token") Long instrumentToken, @Param("since") LocalDateTime since);

    /**
     * Delete old market structure records
     */
    void deleteByAnalysisTimestampBefore(LocalDateTime before);
}
