package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.VolumeProfile;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Repository for Volume Profile Analysis data.
 */
@Repository
public interface VolumeProfileRepository extends JpaRepository<VolumeProfile, Long> {

    /**
     * Find latest volume profile for an instrument and timeframe
     */
    Optional<VolumeProfile> findTopByInstrumentTokenAndTimeframeOrderByAnalysisTimestampDesc(
            Long instrumentToken, String timeframe);

    /**
     * Find all volume profiles for an instrument
     */
    List<VolumeProfile> findByInstrumentTokenOrderByAnalysisTimestampDesc(Long instrumentToken);

    /**
     * Find volume profiles by timeframe
     */
    List<VolumeProfile> findByInstrumentTokenAndTimeframeOrderByAnalysisTimestampDesc(
            Long instrumentToken, String timeframe);

    /**
     * Find volume profile for a specific IOB
     */
    Optional<VolumeProfile> findByIobId(Long iobId);

    /**
     * Find volume profiles with institutional volume type
     */
    List<VolumeProfile> findByInstrumentTokenAndIobVolumeTypeOrderByAnalysisTimestampDesc(
            Long instrumentToken, String iobVolumeType);

    /**
     * Find volume profiles with confirmed displacement
     */
    List<VolumeProfile> findByInstrumentTokenAndDisplacementConfirmedTrueOrderByAnalysisTimestampDesc(
            Long instrumentToken);

    /**
     * Find volume profiles with POC aligned to IOB
     */
    List<VolumeProfile> findByInstrumentTokenAndPocIobAlignedTrueOrderByAnalysisTimestampDesc(
            Long instrumentToken);

    /**
     * Find recent volume profiles within a time range
     */
    List<VolumeProfile> findByInstrumentTokenAndAnalysisTimestampAfterOrderByAnalysisTimestampDesc(
            Long instrumentToken, LocalDateTime after);

    /**
     * Find volume profiles with high confluence score
     */
    @Query("SELECT v FROM VolumeProfile v WHERE v.instrumentToken = :token " +
           "AND v.volumeConfluenceScore >= :minScore ORDER BY v.analysisTimestamp DESC")
    List<VolumeProfile> findHighConfluenceProfiles(
            @Param("token") Long instrumentToken, @Param("minScore") Double minScore);

    /**
     * Find volume profiles by delta direction
     */
    List<VolumeProfile> findByInstrumentTokenAndDeltaDirectionOrderByAnalysisTimestampDesc(
            Long instrumentToken, String deltaDirection);

    /**
     * Get average volume confluence score for an instrument
     */
    @Query("SELECT AVG(v.volumeConfluenceScore) FROM VolumeProfile v WHERE v.instrumentToken = :token " +
           "AND v.analysisTimestamp >= :since")
    Double getAverageConfluenceScore(@Param("token") Long instrumentToken, @Param("since") LocalDateTime since);

    /**
     * Delete old volume profile records
     */
    void deleteByAnalysisTimestampBefore(LocalDateTime before);
}
