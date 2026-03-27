package com.trading.kalyani.KTManager.controller;

import com.trading.kalyani.KTManager.entity.InternalOrderBlock;
import com.trading.kalyani.KTManager.service.InternalOrderBlockService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.NIFTY_INSTRUMENT_TOKEN;

/**
 * REST Controller for Internal Order Block (IOB) Analysis.
 * Provides endpoints for IOB detection, trade setup generation, and trade execution.
 */
@RestController
@RequestMapping("/api/iob")
@CrossOrigin(origins = "*")
public class InternalOrderBlockController {

    private static final Logger logger = LoggerFactory.getLogger(InternalOrderBlockController.class);

    @Autowired
    private InternalOrderBlockService iobService;

    /**
     * Scan for Internal Order Blocks for a specific instrument
     */
    @PostMapping("/scan/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> scanForIOBs(
            @PathVariable Long instrumentToken,
            @RequestParam(defaultValue = "5min") String timeframe) {
        try {
            logger.info("Scanning for IOBs - Token: {}, Timeframe: {}", instrumentToken, timeframe);

            List<InternalOrderBlock> iobs = iobService.scanForIOBs(instrumentToken, timeframe);

            Map<String, Object> result = new HashMap<>();
            result.put("success", true);
            result.put("instrumentToken", instrumentToken);
            result.put("timeframe", timeframe);
            result.put("detectedCount", iobs.size());
            result.put("iobs", iobs.stream().map(this::convertToSummary).collect(Collectors.toList()));

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            logger.error("Error scanning for IOBs: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Scan all configured indices for IOBs
     */
    @PostMapping("/scan-all")
    public ResponseEntity<Map<String, Object>> scanAllIndices() {
        try {
            logger.info("Scanning all indices for IOBs");

            Map<String, Object> result = iobService.analyzeAllIndices();
            result.put("success", true);

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            logger.error("Error scanning all indices: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Get all fresh (unmitigated) IOBs for an instrument
     */
    @GetMapping("/fresh/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getFreshIOBs(@PathVariable Long instrumentToken) {
        try {
            List<InternalOrderBlock> freshIOBs = iobService.getFreshIOBs(instrumentToken);

            Map<String, Object> result = new HashMap<>();
            result.put("success", true);
            result.put("instrumentToken", instrumentToken);
            result.put("count", freshIOBs.size());
            result.put("iobs", freshIOBs.stream().map(this::convertToSummary).collect(Collectors.toList()));

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            logger.error("Error fetching fresh IOBs: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get bullish IOBs
     */
    @GetMapping("/bullish/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getBullishIOBs(@PathVariable Long instrumentToken) {
        try {
            List<InternalOrderBlock> bullishIOBs = iobService.getBullishIOBs(instrumentToken);

            Map<String, Object> result = new HashMap<>();
            result.put("success", true);
            result.put("instrumentToken", instrumentToken);
            result.put("type", "BULLISH_IOB");
            result.put("count", bullishIOBs.size());
            result.put("iobs", bullishIOBs.stream().map(this::convertToSummary).collect(Collectors.toList()));

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            logger.error("Error fetching bullish IOBs: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get bearish IOBs
     */
    @GetMapping("/bearish/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getBearishIOBs(@PathVariable Long instrumentToken) {
        try {
            List<InternalOrderBlock> bearishIOBs = iobService.getBearishIOBs(instrumentToken);

            Map<String, Object> result = new HashMap<>();
            result.put("success", true);
            result.put("instrumentToken", instrumentToken);
            result.put("type", "BEARISH_IOB");
            result.put("count", bearishIOBs.size());
            result.put("iobs", bearishIOBs.stream().map(this::convertToSummary).collect(Collectors.toList()));

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            logger.error("Error fetching bearish IOBs: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get today's detected IOBs
     */
    @GetMapping("/today/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getTodaysIOBs(@PathVariable Long instrumentToken) {
        try {
            List<InternalOrderBlock> todaysIOBs = iobService.getTodaysIOBs(instrumentToken);

            Map<String, Object> result = new HashMap<>();
            result.put("success", true);
            result.put("instrumentToken", instrumentToken);
            result.put("count", todaysIOBs.size());
            result.put("iobs", todaysIOBs.stream().map(this::convertToSummary).collect(Collectors.toList()));

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            logger.error("Error fetching today's IOBs: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get valid tradable IOBs
     */
    @GetMapping("/tradable/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getTradableIOBs(@PathVariable Long instrumentToken) {
        try {
            List<InternalOrderBlock> tradableIOBs = iobService.getValidTradableIOBs(instrumentToken);

            Map<String, Object> result = new HashMap<>();
            result.put("success", true);
            result.put("instrumentToken", instrumentToken);
            result.put("count", tradableIOBs.size());
            result.put("iobs", tradableIOBs.stream().map(this::convertToSummary).collect(Collectors.toList()));

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            logger.error("Error fetching tradable IOBs: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Check mitigation for an instrument at current price
     */
    @PostMapping("/check-mitigation")
    public ResponseEntity<Map<String, Object>> checkMitigation(
            @RequestParam Long instrumentToken,
            @RequestParam Double currentPrice) {
        try {
            List<InternalOrderBlock> mitigatedIOBs = iobService.checkMitigation(instrumentToken, currentPrice);

            Map<String, Object> result = new HashMap<>();
            result.put("success", true);
            result.put("instrumentToken", instrumentToken);
            result.put("currentPrice", currentPrice);
            result.put("mitigatedCount", mitigatedIOBs.size());
            result.put("mitigatedIOBs", mitigatedIOBs.stream().map(this::convertToSummary).collect(Collectors.toList()));

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            logger.error("Error checking mitigation: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get dashboard data for all indices
     */
    @GetMapping("/dashboard")
    public ResponseEntity<Map<String, Object>> getDashboard() {
        try {
            List<Long> tokens = Arrays.asList(NIFTY_INSTRUMENT_TOKEN);
            Map<String, Object> dashboard = iobService.getDashboardData(tokens);

            return ResponseEntity.ok(dashboard);

        } catch (Exception e) {
            logger.error("Error fetching dashboard: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get detailed analysis for an instrument
     */
    @GetMapping("/analysis/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getDetailedAnalysis(@PathVariable Long instrumentToken) {
        try {
            Map<String, Object> analysis = iobService.getDetailedAnalysis(instrumentToken);
            analysis.put("success", true);

            return ResponseEntity.ok(analysis);

        } catch (Exception e) {
            logger.error("Error fetching detailed analysis: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Generate trade setup from IOB
     */
    @GetMapping("/trade-setup/{iobId}")
    public ResponseEntity<Map<String, Object>> generateTradeSetup(@PathVariable Long iobId) {
        try {
            Map<String, Object> setup = iobService.generateTradeSetup(iobId);

            if (setup.isEmpty()) {
                return ResponseEntity.notFound().build();
            }

            setup.put("success", true);
            return ResponseEntity.ok(setup);

        } catch (Exception e) {
            logger.error("Error generating trade setup: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Execute trade based on IOB
     */
    @PostMapping("/execute-trade/{iobId}")
    public ResponseEntity<Map<String, Object>> executeTrade(@PathVariable Long iobId) {
        try {
            Map<String, Object> result = iobService.executeTrade(iobId);

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            logger.error("Error executing trade: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Mark IOB as mitigated
     */
    @PostMapping("/mitigate/{iobId}")
    public ResponseEntity<Map<String, Object>> markAsMitigated(@PathVariable Long iobId) {
        try {
            iobService.markAsMitigated(iobId);

            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "iobId", iobId,
                    "status", "MITIGATED"
            ));

        } catch (Exception e) {
            logger.error("Error marking IOB as mitigated: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Mark all fresh IOBs as mitigated for a given instrument
     */
    @PostMapping("/mitigate-all/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> mitigateAllFresh(@PathVariable Long instrumentToken) {
        try {
            int mitigatedCount = iobService.mitigateAllFresh(instrumentToken);

            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "instrumentToken", instrumentToken,
                    "mitigatedCount", mitigatedCount,
                    "message", "Successfully mitigated " + mitigatedCount + " fresh IOBs"
            ));

        } catch (Exception e) {
            logger.error("Error mitigating all fresh IOBs: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Mark multiple IOBs as completed manually
     */
    @PostMapping("/mark-completed")
    public ResponseEntity<Map<String, Object>> markAsCompleted(@RequestBody Map<String, List<Long>> request) {
        try {
            List<Long> iobIds = request.get("iobIds");
            if (iobIds == null || iobIds.isEmpty()) {
                return ResponseEntity.badRequest().body(Map.of(
                        "success", false,
                        "error", "No IOB IDs provided"
                ));
            }

            int completedCount = iobService.markAsCompleted(iobIds);

            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "completedCount", completedCount,
                    "message", "Successfully marked " + completedCount + " IOB(s) as completed"
            ));

        } catch (Exception e) {
            logger.error("Error marking IOBs as completed: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "success", false,
                    "error", e.getMessage()
            ));
        }
    }

    /**
     * Get IOB statistics
     */
    @GetMapping("/statistics/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getStatistics(@PathVariable Long instrumentToken) {
        try {
            Map<String, Object> stats = iobService.getStatistics(instrumentToken);
            stats.put("success", true);
            stats.put("instrumentToken", instrumentToken);

            return ResponseEntity.ok(stats);

        } catch (Exception e) {
            logger.error("Error fetching statistics: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Expire old IOBs
     */
    @PostMapping("/expire-old")
    public ResponseEntity<Map<String, Object>> expireOldIOBs() {
        try {
            iobService.expireOldIOBs();

            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "message", "Old IOBs expired successfully"
            ));

        } catch (Exception e) {
            logger.error("Error expiring old IOBs: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get data for all indices (similar to liquidity endpoint)
     */
    @GetMapping("/all-indices")
    public ResponseEntity<Map<String, Object>> getAllIndicesData() {
        try {
            Map<String, Object> result = new HashMap<>();

            // NIFTY analysis
            Map<String, Object> niftyAnalysis = iobService.getDetailedAnalysis(NIFTY_INSTRUMENT_TOKEN);
            result.put("NIFTY", niftyAnalysis);

            result.put("success", true);

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            logger.error("Error getting all indices data: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Convert IOB to summary map for API response
     */
    private Map<String, Object> convertToSummary(InternalOrderBlock iob) {
        Map<String, Object> summary = new HashMap<>();

        summary.put("id", iob.getId());
        summary.put("instrumentName", iob.getInstrumentName());
        summary.put("timeframe", iob.getTimeframe());
        summary.put("obType", iob.getObType());
        summary.put("obCandleTime", iob.getObCandleTime());
        summary.put("zoneHigh", iob.getZoneHigh());
        summary.put("zoneLow", iob.getZoneLow());
        summary.put("zoneMidpoint", iob.getZoneMidpoint());
        summary.put("currentPrice", iob.getCurrentPrice());
        summary.put("distanceToZone", iob.getDistanceToZone());
        summary.put("distancePercent", iob.getDistancePercent());
        summary.put("bosLevel", iob.getBosLevel());
        summary.put("bosType", iob.getBosType());
        summary.put("hasFvg", iob.getHasFvg());
        summary.put("tradeDirection", iob.getTradeDirection());
        summary.put("entryPrice", iob.getEntryPrice());
        summary.put("stopLoss", iob.getStopLoss());
        summary.put("target1", iob.getTarget1());
        summary.put("target2", iob.getTarget2());
        summary.put("target3", iob.getTarget3());
        summary.put("riskRewardRatio", iob.getRiskRewardRatio());
        summary.put("status", iob.getStatus());
        summary.put("isValid", iob.getIsValid());
        summary.put("signalConfidence", iob.getSignalConfidence());
        summary.put("validationNotes", iob.getValidationNotes());
        summary.put("tradeTaken", iob.getTradeTaken());
        summary.put("detectionTimestamp", iob.getDetectionTimestamp());

        // Alert tracking flags
        summary.put("detectionAlertSent", iob.getDetectionAlertSent());
        summary.put("mitigationAlertSent", iob.getMitigationAlertSent());
        summary.put("target1AlertSent", iob.getTarget1AlertSent());
        summary.put("target2AlertSent", iob.getTarget2AlertSent());
        summary.put("target3AlertSent", iob.getTarget3AlertSent());

        // Trade timeline tracking
        summary.put("entryTriggeredTime", iob.getEntryTriggeredTime());
        summary.put("actualEntryPrice", iob.getActualEntryPrice());
        summary.put("stopLossHitTime", iob.getStopLossHitTime());
        summary.put("stopLossHitPrice", iob.getStopLossHitPrice());
        summary.put("target1HitTime", iob.getTarget1HitTime());
        summary.put("target1HitPrice", iob.getTarget1HitPrice());
        summary.put("target2HitTime", iob.getTarget2HitTime());
        summary.put("target2HitPrice", iob.getTarget2HitPrice());
        summary.put("target3HitTime", iob.getTarget3HitTime());
        summary.put("target3HitPrice", iob.getTarget3HitPrice());
        summary.put("maxFavorableExcursion", iob.getMaxFavorableExcursion());
        summary.put("maxAdverseExcursion", iob.getMaxAdverseExcursion());
        summary.put("tradeOutcome", iob.getTradeOutcome());
        summary.put("pointsCaptured", iob.getPointsCaptured());
        summary.put("mitigationTime", iob.getMitigationTime());

        return summary;
    }
}
