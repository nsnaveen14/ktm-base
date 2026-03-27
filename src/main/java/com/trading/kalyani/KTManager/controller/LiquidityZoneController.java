package com.trading.kalyani.KTManager.controller;

import com.trading.kalyani.KTManager.entity.LiquidityZoneAnalysis;
import com.trading.kalyani.KTManager.service.LiquidityZoneService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.NIFTY_INSTRUMENT_TOKEN;

/**
 * REST Controller for Liquidity Zone Analysis.
 * Provides endpoints for liquidity grab detection and stop loss cluster analysis.
 */
@RestController
@RequestMapping("/api/liquidity")
@CrossOrigin(origins = "*")
public class LiquidityZoneController {

    private static final Logger logger = LoggerFactory.getLogger(LiquidityZoneController.class);

    @Autowired
    private LiquidityZoneService liquidityZoneService;

    /**
     * Trigger on-demand analysis - useful during non-market hours
     */
    @PostMapping("/trigger-analysis")
    public ResponseEntity<Map<String, Object>> triggerOnDemandAnalysis() {
        try {
            logger.info("Triggering on-demand liquidity analysis");

            List<Long> tokens = Arrays.asList(NIFTY_INSTRUMENT_TOKEN);
            List<String> timeframes = Arrays.asList("5min", "15min", "1hour");

            Map<String, Object> result = new HashMap<>();
            int successCount = 0;
            int failureCount = 0;

            for (Long token : tokens) {
                for (String timeframe : timeframes) {
                    try {
                        liquidityZoneService.analyzeLiquidityZones(token, timeframe);
                        successCount++;
                    } catch (Exception e) {
                        logger.error("Failed to analyze token: {}, timeframe: {}", token, timeframe, e);
                        failureCount++;
                    }
                }
            }

            result.put("success", true);
            result.put("message", "On-demand analysis completed");
            result.put("successCount", successCount);
            result.put("failureCount", failureCount);
            result.put("totalAnalyzed", successCount);

            return ResponseEntity.ok(result);
        } catch (Exception e) {
            logger.error("Error triggering on-demand analysis: {}", e.getMessage(), e);
            Map<String, Object> error = new HashMap<>();
            error.put("success", false);
            error.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(error);
        }
    }

    /**
     * Get comprehensive dashboard data for all indices and timeframes
     */
    @GetMapping("/dashboard")
    public ResponseEntity<Map<String, Object>> getDashboard() {
        try {
            logger.info("Fetching liquidity zone dashboard data");

            List<Long> tokens = Arrays.asList(NIFTY_INSTRUMENT_TOKEN);
            List<String> timeframes = Arrays.asList("5min", "15min", "1hour");

            Map<String, Object> dashboard = liquidityZoneService.getDashboardData(tokens, timeframes);

            return ResponseEntity.ok(dashboard);
        } catch (Exception e) {
            logger.error("Error fetching dashboard data: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Analyze liquidity zones for specific instrument and timeframe
     */
    @PostMapping("/analyze")
    public ResponseEntity<LiquidityZoneAnalysis> analyzeLiquidityZones(
            @RequestParam Long instrumentToken,
            @RequestParam String timeframe) {
        try {
            logger.info("Analyzing liquidity zones for token: {}, timeframe: {}", instrumentToken, timeframe);

            LiquidityZoneAnalysis analysis = liquidityZoneService.analyzeLiquidityZones(instrumentToken, timeframe);

            if (analysis != null) {
                return ResponseEntity.ok(analysis);
            }
            return ResponseEntity.noContent().build();

        } catch (Exception e) {
            logger.error("Error analyzing liquidity zones: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Analyze all indices (NIFTY) across all timeframes
     */
    @PostMapping("/analyze-all")
    public ResponseEntity<Map<String, Object>> analyzeAllIndices() {
        try {
            logger.info("Analyzing all indices across all timeframes");

            Map<String, Object> result = liquidityZoneService.analyzeAllIndices();

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            logger.error("Error analyzing all indices: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get latest analysis for specific instrument and timeframe
     */
    @GetMapping("/latest")
    public ResponseEntity<LiquidityZoneAnalysis> getLatestAnalysis(
            @RequestParam Long instrumentToken,
            @RequestParam String timeframe) {
        try {
            LiquidityZoneAnalysis analysis = liquidityZoneService.getLatestAnalysis(instrumentToken, timeframe);

            if (analysis != null) {
                return ResponseEntity.ok(analysis);
            }
            return ResponseEntity.notFound().build();

        } catch (Exception e) {
            logger.error("Error getting latest analysis: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get multi-timeframe analysis for single instrument
     */
    @GetMapping("/multi-timeframe")
    public ResponseEntity<Map<String, Object>> getMultiTimeframeAnalysis(
            @RequestParam Long instrumentToken) {
        try {
            Map<String, Object> analysis = liquidityZoneService.getMultiTimeframeAnalysis(instrumentToken);
            return ResponseEntity.ok(analysis);

        } catch (Exception e) {
            logger.error("Error getting multi-timeframe analysis: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get today's analyses for an instrument
     */
    @GetMapping("/today")
    public ResponseEntity<List<LiquidityZoneAnalysis>> getTodaysAnalyses(
            @RequestParam Long instrumentToken) {
        try {
            List<LiquidityZoneAnalysis> analyses = liquidityZoneService.getTodaysAnalyses(instrumentToken);
            return ResponseEntity.ok(analyses);

        } catch (Exception e) {
            logger.error("Error getting today's analyses: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get today's analyses for specific timeframe
     */
    @GetMapping("/today/{timeframe}")
    public ResponseEntity<List<LiquidityZoneAnalysis>> getTodaysAnalysesByTimeframe(
            @RequestParam Long instrumentToken,
            @PathVariable String timeframe) {
        try {
            List<LiquidityZoneAnalysis> analyses = liquidityZoneService
                    .getTodaysAnalysesByTimeframe(instrumentToken, timeframe);
            return ResponseEntity.ok(analyses);

        } catch (Exception e) {
            logger.error("Error getting today's analyses by timeframe: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get active trade setups
     */
    @GetMapping("/active-setups")
    public ResponseEntity<Map<String, Object>> getActiveSetups() {
        try {
            List<Long> tokens = Arrays.asList(NIFTY_INSTRUMENT_TOKEN);
            Map<String, Object> setups = liquidityZoneService.getActiveSetups(tokens);
            return ResponseEntity.ok(setups);

        } catch (Exception e) {
            logger.error("Error getting active setups: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get chart data for visualization
     */
    @GetMapping("/chart-data")
    public ResponseEntity<Map<String, Object>> getChartData(
            @RequestParam Long instrumentToken,
            @RequestParam String timeframe) {
        try {
            Map<String, Object> chartData = liquidityZoneService.getChartData(instrumentToken, timeframe);
            return ResponseEntity.ok(chartData);

        } catch (Exception e) {
            logger.error("Error getting chart data: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get comprehensive data for NIFTY
     */
    @GetMapping("/all-indices")
    public ResponseEntity<Map<String, Object>> getAllIndicesData() {
        try {
            Map<String, Object> result = new HashMap<>();

            // NIFTY analysis
            Map<String, Object> niftyAnalysis = liquidityZoneService
                    .getMultiTimeframeAnalysis(NIFTY_INSTRUMENT_TOKEN);
            result.put("NIFTY", niftyAnalysis);

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            logger.error("Error getting all indices data: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }
}

