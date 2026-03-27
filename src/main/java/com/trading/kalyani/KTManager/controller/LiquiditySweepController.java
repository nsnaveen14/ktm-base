package com.trading.kalyani.KTManager.controller;

import com.trading.kalyani.KTManager.entity.LiquiditySweepAnalysis;
import com.trading.kalyani.KTManager.service.LiquiditySweepService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * REST Controller for Liquidity Sweep Analysis.
 *
 * Provides endpoints for:
 * - Liquidity sweep analysis data (BSL/SSL levels)
 * - Whale activity detection (volume anomalies, KER)
 * - Trade signals from liquidity sweeps
 * - Liquidity levels for charting
 */
@RestController
@RequestMapping("/api/liquidity-sweep")
@CrossOrigin(origins = "*")
public class LiquiditySweepController {

    private static final Logger logger = LoggerFactory.getLogger(LiquiditySweepController.class);

    @Autowired
    private LiquiditySweepService liquiditySweepService;

    /**
     * Get latest liquidity sweep analysis for a job config
     */
    @GetMapping("/latest/{appJobConfigNum}")
    public ResponseEntity<LiquiditySweepAnalysis> getLatestAnalysis(
            @PathVariable Integer appJobConfigNum) {
        try {
            Optional<LiquiditySweepAnalysis> analysis = liquiditySweepService.getLatestAnalysis(appJobConfigNum);
            return analysis.map(ResponseEntity::ok)
                    .orElse(ResponseEntity.notFound().build());
        } catch (Exception e) {
            logger.error("Error getting latest liquidity sweep analysis: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get latest valid trade setup from liquidity sweep
     */
    @GetMapping("/latest-setup/{appJobConfigNum}")
    public ResponseEntity<LiquiditySweepAnalysis> getLatestValidSetup(
            @PathVariable Integer appJobConfigNum) {
        try {
            Optional<LiquiditySweepAnalysis> setup = liquiditySweepService.getLatestValidSetup(appJobConfigNum);
            return setup.map(ResponseEntity::ok)
                    .orElse(ResponseEntity.notFound().build());
        } catch (Exception e) {
            logger.error("Error getting latest valid setup: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get today's liquidity sweep analyses
     */
    @GetMapping("/today/{appJobConfigNum}")
    public ResponseEntity<List<LiquiditySweepAnalysis>> getTodaysAnalyses(
            @PathVariable Integer appJobConfigNum) {
        try {
            List<LiquiditySweepAnalysis> analyses = liquiditySweepService.getTodaysAnalyses(appJobConfigNum);
            return ResponseEntity.ok(analyses);
        } catch (Exception e) {
            logger.error("Error getting today's analyses: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Run a fresh liquidity sweep analysis
     */
    @PostMapping("/analyze/{appJobConfigNum}")
    public ResponseEntity<LiquiditySweepAnalysis> runAnalysis(
            @PathVariable Integer appJobConfigNum) {
        try {
            logger.info("Running liquidity sweep analysis for appJobConfigNum: {}", appJobConfigNum);
            LiquiditySweepAnalysis analysis = liquiditySweepService.analyzeLiquiditySweep(appJobConfigNum);
            if (analysis != null) {
                return ResponseEntity.ok(analysis);
            }
            return ResponseEntity.noContent().build();
        } catch (Exception e) {
            logger.error("Error running liquidity sweep analysis: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Check for liquidity sweep signal
     */
    @GetMapping("/signal/{appJobConfigNum}")
    public ResponseEntity<Map<String, Object>> checkSignal(
            @PathVariable Integer appJobConfigNum) {
        try {
            Map<String, Object> signal = liquiditySweepService.checkLiquiditySweepSignal(appJobConfigNum);
            return ResponseEntity.ok(signal);
        } catch (Exception e) {
            logger.error("Error checking liquidity sweep signal: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get trade recommendation based on liquidity sweep
     */
    @GetMapping("/recommendation/{appJobConfigNum}")
    public ResponseEntity<Map<String, Object>> getTradeRecommendation(
            @PathVariable Integer appJobConfigNum) {
        try {
            Map<String, Object> recommendation = liquiditySweepService.getTradeRecommendation(appJobConfigNum);
            return ResponseEntity.ok(recommendation);
        } catch (Exception e) {
            logger.error("Error getting trade recommendation: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get liquidity levels (BSL/SSL) for charting
     */
    @GetMapping("/levels/{appJobConfigNum}")
    public ResponseEntity<Map<String, Object>> getLiquidityLevels(
            @PathVariable Integer appJobConfigNum) {
        try {
            Map<String, Object> levels = liquiditySweepService.getLiquidityLevels(appJobConfigNum);
            return ResponseEntity.ok(levels);
        } catch (Exception e) {
            logger.error("Error getting liquidity levels: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get whale activity indicators
     */
    @GetMapping("/whale-activity/{appJobConfigNum}")
    public ResponseEntity<Map<String, Object>> getWhaleActivity(
            @PathVariable Integer appJobConfigNum) {
        try {
            Map<String, Object> indicators = liquiditySweepService.getWhaleActivityIndicators(appJobConfigNum);
            return ResponseEntity.ok(indicators);
        } catch (Exception e) {
            logger.error("Error getting whale activity: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get comprehensive analysis data for Analytics dashboard
     */
    @GetMapping("/dashboard/{appJobConfigNum}")
    public ResponseEntity<Map<String, Object>> getDashboardData(
            @PathVariable Integer appJobConfigNum) {
        try {
            Map<String, Object> dashboard = new HashMap<>();

            // Latest analysis
            Optional<LiquiditySweepAnalysis> latest = liquiditySweepService.getLatestAnalysis(appJobConfigNum);
            if (latest.isPresent()) {
                LiquiditySweepAnalysis analysis = latest.get();

                // Market Structure
                Map<String, Object> marketStructure = new HashMap<>();
                marketStructure.put("bslLevel1", analysis.getBslLevel1());
                marketStructure.put("bslLevel2", analysis.getBslLevel2());
                marketStructure.put("bslLevel3", analysis.getBslLevel3());
                marketStructure.put("sslLevel1", analysis.getSslLevel1());
                marketStructure.put("sslLevel2", analysis.getSslLevel2());
                marketStructure.put("sslLevel3", analysis.getSslLevel3());
                marketStructure.put("swingHigh1", analysis.getSwingHigh1());
                marketStructure.put("swingHigh2", analysis.getSwingHigh2());
                marketStructure.put("swingLow1", analysis.getSwingLow1());
                marketStructure.put("swingLow2", analysis.getSwingLow2());
                dashboard.put("marketStructure", marketStructure);

                // Quant Engine (Whale Detection)
                Map<String, Object> quantEngine = new HashMap<>();
                quantEngine.put("volumeZScore", analysis.getLogVolumeZScore());
                quantEngine.put("kaufmanEfficiency", analysis.getKaufmanEfficiencyRatio());
                quantEngine.put("whaleType", analysis.getWhaleType());
                quantEngine.put("hasWhaleActivity", analysis.getHasWhaleActivity());
                quantEngine.put("isAbsorption", analysis.getIsAbsorption());
                quantEngine.put("isPropulsion", analysis.getIsPropulsion());
                quantEngine.put("averageVolume", analysis.getAverageVolume());
                quantEngine.put("currentVolume", analysis.getVolume());
                quantEngine.put("whaleThreshold", analysis.getWhaleThreshold());
                dashboard.put("quantEngine", quantEngine);

                // Trend & Momentum
                Map<String, Object> trendMomentum = new HashMap<>();
                trendMomentum.put("ema200", analysis.getEma200());
                trendMomentum.put("isAboveEma200", analysis.getIsAboveEma200());
                trendMomentum.put("trendDirection", analysis.getTrendDirection());
                trendMomentum.put("rsiValue", analysis.getRsiValue());
                trendMomentum.put("isRsiOversold", analysis.getIsRsiOversold());
                trendMomentum.put("isRsiOverbought", analysis.getIsRsiOverbought());
                dashboard.put("trendMomentum", trendMomentum);

                // Sweep Detection
                Map<String, Object> sweepDetection = new HashMap<>();
                sweepDetection.put("bslSwept", analysis.getBslSwept());
                sweepDetection.put("sslSwept", analysis.getSslSwept());
                sweepDetection.put("sweepType", analysis.getSweepType());
                sweepDetection.put("sweptLevel", analysis.getSweptLevel());
                sweepDetection.put("priceClosedBack", analysis.getPriceClosedBack());
                sweepDetection.put("hasInstitutionalConfirmation", analysis.getHasInstitutionalConfirmation());
                sweepDetection.put("isTrendAligned", analysis.getIsTrendAligned());
                sweepDetection.put("isMomentumAligned", analysis.getIsMomentumAligned());
                dashboard.put("sweepDetection", sweepDetection);

                // Trade Signal
                Map<String, Object> tradeSignal = new HashMap<>();
                tradeSignal.put("signalType", analysis.getSignalType());
                tradeSignal.put("signalStrength", analysis.getSignalStrength());
                tradeSignal.put("signalConfidence", analysis.getSignalConfidence());
                tradeSignal.put("isValidSetup", analysis.getIsValidSetup());
                tradeSignal.put("entryPrice", analysis.getEntryPrice());
                tradeSignal.put("stopLossPrice", analysis.getStopLossPrice());
                tradeSignal.put("takeProfit1", analysis.getTakeProfit1());
                tradeSignal.put("takeProfit2", analysis.getTakeProfit2());
                tradeSignal.put("takeProfit3", analysis.getTakeProfit3());
                tradeSignal.put("riskRewardRatio", analysis.getRiskRewardRatio());
                tradeSignal.put("riskPoints", analysis.getRiskPoints());
                tradeSignal.put("atrValue", analysis.getAtrValue());
                tradeSignal.put("suggestedOptionType", analysis.getSuggestedOptionType());
                tradeSignal.put("optionStrategy", analysis.getOptionStrategy());
                dashboard.put("tradeSignal", tradeSignal);

                // Current Price Data
                Map<String, Object> priceData = new HashMap<>();
                priceData.put("spotPrice", analysis.getSpotPrice());
                priceData.put("open", analysis.getOpen());
                priceData.put("high", analysis.getHigh());
                priceData.put("low", analysis.getLow());
                priceData.put("close", analysis.getClose());
                priceData.put("volume", analysis.getVolume());
                dashboard.put("priceData", priceData);

                // Metadata
                dashboard.put("analysisTimestamp", analysis.getAnalysisTimestamp());
                dashboard.put("timeframe", analysis.getTimeframe());
                dashboard.put("lookbackPeriod", analysis.getLookbackPeriod());
                dashboard.put("analysisId", analysis.getId());
                dashboard.put("hasData", true);
            } else {
                dashboard.put("hasData", false);
                dashboard.put("message", "No liquidity sweep analysis available");
            }

            // Today's statistics
            List<LiquiditySweepAnalysis> todaysAnalyses = liquiditySweepService.getTodaysAnalyses(appJobConfigNum);
            Map<String, Object> todayStats = new HashMap<>();
            todayStats.put("totalAnalyses", todaysAnalyses.size());
            todayStats.put("validSetups", todaysAnalyses.stream()
                    .filter(a -> Boolean.TRUE.equals(a.getIsValidSetup())).count());
            todayStats.put("whaleEvents", todaysAnalyses.stream()
                    .filter(a -> Boolean.TRUE.equals(a.getHasWhaleActivity())).count());
            todayStats.put("bslSweeps", todaysAnalyses.stream()
                    .filter(a -> Boolean.TRUE.equals(a.getBslSwept())).count());
            todayStats.put("sslSweeps", todaysAnalyses.stream()
                    .filter(a -> Boolean.TRUE.equals(a.getSslSwept())).count());
            dashboard.put("todayStats", todayStats);

            // Configuration
            dashboard.put("configuration", liquiditySweepService.getConfiguration());

            return ResponseEntity.ok(dashboard);
        } catch (Exception e) {
            logger.error("Error getting dashboard data: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Update whale detection threshold
     */
    @PostMapping("/config/whale-threshold")
    public ResponseEntity<Map<String, Object>> setWhaleThreshold(
            @RequestParam double sigma) {
        try {
            liquiditySweepService.setWhaleThreshold(sigma);
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("whaleThreshold", sigma);
            response.put("message", String.format("Whale threshold updated to %.2f sigma", sigma));
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Error setting whale threshold: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Update lookback period
     */
    @PostMapping("/config/lookback-period")
    public ResponseEntity<Map<String, Object>> setLookbackPeriod(
            @RequestParam int periods) {
        try {
            liquiditySweepService.setLookbackPeriod(periods);
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("lookbackPeriod", periods);
            response.put("message", String.format("Lookback period updated to %d periods", periods));
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Error setting lookback period: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get current configuration
     */
    @GetMapping("/config")
    public ResponseEntity<Map<String, Object>> getConfiguration() {
        try {
            Map<String, Object> config = liquiditySweepService.getConfiguration();
            return ResponseEntity.ok(config);
        } catch (Exception e) {
            logger.error("Error getting configuration: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }
}

