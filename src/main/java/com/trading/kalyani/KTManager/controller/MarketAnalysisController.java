package com.trading.kalyani.KTManager.controller;

import com.trading.kalyani.KTManager.entity.MarketStructure;
import com.trading.kalyani.KTManager.entity.RiskManagement;
import com.trading.kalyani.KTManager.entity.VolumeProfile;
import com.trading.kalyani.KTManager.service.MarketStructureService;
import com.trading.kalyani.KTManager.service.RiskManagementService;
import com.trading.kalyani.KTManager.service.VolumeProfileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;

/**
 * REST Controller for Market Structure, Volume Profile, and Risk Management APIs.
 *
 * Provides endpoints for:
 * - Market Structure Analysis (trend, CHoCH, premium/discount zones)
 * - Volume Profile Analysis (POC, value area, delta analysis)
 * - Risk Management (position sizing, ATR, daily limits)
 */
@RestController
@RequestMapping("/api/analysis")
@CrossOrigin
public class MarketAnalysisController {

    private static final Logger logger = LoggerFactory.getLogger(MarketAnalysisController.class);

    @Autowired
    private MarketStructureService marketStructureService;

    @Autowired
    private VolumeProfileService volumeProfileService;

    @Autowired
    private RiskManagementService riskManagementService;

    // ==================== Market Structure Endpoints ====================

    @GetMapping("/market-structure/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getMarketStructure(
            @PathVariable Long instrumentToken,
            @RequestParam(defaultValue = "5min") String timeframe) {

        Map<String, Object> response = new HashMap<>();
        try {
            MarketStructure structure = marketStructureService.analyzeMarketStructure(instrumentToken, timeframe);

            if (structure != null) {
                response.put("success", true);
                response.put("data", structureToMap(structure));
            } else {
                response.put("success", false);
                response.put("message", "Unable to analyze market structure");
            }
        } catch (Exception e) {
            logger.error("Error analyzing market structure: {}", e.getMessage());
            response.put("success", false);
            response.put("error", e.getMessage());
        }

        return ResponseEntity.ok(response);
    }

    @GetMapping("/market-structure/all-indices")
    public ResponseEntity<Map<String, Object>> getAllIndicesMarketStructure() {
        Map<String, Object> response = new HashMap<>();
        try {
            Map<String, MarketStructure> structures = marketStructureService.analyzeAllIndices();

            Map<String, Object> data = new HashMap<>();
            for (Map.Entry<String, MarketStructure> entry : structures.entrySet()) {
                data.put(entry.getKey(), structureToMap(entry.getValue()));
            }

            response.put("success", true);
            response.put("data", data);
        } catch (Exception e) {
            logger.error("Error analyzing all indices: {}", e.getMessage());
            response.put("success", false);
            response.put("error", e.getMessage());
        }

        return ResponseEntity.ok(response);
    }

    @GetMapping("/market-structure/trend/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getTrend(
            @PathVariable Long instrumentToken,
            @RequestParam(defaultValue = "5min") String timeframe) {

        Map<String, Object> response = new HashMap<>();
        response.put("instrumentToken", instrumentToken);
        response.put("timeframe", timeframe);
        response.put("trend", marketStructureService.getTrendDirection(instrumentToken, timeframe));
        response.put("strength", marketStructureService.getTrendStrength(instrumentToken, timeframe));
        response.put("isUptrend", marketStructureService.isUptrend(instrumentToken, timeframe));
        response.put("isDowntrend", marketStructureService.isDowntrend(instrumentToken, timeframe));

        return ResponseEntity.ok(response);
    }

    @GetMapping("/market-structure/mtf-trend/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getMultiTimeframeTrend(@PathVariable Long instrumentToken) {
        Map<String, Object> response = new HashMap<>();
        List<String> timeframes = Arrays.asList("5min", "15min", "60min");

        response.put("instrumentToken", instrumentToken);
        response.put("trends", marketStructureService.getMultiTimeframeTrend(instrumentToken, timeframes));

        return ResponseEntity.ok(response);
    }

    @GetMapping("/market-structure/choch/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getChochDetails(
            @PathVariable Long instrumentToken,
            @RequestParam(defaultValue = "5min") String timeframe) {

        Map<String, Object> response = new HashMap<>();
        response.put("instrumentToken", instrumentToken);
        response.put("timeframe", timeframe);
        response.put("choch", marketStructureService.getLatestChoch(instrumentToken, timeframe));
        response.put("history", marketStructureService.getChochHistory(instrumentToken, timeframe, 5));

        return ResponseEntity.ok(response);
    }

    @GetMapping("/market-structure/zones/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getPremiumDiscountZones(
            @PathVariable Long instrumentToken,
            @RequestParam(defaultValue = "5min") String timeframe) {

        Map<String, Object> response = new HashMap<>();
        response.put("instrumentToken", instrumentToken);
        response.put("timeframe", timeframe);
        response.put("currentZone", marketStructureService.getPriceZone(instrumentToken, timeframe));
        response.put("levels", marketStructureService.getZoneLevels(instrumentToken, timeframe));
        response.put("isDiscount", marketStructureService.isInDiscountZone(instrumentToken, timeframe));
        response.put("isPremium", marketStructureService.isInPremiumZone(instrumentToken, timeframe));

        return ResponseEntity.ok(response);
    }

    @GetMapping("/market-structure/phase/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getMarketPhase(
            @PathVariable Long instrumentToken,
            @RequestParam(defaultValue = "5min") String timeframe) {

        return ResponseEntity.ok(marketStructureService.getMarketPhaseDetails(instrumentToken, timeframe));
    }

    @GetMapping("/market-structure/bias/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getTradeBias(
            @PathVariable Long instrumentToken,
            @RequestParam(defaultValue = "5min") String timeframe) {

        return ResponseEntity.ok(marketStructureService.getBiasWithConfidence(instrumentToken, timeframe));
    }

    @GetMapping("/market-structure/key-levels/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getKeyLevels(
            @PathVariable Long instrumentToken,
            @RequestParam(defaultValue = "5min") String timeframe) {

        Map<String, Object> response = new HashMap<>();
        response.put("instrumentToken", instrumentToken);
        response.put("timeframe", timeframe);
        response.put("levels", marketStructureService.getKeyLevels(instrumentToken, timeframe));
        response.put("swingHighs", marketStructureService.getSwingHighs(instrumentToken, timeframe, 5));
        response.put("swingLows", marketStructureService.getSwingLows(instrumentToken, timeframe, 5));

        return ResponseEntity.ok(response);
    }

    @GetMapping("/market-structure/dashboard/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getMarketStructureDashboard(@PathVariable Long instrumentToken) {
        return ResponseEntity.ok(marketStructureService.getDashboard(instrumentToken));
    }

    // ==================== Volume Profile Endpoints ====================

    @GetMapping("/volume-profile/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getVolumeProfile(
            @PathVariable Long instrumentToken,
            @RequestParam(defaultValue = "5min") String timeframe) {

        Map<String, Object> response = new HashMap<>();
        try {
            VolumeProfile profile = volumeProfileService.analyzeVolumeProfile(instrumentToken, timeframe);

            if (profile != null) {
                response.put("success", true);
                response.put("data", profileToMap(profile));
            } else {
                response.put("success", false);
                response.put("message", "Unable to analyze volume profile");
            }
        } catch (Exception e) {
            logger.error("Error analyzing volume profile: {}", e.getMessage());
            response.put("success", false);
            response.put("error", e.getMessage());
        }

        return ResponseEntity.ok(response);
    }

    @GetMapping("/volume-profile/all-indices")
    public ResponseEntity<Map<String, Object>> getAllIndicesVolumeProfile() {
        Map<String, Object> response = new HashMap<>();
        try {
            Map<String, VolumeProfile> profiles = volumeProfileService.analyzeAllIndices();

            Map<String, Object> data = new HashMap<>();
            for (Map.Entry<String, VolumeProfile> entry : profiles.entrySet()) {
                data.put(entry.getKey(), profileToMap(entry.getValue()));
            }

            response.put("success", true);
            response.put("data", data);
        } catch (Exception e) {
            logger.error("Error analyzing all indices: {}", e.getMessage());
            response.put("success", false);
            response.put("error", e.getMessage());
        }

        return ResponseEntity.ok(response);
    }

    @GetMapping("/volume-profile/delta/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getVolumeDelta(
            @PathVariable Long instrumentToken,
            @RequestParam(defaultValue = "5min") String timeframe) {

        VolumeProfile profile = volumeProfileService.getLatestAnalysis(instrumentToken, timeframe);

        Map<String, Object> response = new HashMap<>();
        response.put("instrumentToken", instrumentToken);
        response.put("timeframe", timeframe);

        if (profile != null) {
            response.put("buyingVolume", profile.getBuyingVolume());
            response.put("sellingVolume", profile.getSellingVolume());
            response.put("volumeDelta", profile.getVolumeDelta());
            response.put("cumulativeDelta", profile.getCumulativeDelta());
            response.put("deltaDirection", profile.getDeltaDirection());
            response.put("deltaStrength", profile.getDeltaStrength());
        }

        return ResponseEntity.ok(response);
    }

    @GetMapping("/volume-profile/dashboard/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getVolumeProfileDashboard(@PathVariable Long instrumentToken) {
        return ResponseEntity.ok(volumeProfileService.getDashboard(instrumentToken));
    }

    // ==================== Risk Management Endpoints ====================

    @PostMapping("/risk/initialize")
    public ResponseEntity<Map<String, Object>> initializeRiskManagement(
            @RequestParam(required = false) Double accountCapital,
            @RequestParam(required = false) Double riskPerTradePercent,
            @RequestParam(required = false) Double maxDailyLossPercent,
            @RequestParam(required = false) Double maxPortfolioHeatPercent) {

        Map<String, Object> response = new HashMap<>();
        try {
            RiskManagement config = riskManagementService.initializeAccount(
                    accountCapital, riskPerTradePercent, maxDailyLossPercent, maxPortfolioHeatPercent);

            response.put("success", true);
            response.put("message", "Risk management initialized");
            response.put("config", riskConfigToMap(config));
        } catch (Exception e) {
            logger.error("Error initializing risk management: {}", e.getMessage());
            response.put("success", false);
            response.put("error", e.getMessage());
        }

        return ResponseEntity.ok(response);
    }

    @GetMapping("/risk/config")
    public ResponseEntity<Map<String, Object>> getRiskConfig() {
        RiskManagement config = riskManagementService.getAccountConfig();

        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("config", riskConfigToMap(config));

        return ResponseEntity.ok(response);
    }

    @PutMapping("/risk/config")
    public ResponseEntity<Map<String, Object>> updateRiskConfig(
            @RequestParam(required = false) Double riskPerTradePercent,
            @RequestParam(required = false) Double maxDailyLossPercent,
            @RequestParam(required = false) Double maxPortfolioHeatPercent) {

        Map<String, Object> response = new HashMap<>();
        try {
            riskManagementService.updateRiskParameters(riskPerTradePercent, maxDailyLossPercent, maxPortfolioHeatPercent);

            response.put("success", true);
            response.put("message", "Risk parameters updated");
            response.put("config", riskConfigToMap(riskManagementService.getAccountConfig()));
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
        }

        return ResponseEntity.ok(response);
    }

    @GetMapping("/risk/position-size")
    public ResponseEntity<Map<String, Object>> calculatePositionSize(
            @RequestParam Long instrumentToken,
            @RequestParam Double entryPrice,
            @RequestParam Double stopLoss,
            @RequestParam(required = false) Integer lotSize) {

        return ResponseEntity.ok(riskManagementService.calculatePositionSize(
                instrumentToken, entryPrice, stopLoss, lotSize));
    }

    @GetMapping("/risk/atr/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getATR(
            @PathVariable Long instrumentToken,
            @RequestParam(defaultValue = "5min") String timeframe,
            @RequestParam(defaultValue = "14") Integer period) {

        Map<String, Object> response = new HashMap<>();
        response.put("instrumentToken", instrumentToken);
        response.put("timeframe", timeframe);
        response.put("period", period);
        response.put("atr", riskManagementService.calculateATR(instrumentToken, timeframe, period));
        response.put("atrPercent", riskManagementService.getATRPercent(instrumentToken, timeframe));

        return ResponseEntity.ok(response);
    }

    @GetMapping("/risk/dynamic-sl")
    public ResponseEntity<Map<String, Object>> getDynamicStopLoss(
            @RequestParam Long instrumentToken,
            @RequestParam String tradeDirection,
            @RequestParam Double entryPrice,
            @RequestParam(defaultValue = "1.5") Double atrMultiplier) {

        Map<String, Object> response = new HashMap<>();
        response.put("instrumentToken", instrumentToken);
        response.put("tradeDirection", tradeDirection);
        response.put("entryPrice", entryPrice);
        response.put("atrMultiplier", atrMultiplier);
        response.put("dynamicStopLoss", riskManagementService.calculateDynamicStopLoss(
                instrumentToken, tradeDirection, entryPrice, atrMultiplier));

        return ResponseEntity.ok(response);
    }

    @GetMapping("/risk/daily-pnl")
    public ResponseEntity<Map<String, Object>> getDailyPnl() {
        return ResponseEntity.ok(riskManagementService.getDailyPnlSummary());
    }

    @GetMapping("/risk/portfolio-heat")
    public ResponseEntity<Map<String, Object>> getPortfolioHeat() {
        return ResponseEntity.ok(riskManagementService.getPortfolioHeatSummary());
    }

    @GetMapping("/risk/trading-status")
    public ResponseEntity<Map<String, Object>> getTradingStatus() {
        return ResponseEntity.ok(riskManagementService.getTradingStatus());
    }

    @PostMapping("/risk/validate-trade")
    public ResponseEntity<Map<String, Object>> validateTrade(
            @RequestParam Long instrumentToken,
            @RequestParam String tradeDirection,
            @RequestParam Double entryPrice,
            @RequestParam Double stopLoss,
            @RequestParam Integer quantity) {

        return ResponseEntity.ok(riskManagementService.validateTrade(
                instrumentToken, tradeDirection, entryPrice, stopLoss, quantity));
    }

    @GetMapping("/risk/metrics")
    public ResponseEntity<Map<String, Object>> getRiskMetrics(
            @RequestParam(defaultValue = "30") Integer tradeDays) {

        return ResponseEntity.ok(riskManagementService.getRiskMetrics(tradeDays));
    }

    @GetMapping("/risk/alerts")
    public ResponseEntity<List<Map<String, Object>>> getRiskAlerts() {
        return ResponseEntity.ok(riskManagementService.getRiskAlerts());
    }

    @GetMapping("/risk/dashboard")
    public ResponseEntity<Map<String, Object>> getRiskDashboard() {
        return ResponseEntity.ok(riskManagementService.getDashboard());
    }

    @PostMapping("/risk/reset-daily")
    public ResponseEntity<Map<String, Object>> resetDailyMetrics() {
        Map<String, Object> response = new HashMap<>();
        try {
            riskManagementService.resetDailyMetrics();
            response.put("success", true);
            response.put("message", "Daily metrics reset successfully");
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
        }

        return ResponseEntity.ok(response);
    }

    // ==================== Combined Analysis Endpoint ====================

    @GetMapping("/comprehensive/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getComprehensiveAnalysis(
            @PathVariable Long instrumentToken,
            @RequestParam(defaultValue = "5min") String timeframe) {

        Map<String, Object> response = new HashMap<>();
        response.put("instrumentToken", instrumentToken);
        response.put("timeframe", timeframe);

        // Market Structure
        MarketStructure structure = marketStructureService.analyzeMarketStructure(instrumentToken, timeframe);
        if (structure != null) {
            response.put("marketStructure", structureToMap(structure));
        }

        // Volume Profile
        VolumeProfile profile = volumeProfileService.analyzeVolumeProfile(instrumentToken, timeframe);
        if (profile != null) {
            response.put("volumeProfile", profileToMap(profile));
        }

        // Risk Management
        response.put("riskManagement", riskManagementService.getDashboard());

        // Trading recommendation
        Map<String, Object> recommendation = new HashMap<>();
        if (structure != null) {
            recommendation.put("bias", structure.getOverallBias());
            recommendation.put("confidence", structure.getAnalysisConfidence());
            recommendation.put("phase", structure.getMarketPhase());
            recommendation.put("priceZone", structure.getPriceZone());
        }
        if (profile != null) {
            recommendation.put("volumeBias", profile.getVolumeBias());
            recommendation.put("deltaDirection", profile.getDeltaDirection());
        }
        recommendation.put("tradingAllowed", riskManagementService.isTradingAllowed());
        response.put("recommendation", recommendation);

        return ResponseEntity.ok(response);
    }

    // ==================== Helper Methods ====================

    private Map<String, Object> structureToMap(MarketStructure s) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", s.getId());
        map.put("instrumentToken", s.getInstrumentToken());
        map.put("instrumentName", s.getInstrumentName());
        map.put("timeframe", s.getTimeframe());
        map.put("analysisTimestamp", s.getAnalysisTimestamp());
        map.put("currentPrice", s.getCurrentPrice());

        // Trend
        map.put("trendDirection", s.getTrendDirection());
        map.put("trendStrength", s.getTrendStrength());
        map.put("consecutiveHH", s.getConsecutiveHHCount());
        map.put("consecutiveHL", s.getConsecutiveHLCount());
        map.put("consecutiveLH", s.getConsecutiveLHCount());
        map.put("consecutiveLL", s.getConsecutiveLLCount());

        // Swing Points
        map.put("lastSwingHigh", s.getLastSwingHigh());
        map.put("lastSwingLow", s.getLastSwingLow());
        map.put("prevSwingHigh", s.getPrevSwingHigh());
        map.put("prevSwingLow", s.getPrevSwingLow());

        // CHoCH
        map.put("chochDetected", s.getChochDetected());
        map.put("chochType", s.getChochType());
        map.put("chochLevel", s.getChochLevel());
        map.put("chochTimestamp", s.getChochTimestamp());

        // Zones
        map.put("rangeHigh", s.getRangeHigh());
        map.put("rangeLow", s.getRangeLow());
        map.put("equilibrium", s.getEquilibriumLevel());
        map.put("priceZone", s.getPriceZone());
        map.put("pricePositionPercent", s.getPricePositionPercent());

        // Phase
        map.put("marketPhase", s.getMarketPhase());
        map.put("phaseConfidence", s.getPhaseConfidence());
        map.put("volumeBehavior", s.getVolumeBehavior());

        // Bias
        map.put("overallBias", s.getOverallBias());
        map.put("orderFlowDirection", s.getOrderFlowDirection());
        map.put("orderFlowStrength", s.getOrderFlowStrength());
        map.put("analysisConfidence", s.getAnalysisConfidence());

        return map;
    }

    private Map<String, Object> profileToMap(VolumeProfile p) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", p.getId());
        map.put("instrumentToken", p.getInstrumentToken());
        map.put("instrumentName", p.getInstrumentName());
        map.put("timeframe", p.getTimeframe());
        map.put("analysisTimestamp", p.getAnalysisTimestamp());
        map.put("currentPrice", p.getCurrentPrice());

        // Volume Profile Levels
        map.put("pocLevel", p.getPocLevel());
        map.put("valueAreaHigh", p.getValueAreaHigh());
        map.put("valueAreaLow", p.getValueAreaLow());
        map.put("hvn1", p.getHvn1());
        map.put("hvn2", p.getHvn2());
        map.put("lvn1", p.getLvn1());
        map.put("lvn2", p.getLvn2());

        // Statistics
        map.put("totalVolume", p.getTotalVolume());
        map.put("averageVolume", p.getAverageVolume());
        map.put("maxVolume", p.getMaxVolume());
        map.put("minVolume", p.getMinVolume());

        // IOB Volume
        map.put("iobCandleVolume", p.getIobCandleVolume());
        map.put("iobVolumeRatio", p.getIobVolumeRatio());
        map.put("iobVolumeType", p.getIobVolumeType());

        // Displacement
        map.put("displacementVolume", p.getDisplacementVolume());
        map.put("displacementVolumeRatio", p.getDisplacementVolumeRatio());
        map.put("displacementConfirmed", p.getDisplacementConfirmed());

        // Delta
        map.put("buyingVolume", p.getBuyingVolume());
        map.put("sellingVolume", p.getSellingVolume());
        map.put("volumeDelta", p.getVolumeDelta());
        map.put("cumulativeDelta", p.getCumulativeDelta());
        map.put("deltaDirection", p.getDeltaDirection());
        map.put("deltaStrength", p.getDeltaStrength());

        // Confluence
        map.put("pocIobAligned", p.getPocIobAligned());
        map.put("volumeConfluenceScore", p.getVolumeConfluenceScore());
        map.put("volumeBias", p.getVolumeBias());

        return map;
    }

    private Map<String, Object> riskConfigToMap(RiskManagement r) {
        Map<String, Object> map = new HashMap<>();
        map.put("accountCapital", r.getAccountCapital());
        map.put("riskPerTradePercent", r.getRiskPerTradePercent());
        map.put("maxRiskPerTrade", r.getMaxRiskPerTrade());
        map.put("maxDailyLossPercent", r.getMaxDailyLossPercent());
        map.put("maxDailyLossAmount", r.getMaxDailyLossAmount());
        map.put("maxPortfolioHeatPercent", r.getMaxPortfolioHeatPercent());
        map.put("atrPeriod", r.getAtrPeriod());
        map.put("atrSlMultiplier", r.getAtrSlMultiplier());
        map.put("maxDailyTrades", r.getMaxDailyTrades());
        map.put("maxInstrumentExposurePercent", r.getMaxInstrumentExposurePercent());
        map.put("maxCorrelatedExposurePercent", r.getMaxCorrelatedExposurePercent());
        map.put("tradingAllowed", r.getTradingAllowed());
        map.put("riskAssessmentScore", r.getRiskAssessmentScore());
        return map;
    }
}
