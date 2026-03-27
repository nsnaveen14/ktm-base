package com.trading.kalyani.KTManager.controller;

import com.trading.kalyani.KTManager.entity.PredictedCandleStick;
import com.trading.kalyani.KTManager.entity.PredictionDeviation;
import com.trading.kalyani.KTManager.service.CandlePredictionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * REST Controller for Candle Prediction Service.
 * Provides endpoints for generating, retrieving, and managing candle predictions
 * for NIFTY 50 intraday trading.
 */
@RestController
@RequestMapping("/api/prediction")
@CrossOrigin(origins = "*")
public class CandlePredictionController {

    @Autowired
    private CandlePredictionService candlePredictionService;

    /**
     * Generate predictions for the next 5 one-minute candles.
     *
     * @param appJobConfigNum The job configuration number
     * @return List of 5 predicted candles
     */
    @PostMapping("/generate/{appJobConfigNum}")
    public ResponseEntity<List<PredictedCandleStick>> generatePredictions(
            @PathVariable Integer appJobConfigNum, LocalDateTime startTime, LocalDateTime endTime) {
        List<PredictedCandleStick> predictions = candlePredictionService.predictNextFiveCandles(appJobConfigNum,startTime,endTime);
        return ResponseEntity.ok(predictions);
    }

    /**
     * Get the latest predictions without regenerating.
     *
     * @return List of most recent predictions
     */
    @GetMapping("/latest")
    public ResponseEntity<List<PredictedCandleStick>> getLatestPredictions() {
        List<PredictedCandleStick> predictions = candlePredictionService.getLatestPredictions();
        return ResponseEntity.ok(predictions);
    }

    /**
     * Get trade recommendation based on current predictions.
     *
     * @return Trade recommendation (BULLISH, BEARISH, or NEUTRAL)
     */
    @GetMapping("/recommendation")
    public ResponseEntity<Map<String, String>> getTradeRecommendation() {
        String recommendation = candlePredictionService.getTradeRecommendation();
        Map<String, String> response = new HashMap<>();
        response.put("recommendation", recommendation);
        return ResponseEntity.ok(response);
    }

    /**
     * Verify past predictions against actual candle data.
     *
     * @return Number of predictions verified
     */
    @PostMapping("/verify")
    public ResponseEntity<Map<String, Object>> verifyPredictions() {
        Map<String, Object> response = new HashMap<>();
        try {
            int verifiedCount = candlePredictionService.verifyPastPredictions();
            Double accuracy = candlePredictionService.getSessionAccuracy();

            response.put("verifiedCount", verifiedCount);
            response.put("sessionAccuracy", accuracy != null ? accuracy : 0.0);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            response.put("verifiedCount", 0);
            response.put("sessionAccuracy", 0.0);
            response.put("error", e.getMessage());
            return ResponseEntity.ok(response);
        }
    }

    /**
     * Get session accuracy for predictions.
     *
     * @return Average prediction accuracy as percentage
     */
    @GetMapping("/accuracy")
    public ResponseEntity<Map<String, Double>> getSessionAccuracy() {
        Double accuracy = candlePredictionService.getSessionAccuracy();
        Map<String, Double> response = new HashMap<>();
        response.put("accuracy", accuracy);
        return ResponseEntity.ok(response);
    }

    /**
     * Clean up old prediction data.
     *
     * @param daysToRetain Number of days of predictions to retain (default: 7)
     * @return Number of records deleted
     */
    @DeleteMapping("/cleanup")
    public ResponseEntity<Map<String, Integer>> cleanupOldPredictions(
            @RequestParam(defaultValue = "7") int daysToRetain) {
        int deletedCount = candlePredictionService.cleanupOldPredictions(daysToRetain);
        Map<String, Integer> response = new HashMap<>();
        response.put("deletedCount", deletedCount);
        return ResponseEntity.ok(response);
    }

    // ===================== Trade Setup Endpoints =====================

    /**
     * Get the latest trade setup with entry, target, and stop-loss levels.
     *
     * @return Latest trade setup or 404 if none exists
     */
    @GetMapping("/trade-setup/latest")
    public ResponseEntity<?> getLatestTradeSetup() {
        return candlePredictionService.getLatestTradeSetup()
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Get all trade setups created today.
     *
     * @return List of today's trade setups
     */
    @GetMapping("/trade-setup/today")
    public ResponseEntity<List<com.trading.kalyani.KTManager.entity.TradeSetupEntity>> getTodayTradeSetups() {
        return ResponseEntity.ok(candlePredictionService.getTodayTradeSetups());
    }

    /**
     * Get trade setup performance metrics.
     *
     * @return Performance metrics including win rate, P/L, etc.
     */
    @GetMapping("/trade-setup/performance")
    public ResponseEntity<Map<String, Object>> getTradeSetupPerformance() {
        return ResponseEntity.ok(candlePredictionService.getTradeSetupPerformance());
    }

    // ===================== Prediction Job Control Endpoints =====================

    /**
     * Start the automated prediction job.
     * Once started, predictions will be generated every minute during market hours.
     *
     * @return Status message
     */
    @PostMapping("/job/start")
    public ResponseEntity<Map<String, Object>> startPredictionJob() {
        candlePredictionService.startPredictionJob();
        Map<String, Object> response = new HashMap<>();
        response.put("status", "started");
        response.put("message", "Prediction job has been activated");
        response.put("isActive", candlePredictionService.isPredictionJobActive());
        return ResponseEntity.ok(response);
    }

    /**
     * Stop the automated prediction job.
     *
     * @return Status message
     */
    @PostMapping("/job/stop")
    public ResponseEntity<Map<String, Object>> stopPredictionJob() {
        candlePredictionService.stopPredictionJob();
        Map<String, Object> response = new HashMap<>();
        response.put("status", "stopped");
        response.put("message", "Prediction job has been deactivated");
        response.put("isActive", candlePredictionService.isPredictionJobActive());
        return ResponseEntity.ok(response);
    }

    /**
     * Get current status and statistics of the prediction job.
     *
     * @return Job statistics including run counts, accuracy, deviations, etc.
     */
    @GetMapping("/job/status")
    public ResponseEntity<Map<String, Object>> getPredictionJobStatus() {
        return ResponseEntity.ok(candlePredictionService.getPredictionJobStats());
    }

    /**
     * Manually trigger a prediction job execution.
     * Useful for testing or when you want an immediate prediction.
     *
     * @return Status message with prediction results
     */
    @PostMapping("/job/execute")
    public ResponseEntity<Map<String, Object>> executePredictionJobNow() {
        Map<String, Object> response = new HashMap<>();
        try {
            candlePredictionService.executePredictionJob();
            response.put("status", "success");
            response.put("message", "Prediction job executed successfully");
            response.put("predictions", candlePredictionService.getLatestPredictions());
        } catch (Exception e) {
            response.put("status", "error");
            response.put("message", "Error executing prediction job: " + e.getMessage());
        }
        return ResponseEntity.ok(response);
    }

    // ===================== Deviation Tracking Endpoints =====================

    /**
     * Manually trigger verification and deviation calculation.
     * Uses force mode to bypass ticker check and use wider time window.
     *
     * @return Deviation statistics for the verified predictions
     */
    @PostMapping("/deviation/calculate")
    public ResponseEntity<?> calculateDeviation() {
        // Use force=true for manual calculation to bypass ticker check
        PredictionDeviation deviation = candlePredictionService.verifyAndCalculateDeviation(true);
        if (deviation != null) {
            return ResponseEntity.ok(deviation);
        } else {
            Map<String, String> response = new HashMap<>();
            response.put("message", "No verified predictions available. Make sure predictions exist and have actual price data.");
            return ResponseEntity.ok(response);
        }
    }

    /**
     * Get the latest deviation statistics.
     *
     * @return Latest deviation record or 404 if none exists
     */
    @GetMapping("/deviation/latest")
    public ResponseEntity<?> getLatestDeviation() {
        return candlePredictionService.getLatestDeviation()
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Get all deviation records for today's trading session.
     *
     * @return List of deviation records for today
     */
    @GetMapping("/deviation/today")
    public ResponseEntity<List<PredictionDeviation>> getTodayDeviations() {
        return ResponseEntity.ok(candlePredictionService.getTodayDeviations());
    }

    /**
     * Get correction factors calculated from historical deviations.
     * These factors can be used to improve future predictions.
     *
     * @return Map of correction factors
     */
    @GetMapping("/deviation/corrections")
    public ResponseEntity<Map<String, Double>> getCorrectionFactors() {
        return ResponseEntity.ok(candlePredictionService.getCorrectionFactors());
    }

    /**
     * Get a comprehensive deviation summary with analysis.
     * Includes historical data fallback when no today's deviations exist.
     *
     * @return Summary including today's stats, correction factors, and recommendations
     */
    @GetMapping("/deviation/summary")
    public ResponseEntity<Map<String, Object>> getDeviationSummary() {
        Map<String, Object> summary = new HashMap<>();

        // Today's deviations
        List<PredictionDeviation> todayDeviations = candlePredictionService.getTodayDeviations();
        summary.put("todayDeviationCount", todayDeviations.size());

        if (!todayDeviations.isEmpty()) {
            // Calculate aggregated statistics from today's data
            double avgDeviation = todayDeviations.stream()
                    .filter(d -> d.getAvgCloseDeviationPercent() != null)
                    .mapToDouble(PredictionDeviation::getAvgCloseDeviationPercent)
                    .average()
                    .orElse(0);
            summary.put("avgDeviationPercent", avgDeviation);

            double avgDirectionAccuracy = todayDeviations.stream()
                    .filter(d -> d.getDirectionAccuracyPercent() != null)
                    .mapToDouble(PredictionDeviation::getDirectionAccuracyPercent)
                    .average()
                    .orElse(0);
            summary.put("avgDirectionAccuracy", avgDirectionAccuracy);

            // Latest bias information
            PredictionDeviation latest = todayDeviations.get(todayDeviations.size() - 1);
            summary.put("latestBias", latest.getSystematicBias());
            summary.put("latestBiasDirection", latest.getBiasDirection());

            // Cumulative statistics
            summary.put("cumulativeSessions", latest.getCumulativeSessions());
            summary.put("cumulativeAvgDeviation", latest.getCumulativeAvgDeviation());
            summary.put("cumulativeAccuracy", latest.getCumulativeAccuracy());
        } else {
            // No today's deviations - try to get latest historical deviation
            Optional<PredictionDeviation> latestDeviation = candlePredictionService.getLatestDeviation();
            if (latestDeviation.isPresent()) {
                PredictionDeviation latest = latestDeviation.get();
                summary.put("avgDeviationPercent", latest.getAvgCloseDeviationPercent());
                summary.put("avgDirectionAccuracy", latest.getDirectionAccuracyPercent());
                summary.put("latestBias", latest.getSystematicBias());
                summary.put("latestBiasDirection", latest.getBiasDirection());
                summary.put("cumulativeSessions", latest.getCumulativeSessions());
                summary.put("cumulativeAvgDeviation", latest.getCumulativeAvgDeviation());
                summary.put("cumulativeAccuracy", latest.getCumulativeAccuracy());
                summary.put("dataSource", "historical");
                summary.put("lastCalculationDate", latest.getTradingDate() != null ?
                        latest.getTradingDate().toString() : null);
            } else {
                // No deviation data at all - provide placeholder values
                summary.put("avgDeviationPercent", null);
                summary.put("avgDirectionAccuracy", null);
                summary.put("latestBiasDirection", "NEUTRAL");
                summary.put("cumulativeSessions", 0);
                summary.put("cumulativeAccuracy", null);
                summary.put("dataSource", "none");
                summary.put("message", "No deviation data available. Click 'Calculate Now' to generate.");
            }
        }

        // Correction factors
        summary.put("correctionFactors", candlePredictionService.getCorrectionFactors());

        // Session accuracy
        summary.put("sessionAccuracy", candlePredictionService.getSessionAccuracy());

        // Job status
        summary.put("jobActive", candlePredictionService.isPredictionJobActive());
        summary.put("jobStats", candlePredictionService.getPredictionJobStats());

        return ResponseEntity.ok(summary);
    }

    /**
     * Get live tick data for NIFTY 50 and ATM strike prices.
     * Returns real-time LTP for NIFTY index, ATM CE and ATM PE.
     *
     * @return Live tick data including NIFTY LTP, ATM CE LTP, ATM PE LTP
     */
    @GetMapping("/live-tick")
    public ResponseEntity<Map<String, Object>> getLiveTickData() {
        return ResponseEntity.ok(candlePredictionService.getLiveTickData());
    }

    /**
     * Get EMA chart data for displaying 9, 21, and 50 EMA lines with trade signals.
     * Returns time series data for charting with crossover analysis.
     *
     * @return EMA chart data including ema9, ema21, ema50, and trade signals
     */
    @GetMapping("/ema-chart")
    public ResponseEntity<Map<String, Object>> getEMAChartData() {
        return ResponseEntity.ok(candlePredictionService.getEMAChartData());
    }

    /**
     * Get rolling chart data for predicted vs actual candles.
     * Returns 30-minute window with:
     * - First 25 minutes: Both actual and predicted candle close prices
     * - Last 5 minutes: Only predicted candles (future predictions)
     *
     * @return Rolling chart data including actualSeries, predictedSeries, and window metadata
     */
    @GetMapping("/rolling-chart")
    public ResponseEntity<Map<String, Object>> getRollingChartData() {
        return ResponseEntity.ok(candlePredictionService.getRollingChartData());
    }
}

