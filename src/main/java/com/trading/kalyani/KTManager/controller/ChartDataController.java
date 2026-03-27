package com.trading.kalyani.KTManager.controller;

import com.trading.kalyani.KTManager.entity.InternalOrderBlock;
import com.trading.kalyani.KTManager.model.HistoricalDataRequest;
import com.trading.kalyani.KTManager.model.HistoricalDataResponse;
import com.trading.kalyani.KTManager.repository.InternalOrderBlockRepository;
import com.trading.kalyani.KTManager.service.InstrumentService;
import com.trading.kalyani.KTManager.service.RealTimePriceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;

/**
 * REST Controller for chart data and visualization.
 * Provides OHLC data, IOB zones, and other chart overlays.
 */
@RestController
@RequestMapping("/api/chart")
@CrossOrigin(origins = "*")
public class ChartDataController {

    private static final Logger logger = LoggerFactory.getLogger(ChartDataController.class);

    @Autowired
    private InstrumentService instrumentService;

    @Autowired
    private InternalOrderBlockRepository iobRepository;

    @Autowired
    private RealTimePriceService realTimePriceService;

    /**
     * Get OHLC candlestick data for charting
     */
    @GetMapping("/ohlc/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getOHLCData(
            @PathVariable Long instrumentToken,
            @RequestParam(defaultValue = "5minute") String interval,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime fromDate,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime toDate) {
        try {
            // Default to last 5 days
            if (toDate == null) toDate = LocalDateTime.now();
            if (fromDate == null) fromDate = toDate.minusDays(5);

            HistoricalDataRequest request = HistoricalDataRequest.builder()
                    .instrumentToken(String.valueOf(instrumentToken))
                    .fromDate(fromDate)
                    .toDate(toDate)
                    .interval(interval)
                    .continuous(false)
                    .oi(false)
                    .build();

            HistoricalDataResponse response = instrumentService.getHistoricalData(request);

            if (response == null || !response.isSuccess()) {
                return ResponseEntity.ok(Map.of(
                        "success", false,
                        "error", "Failed to fetch historical data"
                ));
            }

            // Convert to chart format (Lightweight Charts compatible)
            List<Map<String, Object>> chartData = new ArrayList<>();
            if (response.getCandles() != null) {
                for (var candle : response.getCandles()) {
                    Map<String, Object> point = new HashMap<>();
                    point.put("time", parseTimestampToEpoch(candle.getTimestamp()));
                    point.put("open", candle.getOpen());
                    point.put("high", candle.getHigh());
                    point.put("low", candle.getLow());
                    point.put("close", candle.getClose());
                    point.put("volume", candle.getVolume());
                    chartData.add(point);
                }
            }

            Map<String, Object> result = new HashMap<>();
            result.put("success", true);
            result.put("instrumentToken", instrumentToken);
            result.put("instrumentName", getInstrumentName(instrumentToken));
            result.put("interval", interval);
            result.put("fromDate", fromDate);
            result.put("toDate", toDate);
            result.put("candleCount", chartData.size());
            result.put("candles", chartData);

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            logger.error("Error fetching OHLC data: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Get IOB zones for chart overlay
     * Only shows FRESH (active) IOBs - excludes mitigated, completed, stopped, expired
     */
    @GetMapping("/iob-zones/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getIOBZones(
            @PathVariable Long instrumentToken,
            @RequestParam(defaultValue = "false") boolean includeExpired) {
        try {
            List<InternalOrderBlock> iobs;

            if (includeExpired) {
                // Include all IOBs for historical view
                iobs = iobRepository.findAll().stream()
                        .filter(iob -> iob.getInstrumentToken().equals(instrumentToken))
                        .collect(Collectors.toList());
            } else {
                // Only show FRESH IOBs on the chart - mitigated IOBs should not be shown
                // This ensures the chart only displays active zones that haven't been touched
                iobs = iobRepository.findFreshIOBs(instrumentToken);
            }

            List<Map<String, Object>> zones = iobs.stream().map(iob -> {
                Map<String, Object> zone = new HashMap<>();
                zone.put("id", iob.getId());
                zone.put("type", iob.getObType());
                zone.put("zoneHigh", iob.getZoneHigh());
                zone.put("zoneLow", iob.getZoneLow());
                zone.put("zoneMidpoint", iob.getZoneMidpoint());
                zone.put("startTime", iob.getObCandleTime() != null ?
                        iob.getObCandleTime().toEpochSecond(java.time.ZoneOffset.of("+05:30")) : null);
                zone.put("bosLevel", iob.getBosLevel());
                zone.put("bosType", iob.getBosType());
                zone.put("direction", iob.getTradeDirection());
                zone.put("status", iob.getStatus());
                zone.put("confidence", iob.getSignalConfidence());
                zone.put("hasFvg", iob.getHasFvg());
                zone.put("fvgHigh", iob.getFvgHigh());
                zone.put("fvgLow", iob.getFvgLow());

                // Trade levels
                zone.put("entryPrice", iob.getEntryPrice());
                zone.put("stopLoss", iob.getStopLoss());
                zone.put("target1", iob.getTarget1());
                zone.put("target2", iob.getTarget2());
                zone.put("target3", iob.getTarget3());

                // Color coding
                zone.put("color", "BULLISH_IOB".equals(iob.getObType()) ?
                        "rgba(76, 175, 80, 0.3)" : "rgba(244, 67, 54, 0.3)");
                zone.put("borderColor", "BULLISH_IOB".equals(iob.getObType()) ?
                        "#4CAF50" : "#F44336");

                return zone;
            }).collect(Collectors.toList());

            Map<String, Object> result = new HashMap<>();
            result.put("success", true);
            result.put("instrumentToken", instrumentToken);
            result.put("zoneCount", zones.size());
            result.put("zones", zones);

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            logger.error("Error fetching IOB zones: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Get swing points for chart markers
     */
    @GetMapping("/swing-points/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getSwingPoints(
            @PathVariable Long instrumentToken,
            @RequestParam(defaultValue = "5minute") String interval) {
        try {
            // Fetch historical data
            LocalDateTime toDate = LocalDateTime.now();
            LocalDateTime fromDate = toDate.minusDays(5);

            HistoricalDataRequest request = HistoricalDataRequest.builder()
                    .instrumentToken(String.valueOf(instrumentToken))
                    .fromDate(fromDate)
                    .toDate(toDate)
                    .interval(interval)
                    .continuous(false)
                    .oi(false)
                    .build();

            HistoricalDataResponse response = instrumentService.getHistoricalData(request);

            if (response == null || response.getCandles() == null) {
                return ResponseEntity.ok(Map.of(
                        "success", false,
                        "error", "Failed to fetch data"
                ));
            }

            var candles = response.getCandles();
            int lookback = 3;

            List<Map<String, Object>> swingHighs = new ArrayList<>();
            List<Map<String, Object>> swingLows = new ArrayList<>();

            for (int i = lookback; i < candles.size() - lookback; i++) {
                var current = candles.get(i);
                boolean isSwingHigh = true;
                boolean isSwingLow = true;

                for (int j = 1; j <= lookback; j++) {
                    if (candles.get(i - j).getHigh() >= current.getHigh() ||
                        candles.get(i + j).getHigh() >= current.getHigh()) {
                        isSwingHigh = false;
                    }
                    if (candles.get(i - j).getLow() <= current.getLow() ||
                        candles.get(i + j).getLow() <= current.getLow()) {
                        isSwingLow = false;
                    }
                }

                if (isSwingHigh) {
                    Map<String, Object> sh = new HashMap<>();
                    sh.put("time", parseTimestampToEpoch(current.getTimestamp()));
                    sh.put("price", current.getHigh());
                    sh.put("type", "SWING_HIGH");
                    sh.put("position", "aboveBar");
                    sh.put("color", "#F44336");
                    sh.put("shape", "arrowDown");
                    sh.put("text", "SH");
                    swingHighs.add(sh);
                }

                if (isSwingLow) {
                    Map<String, Object> sl = new HashMap<>();
                    sl.put("time", parseTimestampToEpoch(current.getTimestamp()));
                    sl.put("price", current.getLow());
                    sl.put("type", "SWING_LOW");
                    sl.put("position", "belowBar");
                    sl.put("color", "#4CAF50");
                    sl.put("shape", "arrowUp");
                    sl.put("text", "SL");
                    swingLows.add(sl);
                }
            }

            Map<String, Object> result = new HashMap<>();
            result.put("success", true);
            result.put("swingHighs", swingHighs);
            result.put("swingLows", swingLows);
            result.put("totalHighs", swingHighs.size());
            result.put("totalLows", swingLows.size());

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            logger.error("Error fetching swing points: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Get complete chart data including candles, zones, and markers
     */
    @GetMapping("/complete/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getCompleteChartData(
            @PathVariable Long instrumentToken,
            @RequestParam(defaultValue = "5minute") String interval,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime fromDate,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime toDate) {
        try {
            Map<String, Object> result = new HashMap<>();

            // Get OHLC data
            var ohlcResponse = getOHLCData(instrumentToken, interval, fromDate, toDate);
            if (ohlcResponse.getBody() != null) {
                result.put("candles", ohlcResponse.getBody().get("candles"));
            }

            // Get IOB zones
            var zonesResponse = getIOBZones(instrumentToken, false);
            if (zonesResponse.getBody() != null) {
                result.put("iobZones", zonesResponse.getBody().get("zones"));
            }

            // Get swing points
            var swingsResponse = getSwingPoints(instrumentToken, interval);
            if (swingsResponse.getBody() != null) {
                result.put("swingHighs", swingsResponse.getBody().get("swingHighs"));
                result.put("swingLows", swingsResponse.getBody().get("swingLows"));
            }

            // Add current price
            Double currentPrice = realTimePriceService.getCurrentPrice(instrumentToken);
            result.put("currentPrice", currentPrice);

            // Add distance to nearest zone
            if (currentPrice != null) {
                result.put("distanceToZone",
                        realTimePriceService.getDistanceToNearestZone(instrumentToken, currentPrice));
            }

            result.put("success", true);
            result.put("instrumentToken", instrumentToken);
            result.put("instrumentName", getInstrumentName(instrumentToken));
            result.put("interval", interval);
            result.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            logger.error("Error fetching complete chart data: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Get trade levels for a specific IOB (for horizontal line overlays)
     */
    @GetMapping("/trade-levels/{iobId}")
    public ResponseEntity<Map<String, Object>> getTradeLevels(@PathVariable Long iobId) {
        try {
            Optional<InternalOrderBlock> iobOpt = iobRepository.findById(iobId);
            if (iobOpt.isEmpty()) {
                return ResponseEntity.notFound().build();
            }

            InternalOrderBlock iob = iobOpt.get();
            List<Map<String, Object>> levels = new ArrayList<>();

            // Entry level
            if (iob.getEntryPrice() != null) {
                levels.add(createLevel("Entry", iob.getEntryPrice(), "#2196F3", "dotted"));
            }

            // Stop Loss
            if (iob.getStopLoss() != null) {
                levels.add(createLevel("Stop Loss", iob.getStopLoss(), "#F44336", "solid"));
            }

            // Targets
            if (iob.getTarget1() != null) {
                levels.add(createLevel("Target 1", iob.getTarget1(), "#4CAF50", "dashed"));
            }
            if (iob.getTarget2() != null) {
                levels.add(createLevel("Target 2", iob.getTarget2(), "#8BC34A", "dashed"));
            }
            if (iob.getTarget3() != null) {
                levels.add(createLevel("Target 3", iob.getTarget3(), "#CDDC39", "dashed"));
            }

            // BOS level
            if (iob.getBosLevel() != null) {
                levels.add(createLevel("BOS", iob.getBosLevel(), "#FF9800", "dotted"));
            }

            Map<String, Object> result = new HashMap<>();
            result.put("success", true);
            result.put("iobId", iobId);
            result.put("levels", levels);

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            logger.error("Error fetching trade levels: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Get real-time price for chart update
     */
    @GetMapping("/realtime-price/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getRealtimePrice(@PathVariable Long instrumentToken) {
        try {
            Double price = realTimePriceService.getCurrentPrice(instrumentToken);
            Map<String, Object> tick = realTimePriceService.getLastTick(instrumentToken);

            Map<String, Object> result = new HashMap<>();
            result.put("success", true);
            result.put("instrumentToken", instrumentToken);
            result.put("price", price);
            result.put("tick", tick);
            result.put("timestamp", LocalDateTime.now());

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            logger.error("Error fetching realtime price: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    // ==================== Helper Methods ====================

    private long parseTimestampToEpoch(String timestamp) {
        try {
            java.time.ZonedDateTime zdt = java.time.ZonedDateTime.parse(timestamp,
                    java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ"));
            return zdt.toEpochSecond();
        } catch (Exception e) {
            return System.currentTimeMillis() / 1000;
        }
    }

    private String getInstrumentName(Long token) {
        if (token.equals(NIFTY_INSTRUMENT_TOKEN)) return "NIFTY";
        return "UNKNOWN";
    }

    private Map<String, Object> createLevel(String label, Double price, String color, String style) {
        Map<String, Object> level = new HashMap<>();
        level.put("label", label);
        level.put("price", price);
        level.put("color", color);
        level.put("lineStyle", style);
        level.put("lineWidth", 1);
        level.put("axisLabelVisible", true);
        return level;
    }
}
