package com.trading.kalyani.KTManager.config;

import com.trading.kalyani.KTManager.entity.InternalOrderBlock;
import com.trading.kalyani.KTManager.service.InternalOrderBlockService;
import com.trading.kalyani.KTManager.service.TelegramNotificationService;
import com.trading.kalyani.KTManager.service.AutoTradingService;
import com.trading.kalyani.KTManager.service.RealTimePriceService;
import com.trading.kalyani.KTManager.service.PerformanceTrackingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;

/**
 * Scheduler for Internal Order Block (IOB) scanning and mitigation checking.
 * <p>
 * This scheduler runs every minute during market hours to:
 * 1. Scan for new IOBs on NIFTY
 * 2. Check if any existing IOBs have been mitigated
 * 3. Send Telegram alerts for high-confidence IOB signals (>51%)
 * <p>
 * The polling technique ensures reliable IOB detection without depending on WebSocket connections.
 */
@Component
public class IOBScheduler {

    private static final Logger logger = LoggerFactory.getLogger(IOBScheduler.class);

    private static final LocalTime MARKET_OPEN = LocalTime.of(9, 15);
    private static final LocalTime MARKET_CLOSE = LocalTime.of(15, 30);

    // Minimum confidence threshold for Telegram alerts (51%)
    private static final double TELEGRAM_ALERT_CONFIDENCE_THRESHOLD = 51.0;
    private static final String IST_ZONE = "Asia/Kolkata";
    private static final ZoneId IST_ZONE_ID = ZoneId.of(IST_ZONE);

    @Autowired
    private InternalOrderBlockService iobService;

    @Autowired(required = false)
    private TelegramNotificationService telegramNotificationService;

    @Autowired(required = false)
    private AutoTradingService autoTradingService;

    @Autowired(required = false)
    private RealTimePriceService realTimePriceService;

    @Autowired(required = false)
    private PerformanceTrackingService performanceTrackingService;

    @Value("${iob.scanner.enabled:true}")
    private boolean iobScannerEnabled;

    // Track the last price for each instrument to detect mitigation
    private final Map<Long, Double> lastPrices = new ConcurrentHashMap<>();

    // Track already alerted IOB IDs to avoid duplicate alerts
    private final Set<Long> alertedIOBs = ConcurrentHashMap.newKeySet();

    /**
     * IOB Scanner Job - Runs every minute during first trading hour (9:16 AM to 9:59 AM)
     */
    @Scheduled(cron = "0 16-59 9 * * MON-FRI", zone = IST_ZONE)
    public void runIOBScannerFirstHour() {
        executeIOBScanner("FirstHour");
    }

    /**
     * IOB Scanner Job - Runs every minute during mid-day (10:00 AM to 2:59 PM)
     */
    @Scheduled(cron = "0 * 10-14 * * MON-FRI", zone = IST_ZONE)
    public void runIOBScannerMidDay() {
        executeIOBScanner("MidDay");
    }

    /**
     * IOB Scanner Job - Runs every minute during last trading hour (3:00 PM to 3:29 PM)
     */
    @Scheduled(cron = "0 0-29 15 * * MON-FRI", zone = IST_ZONE)
    public void runIOBScannerLastHour() {
        executeIOBScanner("LastHour");
    }

    /**
     * Common method to execute IOB scanning with proper checks
     */
    private void executeIOBScanner(String period) {
        if (!iobScannerEnabled) {
            logger.debug("IOB Scanner is disabled via configuration");
            return;
        }

        LocalTime now = LocalTime.now(IST_ZONE_ID);
        if (now.isBefore(MARKET_OPEN) || now.isAfter(MARKET_CLOSE)) {
            return;
        }

        try {
            logger.debug("[{}] Running IOB scanner at {}", period, now);

            // Scan for NIFTY IOBs
            scanAndProcessIOBs(NIFTY_INSTRUMENT_TOKEN, "NIFTY");

            // Fetch prices once for both auto-trading and trade outcome checks
            Map<Long, Double> currentPrices = realTimePriceService != null
                    ? realTimePriceService.getAllCurrentPrices()
                    : new HashMap<>(lastPrices);

            // Process auto trading if enabled
            processAutoTrading(currentPrices);

            // Check and update trade outcomes
            checkTradeOutcomes(currentPrices);

        } catch (Exception e) {
            logger.error("Error in IOB scanner [{}]: {}", period, e.getMessage(), e);
        }
    }

    /**
     * Process auto trading based on current prices
     */
    private void processAutoTrading(Map<Long, Double> currentPrices) {
        if (autoTradingService == null) {
            return;
        }

        try {
            if (!currentPrices.isEmpty()) {
                autoTradingService.processAutoTrades(currentPrices);
            }
        } catch (Exception e) {
            logger.error("Error processing auto trades: {}", e.getMessage(), e);
        }
    }

    /**
     * Check and update outcomes for open trades
     */
    private void checkTradeOutcomes(Map<Long, Double> currentPrices) {
        if (performanceTrackingService == null) {
            return;
        }

        try {
            if (!currentPrices.isEmpty()) {
                performanceTrackingService.checkTradeOutcomes(currentPrices);
            }
        } catch (Exception e) {
            logger.error("Error checking trade outcomes: {}", e.getMessage(), e);
        }
    }

    /**
     * Scan for IOBs and process them (alerts + mitigation check)
     */
    private void scanAndProcessIOBs(Long instrumentToken, String instrumentName) {
        try {
            // Scan for new IOBs
            List<InternalOrderBlock> detectedIOBs = iobService.scanForIOBs(instrumentToken);

            if (detectedIOBs != null && !detectedIOBs.isEmpty()) {
                logger.info("Detected {} IOBs for {}", detectedIOBs.size(), instrumentName);

                for (InternalOrderBlock iob : detectedIOBs) {
                    // Check if we should send a Telegram alert for this IOB
                    if (shouldSendAlert(iob)) {
                        sendIOBAlert(iob);
                        alertedIOBs.add(iob.getId());

                    }
                }
            }

            // Check for mitigation of existing IOBs
            checkMitigationForInstrument(instrumentToken, instrumentName);

        } catch (Exception e) {
            logger.error("Error scanning IOBs for {}: {}", instrumentName, e.getMessage(), e);
        }
    }

    /**
     * Check if any existing IOBs have been mitigated
     */
    private void checkMitigationForInstrument(Long instrumentToken, String instrumentName) {
        try {
            // Get fresh (unmitigated) IOBs
            List<InternalOrderBlock> freshIOBs = iobService.getFreshIOBs(instrumentToken);

            if (freshIOBs == null || freshIOBs.isEmpty()) {
                return;
            }

            // Get current price from the latest IOB or use a default approach
            Double currentPrice = null;
            for (InternalOrderBlock iob : freshIOBs) {
                if (iob.getCurrentPrice() != null) {
                    currentPrice = iob.getCurrentPrice();
                    break;
                }
            }

            if (currentPrice != null) {
                // Check for mitigation
                List<InternalOrderBlock> mitigatedIOBs = iobService.checkMitigation(instrumentToken, currentPrice);

                if (mitigatedIOBs != null && !mitigatedIOBs.isEmpty()) {
                    logger.info("Detected {} mitigated IOBs for {} at price {}",
                            mitigatedIOBs.size(), instrumentName, currentPrice);

                    for (InternalOrderBlock mitigated : mitigatedIOBs) {
                        sendMitigationAlert(mitigated, currentPrice);
                    }
                }

                // Update last price
                lastPrices.put(instrumentToken, currentPrice);
            }

        } catch (Exception e) {
            logger.error("Error checking mitigation for {}: {}", instrumentName, e.getMessage(), e);
        }
    }

    /**
     * Check if we should send an alert for this IOB
     */
    private boolean shouldSendAlert(InternalOrderBlock iob) {
        if (iob == null || iob.getId() == null) {
            return false;
        }

        // Check if already alerted
        if (alertedIOBs.contains(iob.getId())) {
            return false;
        }

        Double confidence = iob.getSignalConfidence();
        return confidence != null && confidence > TELEGRAM_ALERT_CONFIDENCE_THRESHOLD;
    }

    /**
     * Send Telegram alert for new IOB signal
     */
    private void sendIOBAlert(InternalOrderBlock iob) {
        if (telegramNotificationService == null) {
            logger.warn("TelegramNotificationService is not available");
            return;
        }

        try {
            String alertType = iob.getObType() != null && iob.getObType().contains("BULLISH") ? "🟢 BULLISH" : "🔴 BEARISH";

            StringBuilder message = new StringBuilder();
            message.append(String.format("*%s IOB Detected*\n", alertType));
            message.append(String.format("📈 Instrument: *%s*\n", iob.getInstrumentName()));
            message.append(String.format("⏰ Timeframe: %s\n", iob.getTimeframe()));
            message.append(String.format("📅 OB Candle Time: %s\n", iob.getObCandleTime()));
            message.append(String.format("🕐 Detection Time: %s\n", iob.getDetectionTimestamp() != null ? iob.getDetectionTimestamp().toString() : "N/A"));
            message.append("\n");
            message.append(String.format("🎯 Zone High: %.2f\n", iob.getZoneHigh()));
            message.append(String.format("🎯 Zone Low: %.2f\n", iob.getZoneLow()));
            message.append(String.format("💰 Current Price: %.2f\n", iob.getCurrentPrice() != null ? iob.getCurrentPrice() : 0.0));
            message.append("\n");
            message.append(String.format("📍 Trade Direction: *%s*\n", iob.getTradeDirection()));
            message.append(String.format("🚀 Entry Price: %.2f\n", iob.getEntryPrice() != null ? iob.getEntryPrice() : 0.0));
            message.append(String.format("🛡️ Stop Loss: %.2f\n", iob.getStopLoss() != null ? iob.getStopLoss() : 0.0));
            message.append(String.format("🎯 Target 1: %.2f\n", iob.getTarget1() != null ? iob.getTarget1() : 0.0));
            message.append(String.format("🎯 Target 2: %.2f\n", iob.getTarget2() != null ? iob.getTarget2() : 0.0));
            message.append(String.format("🎯 Target 3: %.2f\n", iob.getTarget3() != null ? iob.getTarget3() : 0.0));
            message.append("\n");
            message.append(String.format("📊 Risk:Reward: %.2f\n", iob.getRiskRewardRatio() != null ? iob.getRiskRewardRatio() : 0.0));
            message.append(String.format("💪 Confidence: *%.1f%%*\n", iob.getSignalConfidence()));
            message.append(String.format("✅ Has FVG: %s\n", Boolean.TRUE.equals(iob.getHasFvg()) ? "Yes" : "No"));

            // Create trade data map for the alert
            Map<String, Object> tradeData = new HashMap<>();
            tradeData.put("iobId", iob.getId());
            tradeData.put("obType", iob.getObType());
            tradeData.put("zoneHigh", iob.getZoneHigh());
            tradeData.put("zoneLow", iob.getZoneLow());
            tradeData.put("confidence", iob.getSignalConfidence());

            telegramNotificationService.sendTradeAlert("📊 IOB SIGNAL ALERT", message.toString(), tradeData);
            logger.info("Sent Telegram alert for IOB: {} {}", iob.getObType(), iob.getInstrumentName());

        } catch (Exception e) {
            logger.error("Failed to send IOB Telegram alert: {}", e.getMessage(), e);
        }
    }

    /**
     * Send Telegram alert for IOB mitigation
     */
    private void sendMitigationAlert(InternalOrderBlock iob, Double mitigationPrice) {
        if (telegramNotificationService == null) {
            return;
        }

        try {
            String alertType = iob.getObType() != null && iob.getObType().contains("BULLISH") ? "🟢 BULLISH" : "🔴 BEARISH";

            String message = String.format("*%s IOB Mitigated*\n📈 Instrument: *%s*\n💰 Mitigation Price: %.2f\n🎯 Zone: %.2f - %.2f",
                    alertType, iob.getInstrumentName(), mitigationPrice, iob.getZoneLow(), iob.getZoneHigh());

            telegramNotificationService.sendSystemAlert("⚠️ IOB MITIGATION ALERT", message);
            logger.info("Sent mitigation alert for IOB: {} at {}", iob.getId(), mitigationPrice);

        } catch (Exception e) {
            logger.error("Failed to send mitigation Telegram alert: {}", e.getMessage(), e);
        }
    }

    /**
     * Clear alerted IOBs cache - Called at end of day or manually
     */
    public void clearAlertedIOBsCache() {
        alertedIOBs.clear();
        logger.info("Cleared alerted IOBs cache");
    }

    /**
     * End of Day cleanup - Runs at 3:35 PM
     */
    @Scheduled(cron = "0 35 15 * * MON-FRI", zone = IST_ZONE)
    public void runEndOfDayCleanup() {
        try {
            logger.info("Running IOB end of day cleanup");
            clearAlertedIOBsCache();
            lastPrices.clear();
            logger.info("IOB end of day cleanup completed");
        } catch (Exception e) {
            logger.error("Error in IOB end of day cleanup: {}", e.getMessage(), e);
        }
    }
}
