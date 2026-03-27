package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.entity.InternalOrderBlock;
import com.trading.kalyani.KTManager.entity.VolumeProfile;
import com.trading.kalyani.KTManager.model.HistoricalDataRequest;
import com.trading.kalyani.KTManager.model.HistoricalDataResponse;
import com.trading.kalyani.KTManager.model.HistoricalDataResponse.HistoricalCandle;
import com.trading.kalyani.KTManager.repository.VolumeProfileRepository;
import com.trading.kalyani.KTManager.service.InstrumentService;
import com.trading.kalyani.KTManager.service.VolumeProfileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;

/**
 * Implementation of VolumeProfileService for volume-based analysis.
 */
@Service
public class VolumeProfileServiceImpl implements VolumeProfileService {

    private static final Logger logger = LoggerFactory.getLogger(VolumeProfileServiceImpl.class);

    @Autowired
    private VolumeProfileRepository volumeProfileRepository;

    @Autowired
    private InstrumentService instrumentService;

    private static final double INSTITUTIONAL_VOLUME_RATIO = 1.5;  // >1.5x average = institutional
    private static final double RETAIL_VOLUME_RATIO = 0.5;          // <0.5x average = retail
    private static final double DISPLACEMENT_CONFIRM_RATIO = 1.2;   // >1.2x average confirms
    private static final double POC_ALIGNMENT_THRESHOLD = 0.005;    // 0.5% tolerance

    // ==================== Core Analysis ====================

    @Override
    public VolumeProfile analyzeVolumeProfile(Long instrumentToken, String timeframe) {
        logger.info("Analyzing volume profile for token: {}, timeframe: {}", instrumentToken, timeframe);

        try {
            List<HistoricalCandle> candles = fetchHistoricalCandles(instrumentToken, timeframe);
            if (candles == null || candles.size() < 20) {
                logger.warn("Insufficient candles for volume profile analysis");
                return null;
            }
            return analyzeVolumeProfile(instrumentToken, timeframe, candles);
        } catch (Exception e) {
            logger.error("Error analyzing volume profile: {}", e.getMessage(), e);
            return null;
        }
    }

    @Override
    public VolumeProfile analyzeVolumeProfile(Long instrumentToken, String timeframe, List<HistoricalCandle> candles) {
        if (candles == null || candles.size() < 20) {
            return null;
        }

        VolumeProfile profile = new VolumeProfile();
        profile.setInstrumentToken(instrumentToken);
        profile.setInstrumentName(getInstrumentName(instrumentToken));
        profile.setTimeframe(timeframe);
        profile.setAnalysisTimestamp(LocalDateTime.now());
        profile.setCandlesAnalyzed(candles.size());

        Double currentPrice = candles.get(candles.size() - 1).getClose();
        profile.setCurrentPrice(currentPrice);

        // Set period range
        profile.setPeriodStart(parseTimestamp(candles.get(0).getTimestamp()));
        profile.setPeriodEnd(parseTimestamp(candles.get(candles.size() - 1).getTimestamp()));

        // Calculate price range
        double periodHigh = candles.stream().mapToDouble(HistoricalCandle::getHigh).max().orElse(0);
        double periodLow = candles.stream().mapToDouble(HistoricalCandle::getLow).min().orElse(0);
        profile.setPeriodHigh(periodHigh);
        profile.setPeriodLow(periodLow);

        // Calculate volume statistics
        calculateVolumeStatistics(profile, candles);

        // Calculate POC and Value Area
        calculateVolumeProfileLevels(profile, candles);

        // Calculate volume delta
        calculateVolumeDeltaAnalysis(profile, candles);

        // Determine volume bias
        determineVolumeBias(profile);

        // Save to database
        volumeProfileRepository.save(profile);
        logger.info("Saved volume profile analysis for {}: POC={}, Delta={}, Bias={}",
                profile.getInstrumentName(), profile.getPocLevel(),
                profile.getDeltaDirection(), profile.getVolumeBias());

        return profile;
    }

    @Override
    public VolumeProfile analyzeVolumeForIOB(InternalOrderBlock iob, List<HistoricalCandle> candles) {
        if (iob == null || candles == null || candles.isEmpty()) {
            return null;
        }

        VolumeProfile profile = analyzeVolumeProfile(iob.getInstrumentToken(), iob.getTimeframe(), candles);
        if (profile == null) {
            return null;
        }

        profile.setIobId(iob.getId());

        // Analyze IOB-specific volume
        analyzeIOBSpecificVolume(profile, iob, candles);

        // Check POC alignment
        checkPOCAlignment(profile, iob);

        // Calculate confluence score
        calculateConfluenceScore(profile, iob, candles);

        volumeProfileRepository.save(profile);
        return profile;
    }

    @Override
    public VolumeProfile getLatestAnalysis(Long instrumentToken, String timeframe) {
        return volumeProfileRepository
                .findTopByInstrumentTokenAndTimeframeOrderByAnalysisTimestampDesc(instrumentToken, timeframe)
                .orElse(null);
    }

    @Override
    public Map<String, VolumeProfile> analyzeAllIndices() {
        Map<String, VolumeProfile> results = new HashMap<>();

        VolumeProfile nifty = analyzeVolumeProfile(NIFTY_INSTRUMENT_TOKEN, "5min");
        if (nifty != null) results.put("NIFTY", nifty);


        return results;
    }

    // ==================== Volume Profile Levels ====================

    @Override
    public Double calculatePOC(List<HistoricalCandle> candles) {
        if (candles == null || candles.isEmpty()) return null;

        // Create volume profile bins
        Map<Double, Long> volumeByPrice = buildVolumeProfile(candles, 50);

        // Find POC (price level with highest volume)
        return volumeByPrice.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(null);
    }

    @Override
    public Map<String, Double> calculateValueArea(List<HistoricalCandle> candles) {
        Map<String, Double> valueArea = new HashMap<>();

        if (candles == null || candles.isEmpty()) {
            return valueArea;
        }

        Map<Double, Long> volumeByPrice = buildVolumeProfile(candles, 50);
        long totalVolume = volumeByPrice.values().stream().mapToLong(Long::longValue).sum();
        long targetVolume = (long) (totalVolume * 0.7); // 70% of volume

        // Sort by volume descending
        List<Map.Entry<Double, Long>> sorted = volumeByPrice.entrySet().stream()
                .sorted(Map.Entry.<Double, Long>comparingByValue().reversed())
                .toList();

        long accumulatedVolume = 0;
        double vah = Double.MIN_VALUE;
        double val = Double.MAX_VALUE;

        for (Map.Entry<Double, Long> entry : sorted) {
            accumulatedVolume += entry.getValue();
            vah = Math.max(vah, entry.getKey());
            val = Math.min(val, entry.getKey());

            if (accumulatedVolume >= targetVolume) break;
        }

        valueArea.put("valueAreaHigh", vah);
        valueArea.put("valueAreaLow", val);
        valueArea.put("poc", calculatePOC(candles));

        return valueArea;
    }

    @Override
    public List<Double> getHighVolumeNodes(List<HistoricalCandle> candles, int count) {
        if (candles == null || candles.isEmpty()) return Collections.emptyList();

        Map<Double, Long> volumeByPrice = buildVolumeProfile(candles, 50);
        long avgVolume = volumeByPrice.values().stream().mapToLong(Long::longValue).sum() / volumeByPrice.size();

        return volumeByPrice.entrySet().stream()
                .filter(e -> e.getValue() > avgVolume * 1.3)
                .sorted(Map.Entry.<Double, Long>comparingByValue().reversed())
                .limit(count)
                .map(Map.Entry::getKey)
                .toList();
    }

    @Override
    public List<Double> getLowVolumeNodes(List<HistoricalCandle> candles, int count) {
        if (candles == null || candles.isEmpty()) return Collections.emptyList();

        Map<Double, Long> volumeByPrice = buildVolumeProfile(candles, 50);
        long avgVolume = volumeByPrice.values().stream().mapToLong(Long::longValue).sum() / volumeByPrice.size();

        return volumeByPrice.entrySet().stream()
                .filter(e -> e.getValue() < avgVolume * 0.5)
                .sorted(Map.Entry.comparingByValue())
                .limit(count)
                .map(Map.Entry::getKey)
                .toList();
    }

    // ==================== IOB Volume Analysis ====================

    @Override
    public Map<String, Object> analyzeIOBCandleVolume(InternalOrderBlock iob, List<HistoricalCandle> candles) {
        Map<String, Object> analysis = new HashMap<>();

        if (iob == null || candles == null || candles.isEmpty()) {
            return analysis;
        }

        long avgVolume = calculateAverageVolume(candles);
        Long iobVolume = findIOBCandleVolume(iob, candles);

        if (iobVolume != null && avgVolume > 0) {
            double ratio = (double) iobVolume / avgVolume;
            analysis.put("iobVolume", iobVolume);
            analysis.put("averageVolume", avgVolume);
            analysis.put("volumeRatio", ratio);
            analysis.put("volumeType", classifyVolumeType(ratio));
            analysis.put("isInstitutional", ratio >= INSTITUTIONAL_VOLUME_RATIO);
        }

        return analysis;
    }

    @Override
    public boolean hasInstitutionalVolume(InternalOrderBlock iob, List<HistoricalCandle> candles) {
        Double ratio = calculateIOBVolumeRatio(iob, candles);
        return ratio != null && ratio >= INSTITUTIONAL_VOLUME_RATIO;
    }

    @Override
    public String getIOBVolumeType(InternalOrderBlock iob, List<HistoricalCandle> candles) {
        Double ratio = calculateIOBVolumeRatio(iob, candles);
        return classifyVolumeType(ratio);
    }

    @Override
    public Double calculateIOBVolumeRatio(InternalOrderBlock iob, List<HistoricalCandle> candles) {
        if (iob == null || candles == null || candles.isEmpty()) {
            return null;
        }

        long avgVolume = calculateAverageVolume(candles);
        Long iobVolume = findIOBCandleVolume(iob, candles);

        if (iobVolume != null && avgVolume > 0) {
            return (double) iobVolume / avgVolume;
        }
        return null;
    }

    // ==================== Displacement Analysis ====================

    @Override
    public Map<String, Object> analyzeDisplacementVolume(InternalOrderBlock iob, List<HistoricalCandle> candles) {
        Map<String, Object> analysis = new HashMap<>();

        if (iob == null || candles == null || candles.isEmpty()) {
            return analysis;
        }

        long avgVolume = calculateAverageVolume(candles);
        Long displacementVolume = findDisplacementVolume(iob, candles);

        if (displacementVolume != null && avgVolume > 0) {
            double ratio = (double) displacementVolume / avgVolume;
            analysis.put("displacementVolume", displacementVolume);
            analysis.put("averageVolume", avgVolume);
            analysis.put("volumeRatio", ratio);
            analysis.put("confirmed", ratio >= DISPLACEMENT_CONFIRM_RATIO);
        }

        return analysis;
    }

    @Override
    public boolean isDisplacementConfirmed(InternalOrderBlock iob, List<HistoricalCandle> candles) {
        Double ratio = getDisplacementVolumeRatio(iob, candles);
        return ratio != null && ratio >= DISPLACEMENT_CONFIRM_RATIO;
    }

    @Override
    public Double getDisplacementVolumeRatio(InternalOrderBlock iob, List<HistoricalCandle> candles) {
        if (iob == null || candles == null || candles.isEmpty()) {
            return null;
        }

        long avgVolume = calculateAverageVolume(candles);
        Long displacementVolume = findDisplacementVolume(iob, candles);

        if (displacementVolume != null && avgVolume > 0) {
            return (double) displacementVolume / avgVolume;
        }
        return null;
    }

    // ==================== Volume Delta Analysis ====================

    @Override
    public Map<String, Object> calculateVolumeDelta(List<HistoricalCandle> candles) {
        Map<String, Object> delta = new HashMap<>();

        if (candles == null || candles.isEmpty()) {
            return delta;
        }

        long buyingVolume = 0;
        long sellingVolume = 0;

        for (HistoricalCandle candle : candles) {
            if (candle.getClose() >= candle.getOpen()) {
                buyingVolume += candle.getVolume();
            } else {
                sellingVolume += candle.getVolume();
            }
        }

        long volumeDelta = buyingVolume - sellingVolume;

        delta.put("buyingVolume", buyingVolume);
        delta.put("sellingVolume", sellingVolume);
        delta.put("volumeDelta", volumeDelta);
        delta.put("deltaPercent", (buyingVolume + sellingVolume) > 0 ?
                ((double) volumeDelta / (buyingVolume + sellingVolume)) * 100 : 0);

        return delta;
    }

    @Override
    public Long getCumulativeVolumeDelta(List<HistoricalCandle> candles) {
        if (candles == null || candles.isEmpty()) return 0L;

        long cumulativeDelta = 0;
        for (HistoricalCandle candle : candles) {
            if (candle.getClose() >= candle.getOpen()) {
                cumulativeDelta += candle.getVolume();
            } else {
                cumulativeDelta -= candle.getVolume();
            }
        }
        return cumulativeDelta;
    }

    @Override
    public String getDeltaDirection(List<HistoricalCandle> candles) {
        Long delta = getCumulativeVolumeDelta(candles);
        long totalVolume = candles.stream().mapToLong(HistoricalCandle::getVolume).sum();
        double deltaPercent = totalVolume > 0 ? ((double) Math.abs(delta) / totalVolume) * 100 : 0;

        if (deltaPercent < 10) return "NEUTRAL";
        return delta > 0 ? "BULLISH" : "BEARISH";
    }

    @Override
    public String getDeltaStrength(List<HistoricalCandle> candles) {
        Long delta = getCumulativeVolumeDelta(candles);
        long totalVolume = candles.stream().mapToLong(HistoricalCandle::getVolume).sum();
        double deltaPercent = totalVolume > 0 ? ((double) Math.abs(delta) / totalVolume) * 100 : 0;

        if (deltaPercent >= 30) return "STRONG";
        if (deltaPercent >= 15) return "MODERATE";
        return "WEAK";
    }

    // ==================== POC Alignment ====================

    @Override
    public boolean isPOCAlignedWithIOB(InternalOrderBlock iob, List<HistoricalCandle> candles) {
        Double distance = getPOCDistanceFromIOB(iob, candles);
        return distance != null && distance <= POC_ALIGNMENT_THRESHOLD;
    }

    @Override
    public Double getPOCDistanceFromIOB(InternalOrderBlock iob, List<HistoricalCandle> candles) {
        if (iob == null || iob.getZoneMidpoint() == null) return null;

        Double poc = calculatePOC(candles);
        if (poc == null) return null;

        double zoneMid = iob.getZoneMidpoint();
        return Math.abs(poc - zoneMid) / zoneMid;
    }

    // ==================== Volume Confluence ====================

    @Override
    public Double calculateVolumeConfluenceScore(InternalOrderBlock iob, List<HistoricalCandle> candles) {
        if (iob == null || candles == null || candles.isEmpty()) return 0.0;

        double score = 50.0; // Base score

        // IOB volume factor (+20 for institutional)
        Double iobRatio = calculateIOBVolumeRatio(iob, candles);
        if (iobRatio != null) {
            if (iobRatio >= INSTITUTIONAL_VOLUME_RATIO) {
                score += 20;
            } else if (iobRatio >= 1.0) {
                score += 10;
            } else if (iobRatio < RETAIL_VOLUME_RATIO) {
                score -= 15;
            }
        }

        // Displacement confirmation (+15)
        if (isDisplacementConfirmed(iob, candles)) {
            score += 15;
        }

        // POC alignment (+10)
        if (isPOCAlignedWithIOB(iob, candles)) {
            score += 10;
        }

        // Delta alignment (+10)
        String deltaDir = getDeltaDirection(candles);
        if (("BULLISH".equals(deltaDir) && "LONG".equals(iob.getTradeDirection())) ||
            ("BEARISH".equals(deltaDir) && "SHORT".equals(iob.getTradeDirection()))) {
            score += 10;
        }

        // Delta strength bonus (+5)
        if ("STRONG".equals(getDeltaStrength(candles))) {
            score += 5;
        }

        return Math.min(100, Math.max(0, score));
    }

    @Override
    public Map<String, Object> getVolumeConfluenceDetails(InternalOrderBlock iob, List<HistoricalCandle> candles) {
        Map<String, Object> details = new HashMap<>();

        details.put("iobVolumeRatio", calculateIOBVolumeRatio(iob, candles));
        details.put("iobVolumeType", getIOBVolumeType(iob, candles));
        details.put("hasInstitutionalVolume", hasInstitutionalVolume(iob, candles));
        details.put("displacementConfirmed", isDisplacementConfirmed(iob, candles));
        details.put("displacementRatio", getDisplacementVolumeRatio(iob, candles));
        details.put("pocAligned", isPOCAlignedWithIOB(iob, candles));
        details.put("pocDistance", getPOCDistanceFromIOB(iob, candles));
        details.put("deltaDirection", getDeltaDirection(candles));
        details.put("deltaStrength", getDeltaStrength(candles));
        details.put("confluenceScore", calculateVolumeConfluenceScore(iob, candles));

        return details;
    }

    @Override
    public String getVolumeBias(List<HistoricalCandle> candles) {
        String deltaDir = getDeltaDirection(candles);
        String deltaStrength = getDeltaStrength(candles);

        if ("NEUTRAL".equals(deltaDir) || "WEAK".equals(deltaStrength)) {
            return "NEUTRAL";
        }
        return deltaDir;
    }

    // ==================== Volume Statistics ====================

    @Override
    public Map<String, Object> getVolumeStatistics(List<HistoricalCandle> candles) {
        Map<String, Object> stats = new HashMap<>();

        if (candles == null || candles.isEmpty()) return stats;

        long totalVolume = candles.stream().mapToLong(HistoricalCandle::getVolume).sum();
        long avgVolume = totalVolume / candles.size();
        long maxVolume = candles.stream().mapToLong(HistoricalCandle::getVolume).max().orElse(0);
        long minVolume = candles.stream().mapToLong(HistoricalCandle::getVolume).min().orElse(0);

        stats.put("totalVolume", totalVolume);
        stats.put("averageVolume", avgVolume);
        stats.put("maxVolume", maxVolume);
        stats.put("minVolume", minVolume);
        stats.put("stdDev", calculateVolumeStdDev(candles));
        stats.put("candleCount", candles.size());

        return stats;
    }

    @Override
    public Long calculateAverageVolume(List<HistoricalCandle> candles) {
        if (candles == null || candles.isEmpty()) return 0L;
        return candles.stream().mapToLong(HistoricalCandle::getVolume).sum() / candles.size();
    }

    @Override
    public Double calculateVolumeStdDev(List<HistoricalCandle> candles) {
        if (candles == null || candles.size() < 2) return 0.0;

        long avg = calculateAverageVolume(candles);
        double sumSquares = candles.stream()
                .mapToDouble(c -> Math.pow(c.getVolume() - avg, 2))
                .sum();
        return Math.sqrt(sumSquares / (candles.size() - 1));
    }

    @Override
    public List<Map<String, Object>> identifyVolumeSpikes(List<HistoricalCandle> candles) {
        List<Map<String, Object>> spikes = new ArrayList<>();

        if (candles == null || candles.isEmpty()) return spikes;

        long avgVolume = calculateAverageVolume(candles);

        for (int i = 0; i < candles.size(); i++) {
            HistoricalCandle candle = candles.get(i);
            if (candle.getVolume() > avgVolume * 2) {
                Map<String, Object> spike = new HashMap<>();
                spike.put("index", i);
                spike.put("timestamp", candle.getTimestamp());
                spike.put("volume", candle.getVolume());
                spike.put("ratio", (double) candle.getVolume() / avgVolume);
                spike.put("priceDirection", candle.getClose() >= candle.getOpen() ? "UP" : "DOWN");
                spikes.add(spike);
            }
        }

        return spikes;
    }

    // ==================== Dashboard ====================

    @Override
    public Map<String, Object> getDashboard(Long instrumentToken) {
        Map<String, Object> dashboard = new HashMap<>();

        VolumeProfile profile = analyzeVolumeProfile(instrumentToken, "5min");

        dashboard.put("instrumentToken", instrumentToken);
        dashboard.put("instrumentName", getInstrumentName(instrumentToken));
        dashboard.put("analysisTime", LocalDateTime.now());

        if (profile != null) {
            dashboard.put("poc", profile.getPocLevel());
            dashboard.put("valueAreaHigh", profile.getValueAreaHigh());
            dashboard.put("valueAreaLow", profile.getValueAreaLow());
            dashboard.put("deltaDirection", profile.getDeltaDirection());
            dashboard.put("deltaStrength", profile.getDeltaStrength());
            dashboard.put("volumeBias", profile.getVolumeBias());
            dashboard.put("averageVolume", profile.getAverageVolume());
            dashboard.put("totalVolume", profile.getTotalVolume());
        }

        return dashboard;
    }

    @Override
    public Map<String, Map<String, Object>> getMultiInstrumentSummary(List<Long> instrumentTokens) {
        Map<String, Map<String, Object>> summaries = new HashMap<>();

        for (Long token : instrumentTokens) {
            summaries.put(getInstrumentName(token), getDashboard(token));
        }

        return summaries;
    }

    // ==================== Helper Methods ====================

    private void calculateVolumeStatistics(VolumeProfile profile, List<HistoricalCandle> candles) {
        long totalVolume = candles.stream().mapToLong(HistoricalCandle::getVolume).sum();
        long avgVolume = totalVolume / candles.size();
        long maxVolume = candles.stream().mapToLong(HistoricalCandle::getVolume).max().orElse(0);
        long minVolume = candles.stream().mapToLong(HistoricalCandle::getVolume).min().orElse(0);

        profile.setTotalVolume(totalVolume);
        profile.setAverageVolume(avgVolume);
        profile.setMaxVolume(maxVolume);
        profile.setMinVolume(minVolume);
        profile.setVolumeStdDev(calculateVolumeStdDev(candles));
    }

    private void calculateVolumeProfileLevels(VolumeProfile profile, List<HistoricalCandle> candles) {
        Map<String, Double> valueArea = calculateValueArea(candles);
        profile.setPocLevel(valueArea.get("poc"));
        profile.setValueAreaHigh(valueArea.get("valueAreaHigh"));
        profile.setValueAreaLow(valueArea.get("valueAreaLow"));

        List<Double> hvns = getHighVolumeNodes(candles, 2);
        if (hvns.size() >= 1) profile.setHvn1(hvns.get(0));
        if (hvns.size() >= 2) profile.setHvn2(hvns.get(1));

        List<Double> lvns = getLowVolumeNodes(candles, 2);
        if (lvns.size() >= 1) profile.setLvn1(lvns.get(0));
        if (lvns.size() >= 2) profile.setLvn2(lvns.get(1));
    }

    private void calculateVolumeDeltaAnalysis(VolumeProfile profile, List<HistoricalCandle> candles) {
        Map<String, Object> delta = calculateVolumeDelta(candles);

        profile.setBuyingVolume((Long) delta.get("buyingVolume"));
        profile.setSellingVolume((Long) delta.get("sellingVolume"));
        profile.setVolumeDelta((Long) delta.get("volumeDelta"));
        profile.setCumulativeDelta(getCumulativeVolumeDelta(candles));
        profile.setDeltaDirection(getDeltaDirection(candles));
        profile.setDeltaStrength(getDeltaStrength(candles));
    }

    private void determineVolumeBias(VolumeProfile profile) {
        profile.setVolumeBias(getVolumeBias(
                new ArrayList<>())); // Use calculated delta

        // Use already calculated values
        String deltaDir = profile.getDeltaDirection();
        String deltaStrength = profile.getDeltaStrength();

        if ("NEUTRAL".equals(deltaDir) || "WEAK".equals(deltaStrength)) {
            profile.setVolumeBias("NEUTRAL");
        } else {
            profile.setVolumeBias(deltaDir);
        }
    }

    private void analyzeIOBSpecificVolume(VolumeProfile profile, InternalOrderBlock iob, List<HistoricalCandle> candles) {
        Long iobVolume = findIOBCandleVolume(iob, candles);
        if (iobVolume != null) {
            profile.setIobCandleVolume(iobVolume);
            profile.setIobVolumeRatio(calculateIOBVolumeRatio(iob, candles));
            profile.setIobVolumeType(getIOBVolumeType(iob, candles));
        }

        Long displacementVolume = findDisplacementVolume(iob, candles);
        if (displacementVolume != null) {
            profile.setDisplacementVolume(displacementVolume);
            profile.setDisplacementVolumeRatio(getDisplacementVolumeRatio(iob, candles));
            profile.setDisplacementConfirmed(isDisplacementConfirmed(iob, candles));
        }
    }

    private void checkPOCAlignment(VolumeProfile profile, InternalOrderBlock iob) {
        profile.setPocIobAligned(isPOCAlignedWithIOB(iob, null)); // POC already calculated

        if (profile.getPocLevel() != null && iob.getZoneMidpoint() != null) {
            double distance = Math.abs(profile.getPocLevel() - iob.getZoneMidpoint()) / iob.getZoneMidpoint();
            profile.setPocIobAligned(distance <= POC_ALIGNMENT_THRESHOLD);
        }
    }

    private void calculateConfluenceScore(VolumeProfile profile, InternalOrderBlock iob, List<HistoricalCandle> candles) {
        profile.setVolumeConfluenceScore(calculateVolumeConfluenceScore(iob, candles));
    }

    private Map<Double, Long> buildVolumeProfile(List<HistoricalCandle> candles, int bins) {
        double high = candles.stream().mapToDouble(HistoricalCandle::getHigh).max().orElse(0);
        double low = candles.stream().mapToDouble(HistoricalCandle::getLow).min().orElse(0);
        double binSize = (high - low) / bins;

        Map<Double, Long> volumeByPrice = new TreeMap<>();

        for (HistoricalCandle candle : candles) {
            double midPrice = (candle.getHigh() + candle.getLow()) / 2;
            double bin = Math.floor(midPrice / binSize) * binSize + binSize / 2;
            volumeByPrice.merge(bin, candle.getVolume(), Long::sum);
        }

        return volumeByPrice;
    }

    private Long findIOBCandleVolume(InternalOrderBlock iob, List<HistoricalCandle> candles) {
        if (iob.getObCandleTime() == null) return null;

        for (HistoricalCandle candle : candles) {
            LocalDateTime candleTime = parseTimestamp(candle.getTimestamp());
            if (candleTime != null && candleTime.equals(iob.getObCandleTime())) {
                return candle.getVolume();
            }
        }
        return null;
    }

    private Long findDisplacementVolume(InternalOrderBlock iob, List<HistoricalCandle> candles) {
        if (iob.getObCandleTime() == null) return null;

        // Find the candle after IOB candle (displacement candle)
        for (int i = 0; i < candles.size() - 1; i++) {
            LocalDateTime candleTime = parseTimestamp(candles.get(i).getTimestamp());
            if (candleTime != null && candleTime.equals(iob.getObCandleTime())) {
                return candles.get(i + 1).getVolume();
            }
        }
        return null;
    }

    private String classifyVolumeType(Double ratio) {
        if (ratio == null) return "UNKNOWN";
        if (ratio >= INSTITUTIONAL_VOLUME_RATIO) return "INSTITUTIONAL";
        if (ratio <= RETAIL_VOLUME_RATIO) return "RETAIL";
        return "NORMAL";
    }

    private List<HistoricalCandle> fetchHistoricalCandles(Long instrumentToken, String timeframe) {
        try {
            LocalDate today = LocalDate.now();
            LocalDateTime fromDate = today.minusDays(5).atStartOfDay();
            LocalDateTime toDate = today.atTime(23, 59, 59);

            HistoricalDataRequest request = new HistoricalDataRequest();
            request.setInstrumentToken(String.valueOf(instrumentToken));
            request.setInterval(timeframe.replace("min", "minute"));
            request.setFromDate(fromDate);
            request.setToDate(toDate);

            HistoricalDataResponse response = instrumentService.getHistoricalData(request);
            return response != null && response.isSuccess() ? response.getCandles() : null;
        } catch (Exception e) {
            logger.error("Error fetching candles: {}", e.getMessage());
            return null;
        }
    }

    private String getInstrumentName(Long token) {
        if (NIFTY_INSTRUMENT_TOKEN.equals(token)) return "NIFTY";
        return "UNKNOWN";
    }

    private static final DateTimeFormatter KITE_TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ");

    private LocalDateTime parseTimestamp(String timestamp) {
        if (timestamp == null || timestamp.isBlank()) return null;
        try {
            return java.time.ZonedDateTime.parse(timestamp, KITE_TIMESTAMP_FORMATTER).toLocalDateTime();
        } catch (Exception e) {
            try {
                return java.time.ZonedDateTime.parse(timestamp).toLocalDateTime();
            } catch (Exception e2) {
                try {
                    return LocalDateTime.parse(timestamp.replace("Z", ""), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                } catch (Exception e3) {
                    return null;
                }
            }
        }
    }
}
