package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.entity.AppJobConfig;
import com.trading.kalyani.KTManager.entity.InstrumentEntity;

import com.trading.kalyani.KTManager.entity.OISnapshotEntity;
import com.trading.kalyani.KTManager.model.HistoricalDataRequest;
import com.trading.kalyani.KTManager.model.HistoricalDataResponse;
import com.trading.kalyani.KTManager.model.PreviousDayHighLowResponse;
import com.trading.kalyani.KTManager.repository.AppJobConfigRepository;
import com.trading.kalyani.KTManager.repository.InstrumentRepository;
import com.trading.kalyani.KTManager.repository.OiSnapshotRepository;
import com.trading.kalyani.KTManager.service.InstrumentService;
import com.trading.kalyani.KTManager.utilities.DateUtilities;
import com.zerodhatech.kiteconnect.KiteConnect;
import com.zerodhatech.kiteconnect.kitehttp.exceptions.KiteException;
import com.zerodhatech.models.HistoricalData;
import com.zerodhatech.models.Instrument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;


@Service
public class InstrumentServiceImpl implements InstrumentService {


    @Autowired
    InstrumentRepository instrumentRepository;


    @Autowired
    OiSnapshotRepository oiSnapshotRepository;

    @Autowired
    AppJobConfigRepository appJobConfigRepository;

    @Autowired
    KiteConnect kiteConnect;




    private static final Logger logger = LoggerFactory.getLogger(InstrumentServiceImpl.class);

    @Override

    public boolean saveInstrumentsData(List<Instrument> instruments) {
        try {
            List<InstrumentEntity> instrumentEntityList = new ArrayList<>();
            for (Instrument instrument : instruments) {
                InstrumentEntity instrumentEntity = new InstrumentEntity();
                instrumentEntity.setInstrument(instrument);
                instrumentEntityList.add(instrumentEntity);
            }


            // Save to Redis
          /*  redisTemplate.opsForValue().set(INSTRUMENT_CACHE_KEY, instrumentEntityList, 8, TimeUnit.HOURS);
            logger.info("Instruments data saved to Redis with key: {}", INSTRUMENT_CACHE_KEY); */

            logger.info("Truncate and Loading latest Instruments data with size: {}", instrumentEntityList.size());
            instrumentRepository.deleteAll();

            // Batch processing
            int batchSize = 1000;
            for (int i = 0; i < instrumentEntityList.size(); i += batchSize) {
                int end = Math.min(i + batchSize, instrumentEntityList.size());
                List<InstrumentEntity> batchList = instrumentEntityList.subList(i, end);
                instrumentRepository.saveAll(batchList);
                logger.info("Batch saved: {}/{}", (i / batchSize + 1), (instrumentEntityList.size() / batchSize + 1));
            }
            return true;
        } catch (Exception ex) {
            logger.error("Error saving instruments data: {}", ex.getMessage(), ex);
        }
        return false;
    }

    @Override

    public ArrayList<InstrumentEntity> getInstrumentData() {

        // Check Redis cache first
      /*  List<InstrumentEntity> cachedData = (List<InstrumentEntity>) redisTemplate.opsForValue().get(INSTRUMENT_CACHE_KEY);
        if (cachedData != null) {
            return new ArrayList<>(cachedData);
         } */

        // If not in cache, fetch from database and update cache
        ArrayList<InstrumentEntity> instruments = instrumentRepository.findAllInstruments();
    //    redisTemplate.opsForValue().set(INSTRUMENT_CACHE_KEY, instruments, 8, TimeUnit.HOURS);
        return instruments;
    }

    @Override
    @Cacheable(value = "nfoInstrumentsCurrentWeekCache", unless = "#result == null || #result.isEmpty()")
    public ArrayList<InstrumentEntity> findNFOInstrumentsForCurrentWeek() {
        // Check Redis cache first
     /*   List<InstrumentEntity> cachedData = (List<InstrumentEntity>) redisTemplate.opsForValue().get(NFO_CURRENT_WEEK_INSTRUMENT_CACHE_KEY);
        if (cachedData != null) {
            logger.info("NFO Current Week Instruments data fetched from Redis cache");
            return new ArrayList<>(cachedData);
        } */

        // If not in cache, fetch from database and update cache
        ArrayList<InstrumentEntity> instruments = instrumentRepository.findNFOInstrumentsForCurrentWeek();
   //     redisTemplate.opsForValue().set(NFO_CURRENT_WEEK_INSTRUMENT_CACHE_KEY, instruments, 8, TimeUnit.HOURS);
   //     logger.info("NFO Current Week Instruments data fetched from database and saved to Redis cache");
        return instruments;
    }

    @Override
    public Iterable<OISnapshotEntity> findAllSnapShotTokens() {

        // Check Redis cache first
     /*   Iterable<OISnapshotEntity> cachedData = (Iterable<OISnapshotEntity>) redisTemplate.opsForValue().get(NFO_OI_SNAPSHOT_CACHE_KEY);
        if (cachedData != null) {
            logger.info("NFO Current Week Snapshot Instruments data fetched from Redis cache");
            return cachedData;
        } */

        // If not in cache, fetch from database and update cache
        Iterable<OISnapshotEntity> instruments =oiSnapshotRepository.findAll();
    //    redisTemplate.opsForValue().set(NFO_OI_SNAPSHOT_CACHE_KEY, instruments, 8, TimeUnit.HOURS);
    //    logger.info("NFO Current Week Snapshot Instruments data fetched from database and saved to Redis cache");
        return instruments;

    }

    @Override
    public void clearAllCaches() {
        try {
           /* redisTemplate.delete(INSTRUMENT_CACHE_KEY);
            redisTemplate.delete(NFO_CURRENT_WEEK_INSTRUMENT_CACHE_KEY);
            redisTemplate.delete(NFO_OI_SNAPSHOT_CACHE_KEY);*/
            clearNFOCurrentWeekCache();
            clearNFOCache();
            logger.info("All caches cleared successfully.");
        } catch (Exception ex) {
            logger.error("Error while clearing caches: {}", ex.getMessage());
        }
    }

    @Override
    @Cacheable(value = "nfoInstrumentsCache", unless = "#result == null || #result.isEmpty()")
    public ArrayList<InstrumentEntity> getNFOInstrumentsForCurrentAndFollowingWeek() {

        return instrumentRepository.findNFOInstrumentsForCurrentAndFollowingWeek();
    }

    @CacheEvict(value = "nfoInstrumentsCache", allEntries = true)
    public void clearNFOCache() {
        // Clears all entries in the "users" cache
        logger.info("nfoInstrumentsCache cleared successfully.");
    }

    @CacheEvict(value = "nfoInstrumentsCurrentWeekCache", allEntries = true)
    public void clearNFOCurrentWeekCache() {
        // Clears all entries in the "users" cache
        logger.info("nfoInstrumentsCurrentWeekCache cleared successfully.");
    }

    @Override
    public List<InstrumentEntity> searchInstrumentsByTradingsymbol(String partialText) {
        ArrayList<InstrumentEntity> instruments = getNFOInstrumentsForCurrentAndFollowingWeek();
        return instruments.stream()
                .filter(instrument -> instrument.getInstrument().getTradingsymbol().toLowerCase().contains(partialText.toLowerCase()))
                .toList();
    }

    @Override
    public List<Instrument> getRequiredInstruments(List<Instrument> instruments) {
        List<Instrument> requiredInstruments = new ArrayList<>();
        for (Instrument instrument : instruments) {
            if (instrument.getSegment().equals("NFO-OPT") && (instrument.getName().equals("NIFTY"))){
                requiredInstruments.add(instrument);
            }

            //  if (instrument.getSegment().equals("BFO-OPT") && (instrument.getName().equals("SENSEX"))){
            //     requiredInstruments.add(instrument);
            // }



        }



        return requiredInstruments;
    }

    @Override
    public ArrayList<InstrumentEntity> getInstrumentsFromAppJobConfigNum(Integer appJobConfigNum) {

        ArrayList<InstrumentEntity> resultInstrumentList = new ArrayList<>();

        AppJobConfig appJobConfig = appJobConfigRepository.findById(appJobConfigNum).orElse(null);

        try {
            if (appJobConfig != null) {
                if (appJobConfig.getIsActive()) {

                    String indexName = appJobConfig.getAppIndexConfig().getStrikePriceName();
                    String segment = appJobConfig.getAppIndexConfig().getStrikePriceSegment();

                    Integer jobTypeCode = appJobConfig.getJobType().getJobTypeCode();

                    logger.info("Fetching Instruments for Job Config Num: {}, Index Name: {}, Segment: {}, Job Type Code: {}", appJobConfigNum, indexName, segment, jobTypeCode);

                    if (jobTypeCode.equals(I_ONE))
                        resultInstrumentList = instrumentRepository.findInstrumentsForNearestExpiryByNameANDSegment(indexName, segment);
                    else if (jobTypeCode.equals(I_TWO))
                        resultInstrumentList = instrumentRepository.findInstrumentsForFollowingWeeklyExpiryByNameANDSegment(indexName, segment);
                    else if (jobTypeCode.equals(I_THREE))
                        resultInstrumentList = instrumentRepository.findInstrumentsForMonthlyExpiryByNameANDSegment(indexName, segment);

                    if (!resultInstrumentList.isEmpty())
                        logger.info("Instruments fetched: {}", resultInstrumentList.getFirst().getInstrument().getExpiry());
                } else
                    logger.warn("App Job Config with Num: {} is not active.", appJobConfigNum);

            }
            else
                logger.error("No App Job Config found for Num: {}", appJobConfigNum);

        } catch (RuntimeException e) {
            logger.error("Error fetching instruments for App Job Config Num: {}: {}", appJobConfigNum, e.getMessage());

        }

        return resultInstrumentList;
    }

    @Override
    public ArrayList<OISnapshotEntity> findSnapshotTokenByInstrumentsList(List<Long> instrumentTokensToBeSubscribed) {
        return oiSnapshotRepository.findSnapshotTokenByListInstrument_token(instrumentTokensToBeSubscribed).orElseGet(ArrayList::new);
    }

    @Override
    public HistoricalDataResponse getHistoricalData(HistoricalDataRequest request) {
        HistoricalDataResponse response = new HistoricalDataResponse();
        response.setInstrumentToken(request.getInstrumentToken());
        response.setInterval(request.getInterval());

        try {
            // Validate required parameters
            if (request.getInstrumentToken() == null || request.getInstrumentToken().isEmpty()) {
                response.setSuccess(false);
                response.setMessage("Instrument token is required");
                return response;
            }

            if (request.getFromDate() == null || request.getToDate() == null) {
                response.setSuccess(false);
                response.setMessage("From date and To date are required");
                return response;
            }

            if (request.getInterval() == null || request.getInterval().isEmpty()) {
                response.setSuccess(false);
                response.setMessage("Interval is required. Valid values: minute, 3minute, 5minute, 10minute, 15minute, 30minute, 60minute, day");
                return response;
            }

            // Fetch historical data from Kite Connect API
            HistoricalData historicalData = kiteConnect.getHistoricalData(
                    DateUtilities.convertLocalDateTimeToDate(request.getFromDate()),
                    DateUtilities.convertLocalDateTimeToDate(request.getToDate()),
                    request.getInstrumentToken(),
                    request.getInterval(),
                    request.isContinuous(),
                    request.isOi()
            );

            // Convert Kite HistoricalData to our response model
            List<HistoricalDataResponse.HistoricalCandle> candles = new ArrayList<>();


            if (historicalData != null && historicalData.dataArrayList != null) {
                for (HistoricalData candle : historicalData.dataArrayList) {
                    HistoricalDataResponse.HistoricalCandle historicalCandle = HistoricalDataResponse.HistoricalCandle.builder()
                            .timestamp(candle.timeStamp != null ? candle.timeStamp : "")
                            .open(candle.open)
                            .high(candle.high)
                            .low(candle.low)
                            .close(candle.close)
                            .volume(candle.volume)
                            .oi(candle.oi)
                            .build();
                    candles.add(historicalCandle);
                }
            }

            response.setSuccess(true);
            response.setMessage("Historical data fetched successfully");
            response.setCandles(candles);
            response.setCandleCount(candles.size());

            logger.info("Fetched {} historical candles for instrument {} with interval {}",
                    candles.size(), request.getInstrumentToken(), request.getInterval());

        } catch (KiteException | IOException e) {
            logger.error("Error fetching historical data for instrument {}: {}",
                    request.getInstrumentToken(), e.getMessage(), e);
            response.setSuccess(false);
            response.setMessage("Error fetching historical data: " + e.getMessage());
            response.setCandles(new ArrayList<>());
            response.setCandleCount(0);
        }

        return response;
    }

    @Override
    public PreviousDayHighLowResponse getPreviousDayHighLow(String instrumentToken) {
        PreviousDayHighLowResponse response = new PreviousDayHighLowResponse();
        response.setInstrumentToken(instrumentToken);

        try {
            // Validate instrument token
            if (instrumentToken == null || instrumentToken.isEmpty()) {
                response.setSuccess(false);
                response.setMessage("Instrument token is required");
                return response;
            }

            // Fetch last 10 trading days to handle market holidays
            // This approach fetches data for a range and picks the most recent day
            LocalDate today = LocalDate.now();
            LocalDate fromDate = today.minusDays(10); // Go back 10 days to handle holidays + weekends

            // Create request for historical data with day interval for the date range
            HistoricalDataRequest request = HistoricalDataRequest.builder()
                    .instrumentToken(instrumentToken)
                    .fromDate(LocalDateTime.of(fromDate, LocalTime.of(0, 0, 0)))
                    .toDate(LocalDateTime.of(today.minusDays(1), LocalTime.of(23, 59, 59)))
                    .interval("day")
                    .continuous(false)
                    .oi(false)
                    .build();

            // Fetch historical data
            HistoricalDataResponse historicalResponse = getHistoricalData(request);

            if (!historicalResponse.isSuccess()) {
                response.setSuccess(false);
                response.setMessage("Failed to fetch historical data: " + historicalResponse.getMessage());
                return response;
            }

            if (historicalResponse.getCandles() == null || historicalResponse.getCandles().isEmpty()) {
                response.setSuccess(false);
                response.setMessage("No data available for previous trading days");
                return response;
            }

            // Get the most recent candle (last trading day)
            // Candles are ordered chronologically, so the last one is the most recent
            List<HistoricalDataResponse.HistoricalCandle> candles = historicalResponse.getCandles();
            HistoricalDataResponse.HistoricalCandle candle = candles.get(candles.size() - 1);

            // Set response values
            response.setSuccess(true);
            response.setMessage("Previous day high/low fetched successfully");
            response.setDate(candle.getTimestamp());
            response.setHigh(candle.getHigh());
            response.setLow(candle.getLow());
            response.setOpen(candle.getOpen());
            response.setClose(candle.getClose());

            logger.info("Previous day high/low fetched for instrument {}: High={}, Low={}, Date={}",
                    instrumentToken, candle.getHigh(), candle.getLow(), candle.getTimestamp());

        } catch (Exception e) {
            logger.error("Error fetching previous day high/low for instrument {}: {}",
                    instrumentToken, e.getMessage(), e);
            response.setSuccess(false);
            response.setMessage("Error fetching previous day high/low: " + e.getMessage());
        }

        return response;
    }


}
