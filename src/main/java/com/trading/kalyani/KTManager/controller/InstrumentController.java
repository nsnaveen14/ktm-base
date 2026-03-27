package com.trading.kalyani.KTManager.controller;

import com.trading.kalyani.KTManager.entity.InstrumentEntity;
import com.trading.kalyani.KTManager.model.HistoricalDataRequest;
import com.trading.kalyani.KTManager.model.HistoricalDataResponse;
import com.trading.kalyani.KTManager.model.PreviousDayHighLowResponse;
import com.trading.kalyani.KTManager.service.InstrumentService;
import com.zerodhatech.models.Instrument;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@CrossOrigin(value="*")
public class InstrumentController {

    @Autowired
    InstrumentService instrumentService;

    private static final Logger logger = LogManager.getLogger(InstrumentController.class);

    @PostMapping("/saveInstrumentData")
    public ResponseEntity<Boolean> saveInstrumentData(@RequestBody List<Instrument> instrumentList) {

        return new ResponseEntity<>(instrumentService.saveInstrumentsData(instrumentList), HttpStatus.OK);
    }

    @GetMapping("/getInstrumentsData")
    public ResponseEntity<List<InstrumentEntity>> getInstrumentsData() {
        return new ResponseEntity<>(instrumentService.getInstrumentData(), HttpStatus.OK);
    }

    @GetMapping("/getNFOInstrumentsForCurrentAndFollowingWeek")
    public ResponseEntity<List<InstrumentEntity>> getNFOInstrumentsForCurrentAndFollowingWeek() {
        long startTime = System.currentTimeMillis();
        List<InstrumentEntity> instruments = instrumentService.getNFOInstrumentsForCurrentAndFollowingWeek();
        long endTime = System.currentTimeMillis();
        logger.info("Time taken(s) for calculations in ms: {}", (endTime - startTime) );
        return new ResponseEntity<>(instruments, HttpStatus.OK);
    }

    @GetMapping("/searchInstruments")
    public ResponseEntity<List<InstrumentEntity>> searchInstruments(@RequestParam("searchString") String query) {
     //   logger.info("Searching instruments with query: {}", query);
        return new ResponseEntity<>(instrumentService.searchInstrumentsByTradingsymbol(query), HttpStatus.OK);
    }

    /**
     * Fetches historical candle data using Kite Connect API
     *
     * @param request HistoricalDataRequest containing:
     *                - instrumentToken: Instrument token (required)
     *                - fromDate: Start date (required)
     *                - toDate: End date (required)
     *                - interval: Candle interval - minute, 3minute, 5minute, 10minute, 15minute, 30minute, 60minute, day (required)
     *                - continuous: Set to true for continuous data (for futures)
     *                - oi: Set to true to include Open Interest data
     * @return HistoricalDataResponse containing list of historical candles
     */
    @PostMapping("/getHistoricalData")
    public ResponseEntity<HistoricalDataResponse> getHistoricalData(@RequestBody HistoricalDataRequest request) {
        long startTime = System.currentTimeMillis();
        logger.info("Fetching historical data for instrument: {}, interval: {}, from: {}, to: {}",
                request.getInstrumentToken(), request.getInterval(), request.getFromDate(), request.getToDate());

        HistoricalDataResponse response = instrumentService.getHistoricalData(request);

        long endTime = System.currentTimeMillis();
        logger.info("Historical data fetch completed in {} ms. Candles fetched: {}",
                (endTime - startTime), response.getCandleCount());

        return new ResponseEntity<>(response, response.isSuccess() ? HttpStatus.OK : HttpStatus.BAD_REQUEST);
    }

    /**
     * Fetches the previous trading day's high and low values for a given instrument
     *
     * @param instrumentToken The instrument token for which to fetch previous day high/low
     * @return PreviousDayHighLowResponse containing previous day's high, low, open, and close values
     */
    @GetMapping("/getPreviousDayHighLow")
    public ResponseEntity<PreviousDayHighLowResponse> getPreviousDayHighLow(@RequestParam("instrumentToken") String instrumentToken) {
        long startTime = System.currentTimeMillis();
        logger.info("Fetching previous day high/low for instrument: {}", instrumentToken);

        PreviousDayHighLowResponse response = instrumentService.getPreviousDayHighLow(instrumentToken);

        long endTime = System.currentTimeMillis();
        logger.info("Previous day high/low fetch completed in {} ms. Success: {}",
                (endTime - startTime), response.isSuccess());

        return new ResponseEntity<>(response, response.isSuccess() ? HttpStatus.OK : HttpStatus.BAD_REQUEST);
    }


}
