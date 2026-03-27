package com.trading.kalyani.KTManager.service;

import com.trading.kalyani.KTManager.entity.InstrumentEntity;

import com.trading.kalyani.KTManager.entity.OISnapshotEntity;
import com.trading.kalyani.KTManager.model.HistoricalDataRequest;
import com.trading.kalyani.KTManager.model.HistoricalDataResponse;
import com.trading.kalyani.KTManager.model.PreviousDayHighLowResponse;
import com.zerodhatech.models.Instrument;

import java.util.ArrayList;
import java.util.List;

public interface InstrumentService {

    boolean saveInstrumentsData(List<Instrument> instruments);

    ArrayList<InstrumentEntity> getInstrumentData();

    ArrayList<InstrumentEntity> findNFOInstrumentsForCurrentWeek();

    Iterable<OISnapshotEntity> findAllSnapShotTokens();

     void clearAllCaches();

    ArrayList<InstrumentEntity> getNFOInstrumentsForCurrentAndFollowingWeek();

    List<InstrumentEntity> searchInstrumentsByTradingsymbol(String partialText);

    List<Instrument> getRequiredInstruments(List<Instrument> instruments);

    ArrayList<InstrumentEntity> getInstrumentsFromAppJobConfigNum(Integer appJobConfigNum);

    ArrayList<OISnapshotEntity> findSnapshotTokenByInstrumentsList(List<Long> instrumentTokensToBeSubscribed);

    /**
     * Fetches historical candle data using Kite Connect API
     * @param request HistoricalDataRequest containing instrument token, date range, interval, etc.
     * @return HistoricalDataResponse containing list of historical candles
     */
    HistoricalDataResponse getHistoricalData(HistoricalDataRequest request);

    /**
     * Fetches the previous trading day's high and low values for the given instrument token
     * @param instrumentToken Instrument token for which previous day high/low is required
     * @return PreviousDayHighLowResponse containing previous day's high, low, open, and close values
     */
    PreviousDayHighLowResponse getPreviousDayHighLow(String instrumentToken);
}
