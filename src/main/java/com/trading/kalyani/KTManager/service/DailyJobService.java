package com.trading.kalyani.KTManager.service;

import com.trading.kalyani.KTManager.entity.IndexLTP;
import com.trading.kalyani.KTManager.entity.LTPTrackerConfig;
import com.trading.kalyani.KTManager.entity.MiniDelta;
import com.trading.kalyani.KTManager.entity.TradeDecisions;
import com.trading.kalyani.KTManager.model.SwingHighLow;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public interface DailyJobService {

    boolean startKiteTicker();

    boolean stopKiteTicker();

    void startJobByConfigNumber(Integer appJobConfigNum);

    void stopJobByConfigNumber(Integer appJobConfigNum);

    boolean stopAllJobs();

    boolean startAllJobs();

    Future<Boolean> saveOISnapshot(Integer appJobConfigNum);

    boolean saveOISnapshotAllJobs();

    List<TradeDecisions> getTradeDecisionsByConfigNum(Integer appJobConfigNum);

    List<IndexLTP> getIndexLTPDataByConfigNum(Integer appJobConfigNum);

    List<MiniDelta> getMiniDeltaDataByAppJobConfigNum(Integer appJobConfigNum);

    Map<Integer, SwingHighLow> getSwingHighLowByConfigNum(Integer appJobConfigNum);

    Map<Integer, LTPTrackerConfig> updateLTPTrackerConfig(List<LTPTrackerConfig> ltpTrackerConfigs);

    Map<Integer, Integer> getRetryCounterByConfigNum(int appJobConfigNum);

    void initializeRetryCounter();

    /**
     * Get ticker provider data including tickerMapForJob, niftyLastPrice, and vixLastPrice
     * @return Map containing ticker data
     */
    Map<String, Object> getTickerProviderData();

    /**
     * Get only NIFTY and VIX last prices
     * @return Map containing niftyLastPrice and vixLastPrice
     */
    Map<String, Object> getIndexPrices();

    /**
     * Get tick data for a specific instrument token.
     * If the token is not subscribed, it will be subscribed first.
     * @param instrumentToken The instrument token to get tick data for
     * @return Map containing tick data for the instrument
     */
    Map<String, Object> getTickDataByToken(Long instrumentToken);

    /**
     * Get ticker connection status
     * @return Map containing isTickerConnected boolean
     */
    Map<String, Object> getTickerConnectionStatus();
}
