package com.trading.kalyani.KTManager.service;

import com.trading.kalyani.KTManager.entity.IndexLTP;
import com.trading.kalyani.KTManager.entity.NiftyLTP;
import com.trading.kalyani.KTManager.model.AutoTradeParams;

public interface AutoTradeService {

    // calculate call and put symbols
    void calculateAutoTradeSymbols(AutoTradeParams autoTradeParams, IndexLTP latestIndexLTP);
    // calculate call and put lot sizes
    void calculateAutoTradeLotSizes(AutoTradeParams autoTradeParams, IndexLTP latestIndexLTP);
    // calculate entry price
    void calculateAutoTradeEntryPrice(AutoTradeParams autoTradeParams, IndexLTP latestIndexLTP);
    // calculate stop loss price
    void calculateAutoTradeSLPrice(AutoTradeParams autoTradeParams, IndexLTP latestIndexLTP);

    AutoTradeParams saveAutoTradeParams(AutoTradeParams autoTradeParams);

}
