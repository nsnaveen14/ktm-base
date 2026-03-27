package com.trading.kalyani.KTManager.service;

import com.trading.kalyani.KTManager.entity.IndexLTP;
import com.trading.kalyani.KTManager.entity.TradeDecisions;
import com.trading.kalyani.KTManager.model.CommonReqRes;
import com.trading.kalyani.KTManager.model.KiteModel;

import java.time.LocalDateTime;
import java.util.List;

public interface DataTestingService {

    CommonReqRes getPowerTradeDeltaTable(LocalDateTime reqLocalDateTime);

    List<IndexLTP> getNiftyLTPTestDataFromTable();

    CommonReqRes testCalculateAutoTradeLotSizes(KiteModel kiteModel);

    List<IndexLTP> testIndexLTPMessage();

    TradeDecisions testTradeDecisionMessage(Integer appJobConfigNum);

    CommonReqRes testEmailSending(Integer appJobConfigNum);
}
