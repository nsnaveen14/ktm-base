package com.trading.kalyani.KTManager.service;

import com.trading.kalyani.KTManager.dto.brahmastra.LiveScanResult;
import com.trading.kalyani.KTManager.entity.*;
import com.trading.kalyani.KTManager.model.CommonReqRes;
import com.trading.kalyani.KTManager.model.IndexOHLC;
import com.trading.kalyani.KTManager.model.Message;

import java.util.List;

public interface MessagingService {

    void sendChatMessage(Message message);

    void sendInstrumentMessage(Message message);

    void sendMiniDeltaMessage(List<MiniDelta> message);

    void sendNiftyLTPMessage(NiftyLTP message);

    void sendNiftyLTPValue(CommonReqRes message);

    void sendCommonMessage(CommonReqRes message);

    void sendPowerTradeDeltaOIMessage(PowerTradeDeltaOI powerTradeDeltaOI);

    void sendOHLCMessage(IndexOHLC message);

    void sendIndexLTPMessage(IndexLTP message);

    void sendTradeDecisionMessage(TradeDecisions lastTradeDecisions);

    void sendBrahmastraSignal(LiveScanResult signal);
}
