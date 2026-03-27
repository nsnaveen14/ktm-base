package com.trading.kalyani.KTManager.controller;

import com.trading.kalyani.KTManager.dto.brahmastra.LiveScanResult;
import com.trading.kalyani.KTManager.entity.*;
import com.trading.kalyani.KTManager.model.CommonReqRes;
import com.trading.kalyani.KTManager.model.IndexOHLC;
import com.trading.kalyani.KTManager.model.Message;
import com.trading.kalyani.KTManager.model.OutputMessage;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;

@Controller
@CrossOrigin()
@MessageMapping(CHAT_ENDPOINT)
public class WebSocketController {

    @SendTo(SIMPLE_BROKER+MESSAGE_TOPIC)
    public OutputMessage send(Message message) throws Exception {
        String timeOutputMessage = new SimpleDateFormat("HH:mm").format(new Date());
        System.out.println("Inside send function : "+message.toString());
        return new OutputMessage(message.getInstrumentToken(), message.getOi(), message.getLastTradedPrice(), message.getLastTradedTime(),message.getTickTimestamp(),timeOutputMessage);
    }


    @SendTo(SIMPLE_BROKER+INSTRUMENT_TOPIC)
    public Message sendInstrument(Message message) throws Exception {
        return message;
    }


    @SendTo(SIMPLE_BROKER+MINI_DELTA_TOPIC)
    public List<MiniDelta> sendMiniDelta(List<MiniDelta> message) throws Exception {
        return message;
    }


    @SendTo(SIMPLE_BROKER+NIFTY_LTP_TOPIC)
    public NiftyLTP sendNiftyLTP(NiftyLTP message) throws Exception {
        return message;
    }

    @SendTo(SIMPLE_BROKER+NIFTY_LTP_VALUE_TOPIC)
    public CommonReqRes sendNiftyLTPValue(CommonReqRes message) throws Exception {
        return message;
    }

    @SendTo(SIMPLE_BROKER+COMMON_MESSAGE_TOPIC)
    public CommonReqRes sendCommonMessage(CommonReqRes message) throws Exception {
        return message;
    }

    @SendTo(SIMPLE_BROKER+PT_DELTA_OI_TOPIC)
    public PowerTradeDeltaOI sendPowerTradeDeltaOIMessage(PowerTradeDeltaOI message) throws Exception {
        return message;
    }

    @SendTo(SIMPLE_BROKER+INDEX_OHLC_TOPIC)
    public IndexOHLC sendOHLCMessage(IndexOHLC message) throws Exception {
        return message;
    }

    @SendTo(SIMPLE_BROKER+INDEX_LTP_TOPIC)
    public IndexLTP sendIndexLTP(IndexLTP message) throws Exception {
        return message;
    }

    @SendTo(SIMPLE_BROKER+TRADE_DECISION_TOPIC)
    public TradeDecisions sendTradeDecisionMessage(TradeDecisions message) throws Exception {
        return message;
    }

    @SendTo(SIMPLE_BROKER+BRAHMASTRA_SIGNAL_TOPIC)
    public LiveScanResult sendBrahmastraSignal(LiveScanResult message) throws Exception {
        return message;
    }


}
