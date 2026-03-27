package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.dto.brahmastra.LiveScanResult;
import com.trading.kalyani.KTManager.entity.*;
import com.trading.kalyani.KTManager.model.CommonReqRes;
import com.trading.kalyani.KTManager.model.IndexOHLC;
import com.trading.kalyani.KTManager.model.Message;
import com.trading.kalyani.KTManager.service.MessagingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import java.util.List;
import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;

@Service
public class MessagingServiceImpl implements MessagingService {

    @Autowired
    SimpMessagingTemplate simpleMessagingTemplate;

    private static final Logger logger = LoggerFactory.getLogger(MessagingServiceImpl.class);

    @Override
    public void sendChatMessage(Message message) {
        simpleMessagingTemplate.convertAndSend(SIMPLE_BROKER+MESSAGE_TOPIC ,message);
    }

    @Override
    public void sendInstrumentMessage(Message message) {
        simpleMessagingTemplate.convertAndSend(SIMPLE_BROKER+INSTRUMENT_TOPIC, message);
    }

    @Override
    public void sendMiniDeltaMessage(List<MiniDelta> message) {
        simpleMessagingTemplate.convertAndSend(SIMPLE_BROKER+MINI_DELTA_TOPIC, message);
        logger.debug("MiniDelta message sent to topic: {}", message);
    }

    @Override
    public void sendNiftyLTPMessage(NiftyLTP message) {
        simpleMessagingTemplate.convertAndSend(SIMPLE_BROKER+NIFTY_LTP_TOPIC, message);
        logger.debug("NiftyLTP message sent to topic: {}", message);
    }

    @Override
    public void sendNiftyLTPValue(CommonReqRes message) {
        simpleMessagingTemplate.convertAndSend(SIMPLE_BROKER+NIFTY_LTP_VALUE_TOPIC, message);
        logger.debug("NiftyLTPValue message sent to topic: {}", message);
    }

    @Override
    public void sendCommonMessage(CommonReqRes message) {
        simpleMessagingTemplate.convertAndSend(SIMPLE_BROKER+COMMON_MESSAGE_TOPIC, message);
        logger.info("Common message sent to topic: {}", message);
    }

    @Override
    public void sendPowerTradeDeltaOIMessage(PowerTradeDeltaOI message) {
        simpleMessagingTemplate.convertAndSend(SIMPLE_BROKER+PT_DELTA_OI_TOPIC, message);
        logger.info("PowerTradeDeltaOI message sent to topic: {}",message);
    }

    @Override
    public void sendOHLCMessage(IndexOHLC message) {
        simpleMessagingTemplate.convertAndSend(SIMPLE_BROKER+INDEX_OHLC_TOPIC, message);
        logger.info("IndexOHLC message sent to topic: {}",message);
    }

    @Override
    public void sendIndexLTPMessage(IndexLTP message) {
        simpleMessagingTemplate.convertAndSend(SIMPLE_BROKER+INDEX_LTP_TOPIC, message);
        logger.info("IndexLTP message sent to topic: {}",message);
    }

    @Override
    public void sendTradeDecisionMessage(TradeDecisions lastTradeDecisions) {
        simpleMessagingTemplate.convertAndSend(SIMPLE_BROKER+TRADE_DECISION_TOPIC, lastTradeDecisions);
        logger.info("TradeDecision message sent to topic: {}",lastTradeDecisions);
    }

    @Override
    public void sendBrahmastraSignal(LiveScanResult signal) {
        simpleMessagingTemplate.convertAndSend(SIMPLE_BROKER+BRAHMASTRA_SIGNAL_TOPIC, signal);
        logger.info("Brahmastra signal sent to topic: {} {} at {}", signal.getSignalType(), signal.getSymbol(), signal.getCurrentPrice());
    }


}
