package com.trading.kalyani.KTManager.controller;

import com.trading.kalyani.KTManager.config.KiteConnectConfig;
import com.trading.kalyani.KTManager.entity.*;
import com.trading.kalyani.KTManager.model.CommonReqRes;
import com.trading.kalyani.KTManager.model.IndexOHLC;
import com.trading.kalyani.KTManager.model.KiteModel;
import com.trading.kalyani.KTManager.model.SwingHighLow;
import com.trading.kalyani.KTManager.repository.CandleStickRepository;
import com.trading.kalyani.KTManager.service.DailyJobService;
import com.trading.kalyani.KTManager.service.DataTestingService;
import com.trading.kalyani.KTManager.service.MessagingService;
import com.trading.kalyani.KTManager.utilities.KiteTickerProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.NIFTY_INSTRUMENT_TOKEN;
import static com.trading.kalyani.KTManager.utilities.DateUtilities.convertStringToLocalDateTime;

@RestController
@CrossOrigin(value="*")
public class DataTestController {

    @Autowired
    DataTestingService dataTestingService;

    @Autowired
    MessagingService messagingService;

    @Autowired
    DailyJobService dailyJobService;

    @Autowired
    KiteConnectConfig kiteConnectConfig;

    @Autowired
    CandleStickRepository candleStickRepository;

    @GetMapping("/getPowerTradeDeltaTable")
    public ResponseEntity<CommonReqRes> getPowerTradeDeltaTable(@RequestParam("reqDateTime") String reqDateTime) {

        String format = "yyyy-MM-dd HH:mm:ss";
        LocalDateTime reqLocalDateTime = convertStringToLocalDateTime(reqDateTime, format);
        CommonReqRes res  = dataTestingService.getPowerTradeDeltaTable(reqLocalDateTime);

        return new ResponseEntity<>(res, HttpStatus.OK);
    }



    @PostMapping("/testCalculateAutoTradeLotSizes")
    public ResponseEntity<CommonReqRes> testCalculateAutoTradeLotSizes(@RequestBody KiteModel kiteModel) {

        CommonReqRes res  = dataTestingService.testCalculateAutoTradeLotSizes(kiteModel);

        return new ResponseEntity<>(res, HttpStatus.OK);
    }

    @GetMapping("/testIndexOHLCMessage")
    public ResponseEntity<IndexOHLC> testIndexOHLCMessage() {

        IndexOHLC indexOHLC = new IndexOHLC(1, "24700", "150", "24650","24800");
        messagingService.sendOHLCMessage(indexOHLC);

        return new ResponseEntity<>(indexOHLC, HttpStatus.OK);
    }

    @GetMapping("/testCommonMessage")
    public ResponseEntity<CommonReqRes> testCommonMessage(@RequestParam("messageType") String messageType) {

        CommonReqRes commonReqRes ;
        if(messageType.equalsIgnoreCase("SUCCESS")) {
            commonReqRes = new CommonReqRes(true, "This is a SUCCESS message from server",1,null, "SUCCESS");
        }
        else if(messageType.equalsIgnoreCase("ERROR")) {
            commonReqRes = new CommonReqRes(false, "This is an ERROR message from server",1,null, "ERROR");
        }
        else if(messageType.equalsIgnoreCase("WARNING")) {
            commonReqRes = new CommonReqRes(false, "This is a WARNING message from server",1,null, "WARNING");
        }
        else {
            commonReqRes = new CommonReqRes(false, "Invalid message type. Please use SUCCESS, ERROR, or WARNING.",1,null, "ERROR");
        }

        messagingService.sendCommonMessage(commonReqRes);

        return new ResponseEntity<>(commonReqRes, HttpStatus.OK);
    }

    @GetMapping("/testIndexLTPMessage")
    public ResponseEntity<List<IndexLTP>> testIndexLTPMessage() {

        List<IndexLTP> indexLTPMessages = dataTestingService.testIndexLTPMessage();

        indexLTPMessages.forEach(indexLTP -> messagingService.sendIndexLTPMessage(indexLTP));

        return new ResponseEntity<>(indexLTPMessages, HttpStatus.OK);
    }

    @GetMapping("/testTradeDecisionMessage")
    public ResponseEntity<TradeDecisions> testTradeDecisionMessage(@RequestParam("appJobConfigNum") Integer appJobConfigNum) {
        TradeDecisions tradeDecisions = dataTestingService.testTradeDecisionMessage(appJobConfigNum);

        messagingService.sendTradeDecisionMessage(tradeDecisions);

        return new ResponseEntity<>(tradeDecisions, HttpStatus.OK);
    }

    @GetMapping("/testGetSwingHighLowByConfigNum")
    public ResponseEntity<Map<Integer, SwingHighLow>> getSwingHighLowByConfigNum(@RequestParam("appJobConfigNum") Integer appJobConfigNum) {


        Map<Integer, SwingHighLow> swingHighLowMap = dailyJobService.getSwingHighLowByConfigNum(appJobConfigNum);
        return new ResponseEntity<>(swingHighLowMap, HttpStatus.OK);
    }

    @GetMapping("/testEmailSending" )
    public ResponseEntity<CommonReqRes> testEmailSending(@RequestParam("appJobConfigNum") Integer appJobConfigNum) {
        CommonReqRes res = dataTestingService.testEmailSending(appJobConfigNum);
        return new ResponseEntity<>(res, HttpStatus.OK);
    }


}
