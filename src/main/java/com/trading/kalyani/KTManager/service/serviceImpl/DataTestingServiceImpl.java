package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.entity.*;
import com.trading.kalyani.KTManager.model.AutoTradeParams;
import com.trading.kalyani.KTManager.model.CommonReqRes;
import com.trading.kalyani.KTManager.model.KiteModel;
import com.trading.kalyani.KTManager.repository.*;
import com.trading.kalyani.KTManager.service.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.ERROR;
import static com.trading.kalyani.KTManager.constants.ApplicationConstants.SUCCESS;

@Service
public class DataTestingServiceImpl implements DataTestingService {

    @Autowired
    MessagingService messagingService;

    @Autowired
    PowerTradeDeltaOIRepository powerTradeDeltaOIRepository;

    @Autowired
    IndexLTPRepository indexLTPRepository;

    @Autowired
    LTPTrackerRepository ltpTrackerRepository;

    @Autowired
    JobService jobService;

    @Autowired
    AutoTradeService autoTradeService;

    @Autowired
    TradeDecisionRepository tradeDecisionRepository;

    @Autowired
    EmailManagement emailManagementService;

    private static final Logger logger = LogManager.getLogger(DataTestingServiceImpl.class);

    @Override
    public CommonReqRes getPowerTradeDeltaTable(LocalDateTime reqLocalDateTime) {

        CommonReqRes res = new CommonReqRes();

        List<PowerTradeDeltaOI> ptDeltaList = powerTradeDeltaOIRepository.getDataAfterSpecifiedTS(reqLocalDateTime);
        if(ptDeltaList.isEmpty()) {
            res.setStatus(false);
            res.setMessage("No data found for the given date and time");
            res.setType(ERROR);
        }
        else {
            ptDeltaList.forEach(powerTradeDeltaOI -> messagingService.sendPowerTradeDeltaOIMessage(powerTradeDeltaOI));

            res.setQty(ptDeltaList.size());
            res.setStatus(true);
            res.setMessage("PowerTradeDelta table fetched successfully");
            res.setType(SUCCESS);

        }

        return res;

    }

    @Override
    public List<IndexLTP> getNiftyLTPTestDataFromTable() {

        List<IndexLTP> indexLTPMessageList = indexLTPRepository.findLatestIndexDataByAppJobConfigNum(1);

        indexLTPMessageList.forEach(indexLTP -> {
            LTPTracker lTPTracker = ltpTrackerRepository.getLTPTrackerByJITID(indexLTP.getJobIterationDetails().getId());
            logger.info("Job Iteration ID: {}", indexLTP.getJobIterationDetails().getId());
            logger.info("LTP Tracker: {}", lTPTracker);
          /*  jobService.calculateStraddleFlags(indexLTP,lTPTracker);
            jobService.calculateMaxPainLTP(niftyLTP, lTPTracker);
            jobService.calculateCloseFullFlag(niftyLTP);
            jobService.calculateLotSize(niftyLTP);

            messagingService.sendNiftyLTPMessage(niftyLTP);
            logger.info("NiftyLTP message sent: {}", niftyLTP); */
        });

        return indexLTPMessageList;

    }

    @Override
    public CommonReqRes testCalculateAutoTradeLotSizes(KiteModel kiteModel) {

         CommonReqRes res = new CommonReqRes();

         try {
             AutoTradeParams autoTradeParams = new AutoTradeParams();

             autoTradeParams.setAutoTradeCallInstrumentToken(kiteModel.getInstrumentTokens().getFirst());
             autoTradeParams.setAutoTradePutInstrumentToken(kiteModel.getInstrumentTokens().getLast());
             IndexLTP indexLTP = indexLTPRepository.findLatestIndexDataByAppJobConfigNum(1).getFirst();
            autoTradeService.calculateAutoTradeLotSizes(autoTradeParams, indexLTP);
            res.setStatus(true);
            res.setMessage("Auto trade lot sizes calculated successfully");
            res.setType(SUCCESS);
         }

            catch (Exception e) {
                logger.error("Error calculating auto trade lot sizes: {}", e.getMessage());
                res.setStatus(false);
                res.setMessage("Error during calculation of auto trade lot sizes: " + e.getMessage());
                res.setType(ERROR);
            }

         return res;

    }

    @Override
    public List<IndexLTP> testIndexLTPMessage() {
        List<IndexLTP> indexLTPList = new java.util.ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            IndexLTP indexLTP = new IndexLTP();
            indexLTP.setId((long) i);
            // appJobConfigNum cycles from 1 to 7
            indexLTP.setAppJobConfigNum(((i - 1) % 7) + 1);
            indexLTP.setIndexTS(java.time.LocalDateTime.now().minusMinutes(i));
            indexLTP.setIndexLTP(20000 + i * 10);
            indexLTP.setMeanStrikePCR(1.1 + i * 0.01);
            indexLTP.setMeanRateOI(2.2 + i * 0.02);
            indexLTP.setCombiRate(3.3 + i * 0.03);
            indexLTP.setSupport("Support-" + i);
            indexLTP.setResistance("Resistance-" + i);
            indexLTP.setRange("Range-" + i);
            indexLTP.setTradeDecision("Decision-" + i);
            indexLTP.setMaxPainSP(15000 + i * 5);
            indexLTP.setMaxPainSPSecond(15000 + i * 3);
            indexLTP.setDayHigh("DayHigh-" + i);
            indexLTP.setDayLow("DayLow-" + i);
            indexLTP.setDisplay(true);
            indexLTP.setJobIterationDetails(null); // Set to null or mock if needed
            indexLTP.setMaxPainCELTP(100.0 + i);
            indexLTP.setMaxPainPELTP(200.0 + i);
            indexLTPList.add(indexLTP);
        }
        return indexLTPList;
    }

    @Override
    public TradeDecisions testTradeDecisionMessage(Integer appJobConfigNum) {

        TradeDecisions lastTradeDecision = tradeDecisionRepository.findLatestTradeDecisionByConfigNum(appJobConfigNum)
                .orElseThrow(() -> new IllegalStateException("No trade decision found for config: " + appJobConfigNum));

        lastTradeDecision.setTradeDecisionTS(LocalDateTime.now());
        lastTradeDecision.setStatus("UPDATED_FOR_TESTING");

        return lastTradeDecision;

    }

    @Override
    public CommonReqRes testEmailSending(Integer appJobConfigNum) {
        TradeDecisions lastTradeDecision = tradeDecisionRepository.findLatestTradeDecisionByConfigNum(appJobConfigNum)
                .orElseThrow(() -> new IllegalStateException("No trade decision found for config: " + appJobConfigNum));

        lastTradeDecision.setTradeDecisionTS(LocalDateTime.now());
        lastTradeDecision.setStatus("UPDATED_FOR_TESTING");

        emailManagementService.sendEmailOfPlacingAutoTradeOrder(lastTradeDecision, "TEST_INSTRUMENT_TOKEN");

        return new CommonReqRes(true, "Test email sent successfully for Trade Decision ID: " + lastTradeDecision.getId(),1,null, SUCCESS);
    }


}
