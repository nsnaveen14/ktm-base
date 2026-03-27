package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.config.KiteConnectConfig;
import com.trading.kalyani.KTManager.constants.ApplicationConstants;
import com.trading.kalyani.KTManager.entity.*;
import com.trading.kalyani.KTManager.model.*;
import com.trading.kalyani.KTManager.repository.*;
import com.trading.kalyani.KTManager.service.*;
import com.trading.kalyani.KTManager.utilities.KiteTickerProvider;
import java.util.HashMap;
import com.zerodhatech.kiteconnect.kitehttp.exceptions.KiteException;
import com.zerodhatech.models.Instrument;
import com.zerodhatech.models.OHLC;
import com.zerodhatech.models.Quote;
import com.zerodhatech.models.Tick;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;
import static com.trading.kalyani.KTManager.utilities.DateUtilities.convertDateToLocalDate;
import static com.trading.kalyani.KTManager.utilities.StringUtils.formatSafeNumber;
import static com.zerodhatech.kiteconnect.utils.Constants.*;
import static com.zerodhatech.kiteconnect.utils.Constants.ORDER_LAPSED;

@Service
public class DailyJobServiceImpl implements DailyJobService {

    private final ExecutorService jobExecutor = Executors.newVirtualThreadPerTaskExecutor();

    @Autowired
    AppJobConfigRepository appJobConfigRepository;

    @Autowired
    InstrumentService instrumentService;

    @Autowired
    JobDetailsRepository jobDetailsRepository;

    @Autowired
    KiteConnectConfig kiteConnectConfig;

    @Autowired
    JobIterationRepository jobIterationRepository;

    @Autowired
    OiSnapshotRepository oiSnapshotRepository;

    @Autowired
    FileServiceImpl fileService;

    @Autowired
    MessagingService messagingService;

    @Autowired
    IndexLTPRepository indexLTPRepository;

    @Autowired
    AsyncService asyncService;

    @Autowired
    MiniDeltaRepository miniDeltaRepository;

    @Autowired
    TradeDecisionRepository tradeDecisionRepository;

    @Autowired
    DailyJobPlannerService dailyJobPlannerService;

    @Autowired
    JobDetailsService jobDetailsService;

    @Autowired
    LTPTrackerRepository ltpTrackerRepository;

    @Autowired
    CandleStickRepository candleStickRepository;

    @Autowired
    AutoTradeService autoTradeService;

    @Autowired
    EmailManagement emailManagementService;

    @Autowired
    OrderBookRepository orderBookRepository;

    @Autowired
    TelegramNotificationService telegramNotificationService;

    @Autowired
    InternalOrderBlockService internalOrderBlockService;

    @Autowired
    IOBAutoTradeService iobAutoTradeService;

    @Autowired
    BrahmastraService brahmastraService;

    static final AtomicBoolean isTickerConnected = new AtomicBoolean(false);

    private static final Logger logger = LoggerFactory.getLogger(DailyJobServiceImpl.class);

    KiteTickerProvider kiteTickerProvider;

    Map<String, Double> ltpMap = new ConcurrentHashMap<>();

    static Map<Integer,TradeDecisions> lastTradeDecisionsMap = new ConcurrentHashMap<>();

    static Map<Integer,OrderBook> orderBookMap = new ConcurrentHashMap<>();

    static Map<Integer,Integer> retryCounter = new ConcurrentHashMap<>();

    static Map<Integer,LTPTrackerConfig> ltpTrackerConfigMap = new ConcurrentHashMap<>();

    static Map<Integer,AutoTradeParams> autoTradeParamsJob = new ConcurrentHashMap<>();

    @Override
    public boolean startKiteTicker() {

        if(isTickerConnected.get())
            return true;

        kiteTickerProvider = new KiteTickerProvider(kiteConnectConfig.kiteConnect().getAccessToken(), kiteConnectConfig.kiteConnect().getApiKey(),candleStickRepository);

        // Set IOB services for automatic trade execution on IOB signals (min 85% confidence)
        kiteTickerProvider.setIobAutoTradeService(iobAutoTradeService);
        kiteTickerProvider.setIobService(internalOrderBlockService);

        isTickerConnected.set(kiteTickerProvider.startTickerConnection());
        ArrayList<Long> indexTokens = new ArrayList<>();
        indexTokens.add(NIFTY_INSTRUMENT_TOKEN);
        indexTokens.add(INDIA_VIX_INSTRUMENT_TOKEN);
        kiteTickerProvider.subscribeTokenForJob(indexTokens);

        initializeRetryCounter();

        return isTickerConnected.get();
    }

    public void initializeRetryCounter() {

        appJobConfigRepository.findAllByOrderByAppJobConfigNumAsc().forEach(appJobConfig ->  retryCounter.put(appJobConfig.getAppJobConfigNum(),5));

    }

    @Override
    public boolean stopKiteTicker() {
        try {
            kiteTickerProvider.disconnectTicker();
            isTickerConnected.set(false);
        } catch (Exception e) {
            isTickerConnected.set(true);
            logger.error("Error while stopping kite ticker: {}", e.getMessage());
        }
        return !isTickerConnected.get();
    }


    @Override
    public void startJobByConfigNumber(Integer appJobConfigNum) {

        jobExecutor.submit(() -> runJob(appJobConfigNum));
    }

    @Override
    public Future<Boolean> saveOISnapshot(Integer appJobConfigNum) {
        return jobExecutor.submit(() -> runOISnapshotJob(appJobConfigNum));
    }

    @Override
    public boolean saveOISnapshotAllJobs() {
        appJobConfigRepository.findAll().forEach(appJobConfig -> saveOISnapshot(appJobConfig.getAppJobConfigNum()));
        return true;

    }

    public void runJob(Integer appJobConfigNum) {

        MDC.put("appJobConfigNum", String.valueOf(appJobConfigNum));
        AppJobConfig appJobConfig = appJobConfigRepository.findById(appJobConfigNum)
                .orElseThrow(() -> new IllegalArgumentException("No AppJobConfig found for appJobConfigNum=" + appJobConfigNum));

        logger.info("AppJobConfig details: {}", appJobConfig);
        logger.info("AppJobConfig data {},{}", appJobConfig.getAppIndexConfig().getJobStartExpression(),appJobConfig.getJobType().getJobIterationDelaySeconds());

        ArrayList<InstrumentEntity> listOfRequiredInstruments = instrumentService.getInstrumentsFromAppJobConfigNum(appJobConfigNum);
        logger.info("Total instruments found {} for AppJobConfig: {}", listOfRequiredInstruments.size(),appJobConfigNum);
        logger.info("Processing for expiry: {} for AppJobConfig: {}", listOfRequiredInstruments.getFirst().getInstrument().getExpiry(),appJobConfigNum);

        List<Long> instrumentTokensToBeSubscribed = listOfRequiredInstruments
                .parallelStream()
                .map(i -> i.getInstrument().getInstrument_token())
                .collect(Collectors.toList());

        // find snapshot token OI data
        ArrayList<OISnapshotEntity> oiSnapshotEntities = instrumentService.findSnapshotTokenByInstrumentsList(instrumentTokensToBeSubscribed);
        logger.info("Total snapshot tokens found for AppJobConfigNum {} are: {}", appJobConfigNum, oiSnapshotEntities.size());
        logger.info("Date of snapshot token is: {}", oiSnapshotEntities.getFirst().getTickTimestamp());

        Map<Long,Double> snapshotOI = new HashMap<>();
        oiSnapshotEntities.forEach(o -> snapshotOI.put(o.getInstrument_token(), o.getOi()));

        //Adding spot instrument token to the list of tokens to be subscribed
        Long spotInstrumentToken = appJobConfig.getAppIndexConfig().getInstrumentToken();
        logger.info("Spot instrument token for AppName {} is: {}", appJobConfig.getAppIndexConfig().getIndexName(), spotInstrumentToken);
      //  instrumentTokensToBeSubscribed.add(spotInstrumentToken);

        //create jobdetails object and save it to db
        JobDetails jobDetails = createJobDetailsObj(appJobConfigNum, listOfRequiredInstruments.getFirst().getInstrument().getExpiry(), STATUS_RUNNING, ApplicationConstants.JobName.MARKET);
        logger.info("JobDetails created with id: {} for AppJobConfigNum: {}", jobDetails.getId(), appJobConfigNum);

        int jobIterationDelaySeconds = appJobConfig.getJobType().getJobIterationDelaySeconds();
        logger.info("Job iteration delay seconds: {}", jobIterationDelaySeconds);

        Integer tFactor = appJobConfig.getAppIndexConfig().getTFactor();
        boolean combiRateChanged = false;
        List<MiniDelta> prevItrMiniDeltaList = new ArrayList<>();
        IndexLTP prevItrIndexLTP = null;

        JobIterationDetails jobIterationDetails= new JobIterationDetails();

        try {

            TradeDecisions lastTradeDecisions = tradeDecisionRepository.findLatestTradeDecisionByConfigNum(appJobConfig.getAppJobConfigNum())
                    .orElseGet(() -> new TradeDecisions("NA",TRADE_DECISION_TYPE_REGULAR,LocalDateTime.now(),0,"COMPLETE",appJobConfig,new JobIterationDetails()));
            logger.info("Fetching last trade decision from DB: {}", lastTradeDecisions);

            lastTradeDecisionsMap.put(appJobConfigNum,lastTradeDecisions);

            if(appJobConfigNum == 1)
            {
                 instrumentTokensToBeSubscribed.add(NIFTY_INSTRUMENT_TOKEN); //nifty 50

                 instrumentTokensToBeSubscribed.add(INDIA_VIX_INSTRUMENT_TOKEN); //india vix
            }

            kiteTickerProvider.subscribeTokenForJob((ArrayList<Long>) instrumentTokensToBeSubscribed);

            sendCommonMessage(appJobConfig.getAppIndexConfig().getStrikePriceName() + " Job started successfully!",true,SUCCESS);
            logger.info("Job started successfully for AppJobConfigNum: {}", appJobConfigNum);

            do {
            //    logger.info("Current app jobConfig Number: {}",currentAppJobConfig.getAppJobConfigNum());

                MeanOIParams meanOIParams = new MeanOIParams(0.0,0.0,0.0,0.0,0.0,0.0,0.0);

                logger.info("Within function appJobConfigNum: {}",appJobConfigNum);
                jobIterationDetails = createJobIterationDetails(jobDetails);
                IndexLTP indexLTP = new IndexLTP();
                indexLTP.setAppJobConfigNum(appJobConfigNum);
                indexLTP.setJobIterationDetails(jobIterationDetails);
                //get atm price from spotindex instrument
                int atmPrice = processSpotInstrumentForATMPrice(spotInstrumentToken, indexLTP);
                logger.info("ATM price is: {}", atmPrice);

                try {
                    Thread.sleep((long) appJobConfig.getJobType().getJobIterationDelaySeconds() * ONE_THOUSAND);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                //fetch lastTradeDecision from map
                if(lastTradeDecisionsMap.get(appJobConfigNum)!=null) {
                    lastTradeDecisions = lastTradeDecisionsMap.get(appJobConfigNum);
                    logger.info("Last Trade Decision fetched from map: {}", lastTradeDecisions.toString());
                }

                if (!kiteTickerProvider.tickerMapForJob.isEmpty() && atmPrice != -1) {

                    ArrayList<DeltaOICalculations> deltaOICalculationsList = calculateDeltaOIFromIndexPrice(listOfRequiredInstruments, atmPrice, snapshotOI,tFactor,appJobConfig); //getting saved below
                    int defaultThreshold = appJobConfig.getAppIndexConfig().getDefaultThreshold();
                    int threshold = calculateThreshold(deltaOICalculationsList,defaultThreshold); //it will get saved with job iteration details
                    logger.info("Threshold for appJobConfigNum {} :: {}", appJobConfigNum,threshold);
                    jobIterationDetails.setThreshold(threshold);

                    logger.info("DeltaOICalculations list size before threshold filter is: {}", deltaOICalculationsList.size());

                    sendOHLCMessage(appJobConfigNum,indexLTP,threshold);

                    if (!deltaOICalculationsList.isEmpty()) {
                        List<DeltaOICalculations> deltaOIMiniList = filterListUsingThreshold(deltaOICalculationsList, threshold,meanOIParams); //getting saved below
                        logger.info("DeltaOICalculations list size after threshold filter is: {}", deltaOIMiniList.size());

                        if (!deltaOIMiniList.isEmpty()) {
                            List<MiniDelta> miniDeltaList = calculateMiniDeltaList(deltaOIMiniList, jobIterationDetails,appJobConfigNum,meanOIParams); //getting saved
                            calculateMeanValues(meanOIParams);
                            calculateIndexLTPWithSupportAndResistance(indexLTP,deltaOIMiniList, deltaOICalculationsList,appJobConfig,meanOIParams); //right table

                            logger.info("IndexLTP after support and resistance calculation is: {}", indexLTP);

                            logger.info("Job Iteration ID at time: {},{}", jobIterationDetails.getId(),jobIterationDetails.getIterationStartTime());
                            MaxPain maxPain = calculateMaxPain(miniDeltaList,appJobConfigNum);

                            indexLTP.setMaxPain(maxPain);

                            logger.info("Max Pain Object :: {}", indexLTP.getMaxPain().toString());

                            indexLTP.setMaxPainSP(maxPain.getMaxPainSP());
                            indexLTP.setMaxPainSPSecond(maxPain.getMaxPainSPSecond());

                            if (!prevItrMiniDeltaList.isEmpty()) {
                                MiniDelta totalRec = miniDeltaList.getLast();
                                MiniDelta prevItrTotalRec = prevItrMiniDeltaList.getLast();

                                calculateTradeDecision(totalRec, indexLTP, prevItrIndexLTP);

                                logger.info("Job Iteration ID: {}", jobIterationDetails.getId());
                                logger.info("Previous IndexLTP :: {}", prevItrIndexLTP);

                                if ((!indexLTP.getMeanStrikePCR().equals(prevItrIndexLTP.getMeanStrikePCR()))) {
                                    logger.info("Mean Strike PCR changed from {} to {}", prevItrIndexLTP.getMeanStrikePCR(), indexLTP.getMeanStrikePCR());
                                    combiRateChanged = true;
                                    indexLTP.setDisplay(true);
                                    messagingService.sendIndexLTPMessage(indexLTP);
                                }

                                if(indexLTP.getMaxPainSP()> prevItrIndexLTP.getMaxPainSP() && indexLTP.getMaxPainSPSecond() > prevItrIndexLTP.getMaxPainSPSecond())
                                    sendCommonMessage("Max Pain SP increased from " + prevItrIndexLTP.getMaxPainSP() + " to " + indexLTP.getMaxPainSP(), true, WARNING);
                                else if(indexLTP.getMaxPainSP()< prevItrIndexLTP.getMaxPainSP() && indexLTP.getMaxPainSPSecond() < prevItrIndexLTP.getMaxPainSPSecond() )
                                    sendCommonMessage("Max Pain SP decreased from " + prevItrIndexLTP.getMaxPainSP() + " to " + indexLTP.getMaxPainSP(), true, WARNING);


                            /*    if(!indexLTP.getRange().equalsIgnoreCase(prevItrIndexLTP.getRange()))
                                    emailManagementService.sendEmailForRangeChange(indexLTP,prevItrIndexLTP,appJobConfig); */

                                calculateColorCoding(miniDeltaList, prevItrMiniDeltaList);

                                messagingService.sendMiniDeltaMessage(miniDeltaList);


                                if(appJobConfig.getAppJobConfigNum() == 1 || appJobConfig.getAppJobConfigNum() ==4) {
                                    internalOrderBlockService.checkMitigation(appJobConfig.getAppIndexConfig().getInstrumentToken(), Double.valueOf(indexLTP.getIndexLTP()));
                                }

                                if (indexLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_BUY) || indexLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_SELL)) {
                                    lastTradeDecisions = calculateAndNotifySignalParameters(indexLTP, jobIterationDetails, appJobConfig);
                                    saveLTPTracker(indexLTP,jobIterationDetails,appJobConfig);
                                }

                                logger.info("IndexLTP after trade decision calculation is: {}", indexLTP);

                                if(lastTradeDecisions.getStatus().equalsIgnoreCase(ORDER_OPEN)) {
                                    logger.info("Analyzing Last Trade Decision object on OPEN TRADE: {}", lastTradeDecisions);
                                    // check for niftyLTP value for entry price
                                    if(lastTradeDecisions.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_BUY))
                                        jobIterationDetails = processBuySignal(indexLTP, lastTradeDecisions,jobIterationDetails,appJobConfig);

                                    else if(lastTradeDecisions.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_SELL)) {
                                        jobIterationDetails = processSellSignal(indexLTP, lastTradeDecisions,jobIterationDetails,appJobConfig);
                                    }
                                }
                            }

                            else {
                                messagingService.sendIndexLTPMessage(indexLTP);
                                messagingService.sendMiniDeltaMessage(miniDeltaList);
                            }

                            if (combiRateChanged) {
                                miniDeltaRepository.deleteByAppJobConfigNum(appJobConfigNum);
                                logger.info("Old MiniDelta data deleted successfully for appJobConfigNum {}", appJobConfigNum);
                                miniDeltaList = (List<MiniDelta>) miniDeltaRepository.saveAll(miniDeltaList);
                                logger.info("MiniDelta List saved successfully");
                            }

                            indexLTP = indexLTPRepository.save(indexLTP);
                            logger.info("IndexLTP saved successfully for appJobConfigNum {}", appJobConfigNum);

                            prevItrIndexLTP = indexLTP;
                            prevItrMiniDeltaList = miniDeltaList;

                        }

                    }

                }
                indexLTP = indexLTPRepository.save(indexLTP);
                jobIterationDetails.setIndexLTP(indexLTP.getIndexLTP());
                jobIterationDetails = completeJobIteration(jobIterationDetails);
                logger.info("IndexLTP saved successfully for appJobConfigNum {} jobIteration {}", appJobConfigNum,jobIterationDetails.getId());

                final Long jobDetailsId = jobDetails.getId();
                jobDetails = jobDetailsRepository.findById(jobDetailsId)
                        .orElseThrow(() -> new IllegalStateException("JobDetails not found for id=" + jobDetailsId));
                logger.info("JobStatus: {}, for AppJobConfigNum: {}", jobDetails.getJobStatus(), appJobConfigNum);
            } while (jobDetails.getJobStatus().equalsIgnoreCase(STATUS_RUNNING));

        } catch (Exception e) {
            stopJobByConfigNumber(appJobConfigNum);
            logger.error("Error in job execution for AppJobConfigNum {} in iteration {} with:  {}", appJobConfigNum, jobIterationDetails , e.getMessage());
            logger.error("Retrying job for AppJobConfigNum: {} with retry counter {}", appJobConfigNum,retryCounter);
            if(retryCounter.getOrDefault(appJobConfigNum, 0)>=0) {
                retryCounter.put(appJobConfigNum, retryCounter.getOrDefault(appJobConfigNum, 0) - 1);
                runJob(appJobConfigNum);
            }
            emailManagementService.sendEmailForErrorInJob(e, appJobConfig,retryCounter.get(appJobConfigNum),jobIterationDetails);
        }

        finally {
            try {
                kiteTickerProvider.unsubscribeTokensForDeltaJob((ArrayList<Long>) instrumentTokensToBeSubscribed);
            }

            catch (Exception ex) {
                logger.error("Error unsubscribing tokens: {}", ex.getMessage());
            }

            MDC.remove("appJobConfigNum");
        }


    }



    private LTPTracker saveLTPTracker(IndexLTP indexLTP, JobIterationDetails jobIterationDetails, AppJobConfig appJobConfig) {

        logger.info("Getting LTP values for indexLTP: {}", indexLTP.toString());
        logger.info("LTP for Range: {}, Call: {}, {}", indexLTP.getRange(), ltpMap.get(indexLTP.getRange().split("-")[0]+"CE"), ltpMap.get(indexLTP.getRange().split("-")[1]+"CE"));
        logger.info("LTP for Range: {}, Put: {}, {}", indexLTP.getRange(), ltpMap.get(indexLTP.getRange().split("-")[0]+"PE"), ltpMap.get(indexLTP.getRange().split("-")[1]+"PE"));
        LTPTracker lTPTracker = new LTPTracker();

        try {

            lTPTracker.setJobIterationDetails(jobIterationDetails);
            lTPTracker.setAppJobConfigNum(appJobConfig.getAppJobConfigNum());
            lTPTracker.setIndexTS(indexLTP.getIndexTS());
            lTPTracker.setIndexLTP(indexLTP.getIndexLTP());

            lTPTracker.setRangeLowSP(indexLTP.getRange().split("-")[0]);
            lTPTracker.setRangeLowLTP(ltpMap.get(indexLTP.getRange().split("-")[0] + "PE"));

            lTPTracker.setRangeHighSP(indexLTP.getRange().split("-")[1]);
            lTPTracker.setRangeHighLTP(ltpMap.get(indexLTP.getRange().split("-")[1] + "CE"));

            lTPTracker.setSupportSP(indexLTP.getSupport());
            if (ltpMap.containsKey(indexLTP.getSupport() + "PE"))
                lTPTracker.setSupportLTP(ltpMap.get(indexLTP.getSupport() + "PE"));
            else
                lTPTracker.setSupportLTP(ltpMap.get(Integer.parseInt(indexLTP.getSupport()) - (appJobConfig.getAppIndexConfig().getPriceGap()/I_TWO) + "PE"));

            lTPTracker.setResistanceSP(indexLTP.getResistance());

            if (ltpMap.containsKey(indexLTP.getResistance() + "CE"))
                lTPTracker.setResistanceLTP(ltpMap.get(indexLTP.getResistance() + "CE"));
            else
                lTPTracker.setResistanceLTP(ltpMap.get(Integer.parseInt(indexLTP.getResistance()) - (appJobConfig.getAppIndexConfig().getPriceGap()/I_TWO) + "CE"));

            logger.info("LTP for Support: {}, Call: {}", indexLTP.getSupport(), lTPTracker.getSupportLTP());
            logger.info("LTP for Resistance: {}, Put: {}", indexLTP.getResistance(), lTPTracker.getResistanceLTP());

            String maxPainSP = String.valueOf(indexLTP.getMaxPainSP());

            lTPTracker.setMaxPainSP(maxPainSP);
            lTPTracker.setMaxPainCELTP(ltpMap.get(maxPainSP + "CE"));
            lTPTracker.setMaxPainPELTP(ltpMap.get(maxPainSP + "PE"));

            String strikePriceFromAtm = getStrikePriceFromAtm(indexLTP,appJobConfig);
            String strikePriceForCE = ltpTrackerConfigMap.get(appJobConfig.getAppJobConfigNum()).getAtmStrikePriceCE();
            String strikePriceForPE = ltpTrackerConfigMap.get(appJobConfig.getAppJobConfigNum()).getAtmStrikePricePE();
            logger.info("ATM Strike Price calculated from IndexLTP {} is: CE {} and PE {}", indexLTP.getIndexLTP(), strikePriceForCE, strikePriceForPE);

            if(!strikePriceForCE.isEmpty() || !strikePriceForPE.isEmpty())
            {
               lTPTracker.setAtmStrikePriceCE(strikePriceForCE);
               lTPTracker.setAtmStrikeCELTP(ltpMap.get(strikePriceForCE + "CE"));
               lTPTracker.setAtmStrikePricePE(strikePriceForPE);
               lTPTracker.setAtmStrikePELTP(ltpMap.get(strikePriceForPE + "PE"));
            }
            else if(!strikePriceFromAtm.isEmpty())
            {
                lTPTracker.setAtmStrikePriceCE(strikePriceFromAtm);
                lTPTracker.setAtmStrikeCELTP(ltpMap.get(strikePriceFromAtm + "CE"));
                lTPTracker.setAtmStrikePricePE(strikePriceFromAtm);
                lTPTracker.setAtmStrikePELTP(ltpMap.get(strikePriceFromAtm + "PE"));
            }

            logger.info("LTPTracker before saving: {}", lTPTracker.toString());

            lTPTracker = ltpTrackerRepository.save(lTPTracker);
            logger.info("LTPTracker saved successfully: {}", lTPTracker.toString());

        } catch (RuntimeException e) {
            logger.error("Error while saving LTPTracker: {}", e.getMessage());
        }

        return lTPTracker;
    }

    private TradeDecisions calculateAndNotifySignalParameters(IndexLTP indexLTP, JobIterationDetails jobIterationDetails,AppJobConfig appJobConfig) {


        TradeDecisions lastTradeDecisions = lastTradeDecisionsMap.get(appJobConfig.getAppJobConfigNum());

        emailManagementService.sendEmailOfTradeDecision(indexLTP,appJobConfig);


        if(!lastTradeDecisions.getStatus().equalsIgnoreCase(ORDER_OPEN)) {
            lastTradeDecisions = saveTradeDecision(indexLTP, jobIterationDetails, ORDER_OPEN,appJobConfig);
            logger.info("Trade decision is set successfully to OPEN: {}", lastTradeDecisions.toString());
            sendCommonMessage("Trade decision is set successfully to OPEN",true, WARNING);
            lastTradeDecisionsMap.put(appJobConfig.getAppJobConfigNum(),lastTradeDecisions);
        }
        else if(lastTradeDecisions.getStatus().equalsIgnoreCase(ORDER_OPEN) && !indexLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_NO_TRADE)) {

            if(lastTradeDecisions.getTradeDecision().equalsIgnoreCase(indexLTP.getTradeDecision()))
            {
               IndexLTP prevTradeDecisionIndexLTP = indexLTPRepository.findByJobIterationDetails_Id(lastTradeDecisions.getJobIterationDetails().getId()).get();
               logger.info("Previous Trade Decision IndexLTP: {}",prevTradeDecisionIndexLTP.toString());

               if(indexLTP.getSupport().equalsIgnoreCase(prevTradeDecisionIndexLTP.getSupport()) &&
                  indexLTP.getResistance().equalsIgnoreCase(prevTradeDecisionIndexLTP.getResistance()))
               {
                   logger.info("No change in trade decision parameters, keeping the last trade decision OPEN: {}", lastTradeDecisions.toString());
               }
               else {
                   lastTradeDecisions.setStatus(ORDER_COMPLETE);
                   lastTradeDecisions.setTrade_decision_result(ORDER_LAPSED);
                   lastTradeDecisions.setTrade_decision_result_ts(LocalDateTime.now());
                   tradeDecisionRepository.save(lastTradeDecisions);
                   sendCommonMessage("Previous Trade decision is set successfully to LAPSED", true, WARNING);
                   lastTradeDecisions = saveTradeDecision(indexLTP, jobIterationDetails, ORDER_OPEN, appJobConfig);
                   sendCommonMessage("Trade decision is set successfully to OPEN", true, WARNING);
                   lastTradeDecisionsMap.put(appJobConfig.getAppJobConfigNum(), lastTradeDecisions);
               }

            }

            else {
                lastTradeDecisions.setStatus(ORDER_COMPLETE);
                lastTradeDecisions.setTrade_decision_result(FAILURE);
                lastTradeDecisions.setTrade_decision_result_ts(LocalDateTime.now());
                tradeDecisionRepository.save(lastTradeDecisions);
                sendCommonMessage("Previous Trade decision is set successfully to COMPLETE", true, SUCCESS);
                lastTradeDecisions = saveTradeDecision(indexLTP, jobIterationDetails, ORDER_OPEN, appJobConfig);
                sendCommonMessage("Trade decision is set successfully to OPEN", true, WARNING);
                lastTradeDecisionsMap.put(appJobConfig.getAppJobConfigNum(), lastTradeDecisions);
            }
        }
       // sendTradeDecisionTelegramAlert(indexLTP, lastTradeDecisions, appJobConfig, indexLTP.getTradeDecision() + " Signal Processing");
        messagingService.sendTradeDecisionMessage(lastTradeDecisions);
        return lastTradeDecisionsMap.get(appJobConfig.getAppJobConfigNum());
    }

    private void sendOHLCMessage(Integer appJobConfigNum, IndexLTP indexLTP, int threshold) {

        try
        {
        logger.info("Sending OHLC message with threshold: {}", threshold);
        IndexOHLC ohlcMessage = new IndexOHLC(appJobConfigNum, String.valueOf(indexLTP.getIndexLTP()), String.valueOf(threshold), String.valueOf(indexLTP.getDayLow()), String.valueOf(indexLTP.getDayHigh()));
        messagingService.sendOHLCMessage(ohlcMessage);
        logger.info("OHLC message sent: {}", ohlcMessage.toString());
        }
        catch (Exception e)
        {
            logger.error("Error while sending OHLC message: {}", e.getMessage());
        }


    }

    private void calculateTradeDecision(MiniDelta totalRec,IndexLTP indexLTP, IndexLTP prevItrIndexLTP) {

        if((!indexLTP.getSupport().equals(indexLTP.getResistance())) && (Math.abs(totalRec.getCallOIChange())+Math.abs(totalRec.getPutOIChange()) >= D_TWO_HUNDRED) && (prevItrIndexLTP.getRange().equalsIgnoreCase(indexLTP.getRange()))) {

            if((indexLTP.getIndexLTP()<Integer.parseInt(indexLTP.getSupport())) && (indexLTP.getMeanStrikePCR()>prevItrIndexLTP.getMeanStrikePCR())) {
                indexLTP.setTradeDecision(TRADE_DECISION_BUY);
            }
            else if(indexLTP.getIndexLTP()>Integer.parseInt(indexLTP.getResistance()) && (indexLTP.getMeanStrikePCR()<prevItrIndexLTP.getMeanStrikePCR())) {
                indexLTP.setTradeDecision(TRADE_DECISION_SELL);
            }
            else {
                indexLTP.setTradeDecision(TRADE_DECISION_NO_TRADE);
            }

        }
        else {
            indexLTP.setTradeDecision(TRADE_DECISION_NO_TRADE);
        }
    }

    private void calculateMeanValues(MeanOIParams meanOIParams) {
        if(meanOIParams.getMeanCallOI()!=0 && !Objects.equals(meanOIParams.getMeanCallOIChange(), D_ZERO)) {
            meanOIParams.setMeanStrikePCR(round(meanOIParams.getMeanPutOI() / meanOIParams.getMeanCallOI()));
            meanOIParams.setMeanRateOI(round(rateOICalc(meanOIParams.getMeanCallOIChange(),meanOIParams.getMeanPutOIChange())));
            meanOIParams.setCombiRate(round((meanOIParams.getMeanStrikePCR()+meanOIParams.getMeanRateOI())/I_TWO));
        }

    }

    private int processSpotInstrumentForATMPrice(Long spotInstrumentToken,IndexLTP indexLTP) {
        int atmPrice = -1;

        OHLC spotOhlc = null;
        try {
            String[] spotInstrumentTokenArray = new String[1];
            spotInstrumentTokenArray[0] = String.valueOf(spotInstrumentToken);
            Map<String, Quote> spotIndexQuote = kiteConnectConfig.kiteConnect().getQuote(spotInstrumentTokenArray);
            spotOhlc = spotIndexQuote.get(spotInstrumentTokenArray[0]).ohlc;
            Double spotLtp = spotIndexQuote.get(spotInstrumentTokenArray[0]).lastPrice;
            indexLTP.setIndexLTP(spotLtp.intValue());
            indexLTP.setIndexTS(LocalDateTime.now().withNano(0));
            indexLTP.setDayLow(formatSafeNumber(String.valueOf(spotOhlc.low)));
            indexLTP.setDayHigh(formatSafeNumber(String.valueOf(spotOhlc.high)));
            logger.info("Spot OHLC data is: {}", spotOhlc.toString());
            logger.info("Spot LTP data is: {} at time {}", spotLtp, spotIndexQuote.get(spotInstrumentTokenArray[0]).timestamp);
            atmPrice = (int) (Math.round(spotLtp / D_ONE_HUNDRED) * ONE_HUNDRED);

        } catch (KiteException | IOException | RuntimeException e) {
            logger.error("Error while fetching spot instrument quote: {}", e.getMessage());
            if (spotOhlc != null) {
                logger.error("Last known OHLC before error: {}", spotOhlc);
            }
        }

        return atmPrice;

    }

    @Transactional
    private JobIterationDetails createJobIterationDetails(JobDetails jobDetails) {

        JobIterationDetails jobIterationDetails = new JobIterationDetails();
        jobIterationDetails.setJobDetails(jobDetails);
        jobIterationDetails.setIterationStatus(STATUS_RUNNING);
        jobIterationDetails.setIterationStartTime(LocalDateTime.now().withNano(I_ZERO));

        jobIterationDetails = jobIterationRepository.save(jobIterationDetails);
        logger.info("JobIterationDetails created with id: {} for JobDetails id: {}", jobIterationDetails.getId(), jobDetails.getId());
        return jobIterationDetails;
    }

    @Transactional
    private JobIterationDetails completeJobIteration(JobIterationDetails jobIterationDetails) {

        jobIterationDetails.setIterationStatus(STATUS_COMPLETED);
        jobIterationDetails.setIterationEndTime(LocalDateTime.now().withNano(I_ZERO));
        jobIterationDetails = jobIterationRepository.save(jobIterationDetails);
        logger.info("JobIterationDetails completed with id: {} for JobDetails id: {}", jobIterationDetails.getId(), jobIterationDetails.getJobDetails().getId());
        return jobIterationDetails;
    }

    private JobDetails createJobDetailsObj(Integer appJobConfigNum, Date expiry, String jobStatus, ApplicationConstants.JobName jobName) {

        JobDetails jobDetails = new JobDetails();
        jobDetails.setAppJobConfigNum(appJobConfigNum);
        jobDetails.setJobName(jobName);
        jobDetails.setJobForExpiryDate(convertDateToLocalDate(expiry));
        jobDetails.setJobStatus(jobStatus);
        if(jobStatus.equalsIgnoreCase(STATUS_RUNNING))
            jobDetails.setJobStartTime(LocalDateTime.now().withNano(I_ZERO));
        else
            jobDetails.setJobEndTime(LocalDateTime.now().withNano(I_ZERO));


        return jobDetailsRepository.save(jobDetails);

    }

    @Override
    public void stopJobByConfigNumber(Integer appJobConfigNum) {

        List<JobDetails> jobDetailsList = jobDetailsRepository.findByAppJobConfigNumAndJobStatus(appJobConfigNum, STATUS_RUNNING);

        if (jobDetailsList.isEmpty()) {
            logger.warn("No running job found for AppJobConfigNum: {}", appJobConfigNum);
        } else {
            jobDetailsList.forEach(j -> {

                //complete iteration for jobIterationDetails if any running
                List<JobIterationDetails> jobIterationDetailsList = jobIterationRepository.findByJobDetailsIdAndIterationStatus(j.getId(), STATUS_RUNNING);
                if(!jobIterationDetailsList.isEmpty())
                {
                    jobIterationDetailsList.forEach(jid -> {
                        completeJobIteration(jid);
                        logger.info("JobIterationDetails with id: {} for JobDetails id: {} is set to COMPLETED", jid.getId(), j.getId());
                    });
                }

                j.setJobStatus(STATUS_COMPLETED);
                j.setJobEndTime(LocalDateTime.now().withNano(I_ZERO));
                jobDetailsRepository.save(j);
                logger.info("Job with id: {} for AppJobConfigNum: {} is set to STOPPED", j.getId(), appJobConfigNum);
                emailManagementService.sendEmailForJobStopped(appJobConfigNum);
            });
        }

    }

    @Override
    public boolean stopAllJobs() {

        appJobConfigRepository.findAll().forEach(appJobConfig -> stopJobByConfigNumber(appJobConfig.getAppJobConfigNum()));
        return true;

    }

    @Override
    public boolean startAllJobs() {
        appJobConfigRepository.findAll().forEach(appJobConfig -> {
            if(dailyJobPlannerService.validateDailyPlannerByConfigNum(appJobConfig.getAppJobConfigNum(), LocalDate.now()))
            {
                //check whether job is already running for the config number
                if(jobDetailsService.isJobRunning(appJobConfig.getAppJobConfigNum())) {
                    logger.warn("Job is already running for AppJobConfigNum: {}", appJobConfig.getAppJobConfigNum());
                    return ;
                }
                logger.info("Job can be started for AppJobConfigNum: {}", appJobConfig.getAppJobConfigNum());
                startJobByConfigNumber(appJobConfig.getAppJobConfigNum());
            }
        });
        return true;
    }

    private ArrayList<DeltaOICalculations> calculateDeltaOIFromIndexPrice(ArrayList<InstrumentEntity> listOfRequiredInstruments,int atmPrice,Map<Long,Double> snapshotOI,Integer tFactor,AppJobConfig appJobConfig) {

        ArrayList<DeltaOICalculations> deltaOICalculationsList = new ArrayList<>();
        Map<String,DeltaOICalculations> strikePriceOIDelta = new ConcurrentHashMap<>();

        int atmRange = appJobConfig.getAppIndexConfig().getAtmRange();

        int minStrikePrice = atmPrice - atmRange;
        int maxStrikePrice = atmPrice + atmRange;
        logger.info("For appJobConfigNum {} the minStrikePrice {} and maxStrikePrice is {} ",appJobConfig.getAppJobConfigNum(),minStrikePrice,maxStrikePrice);

        listOfRequiredInstruments.forEach(ie->{

            Instrument i = ie.getInstrument();
            DeltaOICalculations deltaOICalculations;

            if((Integer.parseInt(i.getStrike())>=minStrikePrice) && (Integer.parseInt(i.getStrike())<=maxStrikePrice)) {

                if (!strikePriceOIDelta.containsKey(i.getStrike())) {
                    deltaOICalculations = new DeltaOICalculations();
                    deltaOICalculations.setStrikePrice(i.getStrike());
                    deltaOICalculations.setCallOI(calculatedLotOI(i,tFactor));

                    deltaOICalculations.setCallOIChange(deltaOICalculations.getCallOI() - snapshotOI.get(i.getInstrument_token()));

                    strikePriceOIDelta.put(i.getStrike(), deltaOICalculations);
                    ltpMap.put(i.getStrike()+"CE", getLTPValueFromTick(i.getInstrument_token()));
                    //     logger.info("Checking for last trade price for CE: {}", ltpMap.get(i.getStrike()+"CE"));

                } else {

                    deltaOICalculations = strikePriceOIDelta.get(i.getStrike());
                    deltaOICalculations.setPutOI(calculatedLotOI(i,tFactor));
                    deltaOICalculations.setPutOIChange(deltaOICalculations.getPutOI() - snapshotOI.get(i.getInstrument_token()));
                    deltaOICalculations.setStrikePCR(round(deltaOICalculations.getPutOI() / deltaOICalculations.getCallOI()));
                    deltaOICalculations.setRateOI(round(rateOICalc(deltaOICalculations.getCallOIChange(),deltaOICalculations.getPutOIChange())));
                    deltaOICalculationsList.add(deltaOICalculations);
                    ltpMap.put(i.getStrike()+"PE", getLTPValueFromTick(i.getInstrument_token()));
                    //   logger.info("Checking for last trade price for PE: {}", ltpMap.get(i.getStrike()+"PE"));
                }

            }
        });
        return deltaOICalculationsList;
    }

    private Double calculatedLotOI(Instrument i, Integer tFactor)
    {
        Tick tick = kiteTickerProvider.tickerMapForJob.get(i.getInstrument_token());
        if (tick == null) {
            logger.warn("No tick data for instrument_token={}, returning 0", i.getInstrument_token());
            return D_ZERO;
        }
        double exchangeOI = tick.getOi();
        return ((exchangeOI / i.getLot_size()) * tFactor) / D_ONE_THOUSAND;
    }

    private Double getLTPValueFromTick(long instrumentToken) {
        Tick tick = kiteTickerProvider.tickerMapForJob.get(instrumentToken);
        if (tick == null) {
            logger.warn("No tick data for instrumentToken={}, returning 0", instrumentToken);
            return D_ZERO;
        }
        return tick.getLastTradedPrice();
    }

    private Double round(Double value) {

        return Math.round(value*D_ONE_HUNDRED)/D_ONE_HUNDRED;
    }

    public double rateOICalc(double callOIChange, double putOIChange) {
        double rateOI;

        if (callOIChange < I_ZERO) {
            if (putOIChange < I_ZERO) {
                if (Math.abs(putOIChange) > Math.abs(callOIChange)) {
                    rateOI = - (putOIChange / callOIChange);
                } else {
                    rateOI = Math.abs((Math.abs(callOIChange) - Math.abs(putOIChange)) / callOIChange);
                }
            } else {
                rateOI = Math.abs((putOIChange + Math.abs(callOIChange)) / (putOIChange - Math.abs(callOIChange)));
            }
        } else {
            if (putOIChange < I_ZERO) {
                rateOI = - (Math.abs(putOIChange) + Math.abs(callOIChange)) /
                        Math.min(Math.abs(putOIChange), Math.abs(callOIChange));
            } else {
                rateOI = (putOIChange / callOIChange);
            }
        }

        return rateOI;
    }

    private int calculateThreshold(ArrayList<DeltaOICalculations> deltaOICalculationsList,int defaultThreshold) {
        //Threshold calculation
        double callOIMean = deltaOICalculationsList.stream()
                .mapToDouble(c->c.callOI!=null?c.callOI:D_ZERO)
                .average()
                .orElse(D_ZERO);
        logger.debug("callOIMean::{}", callOIMean);
        double putOIMean = deltaOICalculationsList.stream()
                .mapToDouble(c->c.putOI!=null?c.putOI:D_ZERO)
                .average()
                .orElse(D_ZERO);
        logger.debug("putOIMean::{}", putOIMean);
        int avg = ((int) Math.min(callOIMean,putOIMean));
        logger.debug("avg::{}", avg);

        int threshold = (avg/defaultThreshold) *defaultThreshold;

        if(threshold==0)
            threshold=defaultThreshold;

        return threshold;
    }

    private List<DeltaOICalculations> filterListUsingThreshold(List<DeltaOICalculations> deltaOICalculationsList, int threshold,MeanOIParams meanOIParams) {

        List<DeltaOICalculations> deltaOIList = new ArrayList<>();
        List<Integer> strikePriceListAboveThreshold = new ArrayList<>();
        deltaOICalculationsList.forEach(df -> {
         //   logger.info("StrikePrice: {}, CallOI: {}, PutOI: {}", df.getStrikePrice(), df.getCallOI(), df.getPutOI());
            if(df.getCallOI()>=threshold && df.getPutOI()>=threshold)
            {
                logger.info("StrikePrice above threshold {} :: CallOI: {}, PutOI: {}", df.getStrikePrice(), df.getCallOI(), df.getPutOI());
                strikePriceListAboveThreshold.add(Integer.parseInt(df.getStrikePrice()));
            }
        });


        if(!strikePriceListAboveThreshold.isEmpty()) {
            Collections.sort(strikePriceListAboveThreshold);
            logger.info("MinSP:{}, MaxSP:{}", strikePriceListAboveThreshold.getFirst(), strikePriceListAboveThreshold.getLast());

            int minSP = strikePriceListAboveThreshold.getFirst();
            int maxSP = strikePriceListAboveThreshold.getLast();

            meanOIParams.setMeanPutOIChange(D_ZERO);
            meanOIParams.setMeanCallOIChange(D_ZERO);

            List<DeltaOICalculations> deduped = new ArrayList<>(
                    deltaOICalculationsList.stream()
                            .collect(Collectors.toMap(
                                    DeltaOICalculations::getStrikePrice,      // key for deduplication
                                    Function.identity(),                      // value
                                    (existing, replacement) -> existing,      // keep first occurrence
                                    LinkedHashMap::new                        // preserve order
                            ))
                            .values()
            );

            deduped.forEach(df -> {

                DeltaOICalculations deltaOICalculations;
                if ((Integer.parseInt(df.getStrikePrice()) >= minSP) && (Integer.parseInt(df.getStrikePrice()) <= maxSP)) {
                    deltaOICalculations = new DeltaOICalculations(df.getStrikePrice(), df.getRateOI(), df.getStrikePCR(), df.getCallOI(), df.getPutOI(),df.getCallOIChange(),df.getPutOIChange());
                    deltaOIList.add(deltaOICalculations);

                    meanOIParams.setMeanPutOIChange(meanOIParams.getMeanPutOIChange() + df.getPutOIChange());
                    meanOIParams.setMeanCallOIChange(meanOIParams.getMeanCallOIChange() + df.getCallOIChange());

                }
            });
        }



        deltaOIList.sort(Comparator.comparingInt(o -> Integer.parseInt(o.getStrikePrice())));

        return deltaOIList;
    }


    private List<MiniDelta> calculateMiniDeltaList(List<DeltaOICalculations> deltaOIMiniList, JobIterationDetails jobIterationDetails,Integer appJobConfigNum,MeanOIParams meanOIParams) {

        List<MiniDelta> miniDeltaList = new ArrayList<>();
        MiniDelta totalRec = new MiniDelta();
        Double sumPutOIChange = D_ZERO, sumCallOIChange = D_ZERO;

        Double meanPutOI = D_ZERO;
        Double meanCallOI = D_ZERO;

        for (DeltaOICalculations d : deltaOIMiniList) {

            MiniDelta miniDelta = new MiniDelta();
            miniDelta.setDeltaInstant(LocalDateTime.now().withNano(I_ZERO));
            miniDelta.setStrikePrice(d.getStrikePrice());
            miniDelta.setStrikePCR(d.getStrikePCR());
            miniDelta.setRateOI(d.getRateOI());
            miniDelta.setCallOI(d.getCallOI());
            miniDelta.setPutOI(d.getPutOI());
            miniDelta.setCallOIChange(d.getCallOIChange());
            miniDelta.setPutOIChange(d.getPutOIChange());
            miniDelta.setJobIterationDetails(jobIterationDetails);
            miniDelta.setAppJobConfigNum(appJobConfigNum);

            miniDeltaList.add(miniDelta);

            meanPutOI+=d.getPutOI();
            meanCallOI+=d.getCallOI();
            sumPutOIChange+=d.getPutOIChange();
            sumCallOIChange+=d.getCallOIChange();

        }

        meanOIParams.setMeanPutOI(meanPutOI);
        meanOIParams.setMeanCallOI(meanCallOI);

        totalRec.setStrikePrice(TOTAL);
        totalRec.setPutOI(meanOIParams.getMeanPutOI());
        totalRec.setCallOI(meanOIParams.getMeanCallOI());
        totalRec.setPutOIChange(sumPutOIChange);
        totalRec.setCallOIChange(sumCallOIChange);
        totalRec.setStrikePCRColor(FONT_BOLD);
        totalRec.setRateOIColor(FONT_BOLD);
        totalRec.setCallOIColor(FONT_BOLD);
        totalRec.setPutOIColor(FONT_BOLD);
        totalRec.setCallOIChangeColor(FONT_BOLD);
        totalRec.setPutOIChangeColor(FONT_BOLD);
        totalRec.setJobIterationDetails(jobIterationDetails);
        totalRec.setAppJobConfigNum(appJobConfigNum);

        miniDeltaList.add(totalRec);

        return miniDeltaList;
    }

    private void calculateIndexLTPWithSupportAndResistance(IndexLTP indexLTP, List<DeltaOICalculations> deltaOIMiniList, List<DeltaOICalculations> deltaOICalculationsList, AppJobConfig appJobConfig,MeanOIParams meanOIParams) {

        indexLTP.setMeanStrikePCR(meanOIParams.getMeanStrikePCR());
        indexLTP.setMeanRateOI(meanOIParams.getMeanRateOI());
        indexLTP.setCombiRate(meanOIParams.getCombiRate());
        indexLTP.setRange(deltaOIMiniList.getFirst().getStrikePrice()+"-"+deltaOIMiniList.getLast().getStrikePrice());
        calculateSupportAndResistance(indexLTP,deltaOICalculationsList,appJobConfig);

    }

    private void calculateSupportAndResistance(IndexLTP indexLTP, List<DeltaOICalculations> deltaOICalculationsList, AppJobConfig appJobConfig) {

        Integer support=0;
        Integer resistance=0;

        deltaOICalculationsList.sort(Comparator.comparingInt(o -> Integer.parseInt(o.getStrikePrice())));

        int halfPriceGap = appJobConfig.getAppIndexConfig().getPriceGap()/I_TWO;

        if((indexLTP.getMeanStrikePCR()>UPPER_TARGET_VALUE && indexLTP.getMeanRateOI()>UPPER_TARGET_VALUE) ||
                ((indexLTP.getMeanStrikePCR()>RATE_OI_TARGET_VALUE && indexLTP.getMeanRateOI()>RATE_OI_TARGET_VALUE) && (indexLTP.getMeanStrikePCR()>MID_TARGET_VALUE || indexLTP.getMeanRateOI()>MID_TARGET_VALUE)))

        {
            logger.info("Calculating support and resistance with sentiment: bullish");
            support = findStrikePCR(deltaOICalculationsList,RATE_OI_TARGET_VALUE,RATE_OI_TARGET_VALUE,false) + halfPriceGap;
            resistance = findStrikePCR(deltaOICalculationsList,LOWER_TARGET_VALUE,RATE_OI_TARGET_VALUE,false) + halfPriceGap;
        }
        else if((indexLTP.getMeanStrikePCR()>LOWER_TARGET_VALUE) && (indexLTP.getMeanRateOI()>LOWER_TARGET_VALUE))
        {
            logger.info("Calculating support and resistance with sentiment: neutral");
            support = findStrikePCR(deltaOICalculationsList,UPPER_TARGET_VALUE,RATE_OI_TARGET_VALUE,false) + halfPriceGap;
            resistance = findStrikePCR(deltaOICalculationsList,RATE_OI_TARGET_VALUE,RATE_OI_TARGET_VALUE,false) + halfPriceGap;
        }
        else
        {
            logger.info("Calculating support and resistance with sentiment: bearish");
            support = findStrikePCR(deltaOICalculationsList, UPPER_TARGET_VALUE, RATE_OI_TARGET_VALUE,false);
            resistance = findStrikePCR(deltaOICalculationsList, RATE_OI_TARGET_VALUE,D_ZERO,false);
        }

        //re-adjust support and resistance based on range

        int rangeLow = Integer.parseInt(indexLTP.getRange().split("-")[0]);
        int rangeHigh = Integer.parseInt(indexLTP.getRange().split("-")[1]);

        if(support<rangeLow)
            support=rangeLow;
        else if(support>rangeHigh)
            support=rangeHigh;

        if(resistance<rangeLow)
            resistance=rangeLow;
        else if(resistance>rangeHigh)
            resistance=rangeHigh;

        indexLTP.setSupport(String.valueOf(support));
        indexLTP.setResistance(String.valueOf(resistance));

    }

    private Integer findStrikePCR(List<DeltaOICalculations> deltaOIMiniList, Double targetPCR, Double targetRateOI,boolean considerNegativeRateOI)
    {
        Integer response=0;

        List<DeltaOICalculations> filteredList=null;

        if(considerNegativeRateOI)
            filteredList  = deltaOIMiniList.stream().filter(d->d.getStrikePCR()>targetPCR && (d.getRateOI()>targetRateOI || d.getRateOI()<D_ZERO)).toList();
        else
            filteredList  = deltaOIMiniList.stream().filter(d->d.getStrikePCR()>targetPCR && d.getRateOI()>targetRateOI).toList();

        Optional<DeltaOICalculations> maxStrikePriceItem = filteredList.stream()
                .max(Comparator.comparingInt(d -> Integer.parseInt(d.getStrikePrice())));


        if (maxStrikePriceItem.isPresent()) {
            DeltaOICalculations itemWithMaxStrikePrice = maxStrikePriceItem.get();
            response = Integer.parseInt(itemWithMaxStrikePrice.getStrikePrice());
        }

        return response;
    }

    public MaxPain calculateMaxPain(List<MiniDelta> miniDeltaList,Integer appJobConfigNum) {

        MaxPain maxPain = new MaxPain();

        // Calculate loss at each strike and determine min
        Map<Integer, Long> losses = new HashMap<>();
        miniDeltaList.forEach(i -> {
            if (i.getStrikePrice().equals(TOTAL))
                return;
            losses.put(Integer.parseInt(i.getStrikePrice()), calculateTotalLossForSP(Integer.parseInt(i.getStrikePrice()), miniDeltaList));
        });

        maxPain.setLossMatrix(losses);

        // Sort losses by value and extract the minimum and second minimum
        List<Integer> result = losses.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .limit(I_TWO)
                .map(Map.Entry::getKey)
                .toList();

        logger.info("Max Pain calculated: {}, Second Max Pain: {}", result.get(0), result.size() > 1 ? result.get(1) : "N/A");

        maxPain.setMaxPainSP(result.get(0));
        if (result.size() > 1)
            maxPain.setMaxPainSPSecond(result.get(1));
        else
            maxPain.setMaxPainSPSecond(result.get(0));



     /*   logger.info("Total Losses at Each Strike:");
        losses.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    logger.info("Strike {}: Loss = {}", entry.getKey(), entry.getValue());

                }); */


        int maxPainStrike = result.get(0);

        // Get sorted strikes
        List<Integer> sortedStrikes = losses.keySet().stream().sorted().toList();

        long totalLeft = 0L;
        long totalRight = 0L;
        for (Integer sp : sortedStrikes) {
            long val = losses.getOrDefault(sp, 0L);
            if (sp < maxPainStrike) totalLeft += val;
            else if (sp > maxPainStrike) totalRight += val;
        }

        float maxPainBiasRatio;
        if (totalRight == 0L) {
            maxPainBiasRatio = 0f; // or Float.POSITIVE_INFINITY if you prefer
        } else {
            maxPainBiasRatio = (float) totalLeft / (float) totalRight;
        }

        logger.info("Max Pain Bias Ratio: {}", maxPainBiasRatio);
        maxPain.setMaxPainBiasRatio(maxPainBiasRatio);

        return maxPain;
    }

    private long calculateTotalLossForSP(int expiryStrike, List<MiniDelta> miniDeltaList) {
        long totalLoss = 0;
        for (MiniDelta data : miniDeltaList) {
            // Assuming data.getStrikePrice() returns a string representation of the strike price
            if(data.getStrikePrice().equals(TOTAL)) {
                continue;
            }
            int sp = Integer.parseInt(data.getStrikePrice());
            int peOI = data.getPutOI().intValue();
            int ceOI = data.getCallOI().intValue();

            // Puts: If strike > expiry, loss = (strike - expiry) * OI
            if (sp > expiryStrike) {
                totalLoss += (long) (sp - expiryStrike) * peOI;
            }

            // Calls: If strike < expiry, loss = (expiry - strike) * OI
            if (sp < expiryStrike) {
                totalLoss += (long) (expiryStrike - sp) * ceOI;
            }
        }
        return totalLoss;
    }

    @Transactional
    private boolean runOISnapshotJob(Integer appJobConfigNum) {

        AppJobConfig appJobConfig = appJobConfigRepository.findById(appJobConfigNum)
                .orElseThrow(() -> new IllegalArgumentException("No AppJobConfig found for appJobConfigNum=" + appJobConfigNum));
        logger.info("Taking OI Snapshot for AppJobConfig: {}", appJobConfig);
        ArrayList<InstrumentEntity> listOfRequiredInstruments = instrumentService.getInstrumentsFromAppJobConfigNum(appJobConfigNum);
        logger.info("Total instruments found for snapshot {} for AppJobConfig: {}", listOfRequiredInstruments.size(),appJobConfigNum);
        logger.info("SnapshotJob: Processing for expiry: {} for AppJobConfig: {}", listOfRequiredInstruments.getFirst().getInstrument().getExpiry(),appJobConfigNum);
        ArrayList<OISnapshotEntity> listOfSnapshotTokens = new ArrayList<>();

        Integer tFactor = appJobConfig.getAppIndexConfig().getTFactor();

        JobDetails jobDetails = new JobDetails();
        jobDetails.setAppJobConfigNum(appJobConfigNum);
        jobDetails.setJobName(JobName.OISNAPSHOT);
        jobDetails.setJobStatus(STATUS_RUNNING);
        jobDetails.setJobStartTime(LocalDateTime.now().withNano(I_ZERO));
        jobDetails = jobDetailsRepository.save(jobDetails);


       ArrayList<Long> instrumentTokens = listOfRequiredInstruments
                    .parallelStream()
                    .map(i-> i.getInstrument().getInstrument_token())
                    .collect(Collectors.toCollection(ArrayList::new));

        try {

            logger.info("Subscribing to {} tokens for OI Snapshot with first token: {}", instrumentTokens.size(), instrumentTokens.getFirst());

            kiteTickerProvider.subscribeTokenForJob(instrumentTokens);

           try {
                Thread.sleep(ONE_THOUSAND);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

      //     logger.info("Map of ticker data received for OI Snapshot job has {} entries for AppJobConfig: {}", kiteTickerProvider.tickerMapForJob.size(), appJobConfigNum);

            if (!kiteTickerProvider.tickerMapForJob.isEmpty()) {
                listOfRequiredInstruments.forEach(i -> {

                    OISnapshotEntity oISnapshotEntity = oiSnapshotRepository.findById(i.getInstrument().getInstrument_token()).orElse(new OISnapshotEntity());

                    oISnapshotEntity.setInstrument_token(i.getInstrument().getInstrument_token());
                    oISnapshotEntity.setOi(calculatedLotOI(i.getInstrument(),tFactor));
                    oISnapshotEntity.setName(i.getInstrument().getName());
                    oISnapshotEntity.setTradingsymbol(i.getInstrument().getTradingsymbol());
                    Date tickTimestamp = kiteTickerProvider.tickerMapForJob.get(i.getInstrument().getInstrument_token()).getTickTimestamp();
                    oISnapshotEntity.setTickTimestamp(tickTimestamp != null ? tickTimestamp : new Date());

                    listOfSnapshotTokens.add(oISnapshotEntity);

                });
                oiSnapshotRepository.saveAll(listOfSnapshotTokens);

                kiteTickerProvider.unsubscribeTokensForSnapshotJob(instrumentTokens);

                logger.info("OI Snapshot saved successfully for {} instruments for AppJobConfig: {}", listOfSnapshotTokens.size(), appJobConfigNum);

                String currentDateTime = LocalDateTime.now().toString().split("\\.")[0].replace(":", "_");
                String fileName = JobName.OISNAPSHOT + "_" + appJobConfig.getAppIndexConfig().getStrikePriceName() + "_" + appJobConfig.getJobType().getJobType() + "_"+ currentDateTime + ".json";
                fileName = fileName.replace(" ", "_");
                boolean writeOISnapshotToFile = fileService.writeObjToFile(listOfSnapshotTokens, fileName);

                CommonReqRes message = new CommonReqRes();
                message.setMessage(appJobConfig.getAppIndexConfig().getStrikePriceName() + " OI Snapshot is taken successfully!");
                message.setStatus(true);
                message.setType(SUCCESS);
                messagingService.sendCommonMessage(message);

                jobDetails.setJobEndTime(LocalDateTime.now().withNano(I_ZERO));
                jobDetails.setJobStatus(STATUS_COMPLETED);
                jobDetails.setJobForExpiryDate(convertDateToLocalDate(listOfRequiredInstruments.getFirst().getInstrument().getExpiry()));
                jobDetailsRepository.save(jobDetails);

                emailManagementService.sendEmailAsync(fileName,appJobConfig);

                return true;
            }


        } catch (Exception e) {
            logger.error("Error in taking snapshot : {} ", e.getMessage());
            jobDetails.setJobEndTime(LocalDateTime.now().withNano(I_ZERO));
            jobDetails.setJobStatus(STATUS_FAILED);
            jobDetails.setJobForExpiryDate(convertDateToLocalDate(listOfRequiredInstruments.getFirst().getInstrument().getExpiry()));
            jobDetailsRepository.save(jobDetails);
            kiteTickerProvider.unsubscribeTokensForSnapshotJob(instrumentTokens);
            return false;
        }

        return false;
    }

    private void calculateColorCoding(List<MiniDelta> miniDeltaList, List<MiniDelta> prevItrMiniDeltaList) {


        for (MiniDelta current : miniDeltaList) {

            for (MiniDelta previous : prevItrMiniDeltaList) {
                if (current.getStrikePrice().equals(previous.getStrikePrice())) {
                    current.setRateOIColor(getColor(current.getRateOI(), previous.getRateOI()));
                    current.setStrikePCRColor(getColor(current.getStrikePCR(), previous.getStrikePCR()));
                    current.setCallOIColor(getColor(current.getCallOI(), previous.getCallOI()));
                    current.setPutOIColor(getColor(current.getPutOI(), previous.getPutOI()));
                    current.setCallOIChangeColor(getColor(current.getCallOIChange(), previous.getCallOIChange()));
                    current.setPutOIChangeColor(getColor(current.getPutOIChange(), previous.getPutOIChange()));
                    break;
                }
            }

        }

    }

    private String getColor(Double currentValue, Double previousValue) {
        if (currentValue != null && previousValue != null) {

            if (currentValue > previousValue) {
                return COLOR_GREEN;
            } else if (currentValue < previousValue) {
                return COLOR_RED;
            } else {
                return COLOR_BLACK;
            }
        }
        return COLOR_BLACK;
    }

    @Override
    public List<TradeDecisions> getTradeDecisionsByConfigNum(Integer appJobConfigNum) {

        try {
            List<TradeDecisions> tradeDecisions;
            logger.info("Fetching trade decisions for appJobConfigNum: {}", appJobConfigNum);
            if (appJobConfigNum == 0) {
                tradeDecisions = tradeDecisionRepository.findLatestTradesForAllIndex();
            } else {
                tradeDecisions = tradeDecisionRepository.findLatestTradesByAppJobConfigNum(appJobConfigNum);
            }

            logger.info("Number of trade decisions fetched: {}", (tradeDecisions != null) ? tradeDecisions.size() : 0);
            // Handle null or empty responses
            return (tradeDecisions != null) ? tradeDecisions : new ArrayList<>();

        } catch (Exception e) {
            // Log the exception for debugging purposes
            logger.error("Error fetching trade decisions for appJobConfigNum: {}", appJobConfigNum, e);
            return new ArrayList<>();
        }
    }

    @Override
    public List<IndexLTP> getIndexLTPDataByConfigNum(Integer appJobConfigNum) {
        try {
            List<IndexLTP> indexLTPValues;

            if (appJobConfigNum == 0) {
                indexLTPValues = indexLTPRepository.findLatestIndexDataForAllIndex();
            } else {
                indexLTPValues = indexLTPRepository.findLatestIndexDataByAppJobConfigNum(appJobConfigNum);
            }

            // Handle null or empty responses
            return (indexLTPValues != null) ? indexLTPValues : new ArrayList<>();

        } catch (Exception e) {
            // Log the exception for debugging purposes
            logger.error("Error fetching trade decisions for appJobConfigNum: {}", appJobConfigNum, e);
            return new ArrayList<>();
        }
    }

    @Override
    public List<MiniDelta> getMiniDeltaDataByAppJobConfigNum(Integer appJobConfigNum) {
        return miniDeltaRepository.findByAppJobConfigNumOrderByIdAsc(appJobConfigNum);
    }

    @Override
    public Map<Integer, SwingHighLow> getSwingHighLowByConfigNum(Integer appJobConfigNum) {

        Map<Integer,SwingHighLow> swingHighLowMap = new HashMap<>();
     //   prepareTestCandleSticks();
        SwingHighLow swingHighLow = calculateSwingHighLow();
        swingHighLowMap.put(appJobConfigNum,swingHighLow);
        return swingHighLowMap;

    }

    @Override
    public Map<Integer, LTPTrackerConfig> updateLTPTrackerConfig(List<LTPTrackerConfig> ltpTrackerConfigs) {

        if (ltpTrackerConfigs == null || ltpTrackerConfigs.isEmpty()) {
            return ltpTrackerConfigMap;
        }

        Map<Integer, LTPTrackerConfig> toPut = ltpTrackerConfigs.stream()
                .collect(Collectors.toMap(
                        LTPTrackerConfig::getAppJobConfigNum,
                        Function.identity(),
                        (existing, replacement) -> replacement
                ));

        ltpTrackerConfigMap.putAll(toPut);

        return ltpTrackerConfigMap;
    }

    @Override
    public Map<Integer, Integer> getRetryCounterByConfigNum(int appJobConfigNum) {

            return retryCounter;

    }

    private SwingHighLow calculateSwingHighLow() {

        logger.info("Calculating Swing High and Swing Low from CandleStick data: {}", kiteTickerProvider.candleSticks.toString());

       CandleStick swingHigh = calculateSwingHigh();
       CandleStick swingLow = calculateSwingLow();

       return new SwingHighLow(swingHigh,swingLow);
    }

    private CandleStick calculateSwingLow() {

        List<CandleStick> candleStickList = new ArrayList<>();
        if(!kiteTickerProvider.candleSticks.isEmpty())
            candleStickList = kiteTickerProvider.candleSticks;

        for(int i=candleStickList.size()-2;i>0;i--)
        {
            if(validatedCandleSequence(candleStickList.get(i-1),candleStickList.get(i),candleStickList.get(i+1))) {
                if (candleStickList.get(i).getLowPrice() < candleStickList.get(i + 1).getLowPrice() && candleStickList.get(i).getLowPrice() < candleStickList.get(i - 1).getLowPrice()) {
                    return candleStickList.get(i);
                }
            }

        }
        return new CandleStick();
    }

    private CandleStick calculateSwingHigh() {

        List<CandleStick> candleStickList = new ArrayList<>();
        if(!kiteTickerProvider.candleSticks.isEmpty())
            candleStickList = kiteTickerProvider.candleSticks;

        for(int i=candleStickList.size()-2;i>0;i--)
        {
            if(validatedCandleSequence(candleStickList.get(i-1),candleStickList.get(i),candleStickList.get(i+1))) {
                if (candleStickList.get(i).getHighPrice() > candleStickList.get(i + 1).getHighPrice() && candleStickList.get(i).getHighPrice() > candleStickList.get(i - 1).getHighPrice()) {
                    return candleStickList.get(i);
                }
            }

        }
        return new CandleStick();
    }

    private boolean validatedCandleSequence(CandleStick previous, CandleStick current, CandleStick next) {
        return current.getCandleStartTime().minusMinutes(1).equals(previous.getCandleStartTime()) &&
                current.getCandleStartTime().plusMinutes(1).equals(next.getCandleStartTime());
    }


    public TradeDecisions saveTradeDecision(IndexLTP indexLTP, JobIterationDetails jobIterationDetails, String tradeDecisionStatus,AppJobConfig appJobConfig) {
        String tradeDecision = indexLTP.getTradeDecision();
        TradeDecisions tradeDecisions = new TradeDecisions();
        tradeDecisions.setAppJobConfig(appJobConfig);
        tradeDecisions.setIndexLTP(indexLTP.getIndexLTP());
        tradeDecisions.setStatus(tradeDecisionStatus);
        tradeDecisions.setTradeDecision(tradeDecision);
        tradeDecisions.setJobIterationDetails(jobIterationDetails);
        tradeDecisions.setTradeDecisionTS(indexLTP.getIndexTS());
        tradeDecisions.setEntryIndexLTP(tradeDecision.equalsIgnoreCase(TRADE_DECISION_BUY) ? Integer.parseInt(indexLTP.getSupport()) - TWENTY_FOUR : Integer.parseInt(indexLTP.getResistance()) + TWENTY_FOUR);
        tradeDecisions.setTargetIndexLTP(tradeDecision.equalsIgnoreCase(TRADE_DECISION_BUY) ? Integer.parseInt(indexLTP.getResistance()) - D_FIVE.intValue() : Integer.parseInt(indexLTP.getSupport()) + D_FIVE.intValue());
        tradeDecisions.setStopLossIndexLTP(tradeDecision.equalsIgnoreCase(TRADE_DECISION_BUY) ? Integer.parseInt(indexLTP.getSupport()) - appJobConfig.getAppIndexConfig().getPriceGap() : Integer.parseInt(indexLTP.getResistance()) + appJobConfig.getAppIndexConfig().getPriceGap());

        tradeDecisions = tradeDecisionRepository.save(tradeDecisions);
        logger.info("Trade Decision saved successfully: {}", tradeDecisions.toString());
        return tradeDecisions;
    }

    public TradeDecisions saveTradeDecisionSL(IndexLTP indexLTP, JobIterationDetails jobIterationDetails, String tradeDecisionStatus, AppJobConfig appJobConfig, Integer stopLossIndexLTP) {
        String tradeDecision = indexLTP.getTradeDecision();
        TradeDecisions tradeDecisions = new TradeDecisions();
        tradeDecisions.setAppJobConfig(appJobConfig);
        tradeDecisions.setIndexLTP(indexLTP.getIndexLTP());
        tradeDecisions.setStatus(tradeDecisionStatus);
        tradeDecisions.setTradeDecision(tradeDecision);
        tradeDecisions.setTradeDecisionType(TRADE_DECISION_TYPE_STOPLOSS);
        tradeDecisions.setJobIterationDetails(jobIterationDetails);
        tradeDecisions.setTradeDecisionTS(indexLTP.getIndexTS());
        tradeDecisions.setEntryIndexLTP(indexLTP.getIndexLTP());

        // parse day high/low, take integer portion (truncate decimals) and use ints for setTargetIndexLTP
        double dayHighD = Double.parseDouble(indexLTP.getDayHigh());
        double dayLowD  = Double.parseDouble(indexLTP.getDayLow());

        int dayHigh = (int) Math.floor(dayHighD); // integer portion of dayHigh
        int dayLow  = (int) Math.floor(dayLowD);  // integer portion of dayLow

        TradeDecisions previousTradeDecision = lastTradeDecisionsMap.get(appJobConfig.getAppJobConfigNum());

        if(tradeDecisions.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_BUY)) {
            tradeDecisions.setTargetIndexLTP(Math.min(dayHigh, previousTradeDecision.getStopLossIndexLTP() + appJobConfig.getAppIndexConfig().getPriceGap()/I_TWO));
            if(appJobConfig.getAutoTradeEnabled())
                tradeDecisions.setSwingTarget(calculateSwingLow().getLowPrice());

        }

        else {
            tradeDecisions.setTargetIndexLTP(Math.max(dayLow, previousTradeDecision.getStopLossIndexLTP() - appJobConfig.getAppIndexConfig().getPriceGap()/I_TWO));
            if(appJobConfig.getAutoTradeEnabled())
                tradeDecisions.setSwingTarget(calculateSwingHigh().getHighPrice());

        }

        int stopLossIndexLTP1 = tradeDecision.equalsIgnoreCase(TRADE_DECISION_BUY) ? stopLossIndexLTP - appJobConfig.getAppIndexConfig().getPriceGap()/I_TWO : stopLossIndexLTP + appJobConfig.getAppIndexConfig().getPriceGap()/I_TWO;
        if(appJobConfig.getAutoTradeEnabled()) {
         if (tradeDecisions.isConfirmationTaken())
             tradeDecisions.setStopLossIndexLTP(stopLossIndexLTP1);
         else
             tradeDecisions.setStopLossIndexLTP(0);
     }

     else
         tradeDecisions.setStopLossIndexLTP(stopLossIndexLTP1);

        tradeDecisions = tradeDecisionRepository.save(tradeDecisions);
        logger.info("STOP-LOSS Trade Decision saved successfully: {}", tradeDecisions.toString());
        return tradeDecisions;
    }

    public Boolean sendCommonMessage(String messageBody, Boolean status, String type) {
        try {
            CommonReqRes message = new CommonReqRes();
            message.setMessage(messageBody);
            message.setStatus(status);
            message.setType(type);
            messagingService.sendCommonMessage(message);
            return true;
        } catch (Exception e) {
            logger.error("Error sending notification: {}", e.getMessage());
            return false;
        }
    }

    private JobIterationDetails processSellSignal(IndexLTP indexLTP, TradeDecisions lastTradeDecisions, JobIterationDetails jobIterationDetails,AppJobConfig appJobConfig) {
        logger.info("Checking conditions to check whether Sell signal is hit or failed");
        logger.info("SELL SIGNAL :: Current IndexLTP: {}, Last trade decision:: Target : {}, StopLoss: {}",indexLTP.getIndexLTP(),lastTradeDecisions.getTargetIndexLTP(),lastTradeDecisions.getStopLossIndexLTP());


      if(lastTradeDecisions.getTradeDecisionType().equalsIgnoreCase(TRADE_DECISION_TYPE_REGULAR)) {

          if (indexLTP.getIndexLTP() <= lastTradeDecisions.getTargetIndexLTP()) {
              //exit market if order was placed
              logger.info("Target hit for SELL order, exiting market and updating trade decision");
              // If target is hit, set status to ORDER_LAPSED
              lastTradeDecisions.setStatus(ORDER_COMPLETE);
              lastTradeDecisions.setTrade_decision_result(TARGET_HIT);
              lastTradeDecisions.setTrade_decision_result_ts(LocalDateTime.now());
              tradeDecisionRepository.save(lastTradeDecisions);
              logger.info("Target hit for SELL order, setting status to ORDER_COMPLETE: {}, {}", indexLTP.toString(), lastTradeDecisions.toString());

              sendCommonMessage("Trade decision is set successfully to COMPLETE", true, SUCCESS);

              //trigger email notification for target hit and initiate a new buy signal
              indexLTP.setTradeDecision(TRADE_DECISION_SELL);

              // create a new buy signal
              jobIterationDetails = processTrendingSignal(indexLTP, jobIterationDetails, appJobConfig, lastTradeDecisions);
          } else if (indexLTP.getIndexLTP() >= lastTradeDecisions.getStopLossIndexLTP()) {
              //exit market if order was placed
              logger.info("Stop Loss hit for SELL order, exiting market and updating trade decision");
              // If stop loss is hit, set status to ORDER_LAPSED
              lastTradeDecisions.setStatus(ORDER_LAPSED);
              lastTradeDecisions.setTrade_decision_result(STOPLOSS_HIT);
              lastTradeDecisions.setTrade_decision_result_ts(LocalDateTime.now());
              logger.info("Stop Loss hit for SELL order, setting status to ORDER_LAPSED: {}, {}", indexLTP, lastTradeDecisions);
              tradeDecisionRepository.save(lastTradeDecisions);
              sendCommonMessage("STOP-LOSS HIT: SELL SIGNAL FAILED", true, SUCCESS);
              // trigger email notification for stop loss hit
              // create a new buy signal
              indexLTP.setTradeDecision(TRADE_DECISION_BUY);
              //process stoploss signal
              jobIterationDetails = processSLSignal(indexLTP, jobIterationDetails, appJobConfig, lastTradeDecisions.getStopLossIndexLTP());
          }
      }

      else if(lastTradeDecisions.getTradeDecisionType().equalsIgnoreCase(TRADE_DECISION_TYPE_TRENDING))
      {
          logger.info("Processing TRENDING SELL order conditions");
          logger.info("TRENDING SELL SIGNAL :: Current IndexLTP: {}, Last trade decision:: Target : {}, StopLoss: {}",indexLTP.getIndexLTP(),lastTradeDecisions.getTargetIndexLTP(), lastTradeDecisions);

          //exit clause
          if(!indexLTP.getTradeDecision().equalsIgnoreCase(lastTradeDecisions.getTradeDecision()) && !indexLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_NO_TRADE)) {

              //exit market
              logger.info("Exiting market as opposite signal received for Trending SELL order");
              // If stop loss is hit, set status to ORDER_LAPSED
              lastTradeDecisions.setStatus(ORDER_LAPSED);
              lastTradeDecisions.setTrade_decision_result(FAILURE);
              lastTradeDecisions.setTrade_decision_result_ts(LocalDateTime.now());
              logger.info("Trending SELL order FAILED, setting status to ORDER_LAPSED: {}, {}", indexLTP, lastTradeDecisions);
              tradeDecisionRepository.save(lastTradeDecisions);
              sendCommonMessage(" SELL SIGNAL (TRENDING) FAILED", true, ERROR);
              sendTradeDecisionTelegramAlert(indexLTP, lastTradeDecisions, appJobConfig, indexLTP.getTradeDecision() + " Signal Processing: Close the Trending trade");


          }

          //handle target hit
          if(indexLTP.getIndexLTP() <= lastTradeDecisions.getTargetIndexLTP()) {

              logger.info("Target hit for TRENDING SELL order, exiting market and updating trade decision");
              lastTradeDecisions.setStatus(ORDER_COMPLETE);
              lastTradeDecisions.setTrade_decision_result(TARGET_HIT);
              lastTradeDecisions.setTrade_decision_result_ts(LocalDateTime.now());
              tradeDecisionRepository.save(lastTradeDecisions);
              logger.info("Target hit for TRENDING SELL order, setting status to ORDER_COMPLETE: {}, {}", indexLTP.toString(), lastTradeDecisions.toString());
              sendCommonMessage("Trade decision is set successfully to COMPLETE", true, SUCCESS);
              emailManagementService.sendEmailForClosingTrade(indexLTP, appJobConfig, lastTradeDecisions);

          }

          if(appJobConfig.getAutoTradeEnabled()) {

              logger.info("AutoTrade flag: {},Checking Auto Trade conditions for Trending SELL order", true);

              if (indexLTP.getIndexLTP() >= lastTradeDecisions.getSwingTarget() && !lastTradeDecisions.isSwingTaken()) {
                  logger.info("Swing Target {} hit for TR SELL order", lastTradeDecisions.getSwingTarget());
                  sendCommonMessage("SWING TARGET HIT for TR SELL order", true, SUCCESS);
                  lastTradeDecisions.setSwingTaken(true);
                  lastTradeDecisions = tradeDecisionRepository.save(lastTradeDecisions);
                  lastTradeDecisionsMap.put(appJobConfig.getAppJobConfigNum(), lastTradeDecisions);
              }

              if (lastTradeDecisions.isSwingTaken() && indexLTP.getIndexLTP() <= lastTradeDecisions.getEntryIndexLTP() && !lastTradeDecisions.isConfirmationTaken()) {
                  lastTradeDecisions.setConfirmationTaken(true);
                  lastTradeDecisions = tradeDecisionRepository.save(lastTradeDecisions);
                  lastTradeDecisionsMap.put(appJobConfig.getAppJobConfigNum(), lastTradeDecisions);
                  logger.info("Confirmation condition met for TR SELL order after Swing Target and confirmation hit");
                  sendCommonMessage("Confirmation condition met for TR SELL order after Swing Target and confirmation hit", true, SUCCESS);
                  processAutoTradeSignal(appJobConfig, indexLTP, lastTradeDecisions);
              }


          }

      }

        else if(lastTradeDecisions.getTradeDecisionType().equalsIgnoreCase(TRADE_DECISION_TYPE_STOPLOSS))
        {
            logger.info("Processing SL SELL order conditions");
            logger.info("SL SELL SIGNAL :: Current IndexLTP: {}, Last trade decision:: Target : {}, StopLoss: {}",indexLTP.toString(),lastTradeDecisions.getTargetIndexLTP(),lastTradeDecisions.toString());
            if(!indexLTP.getTradeDecision().equalsIgnoreCase(lastTradeDecisions.getTradeDecision()) && !indexLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_NO_TRADE)) {
                //exit market if order was placed
                logger.info("Exiting market as opposite signal received for SL SELL order");
                // If stop loss is hit, set status to ORDER_LAPSED
                lastTradeDecisions.setStatus(ORDER_LAPSED);
                lastTradeDecisions.setTrade_decision_result(FAILURE);
                lastTradeDecisions.setTrade_decision_result_ts(LocalDateTime.now());
                logger.info("Failed SL SELL order, setting status to ORDER_LAPSED: {}, {}", indexLTP.toString(), lastTradeDecisions.toString());
                tradeDecisionRepository.save(lastTradeDecisions);
                sendCommonMessage("SELL SIGNAL (SL) FAILED", true, ERROR);
                sendTradeDecisionTelegramAlert(indexLTP, lastTradeDecisions, appJobConfig, indexLTP.getTradeDecision() + " Signal Processing: Close the Stoploss trade");

            }

            //handle target hit
            if(indexLTP.getIndexLTP() <= lastTradeDecisions.getTargetIndexLTP()) {

                logger.info("Target hit for SL SELL order, exiting market and updating trade decision");
                lastTradeDecisions.setStatus(ORDER_COMPLETE);
                lastTradeDecisions.setTrade_decision_result(TARGET_HIT);
                lastTradeDecisions.setTrade_decision_result_ts(LocalDateTime.now());
                tradeDecisionRepository.save(lastTradeDecisions);
                logger.info("Target hit for SL SELL order, setting status to ORDER_COMPLETE: {}, {}", indexLTP.toString(), lastTradeDecisions.toString());
                sendCommonMessage("Trade decision is set successfully to COMPLETE", true, SUCCESS);
                emailManagementService.sendEmailForClosingTrade(indexLTP, appJobConfig, lastTradeDecisions);

            }

            if(appJobConfig.getAutoTradeEnabled()) {

                logger.info("AutoTrade flag: {},Checking Auto Trade conditions for SL SELL order", true);

                if (indexLTP.getIndexLTP() >= lastTradeDecisions.getSwingTarget() && !lastTradeDecisions.isSwingTaken()) {
                    lastTradeDecisions.setSwingTaken(true);
                    lastTradeDecisions = tradeDecisionRepository.save(lastTradeDecisions);
                    lastTradeDecisionsMap.put(appJobConfig.getAppJobConfigNum(), lastTradeDecisions);
                    logger.info("Swing Target {} hit for SL SELL order", lastTradeDecisions.getSwingTarget());
                    sendCommonMessage("SWING TARGET HIT for SL SELL order", true, SUCCESS);

                }

                if (lastTradeDecisions.isSwingTaken() && indexLTP.getIndexLTP() <= lastTradeDecisions.getEntryIndexLTP() && !lastTradeDecisions.isConfirmationTaken()) {
                    lastTradeDecisions.setConfirmationTaken(true);
                    lastTradeDecisions = tradeDecisionRepository.save(lastTradeDecisions);
                    lastTradeDecisionsMap.put(appJobConfig.getAppJobConfigNum(), lastTradeDecisions);
                    logger.info("Confirmation condition met for SL SELL order after Swing Target and confirmation hit");
                    sendCommonMessage("Confirmation condition met for SL SELL order after Swing Target and confirmation hit", true, SUCCESS);
                    processAutoTradeSignal(appJobConfig, indexLTP, lastTradeDecisions);
                }


            }

        }

        return jobIterationDetails;
    }


    private void processAutoTradeSignal(AppJobConfig appJobConfig, IndexLTP indexLTP, TradeDecisions lastTradeDecisions) {
        logger.info("Processing Auto Trade signal for AppJobConfig: {}", appJobConfig.toString());

            boolean isOrderPlaced = false;
            // condition when target is within day high/low
            if(lastTradeDecisions.getTradeDecision().equals(TRADE_DECISION_BUY) && Double.parseDouble(indexLTP.getDayHigh()) >= lastTradeDecisions.getTargetIndexLTP()) {

                if(lastTradeDecisions.isConfirmationTaken()) {
                    logger.info("Auto Trade is enabled, placing BUY order for SL/TR signal: {}", indexLTP.toString());
                    //place auto trade order for SL/TR BUY
                    isOrderPlaced = placeOrder(lastTradeDecisions, indexLTP, appJobConfig);
                    logger.info("Auto trade BUY order placed for SL/TR signal: {}", lastTradeDecisions.toString());
                }
            }

            if(lastTradeDecisions.getTradeDecision().equals(TRADE_DECISION_SELL) && Double.parseDouble(indexLTP.getDayLow()) <= lastTradeDecisions.getTargetIndexLTP()) {

                if (lastTradeDecisions.isConfirmationTaken()) {
                    logger.info("Auto Trade is enabled, placing SELL order for SL/TR signal: {}", indexLTP.toString());
                    //place auto trade order for SL/TR SELL
                    isOrderPlaced = placeOrder(lastTradeDecisions, indexLTP, appJobConfig);
                    logger.info("Auto trade SELL order placed for SL/TR signal: {}", lastTradeDecisions.toString());
                }
           }

    }

    private JobIterationDetails processBuySignal(IndexLTP indexLTP, TradeDecisions lastTradeDecisions,JobIterationDetails jobIterationDetails,AppJobConfig appJobConfig) {
//        logger.info("Checking conditions to check whether Buy signal is hit or failed");
        logger.info("BUY SIGNAL :: Current NiftyLTP: {}, Last trade decision:: Target : {}, StopLoss: {}",indexLTP.getIndexLTP(),lastTradeDecisions.getTargetIndexLTP(),lastTradeDecisions.getStopLossIndexLTP());

        // Send telegram notification for Nifty current week (appJobConfigNum == 1)
     //  sendTradeDecisionTelegramAlert(indexLTP, lastTradeDecisions, appJobConfig, "BUY Signal Processing");

        if(lastTradeDecisions.getTradeDecisionType().equalsIgnoreCase(TRADE_DECISION_TYPE_REGULAR)) {

            logger.info("Processing REGULAR BUY order conditions");

            if (indexLTP.getIndexLTP() >= lastTradeDecisions.getTargetIndexLTP()) {

                //exit market if order was placed
                logger.info("Target hit for BUY order, exiting market and updating trade decision");
                lastTradeDecisions.setStatus(ORDER_COMPLETE);
                lastTradeDecisions.setTrade_decision_result(TARGET_HIT);
                lastTradeDecisions.setTrade_decision_result_ts(LocalDateTime.now());
                tradeDecisionRepository.save(lastTradeDecisions);
                logger.info("Target hit for BUY order, setting status to ORDER_COMPLETE: {}, {}", indexLTP.toString(), lastTradeDecisions.toString());
                //   lastTradeDecisions = saveTradeDecision(niftyLTP, jobIterationDetails, ORDER_COMPLETE);
                sendCommonMessage("Trade decision is set successfully to COMPLETE", true, SUCCESS);
                //trigger email notification for target hit and initiate a new buy signal
                indexLTP.setTradeDecision(TRADE_DECISION_BUY);

                // create a new buy signal
                jobIterationDetails = processTrendingSignal(indexLTP, jobIterationDetails, appJobConfig, lastTradeDecisions);
            } else if (indexLTP.getIndexLTP() <= lastTradeDecisions.getStopLossIndexLTP()) {
                //exit market if order was placed
                logger.info("Stop Loss hit for BUY order, exiting market and updating trade decision");
                lastTradeDecisions.setStatus(ORDER_LAPSED);
                lastTradeDecisions.setTrade_decision_result(STOPLOSS_HIT);
                lastTradeDecisions.setTrade_decision_result_ts(LocalDateTime.now());
                logger.info("Stop Loss hit for BUY order, setting status to ORDER_LAPSED: {}, {}", indexLTP, lastTradeDecisions);
                tradeDecisionRepository.save(lastTradeDecisions);
                sendCommonMessage("STOP-LOSS HIT: BUY SIGNAL FAILED", true, SUCCESS);
                // trigger email notification for stop loss hit
                // create a new sell signal
                indexLTP.setTradeDecision(TRADE_DECISION_SELL);

                //process stoploss signal
               jobIterationDetails = processSLSignal(indexLTP, jobIterationDetails, appJobConfig, lastTradeDecisions.getStopLossIndexLTP());
            }
        }

        else if(lastTradeDecisions.getTradeDecisionType().equalsIgnoreCase(TRADE_DECISION_TYPE_TRENDING))
        {
            logger.info("Processing TRENDING BUY order conditions");
            logger.info("TRENDING BUY SIGNAL :: Current IndexLTP: {}, Last trade decision:: Target : {}, StopLoss: {}",indexLTP.getIndexLTP(),lastTradeDecisions.getTargetIndexLTP(), lastTradeDecisions);

            //exit clause
            if(!indexLTP.getTradeDecision().equalsIgnoreCase(lastTradeDecisions.getTradeDecision()) && !indexLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_NO_TRADE)) {

                //exit market
                logger.info("Exiting market as opposite signal received for Trending BUY order");
                // If stop loss is hit, set status to ORDER_LAPSED
                lastTradeDecisions.setStatus(ORDER_LAPSED);
                lastTradeDecisions.setTrade_decision_result(FAILURE);
                lastTradeDecisions.setTrade_decision_result_ts(LocalDateTime.now());
                logger.info("Trending BUY order FAILED, setting status to ORDER_LAPSED: {}, {}", indexLTP, lastTradeDecisions);
                tradeDecisionRepository.save(lastTradeDecisions);
                sendCommonMessage(" BUY SIGNAL (TRENDING) FAILED", true, ERROR);
                emailManagementService.sendEmailForClosingTrade(indexLTP, appJobConfig, lastTradeDecisions);
                sendTradeDecisionTelegramAlert(indexLTP, lastTradeDecisions, appJobConfig, indexLTP.getTradeDecision() + " Signal Processing: Close the Trending trade");

            }

            //handle target hit
            if(indexLTP.getIndexLTP() >= lastTradeDecisions.getTargetIndexLTP()) {

                logger.info("Target hit for TRENDING BUY order, exiting market and updating trade decision");
                lastTradeDecisions.setStatus(ORDER_COMPLETE);
                lastTradeDecisions.setTrade_decision_result(TARGET_HIT);
                lastTradeDecisions.setTrade_decision_result_ts(LocalDateTime.now());
                tradeDecisionRepository.save(lastTradeDecisions);
                logger.info("Target hit for TRENDING BUY order, setting status to ORDER_COMPLETE: {}, {}", indexLTP.toString(), lastTradeDecisions.toString());
                sendCommonMessage("Trade decision is set successfully to COMPLETE", true, SUCCESS);
                emailManagementService.sendEmailForClosingTrade(indexLTP, appJobConfig, lastTradeDecisions);

            }

            if(appJobConfig.getAutoTradeEnabled()) {

                logger.info("AutoTrade flag: {},Checking Auto Trade conditions for Trending BUY order", true);

                if (indexLTP.getIndexLTP() <= lastTradeDecisions.getSwingTarget() && !lastTradeDecisions.isSwingTaken()) {
                    logger.info("Swing Target {} hit for TR BUY order", lastTradeDecisions.getSwingTarget());
                    sendCommonMessage("SWING TARGET HIT for TR BUY order", true, SUCCESS);
                    lastTradeDecisions.setSwingTaken(true);
                    lastTradeDecisions = tradeDecisionRepository.save(lastTradeDecisions);
                    lastTradeDecisionsMap.put(appJobConfig.getAppJobConfigNum(), lastTradeDecisions);
                }

                if (lastTradeDecisions.isSwingTaken() && indexLTP.getIndexLTP() >= lastTradeDecisions.getEntryIndexLTP() && !lastTradeDecisions.isConfirmationTaken()) {
                    lastTradeDecisions.setConfirmationTaken(true);
                    lastTradeDecisions = tradeDecisionRepository.save(lastTradeDecisions);
                    lastTradeDecisionsMap.put(appJobConfig.getAppJobConfigNum(), lastTradeDecisions);
                    logger.info("Confirmation condition met for TR BUY order after Swing Target and confirmation hit");
                    sendCommonMessage("Confirmation condition met for TR BUY order after Swing Target and confirmation hit", true, SUCCESS);
                    processAutoTradeSignal(appJobConfig, indexLTP, lastTradeDecisions);
                }
            }

        }


        else if(lastTradeDecisions.getTradeDecisionType().equalsIgnoreCase(TRADE_DECISION_TYPE_STOPLOSS))
        {
            logger.info("Processing SL BUY order conditions");
            logger.info("SL BUY SIGNAL :: Current IndexLTP: {}, Last trade decision:: Target : {}, StopLoss: {}", indexLTP,lastTradeDecisions.getTargetIndexLTP(),lastTradeDecisions.toString());

            if(!indexLTP.getTradeDecision().equalsIgnoreCase(lastTradeDecisions.getTradeDecision()) && !indexLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_NO_TRADE)) {

                //exit market
                logger.info("Exiting market as opposite signal received for SL BUY order");
                // If stop loss is hit, set status to ORDER_LAPSED
                lastTradeDecisions.setStatus(ORDER_LAPSED);
                lastTradeDecisions.setTrade_decision_result(FAILURE);
                lastTradeDecisions.setTrade_decision_result_ts(LocalDateTime.now());
                logger.info("SL BUY order FAILED, setting status to ORDER_LAPSED: {}, {}", indexLTP.toString(), lastTradeDecisions.toString());
                tradeDecisionRepository.save(lastTradeDecisions);
                sendCommonMessage(" BUY SIGNAL (SL) FAILED", true, ERROR);
                emailManagementService.sendEmailForClosingTrade(indexLTP, appJobConfig, lastTradeDecisions);
                sendTradeDecisionTelegramAlert(indexLTP, lastTradeDecisions, appJobConfig, indexLTP.getTradeDecision() + " Signal Processing: Close the Stoploss trade");

            }

            //handle target hit
            if(indexLTP.getIndexLTP() >= lastTradeDecisions.getTargetIndexLTP()) {

                logger.info("Target hit for SL BUY order, exiting market and updating trade decision");
                lastTradeDecisions.setStatus(ORDER_COMPLETE);
                lastTradeDecisions.setTrade_decision_result(TARGET_HIT);
                lastTradeDecisions.setTrade_decision_result_ts(LocalDateTime.now());
                tradeDecisionRepository.save(lastTradeDecisions);
                logger.info("Target hit for SL BUY order, setting status to ORDER_COMPLETE: {}, {}", indexLTP.toString(), lastTradeDecisions.toString());
                sendCommonMessage("Trade decision is set successfully to COMPLETE", true, SUCCESS);
                emailManagementService.sendEmailForClosingTrade(indexLTP, appJobConfig, lastTradeDecisions);

            }

            if(appJobConfig.getAutoTradeEnabled()) {

                logger.info("AutoTrade flag: {},Checking Auto Trade conditions for SL BUY order", true);

                if (indexLTP.getIndexLTP() <= lastTradeDecisions.getSwingTarget() && !lastTradeDecisions.isSwingTaken()) {
                    logger.info("Swing Target {} hit for SL BUY order", lastTradeDecisions.getSwingTarget());
                    sendCommonMessage("SWING TARGET HIT for SL BUY order", true, SUCCESS);
                    lastTradeDecisions.setSwingTaken(true);
                    lastTradeDecisions = tradeDecisionRepository.save(lastTradeDecisions);
                    lastTradeDecisionsMap.put(appJobConfig.getAppJobConfigNum(), lastTradeDecisions);
                }

                if (lastTradeDecisions.isSwingTaken() && indexLTP.getIndexLTP() >= lastTradeDecisions.getEntryIndexLTP() && !lastTradeDecisions.isConfirmationTaken()) {
                    lastTradeDecisions.setConfirmationTaken(true);
                    lastTradeDecisions = tradeDecisionRepository.save(lastTradeDecisions);
                    lastTradeDecisionsMap.put(appJobConfig.getAppJobConfigNum(), lastTradeDecisions);
                    logger.info("Confirmation condition met for SL BUY order after Swing Target and confirmation hit");
                    sendCommonMessage("Confirmation condition met for SL BUY order after Swing Target and confirmation hit", true, SUCCESS);
                    processAutoTradeSignal(appJobConfig, indexLTP, lastTradeDecisions);
                }


            }
        }

        return jobIterationDetails;

    }


    private JobIterationDetails processSLSignal(IndexLTP indexLTP, JobIterationDetails jobIterationDetails, AppJobConfig appJobConfig, Integer stopLossIndexLTP) {

        logger.info("Processing STOP-LOSS signal: {}",indexLTP.toString());
        //close previous jobIterationDetails and prepare new jobIterationDetails for SL signal
        logger.info("Closing previous JobIterationDetails and preparing new JobIterationDetails for SL signal");
        jobIterationDetails.setIndexLTP(indexLTP.getIndexLTP());
        jobIterationDetails = completeJobIteration(jobIterationDetails);
        jobIterationDetails = createJobIterationDetails(jobIterationDetails.getJobDetails());

        //prepare lastTradeDecision
        TradeDecisions lastTradeDecisions = saveTradeDecisionSL(indexLTP, jobIterationDetails, ORDER_OPEN,appJobConfig,stopLossIndexLTP);
        sendCommonMessage("STOP-LOSS Trade decision is set successfully to OPEN",true, WARNING);
        lastTradeDecisionsMap.put(appJobConfig.getAppJobConfigNum(),lastTradeDecisions);
        logger.info("STOP-LOSS Trade decision is updated on lastTradeDecisionsMap: {}",lastTradeDecisionsMap.get(appJobConfig.getAppJobConfigNum()).toString());
        emailManagementService.sendEmailOfAutoTradeDecision(indexLTP, appJobConfig, lastTradeDecisions);
        sendTradeDecisionTelegramAlert(indexLTP, lastTradeDecisions, appJobConfig, indexLTP.getTradeDecision() + " Signal Processing");
        return jobIterationDetails;
    }

    private JobIterationDetails processTrendingSignal(IndexLTP indexLTP, JobIterationDetails jobIterationDetails, AppJobConfig appJobConfig, TradeDecisions lastTradeDecisions) {

        logger.info("Processing TRENDING signal: {}",indexLTP.toString());
        //close previous jobIterationDetails and prepare new jobIterationDetails for Trending signal
        logger.info("Closing previous JobIterationDetails and preparing new JobIterationDetails for TRENDING signal");
        jobIterationDetails.setIndexLTP(indexLTP.getIndexLTP());
        jobIterationDetails = completeJobIteration(jobIterationDetails);
        jobIterationDetails = createJobIterationDetails(jobIterationDetails.getJobDetails());
        //prepare lastTradeDecision
        lastTradeDecisions = saveTradeDecisionTR(indexLTP, jobIterationDetails, ORDER_OPEN,appJobConfig);
        sendCommonMessage("TRENDING Trade decision is set successfully to OPEN",true, WARNING);
        lastTradeDecisionsMap.put(appJobConfig.getAppJobConfigNum(),lastTradeDecisions);
        logger.info("TRENDING Trade decision is updated on lastTradeDecisionsMap: {}",lastTradeDecisionsMap.get(appJobConfig.getAppJobConfigNum()).toString());
        emailManagementService.sendEmailOfAutoTradeDecision(indexLTP, appJobConfig,lastTradeDecisions);
        sendTradeDecisionTelegramAlert(indexLTP, lastTradeDecisions, appJobConfig, indexLTP.getTradeDecision() + " Signal Processing");
        return jobIterationDetails;
    }

    private TradeDecisions saveTradeDecisionTR(IndexLTP indexLTP, JobIterationDetails jobIterationDetails, String tradeDecisionStatus, AppJobConfig appJobConfig) {

        String tradeDecision = indexLTP.getTradeDecision();
        TradeDecisions tradeDecisions = new TradeDecisions();
        tradeDecisions.setAppJobConfig(appJobConfig);
        tradeDecisions.setIndexLTP(indexLTP.getIndexLTP());
        tradeDecisions.setStatus(tradeDecisionStatus);
        tradeDecisions.setTradeDecision(tradeDecision);
        tradeDecisions.setTradeDecisionType(TRADE_DECISION_TYPE_TRENDING);
        tradeDecisions.setJobIterationDetails(jobIterationDetails);
        tradeDecisions.setTradeDecisionTS(indexLTP.getIndexTS());
        tradeDecisions.setEntryIndexLTP(indexLTP.getIndexLTP());

        // parse day high/low, take integer portion (truncate decimals) and use ints for setTargetIndexLTP
        double dayHighD = Double.parseDouble(indexLTP.getDayHigh());
        double dayLowD  = Double.parseDouble(indexLTP.getDayLow());

        int dayHigh = (int) Math.floor(dayHighD); // integer portion of dayHigh
        int dayLow  = (int) Math.floor(dayLowD);  // integer portion of dayLow

        TradeDecisions prevTradeDecision = lastTradeDecisionsMap.get(appJobConfig.getAppJobConfigNum());

        if(tradeDecisions.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_BUY)) {
            tradeDecisions.setTargetIndexLTP(Math.min(dayHigh, prevTradeDecision.getTargetIndexLTP() + (appJobConfig.getAppIndexConfig().getPriceGap()/I_TWO)));
            if(appJobConfig.getAutoTradeEnabled())
                tradeDecisions.setSwingTarget(calculateSwingLow().getLowPrice());

        }

        else {
            tradeDecisions.setTargetIndexLTP(Math.max(dayLow, prevTradeDecision.getTargetIndexLTP() - (appJobConfig.getAppIndexConfig().getPriceGap()/I_TWO)));
            if(appJobConfig.getAutoTradeEnabled())
                tradeDecisions.setSwingTarget(calculateSwingHigh().getHighPrice());

        }

        int buyStopLossIndexLTP = tradeDecisions.getSwingTarget().intValue() - (appJobConfig.getAppIndexConfig().getPriceGap()/I_TWO);
        int sellStopLossIndexLTP = tradeDecisions.getSwingTarget().intValue() + (appJobConfig.getAppIndexConfig().getPriceGap()/I_TWO);

        int stopLossIndexLTP1 = tradeDecision.equalsIgnoreCase(TRADE_DECISION_BUY) ? buyStopLossIndexLTP : sellStopLossIndexLTP;

        logger.info("Calculated STOP-LOSS Index LTP for TRENDING signal: {}", stopLossIndexLTP1);

        if(appJobConfig.getAutoTradeEnabled()) {
            if (tradeDecisions.isConfirmationTaken())
                tradeDecisions.setStopLossIndexLTP(stopLossIndexLTP1);
            else
                tradeDecisions.setStopLossIndexLTP(0);
        }

        else
            tradeDecisions.setStopLossIndexLTP(stopLossIndexLTP1);

        tradeDecisions = tradeDecisionRepository.save(tradeDecisions);
        logger.info("Trending Trade Decision saved successfully: {}", tradeDecisions);
        return tradeDecisions;

    }

    private boolean placeOrder(TradeDecisions lastTradeDecisions, IndexLTP indexLTP, AppJobConfig appJobConfig) {

        boolean isOrderPlaced =false;

        try {
            String strikePriceFromAtm = getStrikePriceFromAtm(indexLTP,appJobConfig);
            logger.info("Placing auto trade order for TradeDecision: {}, Strike Price from ATM: {}", lastTradeDecisions.toString(), strikePriceFromAtm);
            String instrumentToken = lastTradeDecisions.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_BUY)?strikePriceFromAtm + "CE": strikePriceFromAtm + "PE";
            logger.info("Instrument token for auto trade order: {} with LTP as {}: ", instrumentToken, ltpMap.get(instrumentToken));
            //service call to place order
            processAutoTradeParamsForSL(indexLTP);

            if(Objects.nonNull(autoTradeParamsJob.get(indexLTP.getAppJobConfigNum()))) {
                calculateAndSaveAutoTradeJobParams(autoTradeParamsJob.get(indexLTP.getAppJobConfigNum()), indexLTP);
                logger.info("Auto Trade Params after calculation: {}", autoTradeParamsJob.get(indexLTP.getAppJobConfigNum()).toString());
            }
            isOrderPlaced = true;

            String generatedOrderId = instrumentToken + "_" + UUID.randomUUID().toString();
            logger.info("Generated Order ID for Auto Trade order: {}", generatedOrderId);
            OrderBook orderBook = new OrderBook();
            orderBook.setOrderId(generatedOrderId);
            orderBook.setOrderStatus(ORDER_OPEN);
            orderBook.setJobIterationDetails(lastTradeDecisions.getJobIterationDetails());
            orderBookRepository.save(orderBook);
            logger.info("Order Book entry created for Auto Trade order: {}", orderBook.toString());
            orderBookMap.put(appJobConfig.getAppJobConfigNum(), orderBook);


            if(isOrderPlaced) {
                sendCommonMessage("Auto Trade order placed successfully for" + lastTradeDecisions.getTradeDecisionType() + "signal", true, SUCCESS);
                emailManagementService.sendEmailOfPlacingAutoTradeOrder(lastTradeDecisions,instrumentToken);
            }
            else
                sendCommonMessage("Auto Trade order placement failed for"  + lastTradeDecisions.getTradeDecisionType() + "signal",false, ERROR);

        } catch (RuntimeException e) {
            logger.error("Error in placing auto trade order: {}", e.getMessage());
        }

        return isOrderPlaced;

    }

    public String getStrikePriceFromAtm(IndexLTP indexLTP,AppJobConfig appJobConfig) {

        try {
            String strikePrice = "";

            int halfPriceGap = appJobConfig.getAppIndexConfig().getPriceGap() / I_TWO;

            Integer price = Integer.valueOf(indexLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_BUY) ? indexLTP.getResistance() : indexLTP.getSupport());

            if (((price / halfPriceGap) % I_TWO) != 0)
                strikePrice = String.valueOf(price + halfPriceGap);
            else
                strikePrice = String.valueOf(price);

            logger.info("Latest Index LTP: {}, Strike Price: {}", indexLTP.getIndexLTP(), strikePrice);

            return strikePrice;

        } catch (RuntimeException e) {
            logger.error("Error in fetching strike price instruments: {}", e.getMessage());
            return "";
        }

    }

    public void prepareTestCandleSticks() {
        //initialize kiteTickerProvider.candleSticks with some data before calling this API to get meaningful results

        CandleStick candleStick1 = new CandleStick(1L,NIFTY_INSTRUMENT_TOKEN, 26050.0, 26140.0, 26000.0, 26030.0, LocalDateTime.now().minusMinutes(5).withNano(0), LocalDateTime.now().minusMinutes(5).plusSeconds(59).withNano(0));
        CandleStick candleStick2 = new CandleStick(2L,NIFTY_INSTRUMENT_TOKEN, 26030.0, 26160.0, 26090.0, 26090.0, LocalDateTime.now().minusMinutes(4).withNano(0), LocalDateTime.now().minusMinutes(4).plusSeconds(59).withNano(0));
        CandleStick candleStick3 = new CandleStick(3L,NIFTY_INSTRUMENT_TOKEN, 26090.0, 26150.0, 26110.0, 26120.0, LocalDateTime.now().minusMinutes(3).withNano(0), LocalDateTime.now().minusMinutes(3).plusSeconds(59).withNano(0));
        CandleStick candleStick4 = new CandleStick(4L,NIFTY_INSTRUMENT_TOKEN, 26120.0, 26180.0, 26100.0, 26150.0, LocalDateTime.now().minusMinutes(2).withNano(0), LocalDateTime.now().minusMinutes(2).plusSeconds(59).withNano(0));
        CandleStick candleStick5 = new CandleStick(5L,NIFTY_INSTRUMENT_TOKEN, 26150.0, 26200.0, 26130.0, 26180.0, LocalDateTime.now().minusMinutes(1).withNano(0), LocalDateTime.now().minusMinutes(1).plusSeconds(59).withNano(0));

        kiteTickerProvider.candleSticks.add(candleStick1);
        kiteTickerProvider.candleSticks.add(candleStick2);
        kiteTickerProvider.candleSticks.add(candleStick3);
        kiteTickerProvider.candleSticks.add(candleStick4);
        kiteTickerProvider.candleSticks.add(candleStick5);
    }

    public void calculateAndSaveAutoTradeJobParams(AutoTradeParams autoTradeParamsJob, IndexLTP latestIndexLTP) {

        logger.info("Calculating auto trade job params with latest Index LTP: {}", latestIndexLTP);

        try {

            // calculate call and put symbols
            autoTradeService.calculateAutoTradeSymbols(autoTradeParamsJob, latestIndexLTP);
            // calculate entry price
            autoTradeService.calculateAutoTradeEntryPrice(autoTradeParamsJob, latestIndexLTP);
            // calculate stop loss price
            autoTradeService.calculateAutoTradeSLPrice(autoTradeParamsJob, latestIndexLTP);
            // calculate call and put lot sizes
            autoTradeService.calculateAutoTradeLotSizes(autoTradeParamsJob, latestIndexLTP);
            //setting job id
            autoTradeParamsJob.setJobIterationDetails(latestIndexLTP.getJobIterationDetails());

            logger.info("Auto trade job params calculated successfully: {}", autoTradeParamsJob.toString());
            // save auto trade params
            autoTradeService.saveAutoTradeParams(autoTradeParamsJob);


        } catch (RuntimeException e) {

            logger.error("Error calculating auto trade job params: {}", e.getMessage(), e);

        }
    }

    private void processAutoTradeParamsForSL(IndexLTP latestIndexLTP) {
        if(Objects.isNull(autoTradeParamsJob.get(latestIndexLTP.getAppJobConfigNum()))) {
            AutoTradeParams autoTradeParams = new AutoTradeParams();
            autoTradeParamsJob.put(latestIndexLTP.getAppJobConfigNum(), autoTradeParams);
        }
        autoTradeParamsJob.get(latestIndexLTP.getAppJobConfigNum()).setAutoTradeCallEntryPrice(latestIndexLTP.getIndexLTP());
        autoTradeParamsJob.get(latestIndexLTP.getAppJobConfigNum()).setAutoTradePutEntryPrice(latestIndexLTP.getIndexLTP());
        autoTradeParamsJob.get(latestIndexLTP.getAppJobConfigNum()).setAutoTradeParamsTS(LocalDateTime.now());
        autoTradeParamsJob.get(latestIndexLTP.getAppJobConfigNum()).setJobIterationDetails(latestIndexLTP.getJobIterationDetails());
        autoTradeService.saveAutoTradeParams(autoTradeParamsJob.get(latestIndexLTP.getAppJobConfigNum()));
    }

    @Override
    public Map<String, Object> getTickerProviderData() {
        Map<String, Object> result = new HashMap<>();

        if (kiteTickerProvider == null) {
            result.put("error", "KiteTickerProvider is not initialized");
            result.put("isConnected", false);
            return result;
        }

        result.put("isConnected", isTickerConnected.get());
        result.put("niftyLastPrice", kiteTickerProvider.niftyLastPrice != null ? kiteTickerProvider.niftyLastPrice : 0.0);
        result.put("vixLastPrice", kiteTickerProvider.vixLastPrice != null ? kiteTickerProvider.vixLastPrice : 0.0);
        result.put("lastNifty50TS", kiteTickerProvider.lastNifty50TS != null ? kiteTickerProvider.lastNifty50TS.toString() : null);
        result.put("tickerTokensCount", kiteTickerProvider.tickerTokens != null ? kiteTickerProvider.tickerTokens.size() : 0);
        result.put("tickerMapForJobSize", kiteTickerProvider.tickerMapForJob != null ? kiteTickerProvider.tickerMapForJob.size() : 0);

        // Convert tickerMapForJob to a simplified format for JSON serialization
        if (kiteTickerProvider.tickerMapForJob != null && !kiteTickerProvider.tickerMapForJob.isEmpty()) {
            Map<Long, Map<String, Object>> tickerData = new HashMap<>();
            kiteTickerProvider.tickerMapForJob.forEach((token, tick) -> {
                Map<String, Object> tickInfo = new HashMap<>();
                tickInfo.put("instrumentToken", tick.getInstrumentToken());
                tickInfo.put("lastTradedPrice", tick.getLastTradedPrice());
                tickInfo.put("lastTradedTime", tick.getLastTradedTime());
                tickInfo.put("openPrice", tick.getOpenPrice());
                tickInfo.put("highPrice", tick.getHighPrice());
                tickInfo.put("lowPrice", tick.getLowPrice());
                tickInfo.put("closePrice", tick.getClosePrice());
                tickInfo.put("change", tick.getChange());
                tickInfo.put("oi", tick.getOi());
                tickInfo.put("volumeTradedToday", tick.getVolumeTradedToday());
                tickerData.put(token, tickInfo);
            });
            result.put("tickerMapForJob", tickerData);
        } else {
            result.put("tickerMapForJob", new HashMap<>());
        }

        result.put("timestamp", LocalDateTime.now().toString());

        return result;
    }

    @Override
    public Map<String, Object> getIndexPrices() {
        Map<String, Object> result = new HashMap<>();

        if (kiteTickerProvider == null) {
            result.put("error", "KiteTickerProvider is not initialized");
            result.put("isConnected", false);
            result.put("niftyLastPrice", 0.0);
            result.put("vixLastPrice", 0.0);
            return result;
        }

        result.put("isConnected", isTickerConnected.get());
        result.put("niftyLastPrice", kiteTickerProvider.niftyLastPrice != null ? kiteTickerProvider.niftyLastPrice : 0.0);
        result.put("vixLastPrice", kiteTickerProvider.vixLastPrice != null ? kiteTickerProvider.vixLastPrice : 0.0);
        result.put("lastNifty50TS", kiteTickerProvider.lastNifty50TS != null ? kiteTickerProvider.lastNifty50TS.toString() : null);
        result.put("timestamp", LocalDateTime.now().toString());

        return result;
    }

    @Override
    public Map<String, Object> getTickDataByToken(Long instrumentToken) {
        Map<String, Object> result = new HashMap<>();

        if (kiteTickerProvider == null) {
            result.put("error", "KiteTickerProvider is not initialized");
            result.put("isConnected", false);
            result.put("subscribed", false);
            return result;
        }

        result.put("isConnected", isTickerConnected.get());
        result.put("instrumentToken", instrumentToken);

        // Check if token is already subscribed, if not subscribe it
        boolean wasAlreadySubscribed =  kiteTickerProvider.tickerMapForJob.get(instrumentToken)!= null;

        if (!wasAlreadySubscribed) {
            try {
                ArrayList<Long> tokenToSubscribe = new ArrayList<>();
                tokenToSubscribe.add(instrumentToken);
                kiteTickerProvider.subscribeTokenForJob(tokenToSubscribe);
                logger.info("Subscribed new instrument token: {}", instrumentToken);
                result.put("newlySubscribed", true);
            } catch (Exception e) {
                logger.error("Error subscribing instrument token {}: {}", instrumentToken, e.getMessage());
                result.put("error", "Failed to subscribe token: " + e.getMessage());
                result.put("subscribed", false);
                return result;
            }
        } else {
            result.put("newlySubscribed", false);
        }

        result.put("subscribed", true);

        // Get tick data from tickerMapForJob
        if (kiteTickerProvider.tickerMapForJob != null && kiteTickerProvider.tickerMapForJob.containsKey(instrumentToken)) {
            Tick tick = kiteTickerProvider.tickerMapForJob.get(instrumentToken);
            result.put("lastTradedPrice", tick.getLastTradedPrice());
            result.put("lastTradedTime", tick.getLastTradedTime());
            result.put("openPrice", tick.getOpenPrice());
            result.put("highPrice", tick.getHighPrice());
            result.put("lowPrice", tick.getLowPrice());
            result.put("closePrice", tick.getClosePrice());
            result.put("change", tick.getChange());
            result.put("oi", tick.getOi());
            result.put("volumeTradedToday", tick.getVolumeTradedToday());
            result.put("tickTimestamp", tick.getTickTimestamp() != null ? tick.getTickTimestamp().toString() : null);
            result.put("hasData", true);
        } else {
            result.put("hasData", false);
            result.put("message", wasAlreadySubscribed
                    ? "Token is subscribed but no tick data available yet"
                    : "Token just subscribed, tick data will be available shortly");
        }

        result.put("timestamp", LocalDateTime.now().toString());

        return result;
    }

    @Override
    public Map<String, Object> getTickerConnectionStatus() {
        Map<String, Object> result = new HashMap<>();
        result.put("isTickerConnected", isTickerConnected.get());
        result.put("timestamp", LocalDateTime.now().toString());
        return result;
    }

    /**
     * Sends a telegram notification for trade decision updates (Buy/Sell signals).
     * Only triggered for appJobConfigNum == 1 (Nifty current week).
     */
    private void sendTradeDecisionTelegramAlert(IndexLTP indexLTP, TradeDecisions tradeDecisions, AppJobConfig appJobConfig, String eventType) {
        try {
            // Only send telegram notifications for appJobConfigNum == 1 (Nifty current week)
            if (appJobConfig.getAppJobConfigNum() != 1) {
                return;
            }

            // Check if trade decision alerts are enabled in telegram settings
            if (!telegramNotificationService.isAlertTypeEnabled("TRADE", "TRADE_DECISION")) {
                logger.debug("Trade decision alerts are disabled in Telegram settings, skipping notification");
                return;
            }

            String indexName = appJobConfig.getAppIndexConfig().getIndexName();
            String jobType = appJobConfig.getJobType().getJobType();
            String signalDirection = tradeDecisions.getTradeDecision();
            String tradeType = tradeDecisions.getTradeDecisionType();

            String title = String.format("📊 %s %s Signal - %s (%s)",
                indexName, signalDirection, eventType, tradeType);

            StringBuilder message = new StringBuilder();
            message.append(String.format("*Signal:* %s\n", signalDirection));
            message.append(String.format("*Index:* %s (%s)\n", indexName, jobType));
            message.append(String.format("*Event:* %s\n", eventType));
            message.append(String.format("*Trade Type:* %s\n\n", tradeType));

            message.append("📈 *Current Price Info:*\n");
            message.append(String.format("• LTP: %d\n", indexLTP.getIndexLTP()));
            message.append(String.format("• Support: %s\n", indexLTP.getSupport()));
            message.append(String.format("• Resistance: %s\n", indexLTP.getResistance()));
            message.append(String.format("• Day High: %s\n", indexLTP.getDayHigh()));
            message.append(String.format("• Day Low: %s\n\n", indexLTP.getDayLow()));

            message.append("🎯 *Trade Levels:*\n");
            message.append(String.format("• Entry: %d\n", tradeDecisions.getEntryIndexLTP()));
            message.append(String.format("• Target: %d\n", tradeDecisions.getTargetIndexLTP()));
            message.append(String.format("• Stop Loss: %d\n", tradeDecisions.getStopLossIndexLTP()));

            if (tradeDecisions.getSwingTarget() != null && tradeDecisions.getSwingTarget() > 0) {
                message.append(String.format("• Swing Target: %.2f\n", tradeDecisions.getSwingTarget()));
            }

            message.append(String.format("\n*Status:* %s\n", tradeDecisions.getStatus()));

            if (tradeDecisions.getTrade_decision_result() != null) {
                message.append(String.format("*Result:* %s\n", tradeDecisions.getTrade_decision_result()));
            }

            message.append(String.format("*Timestamp:* %s", LocalDateTime.now().withNano(0)));

            Map<String, Object> tradeData = new HashMap<>();
            tradeData.put("indexName", indexName);
            tradeData.put("jobType", jobType);
            tradeData.put("signalDirection", signalDirection);
            tradeData.put("tradeType", tradeType);
            tradeData.put("eventType", eventType);
            tradeData.put("ltp", indexLTP.getIndexLTP());
            tradeData.put("entry", tradeDecisions.getEntryIndexLTP());
            tradeData.put("target", tradeDecisions.getTargetIndexLTP());
            tradeData.put("stopLoss", tradeDecisions.getStopLossIndexLTP());
            tradeData.put("status", tradeDecisions.getStatus());

            // Preserve MDC context for async logging
            final String currentAppJobConfigNum = MDC.get("appJobConfigNum");

            telegramNotificationService.sendTradeAlertAsync(title, message.toString(), tradeData)
                .thenAccept(response -> {
                    // Restore MDC context in async callback
                    if (currentAppJobConfigNum != null) {
                        MDC.put("appJobConfigNum", currentAppJobConfigNum);
                    }
                    try {
                        if (response.isSuccess()) {
                            logger.info("Telegram notification sent successfully for {} signal: {}", signalDirection, eventType);
                        } else {
                            logger.warn("Failed to send Telegram notification: {}", response.getError());
                        }
                    } finally {
                        MDC.remove("appJobConfigNum");
                    }
                })
                .exceptionally(ex -> {
                    // Restore MDC context in async callback
                    if (currentAppJobConfigNum != null) {
                        MDC.put("appJobConfigNum", currentAppJobConfigNum);
                    }
                    try {
                        logger.error("Error sending Telegram notification: {}", ex.getMessage());
                    } finally {
                        MDC.remove("appJobConfigNum");
                    }
                    return null;
                });

        } catch (Exception e) {
            logger.error("Error preparing Telegram notification for trade decision: {}", e.getMessage());
        }
    }

}
