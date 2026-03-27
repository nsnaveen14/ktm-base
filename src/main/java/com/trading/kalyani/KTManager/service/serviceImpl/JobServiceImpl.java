package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.config.KiteConnectConfig;
import com.trading.kalyani.KTManager.entity.*;
import com.trading.kalyani.KTManager.model.AppJobConfigParams;
import com.trading.kalyani.KTManager.model.AutoTradeParams;
import com.trading.kalyani.KTManager.model.CommonReqRes;
import com.trading.kalyani.KTManager.service.*;
import com.trading.kalyani.KTManager.utilities.KiteTickerProvider;
import com.trading.kalyani.KTManager.model.DeltaOICalculations;
import com.trading.kalyani.KTManager.repository.*;
import com.zerodhatech.models.Instrument;
import com.zerodhatech.models.OHLC;
import com.zerodhatech.models.Order;
import com.zerodhatech.models.Quote;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;
import static com.trading.kalyani.KTManager.utilities.DateUtilities.convertDateToLocalDate;
import static com.zerodhatech.kiteconnect.utils.Constants.*;


@Service
public class JobServiceImpl implements JobService {

    private static final Logger logger = LoggerFactory.getLogger(JobServiceImpl.class);

    @Autowired
    InstrumentRepository instrumentRepository;

    @Autowired
    OiSnapshotRepository oiSnapshotRepository;

    @Autowired
    KiteConnectConfig kiteConnectConfig;

    @Autowired
    AsyncService asyncService;

    @Autowired
    DeltaOIThresholdFilteredRepository deltaOIThresholdFilteredRepository;

    KiteTickerProvider kiteTickerProvider;

    @Autowired
    FileServiceImpl fileService;

    @Autowired
    DeltaOIRepository deltaOIRepository;

    @Autowired
    MiniDeltaRepository miniDeltaRepository;

    @Autowired
    NiftyLTPRepository niftyLTPRepository;

    @Autowired
    MessagingService messagingService;

    @Autowired
    TransactionService transactionService;

    @Autowired
    JobIterationRepository jobIterationRepository;

    @Autowired
    PowerTradeDeltaOIRepository powerTradeDeltaOIRepository;

    @Autowired
    InstrumentService instrumentService;

    @Autowired
    AppIndexRepository appIndexRepository;

    @Autowired
    AppJobConfigRepository appJobConfigRepository;

    @Value("${ktm.s3.bucketName}")
    private String bucketName;

    @Value("${ktm.s3.snapshotOIFolderName}")
    private String snapshotOIFolderName;

    @Autowired
    LTPTrackerRepository ltpTrackerRepository;

    @Autowired
    TradeDecisionRepository tradeDecisionRepository;

    @Autowired
    AutoTradeService autoTradeService;

    @Autowired
    AutoTradeRepository autoTradeRepository;

    @Autowired
    JobDetailsRepository jobDetailsRepository;

    static Double meanCallOIChange=D_ZERO;
    static Double meanPutOIChange=D_ZERO;
    static Double meanCallOI = D_ZERO;
    static Double meanPutOI = D_ZERO;
    static Double meanStrikePCR = D_ZERO;
    static Double meanRateOI = D_ZERO;
    static Double combiRate = D_ZERO;
    static LocalDateTime lastIterationTimestamp;
    static Double niftyLastPrice = D_ZERO;
    static List<MiniDelta> prevItrMiniDeltaList = new ArrayList<>();
    static NiftyLTP prevItrNiftyLTP = null;
    static String lastTradeDecisionValue="";
    static int iceBergPositiveTrendCounter = I_ZERO;
    static int iceBergNegativeTrendCounter = I_ZERO;
    static OHLC niftyOHLC = new OHLC();

    static final AtomicBoolean isTickerConnected = new AtomicBoolean(false);

    List<Order> orderList = new ArrayList<>();

    Map<String, Double> ltpMap = new HashMap<>();

    AutoTradeParams autoTradeParamsJob = new AutoTradeParams();

    TradeDecisions lastTradeDecisions;

    @Override
    @Transactional
    public boolean saveOISnapshot(Integer appJobConfigNum) {

        AppJobConfig appJobConfig = appJobConfigRepository.findById(appJobConfigNum).orElse(null);
        if (appJobConfig == null) {
            logger.error("No AppJobConfig found for num: {}", appJobConfigNum);
            return false;
        }
        logger.info("Taking OI Snapshot for AppJobConfig: {}", appJobConfig.toString());
        ArrayList<InstrumentEntity> listOfRequiredInstruments = instrumentService.getInstrumentsFromAppJobConfigNum(appJobConfigNum);
        logger.info("Total instruments found for snapshot {} for AppJobConfig: {}", listOfRequiredInstruments.size(),appJobConfigNum);
        logger.info("Processing for expiry: {} for AppJobConfig: {}", listOfRequiredInstruments.getFirst().getInstrument().getExpiry(),appJobConfigNum);
        ArrayList<OISnapshotEntity> listOfSnapshotTokens = new ArrayList<>();

        Integer tFactor = appJobConfig.getAppIndexConfig().getTFactor();

        JobDetails jobDetails = new JobDetails();
        jobDetails.setAppJobConfigNum(appJobConfigNum);
        jobDetails.setJobName(JobName.OISNAPSHOT);
        jobDetails.setJobStatus(STATUS_RUNNING);
        jobDetails.setJobStartTime(LocalDateTime.now().withNano(I_ZERO));
        jobDetails = jobDetailsRepository.save(jobDetails);

        try {

            ArrayList<Long> instrumentTokens = listOfRequiredInstruments
                    .parallelStream()
                    .map(i-> i.getInstrument().getInstrument_token())
                    .collect(Collectors.toCollection(ArrayList::new));

            logger.info("Subscribing to {} tokens for OI Snapshot with first token: {}", instrumentTokens.size(), instrumentTokens.getFirst());

            kiteTickerProvider.subscribeTokenForJob(instrumentTokens);

            try {
                Thread.sleep(ONE_THOUSAND);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            if (!kiteTickerProvider.tickerMapForJob.isEmpty()) {
                listOfRequiredInstruments.forEach(i -> {

                    OISnapshotEntity oISnapshotEntity = oiSnapshotRepository.findById(i.getInstrument().getInstrument_token()).orElseGet(OISnapshotEntity::new);

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

                sendEmailAsync(fileName);

                if(isTickerConnected.get())
                    stopKiteTicker();

                return true;
            }


        } catch (Exception e) {
            logger.error("Error in taking snapshot : {} ", e.getMessage());
            logger.debug("Clear Local Storage");
            return false;
        }

        return false;
    }


    private void sendEmailAsync(String fileName) {
        asyncService.sendEmailToAllUsersAsync("KTM: OISnapshot" + " from Profile: "+kiteConnectConfig.getActiveProfile(), "OI Snapshot is taken successfully and attached with this email", fileName)
                .thenRun(() -> {
                    // Code to execute after successful completion
                    logger.info("Email sent successfully!");

                })
                .exceptionally(ex -> {
                    // Handle exceptions
                    logger.error("Error occurred: {}", ex.getMessage());
                    return null;
                });
    }

    @Override
    public List<DeltaOICalculations> calculateOIDelta(JobDetails jobDetails) {

        ArrayList<InstrumentEntity> listOfNFOInstrumentsForCurrentWeek = instrumentService.findNFOInstrumentsForCurrentWeek();
        Iterable<OISnapshotEntity> listOfSnapshotTokens = instrumentService.findAllSnapShotTokens();

        Map<Long,Double> snapshotOI = new HashMap<>();
        List<DeltaOICalculations> deltaOICalculationsList = new ArrayList<>();
        List<DeltaOICalculations> deltaOIMiniList = new ArrayList<>();
        List<MiniDelta> miniDeltaList = new ArrayList<>();
        NiftyLTP niftyLTP =null;
        LTPTracker lTPTracker = null;

        boolean combiRateChanged = false;

        JobIterationDetails jobIterationDetails = new JobIterationDetails();
        jobIterationDetails.setJobDetails(jobDetails);
        jobIterationDetails.setIterationStatus(STATUS_RUNNING);
        jobIterationDetails.setIterationStartTime(LocalDateTime.now().withNano(I_ZERO));


        listOfSnapshotTokens.forEach(t-> {
            snapshotOI.put(t.getInstrument_token(),t.getOi());
        });

        ArrayList<Long> instrumentTokens = new ArrayList<>();
        listOfNFOInstrumentsForCurrentWeek.forEach(i->{
            instrumentTokens.add(i.getInstrument().getInstrument_token());
        });

        if(Objects.isNull(lastTradeDecisions)) {
            final JobIterationDetails defaultJid = jobIterationDetails;
            lastTradeDecisions = tradeDecisionRepository.findLatestTradeDecision()
                    .orElseGet(() -> new TradeDecisions("NA",TRADE_DECISION_TYPE_REGULAR,LocalDateTime.now(),0,"COMPLETE",new AppJobConfig(),defaultJid));
            logger.info("Fetching last trade decision from DB: {}", lastTradeDecisions);
        }

        logger.info("Latest Trade Decision: {}", lastTradeDecisions);

        try {

            kiteTickerProvider = new KiteTickerProvider(kiteConnectConfig.kiteConnect().getAccessToken(), kiteConnectConfig.kiteConnect().getApiKey(),null);
            boolean isTickerConnStarted = kiteTickerProvider.startTickerConnection();

            if (isTickerConnStarted) {

                subscribeForTokensAndNifty(kiteTickerProvider, instrumentTokens);
                niftyLastPrice = kiteTickerProvider.niftyLastPrice != null ? kiteTickerProvider.niftyLastPrice : D_ZERO;
            //    jobIterationDetails.setNiftyLTP(niftyLastPrice.intValue());
                logger.info("niftyLastPrice:: {}", niftyLastPrice);

                ArrayList<Long> niftyToken = new ArrayList<>();
                niftyToken.add(NIFTY_INSTRUMENT_TOKEN);

                Map<String, Quote> niftyQuote = transactionService.getMarketQuote(niftyToken);

                niftyOHLC = niftyQuote.get(String.valueOf(NIFTY_INSTRUMENT_TOKEN)).ohlc;

                logger.info("Nifty Last Price from Market Quote: {} and traded at: {}", niftyQuote.get(String.valueOf(NIFTY_INSTRUMENT_TOKEN)).lastPrice,niftyQuote.get(String.valueOf(NIFTY_INSTRUMENT_TOKEN)).timestamp);
                niftyLastPrice = niftyQuote.get(String.valueOf(NIFTY_INSTRUMENT_TOKEN)).lastPrice;
                int atmPrice = getAtmPrice(niftyLastPrice);

                logger.info("atmPrice:: {}", atmPrice);
                logger.info("Nifty Day high: {}, Nifty Day low: {}", niftyOHLC.high, niftyOHLC.low);

                jobIterationDetails.setIndexLTP(niftyLastPrice.intValue());

                if (!kiteTickerProvider.tickerMapForJob.isEmpty() && atmPrice != D_ZERO) {
                    deltaOICalculationsList = calculateDeltaOIFromNiftyPrice(listOfNFOInstrumentsForCurrentWeek, atmPrice, snapshotOI); //getting saved below

                    int threshold = calculateThreshold(deltaOICalculationsList); //it will get saved with job iteration details
                    logger.info("Threshold:: {}", threshold);

                    jobIterationDetails.setThreshold(threshold);
                    jobIterationDetails = jobIterationRepository.save(jobIterationDetails);

                    if (!deltaOICalculationsList.isEmpty()) {

                        deltaOIMiniList = filterListUsingThreshold(deltaOICalculationsList, threshold); //getting saved below

                        if (!deltaOIMiniList.isEmpty()) {
                            miniDeltaList = calculateMiniDeltaList(deltaOIMiniList, jobIterationDetails); //getting saved
                            calculateMeanValues();
                            niftyLTP = calculateNiftyLTPWithSupportAndResistance(deltaOIMiniList, deltaOICalculationsList); //right table
                            niftyLTP.setJobIterationDetails(jobIterationDetails);

                            logger.info("Job Iteration ID at time: {},{}", jobIterationDetails.getId(),jobIterationDetails.getIterationStartTime());
                            List<Integer> maxPainSPList = calculateMaxPain(miniDeltaList);
                            int maxPainSP = maxPainSPList.get(0);
                            int maxPainSPSecond = maxPainSPList.size() > 1 ? maxPainSPList.get(1) : I_ZERO;

                            niftyLTP.setMaxPainSP(maxPainSP);
                            niftyLTP.setMaxPainSPSecond(maxPainSPSecond);

                            logger.info("Max Pain SP:: {}", maxPainSP);

                            lTPTracker = getLTPTracker(niftyLTP,jobIterationDetails);

                            calculateStraddleFlags(niftyLTP,lTPTracker);
                            calculateMaxPainLTP(niftyLTP, lTPTracker);

                            List<String> commonMessageList = new ArrayList<>();
                            commonMessageList.add(String.valueOf(niftyLastPrice));
                            commonMessageList.add(String.valueOf(threshold));
                            commonMessageList.add(String.valueOf(niftyOHLC.low));
                            commonMessageList.add(String.valueOf(niftyOHLC.high));

                            sendPipeDelimitedCommonMessage(commonMessageList);

                            if (!prevItrMiniDeltaList.isEmpty()) {

                                MiniDelta totalRec = miniDeltaList.getLast();
                                MiniDelta prevItrTotalRec = prevItrMiniDeltaList.getLast();

                                calculateTradeDecision(totalRec, niftyLTP, prevItrNiftyLTP);

                                if (Objects.nonNull(prevItrNiftyLTP)) {

                                    calculatePowerTrades(prevItrNiftyLTP, niftyLTP);

                                    logger.info("Job Iteration ID: {}", jobIterationDetails.getId());

                                    calculateCPTS(prevItrNiftyLTP, niftyLTP);

                                    if (niftyLTP.getPowerTrade())
                                        calculatePowerTradeDeltaParams(niftyLTP, jobIterationDetails,deltaOIMiniList,prevItrTotalRec,totalRec);

                                    checkForIceBergTrades(prevItrNiftyLTP, niftyLTP);

                                    logger.info("Previous NiftyLTP :: {}", prevItrNiftyLTP.toString());

                                    if (Objects.nonNull(prevItrNiftyLTP.getTradeDecision())) {
                                        logger.info("lastTradeDecisionValue before checking whether it goes to trade management section or not :: {}", lastTradeDecisionValue);
                                        if (!prevItrNiftyLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_NO_TRADE) || lastTradeDecisionValue.equalsIgnoreCase(TRADE_DECISION_BUY) || lastTradeDecisionValue.equalsIgnoreCase(TRADE_DECISION_SELL)) {

                                            if (!prevItrNiftyLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_NO_TRADE))
                                                lastTradeDecisionValue = prevItrNiftyLTP.getTradeDecision();

                                            calculateTradeManagement(niftyLTP, prevItrNiftyLTP, lastTradeDecisionValue);

                                            if (niftyLTP.getTradeManagement().equalsIgnoreCase(TRADE_MANAGEMENT_CLOSE)) {
                                                lastTradeDecisionValue = TRADE_DECISION_NO_TRADE;

                                                sendEmailForClosingTrade(niftyLTP);
                                            }
                                            logger.info("NiftyLTP after trade management section :: {}", niftyLTP.toString());
                                            logger.info("lastTradeDecisionValue after trade management section :: {}", lastTradeDecisionValue);
                                        }


                                        if ((!niftyLTP.getMeanStrikePCR().equals(prevItrNiftyLTP.getMeanStrikePCR()))) {
                                            messagingService.sendNiftyLTPMessage(niftyLTP);
                                            combiRateChanged = true;
                                        }
                                    }
                                }

                                calculateColorCoding(miniDeltaList, prevItrMiniDeltaList);
                                messagingService.sendMiniDeltaMessage(miniDeltaList);

                              if (niftyLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_BUY) || niftyLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_SELL))
                                  calculateAndNotifySignalParameters(niftyLTP,jobIterationDetails);

                              logger.info("Last Trade Decision Object before AutoTrade eligibility check: {}", Objects.isNull(lastTradeDecisions)?"":lastTradeDecisions.toString());

                              if(!Objects.isNull(lastTradeDecisions) && lastTradeDecisions.getStatus().equalsIgnoreCase(ORDER_OPEN)) {
                                  logger.info("Checking for conditions of Auto Trade placement:");
                                  logger.info("NiftyLTP object: {}", niftyLTP.toString());
                                  logger.info("Last Trade Decision object: {}", lastTradeDecisions.toString());
                                  logger.info("Auto Trade Params object: {}", autoTradeParamsJob.toString());

                                  boolean placeOrderFlg = false;
                                  // check for indexLTP value for entry price
                                  if(lastTradeDecisions.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_BUY))
                                        placeOrderFlg = processBuySignal(niftyLTP, autoTradeParamsJob, lastTradeDecisions,jobIterationDetails);

                                  else if(lastTradeDecisions.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_SELL)) {
                                        placeOrderFlg = processSellSignal(niftyLTP, autoTradeParamsJob, lastTradeDecisions,jobIterationDetails);
                                  }

                                  if(placeOrderFlg && autoTradeParamsJob.isAutoTradeFlag())
                                      placeAutoTradeOrder(niftyLTP);

                              }

                            } else {
                                messagingService.sendNiftyLTPMessage(niftyLTP);
                                messagingService.sendMiniDeltaMessage(miniDeltaList);
                            }

                            if (combiRateChanged) {
                                miniDeltaList = (List<MiniDelta>) miniDeltaRepository.saveAll(miniDeltaList);
                                logger.info("MiniDelta List saved successfully");
                            }

                            niftyLTP = niftyLTPRepository.save(niftyLTP);
                            logger.info("NiftyLTP saved successfully");

                            prevItrNiftyLTP = niftyLTP;
                            prevItrMiniDeltaList = miniDeltaList;

                        }

                        if (combiRateChanged) {
                            saveDeltaListToDB(deltaOICalculationsList, jobIterationDetails);
                            saveDeltaOIMiniListToDB(deltaOIMiniList, jobIterationDetails);
                        }

                        if(Objects.nonNull(lTPTracker)) {
                            ltpTrackerRepository.save(lTPTracker);
                            logger.info("LTPTracker saved successfully: {}", lTPTracker.toString());
                        }

                    }

                }

                resetStaticVariables();
            }

            if(jobIterationDetails.getIterationStatus().equalsIgnoreCase(STATUS_RUNNING))
            {
                jobIterationDetails.setIterationStatus(STATUS_COMPLETED);
                jobIterationDetails.setIterationEndTime(LocalDateTime.now().withNano(I_ZERO));
                jobIterationDetails=jobIterationRepository.save(jobIterationDetails);

                logger.info("Job iteration details saved successfully with ID: {}", jobIterationDetails.getId());
            }

        } catch (RuntimeException e) {
            logger.error("Error during OI calculations for iteration : {},{}", jobIterationDetails.getIterationStartTime(),e.getMessage());
            jobIterationDetails.setIterationStatus(STATUS_FAILED);
            jobIterationDetails.setIterationEndTime(LocalDateTime.now().withNano(I_ZERO));
            jobIterationDetails=jobIterationRepository.save(jobIterationDetails);

            logger.error("Exception during OI calculations: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }

        return  deltaOIMiniList;
    }

    private void placeAutoTradeOrder(NiftyLTP niftyLTP) {

        logger.info("Conditions met and Placing order asynchronously for NiftyLTP: {} and with autoTradeParamsJob: {}", niftyLTP.toString(), autoTradeParamsJob.toString());

                                   //  lastTradeDecisions.setStatus(ORDER_COMPLETE);

                                   /*   asyncService.placeOrderAsync(niftyLTP, autoTradeParamsJob).thenRun(()-> {
                                            // Code to execute after successful order placement
                                            lastTradeDecisions.setStatus(ORDER_COMPLETE);
                                            logger.info("Order placed successfully for NiftyLTP: {}", lastTradeDecisions.toString());
                                        }).exceptionally(ex -> {
                                            // Handle exceptions
                                            logger.error("Error occurred while placing order: {}", ex.getMessage());
                                            return null;
                                      }); */



    }

    private boolean processSellSignal(NiftyLTP niftyLTP, AutoTradeParams autoTradeParamsJob, TradeDecisions lastTradeDecisions, JobIterationDetails jobIterationDetails) {
        boolean placeOrderFlg = false;
        logger.info("Checking conditions for placing SELL order");
        if (Objects.nonNull(autoTradeParamsJob.getAutoTradeCallEntryPrice()) && niftyLTP.getNiftyLTP() > autoTradeParamsJob.getAutoTradePutEntryPrice())
            placeOrderFlg = true;

        logger.info("Checking conditions to check whether Sell signal is hit or failed");
        logger.info("SELL SIGNAL :: Current NiftyLTP: {}, Last trade decision:: Target : {}, StopLoss: {}",niftyLTP.getNiftyLTP(),lastTradeDecisions.getTargetIndexLTP(),lastTradeDecisions.getStopLossIndexLTP());


        if(niftyLTP.getNiftyLTP() <= lastTradeDecisions.getTargetIndexLTP())
        {
            // If target is hit, set status to ORDER_LAPSED
            lastTradeDecisions.setStatus(ORDER_COMPLETE);
            lastTradeDecisions.setTrade_decision_result(TARGET_HIT);
            lastTradeDecisions.setTrade_decision_result_ts(LocalDateTime.now());
            tradeDecisionRepository.save(lastTradeDecisions);
            logger.info("Target hit for SELL order, setting status to ORDER_COMPLETE: {}, {}",niftyLTP.toString(),lastTradeDecisions.toString());
         //   lastTradeDecisions = saveTradeDecision(niftyLTP, jobIterationDetails, ORDER_COMPLETE);
            sendCommonMessage("Trade decision is set successfully to COMPLETE",true, SUCCESS);
        }

        else if(niftyLTP.getNiftyLTP() >= lastTradeDecisions.getStopLossIndexLTP() && lastTradeDecisions.getTradeDecisionType().equalsIgnoreCase(TRADE_DECISION_TYPE_REGULAR))
        {
            // If stop loss is hit, set status to ORDER_LAPSED
            lastTradeDecisions.setStatus(ORDER_LAPSED);
            lastTradeDecisions.setTrade_decision_result(STOPLOSS_HIT);
            lastTradeDecisions.setTrade_decision_result_ts(LocalDateTime.now());
            logger.info("Stop Loss hit for SELL order, setting status to ORDER_LAPSED: {}, {}",niftyLTP.toString(),lastTradeDecisions.toString());
            tradeDecisionRepository.save(lastTradeDecisions);
            sendCommonMessage("STOP-LOSS HIT: SELL SIGNAL FAILED",true, SUCCESS);
            // trigger email notification for stop loss hit
            // create a new buy signal
            niftyLTP.setTradeDecision(TRADE_DECISION_BUY);
            sendEmailOfSLTradeDecision(niftyLTP);
            //process stoploss signal
            processSLSignal(niftyLTP,jobIterationDetails);
        }

        else if(lastTradeDecisions.getTradeDecisionType().equalsIgnoreCase(TRADE_DECISION_TYPE_STOPLOSS) && !niftyLTP.getTradeDecision().equalsIgnoreCase(lastTradeDecisions.getTradeDecision()) && !niftyLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_NO_TRADE))
        {
            // If stop loss is hit, set status to ORDER_LAPSED
            lastTradeDecisions.setStatus(ORDER_LAPSED);
            lastTradeDecisions.setTrade_decision_result(FAILURE);
            lastTradeDecisions.setTrade_decision_result_ts(LocalDateTime.now());
            logger.info("Failed SL SELL order, setting status to ORDER_LAPSED: {}, {}",niftyLTP.toString(),lastTradeDecisions.toString());
            tradeDecisionRepository.save(lastTradeDecisions);
            sendCommonMessage("SELL SIGNAL (SL) FAILED",true, ERROR);
            //exit positions
          /*  if(autoTradeParamsJob.isAutoTradeFlag())
                transactionService.closeOrders(); */
        }

        return placeOrderFlg;
    }

    private boolean processBuySignal(NiftyLTP niftyLTP, AutoTradeParams autoTradeParamsJob, TradeDecisions lastTradeDecisions,JobIterationDetails jobIterationDetails) {
        boolean placeOrderFlg = false;
        logger.info("Checking conditions for placing BUY order");

        if(Objects.nonNull(autoTradeParamsJob.getAutoTradeCallEntryPrice()) && niftyLTP.getNiftyLTP()<autoTradeParamsJob.getAutoTradeCallEntryPrice())
            placeOrderFlg = true;

        logger.info("Checking conditions to check whether Buy signal is hit or failed");
        logger.info("BUY SIGNAL :: Current NiftyLTP: {}, Last trade decision:: Target : {}, StopLoss: {}",niftyLTP.getNiftyLTP(),lastTradeDecisions.getTargetIndexLTP(),lastTradeDecisions.getStopLossIndexLTP());

        if(niftyLTP.getNiftyLTP() >= lastTradeDecisions.getTargetIndexLTP())
        {
            lastTradeDecisions.setStatus(ORDER_COMPLETE);
            lastTradeDecisions.setTrade_decision_result(TARGET_HIT);
            lastTradeDecisions.setTrade_decision_result_ts(LocalDateTime.now());
            tradeDecisionRepository.save(lastTradeDecisions);
            logger.info("Target hit for BUY order, setting status to ORDER_COMPLETE: {}, {}",niftyLTP.toString(),lastTradeDecisions.toString());
         //   lastTradeDecisions = saveTradeDecision(niftyLTP, jobIterationDetails, ORDER_COMPLETE);
            sendCommonMessage("Trade decision is set successfully to COMPLETE",true, SUCCESS);
        }

        else if(niftyLTP.getNiftyLTP() <= lastTradeDecisions.getStopLossIndexLTP() && lastTradeDecisions.getTradeDecisionType().equalsIgnoreCase(TRADE_DECISION_TYPE_REGULAR))
        {
            lastTradeDecisions.setStatus(ORDER_LAPSED);
            lastTradeDecisions.setTrade_decision_result(STOPLOSS_HIT);
            lastTradeDecisions.setTrade_decision_result_ts(LocalDateTime.now());
            logger.info("Stop Loss hit for BUY order, setting status to ORDER_LAPSED: {}, {}",niftyLTP.toString(),lastTradeDecisions.toString());
            tradeDecisionRepository.save(lastTradeDecisions);
            sendCommonMessage("STOP-LOSS HIT: BUY SIGNAL FAILED",true, SUCCESS);
            // trigger email notification for stop loss hit
            // create a new sell signal
            niftyLTP.setTradeDecision(TRADE_DECISION_SELL);
            sendEmailOfSLTradeDecision(niftyLTP);
            //process stoploss signal
            processSLSignal(niftyLTP,jobIterationDetails);
        }

        else if(lastTradeDecisions.getTradeDecisionType().equalsIgnoreCase(TRADE_DECISION_TYPE_STOPLOSS) && !niftyLTP.getTradeDecision().equalsIgnoreCase(lastTradeDecisions.getTradeDecision()) && !niftyLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_NO_TRADE))
        {
            // If stop loss is hit, set status to ORDER_LAPSED
            lastTradeDecisions.setStatus(ORDER_LAPSED);
            lastTradeDecisions.setTrade_decision_result(FAILURE);
            lastTradeDecisions.setTrade_decision_result_ts(LocalDateTime.now());
            logger.info("SL BUY order FAILED, setting status to ORDER_LAPSED: {}, {}",niftyLTP.toString(),lastTradeDecisions.toString());
            tradeDecisionRepository.save(lastTradeDecisions);
            sendCommonMessage(" BUY SIGNAL (SL) FAILED",true, ERROR);
            //exit positions
          /*  if(autoTradeParamsJob.isAutoTradeFlag())
                transactionService.closeOrders(); */
        }

        return placeOrderFlg;
    }

    private void sendEmailOfSLTradeDecision(NiftyLTP niftyLTP) {

        asyncService.sendEmailToAllUsersAsync("KTM: STOPLOSS: Trade Decision: "+niftyLTP.getTradeDecision()+" from Profile: "+kiteConnectConfig.getActiveProfile(), "SL Trade Decision generated: "+niftyLTP.toString() + "|| DAY LOW: "+niftyOHLC.low + " || DAY HIGH: "+niftyOHLC.high, "")
                .thenRun(() -> {
                    // Code to execute after successful completion
                    logger.info("Email for SL sent successfully!");
                })
                .exceptionally(ex -> {
                    // Handle exceptions
                    logger.error("Error occurred: {}", ex.getMessage());
                    return null;
                });

    }

    private void processSLSignal(NiftyLTP niftyLTP,JobIterationDetails jobIterationDetails) {

        //prepare lastTradeDecision
        lastTradeDecisions = saveTradeDecisionSL(niftyLTP, jobIterationDetails, ORDER_OPEN);
        sendCommonMessage("STOP-LOSS Trade decision is set successfully to OPEN",true, WARNING);
        //prepare autoTradeParams
        processAutoTradeParamsForSL(niftyLTP,jobIterationDetails);


    }

    private void processAutoTradeParamsForSL(NiftyLTP niftyLTP,JobIterationDetails jobIterationDetails) {
        autoTradeParamsJob.setAutoTradeCallEntryPrice(niftyLTP.getNiftyLTP());
        autoTradeParamsJob.setAutoTradePutEntryPrice(niftyLTP.getNiftyLTP());
        autoTradeParamsJob.setAutoTradeParamsTS(LocalDateTime.now());
        autoTradeParamsJob.setJobIterationDetails(jobIterationDetails);
        autoTradeService.saveAutoTradeParams(autoTradeParamsJob);
    }

    private void calculateAndNotifySignalParameters(NiftyLTP niftyLTP, JobIterationDetails jobIterationDetails) {

        calculateCloseFullFlag(niftyLTP);
        calculateLotSize(niftyLTP);
        sendEmailOfTradeDecision(niftyLTP);

        if (autoTradeParamsJob.isAutoTradeFlag()) {
            //   asyncService.closeOrdersAsync(niftyLTP);
            calculateAndSaveAutoTradeJobParams(autoTradeParamsJob, niftyLTP);
            sendCommonMessage("Auto Trade Job Parameters are set successfully",true, SUCCESS);

                                     /*   if(!lastTradeDecisions.getStatus().equalsIgnoreCase(ORDER_OPEN) && lastTradeDecisions.getTradeDecisionTS().toLocalDate().equals(LocalDateTime.now().toLocalDate()))
                                            asyncService.placeOrderAsync(niftyLTP, autoTradeParamsJob); */
        }

        if(!lastTradeDecisions.getStatus().equalsIgnoreCase(ORDER_OPEN)) {
            lastTradeDecisions = saveTradeDecision(niftyLTP, jobIterationDetails, ORDER_OPEN);
            logger.info("Trade decision is set successfully to OPEN: {}", lastTradeDecisions.toString());
            sendCommonMessage("Trade decision is set successfully to OPEN",true, WARNING);
        }
        else if(lastTradeDecisions.getStatus().equalsIgnoreCase(ORDER_OPEN) && !niftyLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_NO_TRADE) && !lastTradeDecisions.getTradeDecision().equalsIgnoreCase(niftyLTP.getTradeDecision())) {
            lastTradeDecisions.setStatus(ORDER_COMPLETE);
            lastTradeDecisions.setTrade_decision_result(FAILURE);
            lastTradeDecisions.setTrade_decision_result_ts(LocalDateTime.now());
            tradeDecisionRepository.save(lastTradeDecisions);
            sendCommonMessage("Previous Trade decision is set successfully to COMPLETE",true, SUCCESS);
            lastTradeDecisions = saveTradeDecision(niftyLTP, jobIterationDetails, ORDER_OPEN);
            sendCommonMessage("Trade decision is set successfully to OPEN",true, WARNING);
        }

    }

    public TradeDecisions saveTradeDecision(NiftyLTP niftyLTP, JobIterationDetails jobIterationDetails, String tradeDecisionStatus) {
        String tradeDecision = niftyLTP.getTradeDecision();
        TradeDecisions tradeDecisions = new TradeDecisions();
        tradeDecisions.setIndexLTP(niftyLTP.getNiftyLTP());
        tradeDecisions.setStatus(tradeDecisionStatus);
        tradeDecisions.setTradeDecision(tradeDecision);
        tradeDecisions.setJobIterationDetails(jobIterationDetails);
        tradeDecisions.setTradeDecisionTS(niftyLTP.getNiftyTS());
        tradeDecisions.setEntryIndexLTP(tradeDecision.equalsIgnoreCase(TRADE_DECISION_BUY) ? Integer.parseInt(niftyLTP.getSupport()) - TWENTY_FOUR : Integer.parseInt(niftyLTP.getResistance()) + TWENTY_FOUR);
        tradeDecisions.setTargetIndexLTP(tradeDecision.equalsIgnoreCase(TRADE_DECISION_BUY) ? Integer.parseInt(niftyLTP.getResistance()) - D_FIVE.intValue() : Integer.parseInt(niftyLTP.getSupport()) + D_FIVE.intValue());
        tradeDecisions.setStopLossIndexLTP(tradeDecision.equalsIgnoreCase(TRADE_DECISION_BUY) ? Integer.parseInt(niftyLTP.getSupport()) - FIFTY : Integer.parseInt(niftyLTP.getResistance()) + FIFTY);

        tradeDecisions = tradeDecisionRepository.save(tradeDecisions);
        logger.info("Trade Decision saved successfully: {}", tradeDecisions.toString());
        return tradeDecisions;
    }

    public TradeDecisions saveTradeDecisionSL(NiftyLTP niftyLTP, JobIterationDetails jobIterationDetails, String tradeDecisionStatus) {
        String tradeDecision = niftyLTP.getTradeDecision();
        TradeDecisions tradeDecisions = new TradeDecisions();
        tradeDecisions.setIndexLTP(niftyLTP.getNiftyLTP());
        tradeDecisions.setStatus(tradeDecisionStatus);
        tradeDecisions.setTradeDecision(tradeDecision);
        tradeDecisions.setTradeDecisionType(TRADE_DECISION_TYPE_STOPLOSS);
        tradeDecisions.setJobIterationDetails(jobIterationDetails);
        tradeDecisions.setTradeDecisionTS(niftyLTP.getNiftyTS());
        tradeDecisions.setEntryIndexLTP(niftyLTP.getNiftyLTP());
        tradeDecisions.setTargetIndexLTP(tradeDecision.equalsIgnoreCase(TRADE_DECISION_BUY) ? niftyLTP.getNiftyLTP() + TWENTY_FIVE : niftyLTP.getNiftyLTP() - TWENTY_FIVE);
        tradeDecisions.setStopLossIndexLTP(tradeDecision.equalsIgnoreCase(TRADE_DECISION_BUY) ? niftyLTP.getNiftyLTP() - TWENTY_FIVE : niftyLTP.getNiftyLTP() + TWENTY_FIVE);

        tradeDecisions = tradeDecisionRepository.save(tradeDecisions);
        logger.info("STOP-LOSS Trade Decision saved successfully: {}", tradeDecisions.toString());
        return tradeDecisions;
    }

    @Override
    public void calculateLotSize(NiftyLTP niftyLTP) {

        int lotSize=0;

        if (niftyLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_SELL) && (niftyLTP.getNiftyLTP() > niftyLTP.getMaxPainSP()))
        {
            lotSize = Math.abs(niftyLTP.getNiftyLTP()-niftyLTP.getMaxPainSP()) / 10;
        }
        else if (niftyLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_BUY) && (niftyLTP.getNiftyLTP() < niftyLTP.getMaxPainSP()))
        {
            lotSize = Math.abs(niftyLTP.getNiftyLTP()-niftyLTP.getMaxPainSP()) / 10;
        }

        niftyLTP.setLotSize(lotSize);

    }

    @Override
    public AutoTradeParams resetAutoTradeParams() {

        NiftyLTP latestNiftyLTP = niftyLTPRepository.getLatestNiftyLTP();

        calculateAndSaveAutoTradeJobParams(autoTradeParamsJob,latestNiftyLTP);
        return autoTradeParamsJob;
    }

    @Override
    public boolean startKiteTicker() {

        if(isTickerConnected.get())
            return true;

        kiteTickerProvider = new KiteTickerProvider(kiteConnectConfig.kiteConnect().getAccessToken(), kiteConnectConfig.kiteConnect().getApiKey(),null);
        isTickerConnected.set(kiteTickerProvider.startTickerConnection());
        return isTickerConnected.get();
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
    public Boolean setAutoTradeEnabled(AppJobConfigParams appJobConfigParams) {

        AppJobConfig appJobConfig = appJobConfigRepository.findById(appJobConfigParams.getAppJobConfigNum()).orElse(null);
        if (appJobConfig == null) {
            logger.error("No AppJobConfig found for num: {}", appJobConfigParams.getAppJobConfigNum());
            return false;
        }
        appJobConfig.setAutoTradeEnabled(appJobConfigParams.getIsAutoTradeEnabled());
        appJobConfig = appJobConfigRepository.save(appJobConfig);
        return appJobConfig.getAutoTradeEnabled();
    }

    @Override
    public void calculateMaxPainLTP(NiftyLTP niftyLTP, LTPTracker lTPTracker) {

        niftyLTP.setMaxPainCELTP(lTPTracker.getMaxPainCELTP());
        niftyLTP.setMaxPainPELTP(lTPTracker.getMaxPainPELTP());

    }

    @Override
    public void calculateStraddleFlags(NiftyLTP niftyLTP, LTPTracker lTPTracker) {

        double mpCE = niftyLTP.getMaxPainSP() + lTPTracker.getMaxPainCELTP();
        double mpPE = niftyLTP.getMaxPainSP() - lTPTracker.getMaxPainPELTP();

        logger.info("Max Pain CE LTP: {}, Max Pain PE LTP: {}", mpCE, mpPE);

        niftyLTP.setStraddleUpside(mpCE > Integer.parseInt(niftyLTP.getResistance()));
        niftyLTP.setStraddleDownside(mpPE < Integer.parseInt(niftyLTP.getSupport()));

        logger.info("Straddle Upside: {}, Straddle Downside: {}", niftyLTP.isStraddleUpside(), niftyLTP.isStraddleDownside());


    }

    @Override
    public void calculateCloseFullFlag(NiftyLTP niftyLTP) {

        logger.info("Calculating Close Full flag for NiftyLTP trade decision: {} , and straddle points isStraddleUpside {}, isStraddleDownside {}", niftyLTP.getTradeDecision(), niftyLTP.isStraddleUpside(), niftyLTP.isStraddleDownside());

        if (niftyLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_BUY) && niftyLTP.isStraddleUpside())
            niftyLTP.setCloseFull(true);
        else if (niftyLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_SELL) && niftyLTP.isStraddleDownside())
            niftyLTP.setCloseFull(true);

        logger.info("Close Full flag value: {}", niftyLTP.isCloseFull());

    }

    private void sendPipeDelimitedCommonMessage(List<String> commonMessageList) {
        CommonReqRes thresholdMessage = new CommonReqRes();
        StringBuilder messageBuilder = new StringBuilder();
        commonMessageList.forEach(i->messageBuilder.append(i).append("|"));
        // Remove the last pipe character
        if (!messageBuilder.isEmpty()) {
            messageBuilder.deleteCharAt(messageBuilder.length() - 1);
        }
        logger.info("Pipe delimited message: {}", messageBuilder.toString());
        String message = messageBuilder.toString();
        thresholdMessage.setMessage(message);
        thresholdMessage.setStatus(true);
        thresholdMessage.setQty(commonMessageList.size());
        messagingService.sendNiftyLTPValue(thresholdMessage);
        logger.info("Pipe delimited message sent successfully");
    }

    public List<Integer> calculateMaxPain(List<MiniDelta> miniDeltaList) {

        // Calculate loss at each strike and determine min
        Map<Integer, Long> losses = new HashMap<>();
        miniDeltaList.forEach(i -> {
            if (i.getStrikePrice().equals(TOTAL))
                return;
            losses.put(Integer.parseInt(i.getStrikePrice()), calculateTotalLossForSP(Integer.parseInt(i.getStrikePrice()), miniDeltaList));
        });

        // Output
        logger.info("Total Losses at Each Strike:");
        losses.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> logger.info("Strike {}: Loss = {}", entry.getKey(), entry.getValue()));

        // Sort losses by value and extract the minimum and second minimum
        List<Integer> result = losses.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .limit(2)
                .map(Map.Entry::getKey)
                .toList();

        logger.info("Max Pain calculated: {}, Second Max Pain: {}", result.get(0), result.size() > 1 ? result.get(1) : "N/A");

        return result;
    }

    private LTPTracker getLTPTracker(NiftyLTP niftyLTP, JobIterationDetails jobIterationDetails) {

        logger.info("Getting LTP values for NiftyLTP: {}", niftyLTP.toString());
        logger.info("LTP for Range: {}, Call: {}, {}", niftyLTP.getRange(), ltpMap.get(niftyLTP.getRange().split("-")[0]+"CE"), ltpMap.get(niftyLTP.getRange().split("-")[1]+"CE"));
        logger.info("LTP for Range: {}, Put: {}, {}", niftyLTP.getRange(), ltpMap.get(niftyLTP.getRange().split("-")[0]+"PE"), ltpMap.get(niftyLTP.getRange().split("-")[1]+"PE"));


        LTPTracker lTPTracker = new LTPTracker();

        lTPTracker.setJobIterationDetails(jobIterationDetails);
        lTPTracker.setIndexTS(niftyLTP.getNiftyTS());
        lTPTracker.setIndexLTP(niftyLTP.getNiftyLTP());

        lTPTracker.setRangeLowSP(niftyLTP.getRange().split("-")[0]);
        lTPTracker.setRangeLowLTP(ltpMap.get(niftyLTP.getRange().split("-")[0]+"PE"));

        lTPTracker.setRangeHighSP(niftyLTP.getRange().split("-")[1]);
        lTPTracker.setRangeHighLTP(ltpMap.get(niftyLTP.getRange().split("-")[1]+"CE"));

        lTPTracker.setSupportSP(niftyLTP.getSupport());
         if(ltpMap.containsKey(niftyLTP.getSupport()+"PE"))
            lTPTracker.setSupportLTP(ltpMap.get(niftyLTP.getSupport()+"PE"));
         else
            lTPTracker.setSupportLTP(ltpMap.get(Integer.parseInt(niftyLTP.getSupport()) - 25 +"PE"));

        lTPTracker.setResistanceSP(niftyLTP.getResistance());

        if(ltpMap.containsKey(niftyLTP.getResistance()+"CE"))
            lTPTracker.setResistanceLTP(ltpMap.get(niftyLTP.getResistance()+"CE"));
        else
            lTPTracker.setResistanceLTP(ltpMap.get(Integer.parseInt(niftyLTP.getResistance()) - 25 +"CE"));

        logger.info("LTP for Support: {}, Call: {}",niftyLTP.getSupport(), lTPTracker.getSupportLTP());
        logger.info("LTP for Resistance: {}, Put: {}",niftyLTP.getResistance(), lTPTracker.getResistanceLTP());

        String maxPainSP = String.valueOf(niftyLTP.getMaxPainSP());

        lTPTracker.setMaxPainSP(maxPainSP);
        lTPTracker.setMaxPainCELTP(ltpMap.get(maxPainSP+"CE"));
        lTPTracker.setMaxPainPELTP(ltpMap.get(maxPainSP+"PE"));

        return lTPTracker;

    }

    private void calculateTradeDecisionOnPowerTrade(NiftyLTP niftyLTP) {

        if(!niftyLTP.getSupport().equalsIgnoreCase(niftyLTP.getResistance())) {
            if ((Integer.parseInt(niftyLTP.getResistance()) - niftyLTP.getNiftyLTP() >= TWENTY_FIVE) && niftyLTP.getPowerTrade() && niftyLTP.getPowerTradeType().startsWith(TRADER_TYPE_BUY)) {
                niftyLTP.setTradeDecision(POWER_TRADE_DECISION_BUY);
            } else if ((niftyLTP.getNiftyLTP() - Integer.parseInt(niftyLTP.getSupport()) >= TWENTY_FIVE) && niftyLTP.getPowerTrade() && niftyLTP.getPowerTradeType().startsWith(TRADER_TYPE_SELL)) {
                niftyLTP.setTradeDecision(POWER_TRADE_DECISION_SELL);
            } else {
                niftyLTP.setTradeDecision(TRADE_DECISION_NO_TRADE);
            }
        }

    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    private PowerTradeDeltaOI calculatePowerTradeDeltaOI(NiftyLTP niftyLTP, JobIterationDetails jobIterationDetails, MiniDelta prevItrTotalRec, MiniDelta totalRec) {

        PowerTradeDeltaOI powerTradeDeltaOI = new PowerTradeDeltaOI();

        powerTradeDeltaOI.setJobIterationDetails(jobIterationDetails);

        powerTradeDeltaOI.setNiftyTS(niftyLTP.getNiftyTS());

        powerTradeDeltaOI.setPowerTradeType(niftyLTP.getPowerTradeType());

        powerTradeDeltaOI.setPutOIChange(totalRec.getPutOI()-prevItrTotalRec.getPutOI());

        powerTradeDeltaOI.setCallOIChange(totalRec.getCallOI()-prevItrTotalRec.getCallOI());

        powerTradeDeltaOI.setNiftyLTP(niftyLTP.getNiftyLTP());

        powerTradeDeltaOI = powerTradeDeltaOIRepository.save(powerTradeDeltaOI);

        return powerTradeDeltaOI;

    }

    private void calculateCPTS(NiftyLTP prevItrNiftyLTP, NiftyLTP niftyLTP) {
        double changePCR = niftyLTP.getMeanStrikePCR() - prevItrNiftyLTP.getMeanStrikePCR();
        double currentCPTS = prevItrNiftyLTP.getCpts() != null ? prevItrNiftyLTP.getCpts() : 0.0;

        if (niftyLTP.getPowerTradeType().startsWith(TRADER_TYPE_BUY) || niftyLTP.getPowerTradeType().startsWith(TRADER_TYPE_SELL)) {
            currentCPTS += changePCR;
        }

        niftyLTP.setCpts(currentCPTS);
        logger.info("Updated CPTS: {}", currentCPTS);
    }

    private void sendEmailOfTradeDecision(NiftyLTP niftyLTP) {

        asyncService.sendEmailToAllUsersAsync("KTM: Trade Decision: "+niftyLTP.getTradeDecision()+" from Profile: "+kiteConnectConfig.getActiveProfile(), "Trade Decision generated: "+niftyLTP.toString(), "")
                .thenRun(() -> {
                    // Code to execute after successful completion
                    logger.info("Email sent successfully!");
                })
                .exceptionally(ex -> {
                    // Handle exceptions
                    logger.error("Error occurred: {}", ex.getMessage());
                    return null;
                });

    }

    private void sendEmailForClosingTrade(NiftyLTP niftyLTP) {

        asyncService.sendEmailToAllUsersAsync("KTM: Trade Management: "+niftyLTP.getTradeManagement(), "Trade Management generated: "+niftyLTP.toString(), "")
                .thenRun(() -> {
                    // Code to execute after successful completion
                    logger.info("Email sent successfully!");
                })
                .exceptionally(ex -> {
                    // Handle exceptions
                    logger.error("Error occurred: {}", ex.getMessage());
                    return null;
                });

    }

    private void checkForIceBergTrades(NiftyLTP prevItrNiftyLTP, NiftyLTP niftyLTP ) {

        if(niftyLTP.getRange().equalsIgnoreCase(prevItrNiftyLTP.getRange())) {

            double changePCR = niftyLTP.getMeanStrikePCR() - prevItrNiftyLTP.getMeanStrikePCR();
            if (changePCR > D_ZERO) {
                iceBergPositiveTrendCounter++;
                iceBergNegativeTrendCounter = I_ZERO;
            } else if (changePCR < 0) {
                iceBergNegativeTrendCounter++;
                iceBergPositiveTrendCounter = I_ZERO;
            }

            if (iceBergPositiveTrendCounter > 0) {
                niftyLTP.setIcebergTrendCounter(iceBergPositiveTrendCounter);
                niftyLTP.setIcebergTradeType(TRADER_TYPE_BUY);
            } else if (iceBergNegativeTrendCounter > 0) {
                niftyLTP.setIcebergTrendCounter(iceBergNegativeTrendCounter);
                niftyLTP.setIcebergTradeType(TRADER_TYPE_SELL);
            }
        }

        else
        {
            iceBergPositiveTrendCounter = I_ZERO;
            iceBergNegativeTrendCounter = I_ZERO;
        }

    }

     private void calculatePowerTrades(NiftyLTP prevItrNiftyLTP, NiftyLTP niftyLTP) {

        try {

            if(niftyLTP.getRange().equalsIgnoreCase(prevItrNiftyLTP.getRange()) && !prevItrNiftyLTP.getMeanStrikePCR().equals(D_ZERO)) {

                double changePCR = niftyLTP.getMeanStrikePCR() - prevItrNiftyLTP.getMeanStrikePCR();
                double changePCRPC = (changePCR / prevItrNiftyLTP.getMeanStrikePCR()) * ONE_HUNDRED;

                if (Math.abs(changePCRPC) >= PTC_3) {
                    niftyLTP.setPowerTrade(true);
                    niftyLTP.setPowerTradeType(changePCRPC > 0 ? TRADER_TYPE_BBB : TRADER_TYPE_SSS);
                }
                else if(Math.abs(changePCRPC) >= PTC_2) {
                    niftyLTP.setPowerTrade(true);
                    niftyLTP.setPowerTradeType(changePCRPC > 0 ? TRADER_TYPE_BB : TRADER_TYPE_SS);
                }
                else if(Math.abs(changePCRPC) >= PTC_1) {
                    niftyLTP.setPowerTrade(true);
                    niftyLTP.setPowerTradeType(changePCRPC > 0 ? TRADER_TYPE_BUY : TRADER_TYPE_SELL);
                }

                else {
                    niftyLTP.setPowerTrade(false);
                    niftyLTP.setPowerTradeType("");
                }

            }

            else {
                niftyLTP.setPowerTrade(false);
                niftyLTP.setPowerTradeType("");
            }

        } catch (RuntimeException e) {
            throw new RuntimeException(e);
        }
    }

    private void calculateInitiativeTraders(NiftyLTP prevItrNiftyLTP, NiftyLTP niftyLTP) {
        if ((niftyLTP.getNiftyLTP() > prevItrNiftyLTP.getNiftyLTP()) || (niftyLTP.getNiftyLTP() > Integer.parseInt(niftyLTP.getResistance())) && niftyLTP.getPowerTradeType().equalsIgnoreCase(TRADER_TYPE_BUY))
             niftyLTP.setIBuyers(true);

        if ((niftyLTP.getNiftyLTP() < prevItrNiftyLTP.getNiftyLTP()) || (niftyLTP.getNiftyLTP() < Integer.parseInt(niftyLTP.getSupport())) && niftyLTP.getPowerTradeType().equalsIgnoreCase(TRADER_TYPE_SELL))
            niftyLTP.setISellers(true);
    }

    private void calculateTradeManagement(NiftyLTP niftyLTP, NiftyLTP prevItrNiftyLTP,String lastTradeDecision) {
        logger.info("Entering trade management with niftyLTP::"+prevItrNiftyLTP.getTradeDecision());

        String tradeManagement = TRADE_MANAGEMENT_HOLD;
      //  String lastTradeDecision = prevItrNiftyLTP.getTradeDecision();
        double pcr = niftyLTP.getMeanStrikePCR();
        double rateOI = niftyLTP.getMeanRateOI();
        double niftyLTPValue = niftyLTP.getNiftyLTP();
        double resistance = Double.parseDouble(niftyLTP.getResistance());
        double support = Double.parseDouble(niftyLTP.getSupport());
        double changePCR = niftyLTP.getMeanStrikePCR() - prevItrNiftyLTP.getMeanStrikePCR();
        double changeOI = niftyLTP.getMeanRateOI()- prevItrNiftyLTP.getMeanRateOI();
        Integer tradeHoldCount = prevItrNiftyLTP.getTradeHoldCount()!=null?prevItrNiftyLTP.getTradeHoldCount():0;

        if(prevItrNiftyLTP.getRange().equalsIgnoreCase(niftyLTP.getRange())) {

            tradeHoldCount = getTradeHoldCount(lastTradeDecision, changePCR, changeOI, tradeHoldCount);
            logger.info("tradeHoldCount::" + tradeHoldCount);
            if (TRADE_DECISION_BUY.equals(lastTradeDecision)) {
                if (pcr < I_ONE || rateOI < I_ONE) {
                    if ((niftyLTPValue >= resistance - I_TWO) || tradeHoldCount >= I_TWO) {
                        tradeManagement = TRADE_MANAGEMENT_CLOSE;
                    }
                } else if (changePCR < D_ZERO && changeOI < D_ZERO) {
                    tradeManagement = TRADE_MANAGEMENT_CLOSE;
                }

            } else if (TRADE_DECISION_SELL.equals(lastTradeDecision)) {
                if (pcr > I_ONE || rateOI > I_ONE) {
                    if ((niftyLTPValue <= support + I_TWO) || (tradeHoldCount >= I_TWO)) {
                        tradeManagement = TRADE_MANAGEMENT_CLOSE;
                    }
                } else if (changePCR > D_ZERO || changeOI > D_ZERO) {
                    tradeManagement = TRADE_MANAGEMENT_CLOSE;
                }
            }
        }

        niftyLTP.setTradeManagement(tradeManagement);
        niftyLTP.setTradeHoldCount(tradeHoldCount);
    }

    private Integer getTradeHoldCount(String tradeDecision,double changePCR, double changeOI, Integer tradeHoldCount) {


        if(TRADE_DECISION_BUY.equals(tradeDecision))
        {
            if(changePCR<D_ZERO || changeOI<D_ZERO)
                tradeHoldCount+=I_ONE;

           else if(changePCR>D_ZERO || changeOI>D_ZERO)
                tradeHoldCount=I_ZERO;

        }

        else
        {
            if(changePCR>D_ZERO || changeOI>D_ZERO)
                tradeHoldCount+=I_ONE;

            else if(changePCR<D_ZERO || changeOI<D_ZERO)
                tradeHoldCount=I_ZERO;
        }


        return tradeHoldCount;

    }

    private void resetStaticVariables() {
        meanCallOIChange=D_ZERO;
        meanPutOIChange=D_ZERO;
        meanCallOI = D_ZERO;
        meanPutOI = D_ZERO;
        meanStrikePCR = D_ZERO;
        meanRateOI = D_ZERO;
        combiRate = D_ZERO;

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

    private void calculateTradeDecision(MiniDelta totalRec,NiftyLTP niftyLTP, NiftyLTP prevItrNiftyLTP) {

        if((!niftyLTP.getSupport().equals(niftyLTP.getResistance())) && (Math.abs(totalRec.getCallOIChange())+Math.abs(totalRec.getPutOIChange()) >= D_TWO_HUNDRED) && (prevItrNiftyLTP.getRange().equalsIgnoreCase(niftyLTP.getRange()))) {

            if((niftyLTP.getNiftyLTP()<Integer.parseInt(niftyLTP.getSupport())) && (niftyLTP.getMeanStrikePCR()>prevItrNiftyLTP.getMeanStrikePCR())) {
                niftyLTP.setTradeDecision(TRADE_DECISION_BUY);
            }
            else if(niftyLTP.getNiftyLTP()>Integer.parseInt(niftyLTP.getResistance()) && (niftyLTP.getMeanStrikePCR()<prevItrNiftyLTP.getMeanStrikePCR())) {
                niftyLTP.setTradeDecision(TRADE_DECISION_SELL);
            }
            else {
                niftyLTP.setTradeDecision(TRADE_DECISION_NO_TRADE);
            }

        }
        else {
            niftyLTP.setTradeDecision(TRADE_DECISION_NO_TRADE);
        }
    }

    private void calculateMeanValues() {
        if(meanCallOI!=0 && !Objects.equals(meanCallOIChange, D_ZERO)) {
            meanStrikePCR = round(meanPutOI / meanCallOI);
            meanRateOI = round(rateOICalc(meanCallOIChange,meanPutOIChange));
           combiRate = round((meanStrikePCR+meanRateOI)/I_TWO);
        }

    }

    @Override
    public List<NiftyLTP> getDataForNiftyChart(Long startIndex) {

        return niftyLTPRepository.getDataForChart(startIndex);
    }


    @Transactional
    private List<MiniDelta> calculateMiniDeltaList(List<DeltaOICalculations> deltaOIMiniList, JobIterationDetails jobIterationDetails) {

        List<MiniDelta> miniDeltaList = new ArrayList<>();
        MiniDelta totalRec = new MiniDelta();
        Double sumPutOIChange = D_ZERO, sumCallOIChange = D_ZERO;

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

            miniDeltaList.add(miniDelta);
            meanPutOI+=d.getPutOI();
            meanCallOI+=d.getCallOI();
            sumPutOIChange+=d.getPutOIChange();
            sumCallOIChange+=d.getCallOIChange();

        }

        totalRec.setStrikePrice(TOTAL);
        totalRec.setPutOI(meanPutOI);
        totalRec.setCallOI(meanCallOI);
        totalRec.setPutOIChange(sumPutOIChange);
        totalRec.setCallOIChange(sumCallOIChange);
        totalRec.setStrikePCRColor(FONT_BOLD);
        totalRec.setRateOIColor(FONT_BOLD);
        totalRec.setCallOIColor(FONT_BOLD);
        totalRec.setPutOIColor(FONT_BOLD);
        totalRec.setCallOIChangeColor(FONT_BOLD);
        totalRec.setPutOIChangeColor(FONT_BOLD);
        totalRec.setJobIterationDetails(jobIterationDetails);

        miniDeltaList.add(totalRec);

        return miniDeltaList;
    }

    private NiftyLTP calculateNiftyLTPWithSupportAndResistance(List<DeltaOICalculations> deltaOIMiniList,List<DeltaOICalculations> deltaOICalculationsList) {
        NiftyLTP niftyLTP = new NiftyLTP();
        niftyLTP.setNiftyTS(LocalDateTime.now().withNano(I_ZERO));
        niftyLTP.setNiftyLTP(!Objects.isNull(niftyLastPrice)? niftyLastPrice.intValue():0);
        niftyLTP.setMeanStrikePCR(meanStrikePCR);
        niftyLTP.setMeanRateOI(meanRateOI);
        niftyLTP.setCombiRate(combiRate);
        niftyLTP.setRange(deltaOIMiniList.getFirst().getStrikePrice()+"-"+deltaOIMiniList.getLast().getStrikePrice());
        calculateSupportAndResistance(niftyLTP,deltaOICalculationsList);
        return niftyLTP;
    }


    private void calculateSupportAndResistance(NiftyLTP niftyLTP, List<DeltaOICalculations> deltaOICalculationsList) {

        Integer support=0;
        Integer resistance=0;

        deltaOICalculationsList.sort(Comparator.comparingInt(o -> Integer.parseInt(o.getStrikePrice())));



        if((niftyLTP.getMeanStrikePCR()>UPPER_TARGET_VALUE && niftyLTP.getMeanRateOI()>UPPER_TARGET_VALUE) ||
                ((niftyLTP.getMeanStrikePCR()>RATE_OI_TARGET_VALUE && niftyLTP.getMeanRateOI()>RATE_OI_TARGET_VALUE) && (niftyLTP.getMeanStrikePCR()>MID_TARGET_VALUE || niftyLTP.getMeanRateOI()>MID_TARGET_VALUE)))

        {
            logger.info("Calculating support and resistance with sentiment: bullish");
            support = findStrikePCR(deltaOICalculationsList,RATE_OI_TARGET_VALUE,RATE_OI_TARGET_VALUE,false) + TWENTY_FIVE;
            resistance = findStrikePCR(deltaOICalculationsList,LOWER_TARGET_VALUE,RATE_OI_TARGET_VALUE,false) + TWENTY_FIVE;
        }
        else if((niftyLTP.getMeanStrikePCR()>LOWER_TARGET_VALUE) && (niftyLTP.getMeanRateOI()>LOWER_TARGET_VALUE))
        {
            logger.info("Calculating support and resistance with sentiment: neutral");
            support = findStrikePCR(deltaOICalculationsList,UPPER_TARGET_VALUE,RATE_OI_TARGET_VALUE,false) + TWENTY_FIVE;
            resistance = findStrikePCR(deltaOICalculationsList,RATE_OI_TARGET_VALUE,RATE_OI_TARGET_VALUE,false) + TWENTY_FIVE;
        }
        else
        {
            logger.info("Calculating support and resistance with sentiment: bearish");
            support = findStrikePCR(deltaOICalculationsList, UPPER_TARGET_VALUE, RATE_OI_TARGET_VALUE,false);
            resistance = findStrikePCR(deltaOICalculationsList, RATE_OI_TARGET_VALUE,D_ZERO,false);
        }

        //re-adjust support and resistance based on range

        int rangeLow = Integer.parseInt(niftyLTP.getRange().split("-")[0]);
        int rangeHigh = Integer.parseInt(niftyLTP.getRange().split("-")[1]);

        if(support<rangeLow)
            support=rangeLow;
        else if(support>rangeHigh)
            support=rangeHigh;

        if(resistance<rangeLow)
            resistance=rangeLow;
        else if(resistance>rangeHigh)
            resistance=rangeHigh;

        niftyLTP.setSupport(String.valueOf(support));
        niftyLTP.setResistance(String.valueOf(resistance));

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

    private Integer findStrikePCRWORateOI(List<DeltaOICalculations> deltaOIMiniList, Double targetPCR)
    {
        Integer response=0;

        List<DeltaOICalculations> filteredList = deltaOIMiniList.stream().filter(d->d.getStrikePCR()>targetPCR).toList();

        Optional<DeltaOICalculations> maxStrikePriceItem = filteredList.stream()
                .max(Comparator.comparingInt(d -> Integer.parseInt(d.getStrikePrice())));


        if (maxStrikePriceItem.isPresent()) {
            DeltaOICalculations itemWithMaxStrikePrice = maxStrikePriceItem.get();
            response = Integer.parseInt(itemWithMaxStrikePrice.getStrikePrice());
        }

        return response;
    }

    private void subscribeForTokensAndNifty(KiteTickerProvider kiteTickerProvider,ArrayList<Long> instrumentTokens) {
        kiteTickerProvider.subscribeTokenForJob(instrumentTokens);
        try {
            Thread.sleep(FIVE_HUNDRED);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        kiteTickerProvider.unsubscribeTokensForDeltaJob(instrumentTokens);
        kiteTickerProvider.getNiftyLastPrice();
        try {
            Thread.sleep(FIVE_HUNDRED);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        kiteTickerProvider.disconnectTicker();
    }
    private List<DeltaOICalculations> calculateDeltaOIFromNiftyPrice(ArrayList<InstrumentEntity> listOfNFOInstrumentsForCurrentWeek,int atmPrice,Map<Long,Double> snapshotOI) {
        List<DeltaOICalculations> deltaOICalculationsList = new ArrayList<>();
        Map<String,DeltaOICalculations> strikePriceOIDelta = new ConcurrentHashMap<>();
        int minStrikePrice = atmPrice - (ONE_THOUSAND);
        int maxStrikePrice = atmPrice + (ONE_THOUSAND);
        logger.debug("minStrikePrice::{}", minStrikePrice);
        logger.debug("maxStrikePrice::{}", maxStrikePrice);

        listOfNFOInstrumentsForCurrentWeek.forEach(ie->{

            Instrument i = ie.getInstrument();
            DeltaOICalculations deltaOICalculations;

            if((Integer.parseInt(i.getStrike())>=minStrikePrice) && (Integer.parseInt(i.getStrike())<=maxStrikePrice)) {

                if (!strikePriceOIDelta.containsKey(i.getStrike())) {
                    deltaOICalculations = new DeltaOICalculations();
                    deltaOICalculations.setStrikePrice(i.getStrike());
                    deltaOICalculations.setCallOI(calculatedLotOI(i,3));

                    deltaOICalculations.setCallOIChange(deltaOICalculations.getCallOI() - snapshotOI.get(i.getInstrument_token()));

                    strikePriceOIDelta.put(i.getStrike(), deltaOICalculations);
                    ltpMap.put(i.getStrike()+"CE", getLTPValueFromTick(i.getInstrument_token()));
               //     logger.info("Checking for last trade price for CE: {}", ltpMap.get(i.getStrike()+"CE"));

                } else {

                    deltaOICalculations = strikePriceOIDelta.get(i.getStrike());
                    deltaOICalculations.setPutOI(calculatedLotOI(i,3));
                    deltaOICalculations.setPutOIChange(deltaOICalculations.getPutOI() - snapshotOI.get(i.getInstrument_token()));
                    deltaOICalculations.setStrikePCR(round(deltaOICalculations.getPutOI() / deltaOICalculations.getCallOI()));
                //    deltaOICalculations.setRateOI(round(deltaOICalculations.getPutOIChange() / deltaOICalculations.getCallOIChange()));
                    deltaOICalculations.setRateOI(round(rateOICalc(deltaOICalculations.getCallOIChange(),deltaOICalculations.getPutOIChange())));
                    deltaOICalculationsList.add(deltaOICalculations);
                    ltpMap.put(i.getStrike()+"PE", getLTPValueFromTick(i.getInstrument_token()));
                 //   logger.info("Checking for last trade price for PE: {}", ltpMap.get(i.getStrike()+"PE"));
                }

            }
        });
        return deltaOICalculationsList;
    }

    private Double getLTPValueFromTick(long instrumentToken) {
        var tick = kiteTickerProvider.tickerMapForJob.get(instrumentToken);
        if (tick == null) {
            logger.warn("No tick data for token {} — returning 0.0", instrumentToken);
            return D_ZERO;
        }
        return tick.getLastTradedPrice();
    }

    private void saveDeltaListToDB(List<DeltaOICalculations> deltaOICalculationsList, JobIterationDetails jobIterationDetails) {
        List<DeltaOIEntity> deltaOIEntityList = new ArrayList<>();
        deltaOICalculationsList.forEach(d-> {
            DeltaOIEntity deltaOIEntity = new DeltaOIEntity();
            deltaOIEntity.setStrikePrice(d.getStrikePrice());
            deltaOIEntity.setRateOI(d.getRateOI());
            deltaOIEntity.setStrikePCR(d.getStrikePCR());
            deltaOIEntity.setPutOI(d.getPutOI());
            deltaOIEntity.setCallOI(d.getCallOI());
            deltaOIEntity.setCallOIChange(d.getCallOIChange());
            deltaOIEntity.setPutOIChange(d.getPutOIChange());
            deltaOIEntity.setJobIterationDetails(jobIterationDetails);
            deltaOIEntityList.add(deltaOIEntity);
        });
   //     deltaOIRepository.deleteAll();
        deltaOIRepository.saveAll(deltaOIEntityList);
    }

    private void saveDeltaOIMiniListToDB(List<DeltaOICalculations> deltaOICalculationsList, JobIterationDetails jobIterationDetails) {
        List<DeltaOIThresholdFiltered> deltaOIEntityList = new ArrayList<>();
        deltaOICalculationsList.forEach(d-> {
            DeltaOIThresholdFiltered deltaOIEntity = new DeltaOIThresholdFiltered();
            deltaOIEntity.setStrikePrice(d.getStrikePrice());
            deltaOIEntity.setRateOI(d.getRateOI());
            deltaOIEntity.setStrikePCR(d.getStrikePCR());
            deltaOIEntity.setPutOI(d.getPutOI());
            deltaOIEntity.setCallOI(d.getCallOI());
            deltaOIEntity.setCallOIChange(d.getCallOIChange());
            deltaOIEntity.setPutOIChange(d.getPutOIChange());
            deltaOIEntity.setJobIterationDetails(jobIterationDetails);
            deltaOIEntityList.add(deltaOIEntity);
        });
        //     deltaOIRepository.deleteAll();
        deltaOIThresholdFilteredRepository.saveAll(deltaOIEntityList);
    }

    private static int getAtmPrice(@NonNull Double currentNiftyPrice)
    {
        return (int) (Math.round(currentNiftyPrice / D_ONE_HUNDRED) * ONE_HUNDRED);
    }

    private List<DeltaOICalculations> filterListUsingThreshold(List<DeltaOICalculations> deltaOICalculationsList, int threshold) {

        List<DeltaOICalculations> deltaOIList = new ArrayList<>();
        List<Integer> strikePriceListAboveThreshold = new ArrayList<>();
        deltaOICalculationsList.forEach(df -> {
            if(df.getCallOI()>=threshold && df.getPutOI()>=threshold)
            {
                strikePriceListAboveThreshold.add(Integer.parseInt(df.getStrikePrice()));
            }
        });


        if(!strikePriceListAboveThreshold.isEmpty()) {
            Collections.sort(strikePriceListAboveThreshold);
            logger.debug("MinSP:{}, MaxSP:{}", strikePriceListAboveThreshold.getFirst(), strikePriceListAboveThreshold.getLast());

            int minSP = strikePriceListAboveThreshold.getFirst();
            int maxSP = strikePriceListAboveThreshold.getLast();

            meanPutOIChange = D_ZERO;
            meanCallOIChange = D_ZERO;

            deltaOICalculationsList.forEach(df -> {

                DeltaOICalculations deltaOICalculations;
                if ((Integer.parseInt(df.getStrikePrice()) >= minSP) && (Integer.parseInt(df.getStrikePrice()) <= maxSP)) {
                    deltaOICalculations = new DeltaOICalculations(df.getStrikePrice(), df.getRateOI(), df.getStrikePCR(), df.getCallOI(), df.getPutOI(),df.getCallOIChange(),df.getPutOIChange());
                    deltaOIList.add(deltaOICalculations);
                    meanPutOIChange += df.getPutOIChange();
                    meanCallOIChange += df.getCallOIChange();
                }
            });
        }

        deltaOIList.sort(Comparator.comparingInt(o -> Integer.parseInt(o.getStrikePrice())));

        return deltaOIList;
    }

    private int calculateThreshold(List<DeltaOICalculations> deltaOICalculationsList) {
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
        int tFactor=TWENTY_FIVE;

        int threshold = ((int)(avg/tFactor))*tFactor;

        if(threshold==0)
            threshold=TWENTY_FIVE;

        return threshold;
    }

    private Double calculatedLotOI(Instrument i, Integer tFactor)
    {
      return (kiteTickerProvider.tickerMapForJob.get(i.getInstrument_token()).getOi()/((i.getLot_size()/tFactor)*D_ONE_THOUSAND));
    }

    private Double round(Double value) {

        return Math.round(value*D_ONE_HUNDRED)/D_ONE_HUNDRED;
    }

    @Override
    public void loadBackUp() {

        String currentDateTime = LocalDateTime.now().toString().split("\\.")[0].replace(":", "_");
        String niftyfileName = "NIFTYLTP" + "_" + currentDateTime;
        String minideltafileName = "MINI_DELTA" + "_" + currentDateTime;

        List<NiftyLTP> niftyLTPList = niftyLTPRepository.getAllDataForChart();

        boolean writeniftyLTPListToFile = fileService.writeObjToFile(niftyLTPList, niftyfileName);

        List<MiniDelta> miniDeltaList = miniDeltaRepository.getAllDataForMiniDelta();

        boolean writeminiDeltaListToFile = fileService.writeObjToFile(miniDeltaList, minideltafileName);
    }

    @Override
    public List<MiniDelta> getLatestMiniDelta() {
        return miniDeltaRepository.getLatestMiniDelta();
    }

    @Override
    public LocalDateTime getLastIterationTimestamp() {
        return jobIterationRepository.getLastSuccessfulIterationTimestamp().getIterationEndTime();

    }

    @Override
    public LocalDateTime getLastTickerTS() {
        logger.info("getLastTickerTS:: {}",kiteTickerProvider.lastNifty50TS);
        return kiteTickerProvider.lastNifty50TS;

    }

    @Override
    public List<NiftyLTP> getNiftyLTPDataAfterRequestedTime(LocalDateTime reqDateTime) {
        logger.info("getNiftyLTPDataAfterRequestedTime:: {}",reqDateTime);
        List<NiftyLTP> niftyLTPData = niftyLTPRepository.getDataAfterSpecifiedTS(reqDateTime);

        return filterNiftyLTPData(niftyLTPData);
    }


    @Override
    public AutoTradeParams setAutoTrade(@NonNull AutoTradeParams autoTradeParams) {

        logger.info("AutoTrade parameters:: {}", autoTradeParams.toString());

        try {

            autoTradeParamsJob.setAutoTradeFlag(autoTradeParams.isAutoTradeFlag());

            logger.info("Auto trade parameters set successfully:: {}", autoTradeParamsJob.toString());
            return autoTradeParamsJob;
        } catch (Exception e) {
            logger.error(e.getMessage());
            return new AutoTradeParams();
        }

    }

    public void calculateAndSaveAutoTradeJobParams(AutoTradeParams autoTradeParamsJob, NiftyLTP latestNiftyLTP) {

        logger.info("Calculating auto trade job params with latest Nifty LTP: {}", latestNiftyLTP);

        try {

            // calculate call and put symbols
          //  autoTradeService.calculateAutoTradeSymbols(autoTradeParamsJob, latestNiftyLTP);
            // calculate entry price
         //   autoTradeService.calculateAutoTradeEntryPrice(autoTradeParamsJob, latestNiftyLTP);
            // calculate stop loss price
         //   autoTradeService.calculateAutoTradeSLPrice(autoTradeParamsJob, latestNiftyLTP);
            // calculate call and put lot sizes
         //   autoTradeService.calculateAutoTradeLotSizes(autoTradeParamsJob, latestNiftyLTP);
           //setting job id
       //     autoTradeParamsJob.setJobIterationDetails(latestNiftyLTP.getJobIterationDetails());

            logger.info("Auto trade job params calculated successfully: {}", autoTradeParamsJob.toString());
            // save auto trade params
        //    autoTradeService.saveAutoTradeParams(autoTradeParamsJob);


        } catch (RuntimeException e) {

            logger.error("Error calculating auto trade job params: {}", e.getMessage(), e);

        }

    }


    @Override
    public AutoTradeParams getAutoTrade() {

        logger.info("getAutoTrade params:: {}",autoTradeParamsJob);

        if(Objects.isNull(autoTradeParamsJob) && lastTradeDecisions.getStatus().equalsIgnoreCase(ORDER_OPEN) )
        {
          autoTradeParamsJob = autoTradeRepository.findLatestAutoTradeParamsByJobIterationID(lastTradeDecisions.getJobIterationDetails().getId());
          logger.info("AutoTradeParams fetched from DB: {}", autoTradeParamsJob);
        }

        return Objects.isNull(autoTradeParamsJob)?new AutoTradeParams(): autoTradeParamsJob;
    }

    public List<NiftyLTP> filterNiftyLTPData(List<NiftyLTP> niftyLTPData) {
        List<NiftyLTP> filteredList = new ArrayList<>();
        if (niftyLTPData == null || niftyLTPData.isEmpty()) {
            return filteredList;
        }

        NiftyLTP previousRecord = null;
        for (NiftyLTP currentRecord : niftyLTPData) {
            if (previousRecord == null || !currentRecord.getCombiRate().equals(previousRecord.getCombiRate())) {
                filteredList.add(currentRecord);
            }
            previousRecord = currentRecord;
        }

        return filteredList;
    }

    public String calculateSentiment(Double PCR, Double rateOI, Double callOI) {
        String sentiment;

        if (PCR <= 0.49) {
            if (rateOI < 0) {
                if (callOI < 0) {
                    sentiment = "neutral";
                } else {
                    sentiment = "very bearish";
                }
            } else if (rateOI <= 0.99) {
                sentiment = "bearish";
            } else {
                sentiment = "neutral";
            }
        } else if (PCR <= 1.49) {
            if (rateOI < 0) {
                if (callOI < 0) {
                    sentiment = "bullish";
                } else {
                    sentiment = "bearish";
                }
            } else if (rateOI <= 0.99) {
                sentiment = "neutral";
            } else {
                sentiment = "bullish";
            }
        } else {
            if (rateOI < 0) {
                if (callOI < 0) {
                    sentiment = "very bullish";
                } else {
                    sentiment = "bearish";
                }
            } else if (rateOI <= 0.99) {
                sentiment = "bullish";
            } else {
                sentiment = "very bullish";
            }
        }

        return sentiment;
    }


    public double rateOICalc(double callOIChange, double putOIChange) {
        double rateOI;

        if (callOIChange < 0) {
            if (putOIChange < 0) {
                if (Math.abs(putOIChange) > Math.abs(callOIChange)) {
                    rateOI = - (putOIChange / callOIChange);
                } else {
                    rateOI = Math.abs((Math.abs(callOIChange) - Math.abs(putOIChange)) / callOIChange);
                }
            } else {
                rateOI = Math.abs((putOIChange + Math.abs(callOIChange)) / (putOIChange - Math.abs(callOIChange)));
            }
        } else {
            if (putOIChange < 0) {
                rateOI = - (Math.abs(putOIChange) + Math.abs(callOIChange)) /
                        Math.min(Math.abs(putOIChange), Math.abs(callOIChange));
            } else {
                rateOI = (putOIChange / callOIChange);
            }
        }

        return rateOI;
    }

    private void findMaxCallAndPutOIChange(List<DeltaOICalculations> deltaOIMiniList,PowerTradeDeltaOI powerTradeDeltaOI) {

        logger.info("Finding strike price for max call and put OI change");

        String maxCallOIChangeStrikePrice = null;
        String maxPutOIChangeStrikePrice = null;
        double maxCallOIChange = 0.0;
        double maxPutOIChange = 0.0;

        for (DeltaOICalculations delta : deltaOIMiniList) {

            logger.info("deltaOIMiniList obj: {}", delta.toString());

            if (Math.abs(delta.getCallOIChange()) > maxCallOIChange) {
                maxCallOIChange = Math.abs(delta.getCallOIChange());
                maxCallOIChangeStrikePrice = delta.getStrikePrice();
            }
            if (Math.abs(delta.getPutOIChange()) > maxPutOIChange) {
                maxPutOIChange = Math.abs(delta.getPutOIChange());
                maxPutOIChangeStrikePrice = delta.getStrikePrice();
            }
        }

        powerTradeDeltaOI.setMaxCallOIChangeStrikePrice(maxCallOIChangeStrikePrice);
        powerTradeDeltaOI.setMaxPutOIChangeStrikePrice(maxPutOIChangeStrikePrice);
        powerTradeDeltaOI.setMaxCallOIChange(maxCallOIChange);
        powerTradeDeltaOI.setMaxPutOIChange(maxPutOIChange);

        logger.info("Max Call OI Change: {}, Strike Price: {}", maxCallOIChange, maxCallOIChangeStrikePrice);
        logger.info("Max Put OI Change: {}, Strike Price: {}", maxPutOIChange, maxPutOIChangeStrikePrice);
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

    private void calculatePowerTradeDeltaParams(NiftyLTP niftyLTP, JobIterationDetails jobIterationDetails,  List<DeltaOICalculations> deltaOIMiniList,MiniDelta prevItrTotalRec, MiniDelta totalRec) {

        calculateInitiativeTraders(prevItrNiftyLTP, niftyLTP);
        PowerTradeDeltaOI powerTradeDeltaOI = calculatePowerTradeDeltaOI(niftyLTP, jobIterationDetails, prevItrTotalRec, totalRec);
        findMaxCallAndPutOIChange(deltaOIMiniList, powerTradeDeltaOI);
        messagingService.sendPowerTradeDeltaOIMessage(powerTradeDeltaOI);

        if(!(niftyLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_BUY) || niftyLTP.getTradeDecision().equalsIgnoreCase(TRADE_DECISION_SELL)))
            calculateTradeDecisionOnPowerTrade(niftyLTP);
    }

}
