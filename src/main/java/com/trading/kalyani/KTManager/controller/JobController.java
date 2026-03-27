package com.trading.kalyani.KTManager.controller;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.trading.kalyani.KTManager.config.KiteConnectConfig;
import com.trading.kalyani.KTManager.entity.*;
import com.trading.kalyani.KTManager.model.*;
import com.trading.kalyani.KTManager.repository.AppJobConfigRepository;
import com.trading.kalyani.KTManager.repository.JobDetailsRepository;
import com.trading.kalyani.KTManager.repository.NiftyLTPRepository;
import com.trading.kalyani.KTManager.repository.OiSnapshotRepository;
import com.trading.kalyani.KTManager.service.*;
import com.trading.kalyani.KTManager.service.JobDetailsService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.*;
import static com.trading.kalyani.KTManager.utilities.DateUtilities.convertStringToLocalDateTime;

@RestController
@CrossOrigin(value="*")
public class JobController {

    @Autowired
    JobService jobService;

    @Autowired
    JobDetailsRepository jobDetailsRepository;

    @Autowired
    OiSnapshotRepository oiSnapshotRepository;

    @Autowired
    MessagingService messagingService;

    @Autowired
    AsyncDailyJobServices asyncDailyJobServices;

    @Autowired
    AsyncService asyncService;

    @Autowired
    KiteConnectConfig kiteConnectConfig;

    @Autowired
    NiftyLTPRepository niftyLTPRepository;

    @Autowired
    AppJobConfigRepository appJobConfigRepository;

    @Autowired
    DailyJobPlannerService dailyJobPlannerService;

    @Autowired
    DailyJobService dailyJobService;

    @Autowired
    JobDetailsService jobDetailsService;

    @Autowired
    OISnapshotService oiSnapshotService;

    @Autowired
    EmailManagement emailManagementService;

    boolean isJobRunning = false;
    boolean isKiteTickerRunning = false;

    Long currentJobId = 0L;

    private static final Logger logger = LogManager.getLogger(JobController.class);

    @PostMapping("startKiteTicker")
    public boolean startKiteTicker() {

        if(dailyJobService.startKiteTicker())
            isKiteTickerRunning = true;

        return isKiteTickerRunning;
    }

    @PostMapping("stopKiteTicker")
    public boolean stopKiteTicker() {

        if(dailyJobService.stopKiteTicker())
            isKiteTickerRunning = false;

        return isKiteTickerRunning;
    }

    @PostMapping("startJobToCreateOISnapshot")
    public boolean startJobToCreateOISnapshot(@RequestBody Integer appJobConfigNum) {

        if(appJobConfigNum.equals(-1))
        {
            return dailyJobService.saveOISnapshotAllJobs();
        }

        logger.info("Creating OI Snapshot for ConfigNum: {}", appJobConfigNum);
        if(dailyJobPlannerService.validateDailyPlannerByConfigNum(appJobConfigNum, LocalDate.now())) {
            asyncDailyJobServices.saveOISnapshot(appJobConfigNum);
            return true;
        }
        else {
            logger.info("Job not required for ConfigNum: {} as per Daily Job Planner", appJobConfigNum);
            return false;
        }

    }

    @PostMapping("startJob")
    public boolean startJob(@RequestBody Integer appJobConfigNum) {

        if(appJobConfigNum.equals(-1))
        {
            return dailyJobService.startAllJobs();
        }

        if(dailyJobPlannerService.validateDailyPlannerByConfigNum(appJobConfigNum, LocalDate.now())) {
            logger.info("Job can be started for AppJobConfigNum: {}", appJobConfigNum);

            //check whether job is already running for the config number
            if(jobDetailsService.isJobRunning(appJobConfigNum)) {
                logger.warn("Job is already running for AppJobConfigNum: {}", appJobConfigNum);
                return false;
            }

            // service to start the job
            logger.info("Starting job for AppJobConfigNum: {}", appJobConfigNum);
            //call async service to start the job
            asyncDailyJobServices.startJobByConfigNumberAsync(appJobConfigNum)
                    .thenRun(()-> {
                        logger.info("Job completed successfully for AppJobConfigNum: {}", appJobConfigNum);
                    });
            return true;
        }
        else
        {
            logger.info("Job not required to start for ConfigNum: {} as per Daily Job Planner", appJobConfigNum);
            return false;
        }

    }

    @PostMapping("stopJob")
    public boolean stopJob(@RequestBody Integer appJobConfigNum) {

        if(appJobConfigNum.equals(-1))
        {
            return dailyJobService.stopAllJobs();
        }

        if(dailyJobPlannerService.validateDailyPlannerByConfigNum(appJobConfigNum, LocalDate.now())) {
            logger.info("Job can be stopped for AppJobConfigNum: {}", appJobConfigNum);

            AppJobConfig appJobConfig = appJobConfigRepository.findById(appJobConfigNum).get();

            logger.info("AppJobConfig details: {}", appJobConfig);

            logger.info("AppJobConfig data {},{}", appJobConfig.getAppIndexConfig().getJobStartExpression(),appJobConfig.getJobType().getJobIterationDelaySeconds());

            // service to stop the job
            logger.info("Stopping job for AppJobConfigNum: {}", appJobConfigNum);
            asyncDailyJobServices.stopJobByConfigNumberAsync(appJobConfigNum)
                    .thenRun(()-> {
                        logger.info("Job stopped successfully for AppJobConfigNum: {}", appJobConfigNum);
                    });
            return true;
        }
        else
        {
            logger.info("Job not required to stop for ConfigNum: {} as per Daily Job Planner", appJobConfigNum);
            return false;
        }

    }

    @GetMapping("startJobForOICalculations")
    public void startJobForOICalculations() {

        List<DeltaOICalculations> deltaOICalculationsList;
        try {
            if (!this.isJobRunning)
                this.isJobRunning = true;

            System.out.println(this.isJobRunning);

            JobDetails jobDetails = new JobDetails(LocalDateTime.now().withNano(I_ZERO),null,STATUS_RUNNING,JOB_TYPE_NIFTY);
            jobDetails = jobDetailsRepository.save(jobDetails);
            currentJobId = jobDetails.getId();
            logger.info("Job started with ID: {}", currentJobId);

            CommonReqRes message = new CommonReqRes();
            message.setMessage("Job started with ID: "+ currentJobId);
            message.setStatus(true);
            message.setType(SUCCESS);
            messagingService.sendCommonMessage(message);

            asyncService.sendEmailToAllUsersAsync("KTM: Nifty Job started with ID: " + currentJobId + " from Profile: "+kiteConnectConfig.getActiveProfile(),"Nifty Job started with ID: "+ currentJobId ,"");

            while (this.isJobRunning) {
                long startTime = System.currentTimeMillis();
                deltaOICalculationsList = jobService.calculateOIDelta(jobDetails);
                long endTime = System.currentTimeMillis();
                logger.info("Time taken(s) for calculations: {}", (endTime - startTime) / 1000);

                Thread.sleep(2000); // Sleep for 2 second before next iteration
            }
        }
        catch (Exception e) {
            logger.error("Error occurred while running the job for OI calculations: {}", e.getMessage());
            stopJobForOICalculations();
            e.printStackTrace();
            }

        }


    @GetMapping("stopJobForOICalculations")
    public void stopJobForOICalculations() {

        this.isJobRunning = false;

        NiftyLTP lastNiftyLTP = niftyLTPRepository.getLatestNiftyLTP();

        asyncService.sendEmailToAllUsersAsync("KTM: Nifty Job stopped with ID: "+ currentJobId + " from Profile: "+kiteConnectConfig.getActiveProfile(),"Nifty Job stopped with ID: "+ currentJobId+ " with last NiftyLTP as: "+lastNiftyLTP,"");

        Optional<JobDetails> jobDetailsOptional = jobDetailsRepository.findById(currentJobId);

        if(jobDetailsOptional.isPresent())
        {
            JobDetails jobDetails = jobDetailsOptional.get();
            jobDetails.setJobEndTime(LocalDateTime.now().withNano(I_ZERO));
            jobDetails.setJobStatus(STATUS_COMPLETED);
            jobDetailsRepository.save(jobDetails);
            logger.info("Job stopped with ID: {}", currentJobId);
            CommonReqRes message = new CommonReqRes();
            message.setMessage("Job stopped with ID: "+ currentJobId);
            message.setStatus(true);
            message.setType(WARNING);
            messagingService.sendCommonMessage(message);
        }
        else
        {
            logger.error("Job with ID: {} not found.", currentJobId);
        }
    }


    @GetMapping("getNiftyLTPForChart")
    public ResponseEntity<List<NiftyLTP>> getNiftyLTPForChart(@RequestParam("startIndex") Long startIndex) {

        List<NiftyLTP> niftyLTPList;

        niftyLTPList = jobService.getDataForNiftyChart(startIndex);

        return new ResponseEntity<>(niftyLTPList, HttpStatus.OK);

    }

    @GetMapping("takeBackUpForNiftyData")
    public String takeBackUpForNiftyData() {

        jobService.loadBackUp();

        return "Success";
    }

    @GetMapping("getMiniDeltaTable")
    public ResponseEntity<List<MiniDelta>> getMiniDeltaTable() {

        List<MiniDelta> miniDeltaList;

        miniDeltaList = jobService.getLatestMiniDelta();

        return new ResponseEntity<>(miniDeltaList, HttpStatus.OK);

    }

    @GetMapping("getLastIterationTimestamp")
    public LocalDateTime getLastIterationTimestamp() {

       // return jobService.getLastIterationTimestamp();
        return jobService.getLastTickerTS();
    }

    @GetMapping("/loadOISnapshotFromFileToDB")
    public Boolean loadOISnapshotFromFileToDB() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        // Register the module to handle Java 8 date and time (LocalDateTime)
        objectMapper.registerModule(new JavaTimeModule());
        File file = new File("OISNAPSHOT_2025-04-25T09_10_02.json");
        List<OISnapshotEntity> oiSnapshotEntityList = objectMapper.readValue(file, new TypeReference<List<OISnapshotEntity>>(){});
        System.out.println(oiSnapshotEntityList.size());
        oiSnapshotRepository.deleteAll();
        oiSnapshotRepository.saveAll(oiSnapshotEntityList);
        return true;
    }

    @PostMapping("/uploadOISnapshotFile")
    public ResponseEntity<CommonReqRes> uploadOISnapshotFile(@RequestParam("file") MultipartFile file) throws IOException {

        CommonReqRes response = new CommonReqRes();

        if (file.isEmpty()) {
            response.setMessage("File is empty");
            response.setStatus(false);
            response.setType(FAILURE);
            return new ResponseEntity<>(response, HttpStatus.BAD_REQUEST);
        }

     try {

         boolean isProcessed = oiSnapshotService.processOISnapshot(file);

         if(isProcessed)
         {
         response.setMessage("File uploaded successfully");
         response.setStatus(true);
         response.setType(SUCCESS);

         return new ResponseEntity<>(response, HttpStatus.OK);
         }
         else
         {
             response.setMessage("Error processing file: Check the server logs for details");
             response.setStatus(false);
             response.setType(ERROR);
             return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);
         }
     } catch (RuntimeException e) {
            logger.error("Error processing file: {}", e.getMessage());
            response.setMessage("Error processing file: Check the server logs for details");
            response.setStatus(false);
            response.setType(ERROR);
            return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/getNiftyLTPDataAfterRequestedTime")
    public ResponseEntity<List<NiftyLTP>> getNiftyLTPDataAfterRequestedTime(@RequestParam("reqDateTime") String reqDateTime) {

        String format = "yyyy-MM-dd HH:mm:ss";
        LocalDateTime reqLocalDateTime = convertStringToLocalDateTime(reqDateTime, format);

        List<NiftyLTP> niftyLTPList = jobService.getNiftyLTPDataAfterRequestedTime(reqLocalDateTime);
        return new ResponseEntity<>(niftyLTPList, HttpStatus.OK);
    }

    @PostMapping("/setAutoTradeParams")
    public ResponseEntity<AutoTradeParams> setupAutoTradeParams(@RequestBody AutoTradeParams autoTradeParams) {

        return new ResponseEntity<>(jobService.setAutoTrade(autoTradeParams), HttpStatus.OK);

    }

    @GetMapping("/getAutoTradeParams")
    public ResponseEntity<AutoTradeParams> getAutoTradeParams() {
        return new ResponseEntity<>(jobService.getAutoTrade(), HttpStatus.OK);
    }

    @GetMapping("/getJobRunningParameters")
    public ResponseEntity<Map<String,Object>> getJobRunningParameters() {
        Map<String,Object> jobRunningParameters = new HashMap<>();

        this.isJobRunning = !jobDetailsRepository.findByAppJobConfigNumAndJobStatus(1, STATUS_RUNNING).isEmpty();

        jobRunningParameters.put("isKiteTickerRunning", this.isKiteTickerRunning);
        jobRunningParameters.put("isJobRunning", this.isJobRunning);
        //jobRunningParameters.put("currentJobId", this.currentJobId != null ? this.currentJobId : 0L);

        return new ResponseEntity<>(jobRunningParameters, HttpStatus.OK);
    }

    @GetMapping("/resetAutoTradeParams")
    public ResponseEntity<AutoTradeParams> resetAutoTradeParams() {
        AutoTradeParams autoTradeParams = jobService.resetAutoTradeParams();
        return new ResponseEntity<>(autoTradeParams, HttpStatus.OK);
    }

    @GetMapping("/getTradeDecisionsByConfigNum")
    public ResponseEntity<List<TradeDecisions>> getTradeDecisionsByConfigNum(@RequestParam("appJobConfigNum") Integer appJobConfigNum) {

        List<TradeDecisions> tradeDecisionsList = dailyJobService.getTradeDecisionsByConfigNum(appJobConfigNum);

        return new ResponseEntity<>(tradeDecisionsList, HttpStatus.OK);
    }

    @GetMapping("/getIndexLTPDataByConfigNum")
    public ResponseEntity<List<IndexLTP>> getIndexLTPDataByConfigNum(@RequestParam("appJobConfigNum") Integer appJobConfigNum) {
        List<IndexLTP> indexLTPList = dailyJobService.getIndexLTPDataByConfigNum(appJobConfigNum);
        return new ResponseEntity<>(indexLTPList, HttpStatus.OK);
    }

    @GetMapping("/getMiniDeltaDataByAppJobConfigNum")
    public ResponseEntity<List<MiniDelta>> getMiniDeltaDataByAppJobConfigNum(@RequestParam("appJobConfigNum") Integer appJobConfigNum) {
        List<MiniDelta> miniDeltaList = dailyJobService.getMiniDeltaDataByAppJobConfigNum(appJobConfigNum);
        return new ResponseEntity<>(miniDeltaList, HttpStatus.OK);
    }


    @GetMapping("/getSwingHighLowByConfigNum")
    public ResponseEntity<Map<Integer,SwingHighLow>> getSwingHighLowByConfigNum(@RequestParam("appJobConfigNum") Integer appJobConfigNum) {
        Map<Integer, SwingHighLow> swingHighLowMap = dailyJobService.getSwingHighLowByConfigNum(appJobConfigNum);
        return new ResponseEntity<>(swingHighLowMap, HttpStatus.OK);
    }

    @PostMapping("/autoTradeToggle")
    public ResponseEntity<Boolean> autoTradeToggle(@RequestBody AppJobConfigParams appJobConfigParams) {
        Boolean updatedStatus = jobService.setAutoTradeEnabled(appJobConfigParams);
        return new ResponseEntity<>(updatedStatus, HttpStatus.OK);
    }

    @GetMapping("/getJobRunningStatusByConfigNum")
    public ResponseEntity<List<JobDetails>> getJobRunningStatusByConfigNum(@RequestParam("appJobConfigNum") Integer appJobConfigNum) {
        List<JobDetails> jobDetailsList = jobDetailsService.getJobRunningStatusByConfigNum(appJobConfigNum);
        return new ResponseEntity<>(jobDetailsList, HttpStatus.OK);
    }

    @GetMapping("/getRetryCounter")
    public ResponseEntity<Map<Integer,Integer>> getRetryCounter(@RequestParam int appJobConfigNum) {
        Map<Integer,Integer> retryCounterMap = dailyJobService.getRetryCounterByConfigNum(appJobConfigNum);
        return new ResponseEntity<>(retryCounterMap, HttpStatus.OK);
    }

    @PostMapping("/updateRetryCounter")
    public ResponseEntity<Map<Integer,Integer>> updateRetryCounter() {

        dailyJobService.initializeRetryCounter();

        return new ResponseEntity<>(dailyJobService.getRetryCounterByConfigNum(0), HttpStatus.OK);
    }


}
