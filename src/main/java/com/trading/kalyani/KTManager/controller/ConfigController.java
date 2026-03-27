package com.trading.kalyani.KTManager.controller;

import com.trading.kalyani.KTManager.entity.*;
import com.trading.kalyani.KTManager.repository.AppJobConfigRepository;
import com.trading.kalyani.KTManager.repository.LTPTrackerConfigRepository;
import com.trading.kalyani.KTManager.service.DailyJobPlannerService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RestController
@CrossOrigin(value="*")
public class ConfigController {

    @Autowired
    AppJobConfigRepository appJobConfigRepository;

    @Autowired
    LTPTrackerConfigRepository ltpTrackerConfigRepository;

    @Autowired
    DailyJobPlannerService dailyJobPlannerService;

    private static final Logger logger = LogManager.getLogger(ConfigController.class);

    @GetMapping("/getAppJobConfigDetails")
    public ResponseEntity<ArrayList<AppJobConfig>> getAppJobConfigDetails() {

        AppIndexConfig summaryIndexConfig = new AppIndexConfig();
        summaryIndexConfig.setActive(true);
        summaryIndexConfig.setIndexName("Summary");

        JobType summaryJobType = new JobType();
        summaryJobType.setJobType("");

        AppJobConfig defaultConfig = new AppJobConfig();
        defaultConfig.setAppJobConfigNum(0);
        defaultConfig.setAppIndexConfig(summaryIndexConfig);
        defaultConfig.setJobType(summaryJobType);
        ArrayList<AppJobConfig> appJobConfigList = new ArrayList<>();
        appJobConfigList.add(defaultConfig);
         appJobConfigList.addAll((Collection<? extends AppJobConfig>) appJobConfigRepository.findAll());
        logger.info("Fetched AppJobConfig details of size: {}", appJobConfigList.size());
        return ResponseEntity.ok(appJobConfigList);

    }

    @GetMapping("/getLTPTrackerConfig")
    public ResponseEntity<List<LTPTrackerConfig>> getLTPTrackerConfig() {
        logger.info("Fetching LTPTrackerConfig details");
        List<LTPTrackerConfig> ltpTrackerConfigList = new ArrayList<>();
        ltpTrackerConfigRepository.findAll().forEach(ltpTrackerConfigList::add);
        ltpTrackerConfigList.sort(java.util.Comparator.comparingInt(l -> {
            Integer cfg = l.getAppJobConfigNum();
            return (cfg != null) ? cfg : Integer.MAX_VALUE;
        }));
        logger.info("Fetched LTPTrackerConfig details of size: {}", ltpTrackerConfigList.size());
        return ResponseEntity.ok(ltpTrackerConfigList);
    }

    @PostMapping("/setLTPTrackerConfig")
    public ResponseEntity<LTPTrackerConfig> setLTPTrackerConfig(@RequestBody LTPTrackerConfig ltpTrackerConfig) {
        logger.info("Setting LTPTrackerConfig: {}", ltpTrackerConfig);
        ltpTrackerConfig.setConfigSTP(LocalDateTime.now().withNano(0));
        LTPTrackerConfig ltpTrackerConfigResponse = ltpTrackerConfigRepository.save(ltpTrackerConfig);
        logger.info("LTPTrackerConfig saved successfully");
        return ResponseEntity.ok(ltpTrackerConfigResponse);
    }

    @GetMapping("/getDailyJobPlannerConfig")
    public ResponseEntity<List<DailyJobPlanner>> getDailyJobPlanner() {
     List<DailyJobPlanner> dailyJobPlannerList = dailyJobPlannerService.getDailyJobPlannerList();
     logger.info("Fetched DailyJobPlanner details of size: {}", dailyJobPlannerList.size());
     return ResponseEntity.ok(dailyJobPlannerList);
    }

    @PostMapping("/modifyDailyJobPlannerConfig")
    public ResponseEntity<DailyJobPlanner> modifyDailyJobPlannerConfig(@RequestBody DailyJobPlanner dailyJobPlanner) {
        logger.info("Modifying DailyJobPlanner configuration: {}", dailyJobPlanner);
        DailyJobPlanner updatedDailyJobPlanner = dailyJobPlannerService.modifyDailyJobPlannerConfig(dailyJobPlanner);
        logger.info("DailyJobPlanner configuration modified successfully");
        return ResponseEntity.ok(updatedDailyJobPlanner);
    }


}
