package com.trading.kalyani.KTManager.config;

import com.trading.kalyani.KTManager.controller.JobController;
import com.trading.kalyani.KTManager.repository.AppJobConfigRepository;
import com.trading.kalyani.KTManager.service.CronExpressionProvider;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

@Configuration
@EnableScheduling
public class DynamicJobScheduler implements SchedulingConfigurer {

    @Autowired
    private JobController jobController;

    @Autowired
    private CronExpressionProvider cronProvider; // Custom service to fetch cron from DB/config

    @Autowired
    AppJobConfigRepository appJobConfigRepository;

    @Value("${market.task.pre.cron.startTicker}")
    private String startTickerCron;

    @Value("${market.task.post.cron.stopTicker}")
    private String stopTickerCron;


    private static final Logger logger = LoggerFactory.getLogger(DynamicJobScheduler.class);

    @Override
    public void configureTasks(@NotNull ScheduledTaskRegistrar taskRegistrar) {

        taskRegistrar.addCronTask(
                () -> jobController.startKiteTicker(),startTickerCron
        );

        appJobConfigRepository.findAll().forEach(appJobConfig -> {

            logger.info("Scheduling tasks for AppJobConfig: {}", appJobConfig);

            if(appJobConfig.getIsActive()) {

                logger.info("Scheduling tasks for AppIndex: {} with ID: {}", appJobConfig.getAppIndexConfig().getIndexName(), appJobConfig.getJobType().getJobTypeCode());

                taskRegistrar.addCronTask(
                        () -> jobController.startJobToCreateOISnapshot(appJobConfig.getAppJobConfigNum()),
                        cronProvider.getCronExpressionForSnapshot(appJobConfig.getAppIndexConfig().getIndexId())
                );
                taskRegistrar.addCronTask(
                        () -> jobController.startJob(appJobConfig.getAppJobConfigNum()),
                        cronProvider.getCronExpressionForStartJob(appJobConfig.getAppIndexConfig().getIndexId())
                );
                taskRegistrar.addCronTask(
                        () -> jobController.stopJob(appJobConfig.getAppJobConfigNum()),
                        cronProvider.getCronExpressionForEndJob(appJobConfig.getAppIndexConfig().getIndexId())
                );
            }
    });

        taskRegistrar.addCronTask(
                () -> jobController.stopKiteTicker(),stopTickerCron
        );

    }
}

