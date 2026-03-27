package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.service.AsyncDailyJobServices;
import com.trading.kalyani.KTManager.service.DailyJobService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class AsyncDailyJobServicesImpl implements AsyncDailyJobServices {

    private static final Logger logger = LoggerFactory.getLogger(AsyncDailyJobServicesImpl.class);

    @Autowired
    DailyJobService dailyJobService;

    @Async("asyncExecutor")
    @Override
    public CompletableFuture<Void> startJobByConfigNumberAsync(Integer appJobConfigNum) {
        try {
            dailyJobService.startJobByConfigNumber(appJobConfigNum);
        } catch (Exception e) {
            logger.error("Error starting job asynchronously: {}", e.getMessage());
            throw new RuntimeException("Error starting job asynchronously", e);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Async("asyncExecutor")
    @Override
    public CompletableFuture<Void> stopJobByConfigNumberAsync(Integer appJobConfigNum) {
        try {
            dailyJobService.stopJobByConfigNumber(appJobConfigNum);
        } catch (Exception e) {
            logger.error("Error stopping job asynchronously: {}", e.getMessage());
            throw new RuntimeException("Error stopping job asynchronously", e);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Async("asyncExecutor")
    @Override
    public CompletableFuture<Void> saveOISnapshot(Integer appJobConfigNum) {
        try {
            dailyJobService.saveOISnapshot(appJobConfigNum);
        } catch (Exception e) {
            logger.error("Error starting oisnapshot job asynchronously: {}", e.getMessage());
            throw new RuntimeException("Error starting oisnapshot job asynchronously", e);
        }
        return CompletableFuture.completedFuture(null);
    }

}
