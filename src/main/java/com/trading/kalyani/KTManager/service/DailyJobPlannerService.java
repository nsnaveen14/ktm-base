package com.trading.kalyani.KTManager.service;

import com.trading.kalyani.KTManager.entity.DailyJobPlanner;

import java.time.LocalDate;
import java.util.List;

public interface DailyJobPlannerService {
    List<DailyJobPlanner> createDailyJobPlannerEntry();

    boolean validateDailyPlannerByConfigNum(Integer appConfigNum, LocalDate currentDate);

    List<DailyJobPlanner> getDailyJobPlannerList();

    DailyJobPlanner modifyDailyJobPlannerConfig(DailyJobPlanner dailyJobPlanner);
}
