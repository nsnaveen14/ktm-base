package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.DailyJobPlanner;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

public interface DailyJobPlannerRepository extends CrudRepository<DailyJobPlanner,Long> {
    Optional<DailyJobPlanner> findById_AppJobConfigNumAndId_JobDate(Integer appJobConfigNum, LocalDate jobDate);

    @Query(value="SELECT * FROM daily_job_planner where job_date = :latestJobDate order by app_job_config_num asc", nativeQuery = true)
    List<DailyJobPlanner> findByLatestJobDate(LocalDate latestJobDate);
}
