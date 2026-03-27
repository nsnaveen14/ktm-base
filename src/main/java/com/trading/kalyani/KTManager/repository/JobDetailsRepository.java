package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.JobDetails;
import org.springframework.data.jpa.repository.NativeQuery;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

@Repository
public interface JobDetailsRepository extends CrudRepository<JobDetails,Long> {

    @Query(value = """
            SELECT * FROM job_details
            WHERE DATE(job_end_time) = :jobDate
            AND job_name = :jobName
            AND job_status = 'COMPLETED'
            ORDER BY job_start_time DESC
            """, nativeQuery = true)
    List<JobDetails> findJobDetailsByJobDateAndJobName(LocalDate jobDate, String jobName);

    Optional<JobDetails> findTopByAppJobConfigNumOrderByJobStartTimeDesc(Integer appJobConfigNum);

    List<JobDetails> findByAppJobConfigNumAndJobStatus(Integer appJobConfigNum, String statusRunning);

    @Query(value="SELECT * FROM job_details where date(job_start_time) = :currentDate order by job_start_time desc, job_name ,app_job_config_num", nativeQuery = true)
    List<JobDetails> findLatestJobDetailsStatusForAllConfigs(LocalDate currentDate);

    @Query(value="SELECT * FROM job_details where app_job_config_num = :appJobConfigNum and date(job_start_time) = :currentDate order by job_start_time desc, job_name ,app_job_config_num", nativeQuery = true)
    List<JobDetails> findLatestJobDetailsStatusForConfigNum(Integer appJobConfigNum,LocalDate currentDate);
}
