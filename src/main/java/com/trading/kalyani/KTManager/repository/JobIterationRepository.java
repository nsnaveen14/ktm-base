package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.JobIterationDetails;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface JobIterationRepository extends CrudRepository<JobIterationDetails, Long> {

    @Query(value = """
            SELECT *
            FROM job_iteration_details
            WHERE iteration_status = 'COMPLETED' and iteration_start_time >= current_date
            ORDER BY id DESC
            LIMIT 1;
            """,nativeQuery = true)
    JobIterationDetails getLastSuccessfulIterationTimestamp();

    List<JobIterationDetails> findByJobDetailsIdAndIterationStatus(Long id, String statusRunning);
    // Custom query methods can be defined here if needed
}
