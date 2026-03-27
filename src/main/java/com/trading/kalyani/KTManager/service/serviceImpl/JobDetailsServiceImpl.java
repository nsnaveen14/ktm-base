package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.entity.JobDetails;
import com.trading.kalyani.KTManager.repository.JobDetailsRepository;
import com.trading.kalyani.KTManager.service.JobDetailsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.STATUS_RUNNING;

@Service
public class JobDetailsServiceImpl implements JobDetailsService {

    @Autowired
    private JobDetailsRepository jobDetailsRepository;

    /**
     * Checks if a job is currently running for the given appJobConfigNum.
     * @param appJobConfigNum the config number
     * @return true if a job is running, false otherwise
     */
    @Override
    public boolean isJobRunning(Integer appJobConfigNum) {
        List<JobDetails> runningJobs = jobDetailsRepository.findByAppJobConfigNumAndJobStatus(appJobConfigNum, STATUS_RUNNING);
        return !runningJobs.isEmpty();
    }

    @Override
    public List<JobDetails> getJobRunningStatusByConfigNum(Integer appJobConfigNum) {

        LocalDate selectedDate = LocalDate.now();

       // LocalDate selectedDate = LocalDate.of(2025,11,28); // For testing purpose only, to be removed later

        if(appJobConfigNum==-1)
            return jobDetailsRepository.findLatestJobDetailsStatusForAllConfigs(selectedDate);
        else
            return jobDetailsRepository.findLatestJobDetailsStatusForConfigNum(appJobConfigNum,selectedDate);

    }
}

