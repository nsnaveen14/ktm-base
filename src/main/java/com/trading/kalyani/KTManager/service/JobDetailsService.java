package com.trading.kalyani.KTManager.service;

import com.trading.kalyani.KTManager.entity.JobDetails;

import java.util.List;

public interface JobDetailsService {

    public boolean isJobRunning(Integer appJobConfigNum);

    public List<JobDetails> getJobRunningStatusByConfigNum(Integer appJobConfigNum);

}
