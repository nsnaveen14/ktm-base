package com.trading.kalyani.KTManager.entity;


import com.trading.kalyani.KTManager.constants.ApplicationConstants;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Entity
@Table(name = "JobDetails")
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class JobDetails {


    @Id
    @GeneratedValue(strategy= GenerationType.SEQUENCE)
    private Long id;

    private LocalDateTime jobStartTime;

    private LocalDateTime jobEndTime;

    private String jobStatus;

    @Enumerated(EnumType.STRING)
    private ApplicationConstants.JobName jobName;

    private LocalDate jobForExpiryDate;

    private Integer appJobConfigNum;


    public JobDetails(LocalDateTime localDateTime, Object o, String statusRunning, String jobTypeNifty) {
        this.jobStartTime = localDateTime;
        this.jobStatus = statusRunning;
        this.jobName = ApplicationConstants.JobName.valueOf(jobTypeNifty);
    }
}
