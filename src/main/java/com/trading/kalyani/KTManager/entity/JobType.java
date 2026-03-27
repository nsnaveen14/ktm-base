package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;

@Entity
@Getter
@Setter
@Table(name="JobType")
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class JobType {


    @Id
    @Column(name = "job_type_code")
    private Integer jobTypeCode;

    @Column(name = "job_type", nullable = false, length = 255)
    private String jobType;

    @Column(name = "job_iteration_delay_seconds", nullable = false)
    private Integer jobIterationDelaySeconds;

}
