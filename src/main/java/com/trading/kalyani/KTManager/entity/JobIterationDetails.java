package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@Entity
@Table(name = "JobIterationDetails")
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class JobIterationDetails {

    @Id
    @GeneratedValue(strategy= GenerationType.SEQUENCE)
    private Long id;

    @ManyToOne
    @JoinColumn(name = "jobId", referencedColumnName = "id", nullable = false)
    private JobDetails jobDetails;

    private LocalDateTime iterationStartTime;

    private LocalDateTime iterationEndTime;

    private String iterationStatus;

    private Integer indexLTP;

    private Integer threshold;

}
