package com.trading.kalyani.KTManager.entity;

import com.trading.kalyani.KTManager.model.MaxPain;
import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Entity
@Getter
@Setter
@Table(name="IndexLTP")
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class IndexLTP {

    @Id
    @GeneratedValue(strategy= GenerationType.SEQUENCE)
    private Long id;

    private Integer appJobConfigNum;

    private LocalDateTime indexTS;

    private Integer indexLTP;

    private Double meanStrikePCR;

    private Double meanRateOI;

    private Double combiRate;

    private String support;

    private String resistance;

    private String range;

    private String tradeDecision;

    private Integer maxPainSP;

    private Integer maxPainSPSecond;

    private String dayHigh;

    private String dayLow;

    @Column(columnDefinition = "boolean default false")
    private Boolean display = false;

    @OneToOne
    @JoinColumn(name = "jobIterationId", referencedColumnName = "id", nullable = false)
    private JobIterationDetails jobIterationDetails;

    @Transient
    private Double maxPainCELTP;

    @Transient
    private Double maxPainPELTP;

    @Transient
    MaxPain maxPain;



}
