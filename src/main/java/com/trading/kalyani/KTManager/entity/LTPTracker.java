package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Entity
@Getter
@Setter
@Table(name="LTPTracker")
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class LTPTracker {

    @Id
    @GeneratedValue(strategy= GenerationType.SEQUENCE)
    private Long id;

    @OneToOne
    @JoinColumn(name = "jobIterationId", referencedColumnName = "id", nullable = false)
    public JobIterationDetails jobIterationDetails;

    private Integer appJobConfigNum;

    private Integer indexLTP;

    private LocalDateTime indexTS;

    private String atmStrikePriceCE;

    private Double atmStrikeCELTP;

    private String atmStrikePricePE;

    private Double atmStrikePELTP;

    private String rangeLowSP;

    private Double rangeLowLTP;

    private String rangeHighSP;

    private Double rangeHighLTP;

    private String supportSP;

    private Double supportLTP;

    private String resistanceSP;

    private Double resistanceLTP;

    private String maxPainSP;

    private Double maxPainCELTP;

    private Double maxPainPELTP;


}
