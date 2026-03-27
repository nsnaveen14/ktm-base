package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Entity
@Getter
@Setter
@Table(name="NiftyLTP")
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class NiftyLTP {

    @Id
    @GeneratedValue(strategy= GenerationType.AUTO)
    private Long id;

    private LocalDateTime niftyTS;

    private Integer niftyLTP;

    private Double meanStrikePCR;

    private Double meanRateOI;

    private Double combiRate;

    private String support;

    private String resistance;

    private String range;

    private String tradeDecision;

    private String tradeManagement;

    private Integer tradeHoldCount;

    private Boolean powerTrade;

    private String powerTradeType;

    private Boolean iBuyers;

    private Boolean iSellers;

    private Integer icebergTrendCounter;

    private String icebergTradeType;

    private Double cpts;

    private Integer maxPainSP;

    private Integer maxPainSPSecond;

    @OneToOne
    @JoinColumn(name = "jobIterationId", referencedColumnName = "id", nullable = false)
    private JobIterationDetails jobIterationDetails;

    @Transient
    private boolean straddleUpside;
    @Transient
    private boolean straddleDownside;
    @Transient
    private boolean closeFull;

    @Transient
    private Double maxPainCELTP;

    @Transient
    private Double maxPainPELTP;

    @Transient
    private Integer lotSize;


}
