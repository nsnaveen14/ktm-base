package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.TRADE_DECISION_TYPE_REGULAR;

@Entity
@Getter
@Setter
@Table(name="TradeDecisions")
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class TradeDecisions {

    @Id
    @GeneratedValue(strategy= GenerationType.SEQUENCE)
    private Long id;

    private String tradeDecision;

    private String tradeDecisionType = TRADE_DECISION_TYPE_REGULAR;

    private LocalDateTime tradeDecisionTS;

    private Integer indexLTP;

    private Integer entryIndexLTP;

    private Integer targetIndexLTP;

    private Integer stopLossIndexLTP;

    private String status;

    private String trade_decision_result;

    private LocalDateTime trade_decision_result_ts;

    @ManyToOne
    @JoinColumn(name = "appJobConfigNum", referencedColumnName = "app_job_config_num", nullable = false)
    private AppJobConfig appJobConfig;

    @Column(nullable = true, columnDefinition = "double precision default 0.0")
    private Double swingTarget = 0.0;

    @Column(nullable = true, columnDefinition = "boolean default false")
    private boolean swingTaken = false;

    @Column(nullable = true, columnDefinition = "boolean default false")
    private boolean confirmationTaken = false;


    @ManyToOne
    @JoinColumn(name = "jobIterationId", referencedColumnName = "id", nullable = false)
    private JobIterationDetails jobIterationDetails;

    public TradeDecisions( String tradeDecision, String tradeDecisionType, LocalDateTime tradeDecisionTS, Integer indexLTP,  String status, AppJobConfig appJobConfig, JobIterationDetails jobIterationDetails) {

        this.tradeDecision = tradeDecision;
        this.tradeDecisionType = tradeDecisionType;
        this.tradeDecisionTS = tradeDecisionTS;
        this.indexLTP = indexLTP;
        this.status = status;
        this.appJobConfig = appJobConfig;
        this.jobIterationDetails = jobIterationDetails;
    }
}
