package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Getter
@Setter
@Table(name="AppJobConfig")
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class AppJobConfig {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "app_job_config_num")
    private Integer appJobConfigNum;

    @ManyToOne
    @JoinColumn(name = "indexId", referencedColumnName = "indexId", nullable = false)
    private AppIndexConfig appIndexConfig;

    @ManyToOne
    @JoinColumn(name = "job_type_code", referencedColumnName = "job_type_code", nullable = false)
    private JobType jobType;

    @Column(name = "is_active", nullable = false)
    private Boolean isActive = true;

    @Column(name = "auto_trade_enabled", nullable = false)
    private Boolean autoTradeEnabled;

}
