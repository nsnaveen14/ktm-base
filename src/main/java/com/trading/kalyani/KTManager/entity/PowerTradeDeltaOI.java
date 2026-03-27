package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Entity
@Getter
@Setter
@Table(name="PowerTradeDeltaOI")
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class PowerTradeDeltaOI {

    @Id
    @GeneratedValue(strategy= GenerationType.AUTO)
    private Long id;

    @OneToOne
    @JoinColumn(name = "jobIterationId", referencedColumnName = "id", nullable = false)
    private JobIterationDetails jobIterationDetails;

    private LocalDateTime niftyTS;

    private String powerTradeType;

    private Double callOIChange;

    private Double putOIChange;

    private Integer niftyLTP;

    private String maxCallOIChangeStrikePrice;

    private String maxPutOIChangeStrikePrice;

    private Double maxCallOIChange;

    private Double maxPutOIChange;

}
