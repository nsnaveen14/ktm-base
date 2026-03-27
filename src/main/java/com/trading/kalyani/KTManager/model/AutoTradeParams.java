package com.trading.kalyani.KTManager.model;


import com.trading.kalyani.KTManager.entity.JobIterationDetails;
import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "AutoTradeParams")
public class AutoTradeParams {

    @Id
    @GeneratedValue(strategy= GenerationType.AUTO)
    private Long id;

    private boolean autoTradeFlag;
    private boolean closeTradeFlag;
    private String autoTradeCallSymbol;
    private Long autoTradeCallInstrumentToken;
    private Integer autoTradeCallLotSize;
    private Integer autoTradeCallEntryPrice;
    private Integer autoTradeCallSLPrice;
    private String autoTradePutSymbol;
    private Long autoTradePutInstrumentToken;
    private Integer autoTradePutLotSize;
    private Integer autoTradePutEntryPrice;
    private Integer autoTradePutSLPrice;
    private LocalDateTime autoTradeParamsTS;

    @OneToOne
    @JoinColumn(name = "jobIterationId", referencedColumnName = "id", nullable = false)
    private JobIterationDetails jobIterationDetails;


}
