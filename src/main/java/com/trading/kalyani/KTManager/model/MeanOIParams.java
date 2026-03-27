package com.trading.kalyani.KTManager.model;


import lombok.*;


@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor

public class MeanOIParams {

    private Double meanCallOIChange;
    private Double meanPutOIChange;
    private Double meanCallOI;
    private Double meanPutOI;
    private Double meanStrikePCR;
    private Double meanRateOI;
    private Double combiRate;


}
