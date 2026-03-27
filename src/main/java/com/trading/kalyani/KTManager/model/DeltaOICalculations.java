package com.trading.kalyani.KTManager.model;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class DeltaOICalculations extends DeltaOI{

    public Double callOIChange,putOIChange;

    public DeltaOICalculations(String strikePrice, Double rateOI, Double strikePCR, Double callOI, Double putOI, Double callOIChange, Double putOIChange) {
        super(strikePrice, rateOI, strikePCR, callOI, putOI);
        this.callOIChange = callOIChange;
        this.putOIChange = putOIChange;
    }
}
