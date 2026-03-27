package com.trading.kalyani.KTManager.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class DeltaOI {

    public String strikePrice;

    public Double rateOI,strikePCR,callOI,putOI;

}
