package com.trading.kalyani.KTManager.model;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class IndexOHLC {

    Integer appJobConfigNum;

    String indexLTP;

    String thresholdValue;

    String dayLow;

    String dayHigh;


}
