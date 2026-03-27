package com.trading.kalyani.KTManager.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@AllArgsConstructor
@ToString
public class AppJobConfigParams {

    private Integer appJobConfigNum;

    private Boolean isAutoTradeEnabled;

}
