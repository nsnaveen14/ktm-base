package com.trading.kalyani.KTManager.model;

import lombok.*;

import java.util.ArrayList;

@Getter
@Setter
@AllArgsConstructor
@ToString
@NoArgsConstructor
public class KiteModel {

    private ArrayList<Long> instrumentTokens;

    private ArrayList<Long> exchangeTokens;

}
