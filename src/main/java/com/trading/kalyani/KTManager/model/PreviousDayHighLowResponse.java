package com.trading.kalyani.KTManager.model;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
@Builder
public class PreviousDayHighLowResponse {

    private boolean success;
    private String message;
    private String instrumentToken;
    private String date;
    private double high;
    private double low;
    private double open;
    private double close;
}

