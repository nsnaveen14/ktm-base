package com.trading.kalyani.KTManager.model;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class CommonReqRes {

    private boolean status;
    private String message;
    private int qty;
    private Object data;
    private String type;


}
