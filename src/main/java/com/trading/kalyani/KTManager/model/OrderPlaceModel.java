package com.trading.kalyani.KTManager.model;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class OrderPlaceModel {


    private String tradingSymbol;

    private String exchange;

    private String transaction_type;

    private String order_type;

    private Integer quantity;

    private String product;

    private Double price;

    private Double trigger_price;

    private String validity;
}
