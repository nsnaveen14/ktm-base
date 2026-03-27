package com.trading.kalyani.KTManager.model;

import com.google.gson.annotations.SerializedName;
import lombok.*;

import java.util.Date;
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class OutputMessage {
    @SerializedName("instrumentToken")
    private long instrumentToken;

    @SerializedName("oi")
    private double oi;

    @SerializedName("lastTradedPrice")
    private double lastTradedPrice;

    @SerializedName("lastTradedTime")
    private Date lastTradedTime;

    @SerializedName("tickTimestamp")
    private Date tickTimestamp;

    @SerializedName("timeOutputMessage")
    private String timeOutputMessage;
}
