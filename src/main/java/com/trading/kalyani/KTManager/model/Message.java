package com.trading.kalyani.KTManager.model;

import com.google.gson.annotations.SerializedName;
import lombok.*;

import java.util.Date;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString

public class Message {

    @SerializedName("instrumentToken")
    private long instrumentToken;

    @SerializedName("oi")
    private double oi;

    @SerializedName("lastTradedTime")
    private Date lastTradedTime;

    @SerializedName("lastTradedPrice")
    private double lastTradedPrice;

    @SerializedName("tickTimestamp")
    private Date tickTimestamp;

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
