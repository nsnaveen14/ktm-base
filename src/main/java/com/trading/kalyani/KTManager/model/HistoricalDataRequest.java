package com.trading.kalyani.KTManager.model;

import lombok.*;

import java.time.LocalDateTime;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
@Builder
public class HistoricalDataRequest {

    /**
     * Instrument token for which historical data is required
     */
    private String instrumentToken;

    /**
     * Start date for historical data
     */
    private LocalDateTime fromDate;

    /**
     * End date for historical data
     */
    private LocalDateTime toDate;

    /**
     * Candle interval: minute, 3minute, 5minute, 10minute, 15minute, 30minute, 60minute, day
     */
    private String interval;

    /**
     * Set to true for continuous data (for futures)
     */
    private boolean continuous;

    /**
     * Set to true to include Open Interest data
     */
    private boolean oi;

}
