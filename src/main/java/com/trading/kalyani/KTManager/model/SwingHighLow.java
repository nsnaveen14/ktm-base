package com.trading.kalyani.KTManager.model;

import com.trading.kalyani.KTManager.entity.CandleStick;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class SwingHighLow {

    CandleStick swingHigh;
    CandleStick swingLow;
}
