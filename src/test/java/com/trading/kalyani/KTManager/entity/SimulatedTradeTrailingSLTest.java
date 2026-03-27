package com.trading.kalyani.KTManager.entity;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SimulatedTradeTrailingSLTest {

    @Test
    public void testNoTrailingWhenNotProfitable() {
        SimulatedTrade t = SimulatedTrade.builder()
                .tradeId("T1")
                .entryPrice(100.0)
                .targetPrice(140.0)
                .quantity(300)
                .build();

        // currentPrice below entry -> no trailing
        t.updateTrailingStopLoss(95.0, 50.0, 50.0);
        assertNull(t.getTrailingStopLoss(), "Trailing SL should not be set when trade is not profitable");
    }

    @Test
    public void testNoTrailingWhenBelowThreshold() {
        SimulatedTrade t = SimulatedTrade.builder()
                .tradeId("T2")
                .entryPrice(50.0)
                .targetPrice(80.0)
                .quantity(300)
                .build();

        // threshold at 50% -> 65. current=60 < threshold
        t.updateTrailingStopLoss(60.0, 50.0, 50.0);
        assertNull(t.getTrailingStopLoss(), "Trailing SL should not be set when below activation threshold");
    }

    @Test
    public void testSetTrailingAtThreshold() {
        SimulatedTrade t = SimulatedTrade.builder()
                .tradeId("T3")
                .entryPrice(50.0)
                .targetPrice(80.0)
                .quantity(300)
                .build();

        // at threshold current=65
        t.updateTrailingStopLoss(65.0, 50.0, 50.0);
        // profitPerUnit = 15 -> trailing at entry + 50%*15 = 50 + 7.5 = 57.5
        assertEquals(57.5, t.getTrailingStopLoss(), 0.0001);
    }

    @Test
    public void testAdvanceTrailingWhenPriceIncreases() {
        SimulatedTrade t = SimulatedTrade.builder()
                .tradeId("T4")
                .entryPrice(50.0)
                .targetPrice(80.0)
                .quantity(300)
                .build();

        t.updateTrailingStopLoss(66.0, 50.0, 50.0);
        Double first = t.getTrailingStopLoss();
        assertNotNull(first);

        // price rises -> trailing should advance
        t.updateTrailingStopLoss(70.0, 50.0, 50.0);
        Double second = t.getTrailingStopLoss();
        assertTrue(second >= first, "Trailing SL should advance or stay same when price rises");
    }

    @Test
    public void testPreserveTrailingWhenPriceFallsBelowThreshold() {
        SimulatedTrade t = SimulatedTrade.builder()
                .tradeId("T5")
                .entryPrice(50.0)
                .targetPrice(80.0)
                .quantity(300)
                .build();

        // set trailing
        t.updateTrailingStopLoss(66.0, 50.0, 50.0);
        Double setVal = t.getTrailingStopLoss();
        assertNotNull(setVal);

        // price dips below threshold (threshold=65) to 64
        t.updateTrailingStopLoss(64.0, 50.0, 50.0);
        // since we now preserve trailing once set, value should stay same
        assertEquals(setVal, t.getTrailingStopLoss(), "Trailing SL should be preserved even if price dips below threshold");
    }
}
