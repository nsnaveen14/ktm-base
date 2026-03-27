package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.entity.NiftyLTP;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JobServiceImplTest {

    @Test
    void testCalculateSentiment() {
        JobServiceImpl jobService = new JobServiceImpl();

        // Test cases for PCR = 0.48
        assertEquals("neutral", jobService.calculateSentiment(0.48, -2.0, -1.0));
        assertEquals("very bearish", jobService.calculateSentiment(0.48, -2.0, 1.0));
        assertEquals("bearish", jobService.calculateSentiment(0.48, 0.5, 1.0));
        assertEquals("neutral", jobService.calculateSentiment(0.48, 1.5, 1.0));

        // Test cases for PCR = 1.05
        assertEquals("bullish", jobService.calculateSentiment(1.05, -2.0, -1.0));
        assertEquals("bearish", jobService.calculateSentiment(1.05, -2.0, 1.0));
        assertEquals("neutral", jobService.calculateSentiment(1.05, 0.5, 1.0));
        assertEquals("bullish", jobService.calculateSentiment(1.05, 1.5, 1.0));

        // Test cases for PCR = 1.50
        assertEquals("very bullish", jobService.calculateSentiment(1.50, -2.0, -1.0));
        assertEquals("bearish", jobService.calculateSentiment(1.50, -2.0, 1.0));
        assertEquals("bullish", jobService.calculateSentiment(1.50, 0.5, 1.0));
        assertEquals("very bullish", jobService.calculateSentiment(1.50, 1.5, 1.0));
    }

    @Test
    void rateOICalc() {

        JobServiceImpl jobService = new JobServiceImpl();

        // Test cases with expected values
        assertEquals(2.0, jobService.rateOICalc(100, 200), 0.0001);
        assertEquals(-2.0, jobService.rateOICalc(-100, -200), 0.0001);
        assertEquals(0.5, jobService.rateOICalc(-200, -100), 0.0001);
        assertEquals(0.75, jobService.rateOICalc(-200, -50), 0.0001);
        assertEquals(-3.0, jobService.rateOICalc(100, -200), 0.0001);
        assertEquals(-2.5, jobService.rateOICalc(100, -150), 0.0001);
        assertEquals(-3.0, jobService.rateOICalc(200, -100), 0.0001);
        assertEquals(7.0, jobService.rateOICalc(-150, 200), 0.0001);
        assertEquals(2.0, jobService.rateOICalc(-150, 50), 0.0001);

    }

    @Test
    void testCalculateLotSize() {
        JobServiceImpl jobService = new JobServiceImpl();
        NiftyLTP niftyLTP = new NiftyLTP();

        // Test case 1: Trade decision is SELL and NiftyLTP > MaxPainSP
        niftyLTP.setTradeDecision("SELL");
        niftyLTP.setNiftyLTP(12000);
        niftyLTP.setMaxPainSP(11800);
        jobService.calculateLotSize(niftyLTP);
        assertEquals(20, niftyLTP.getLotSize());

        // Test case 2: Trade decision is BUY and NiftyLTP < MaxPainSP
        niftyLTP.setTradeDecision("BUY");
        niftyLTP.setNiftyLTP(11500);
        niftyLTP.setMaxPainSP(11800);
        jobService.calculateLotSize(niftyLTP);
        assertEquals(30, niftyLTP.getLotSize());

        // Test case 3: Trade decision is SELL but NiftyLTP <= MaxPainSP
        niftyLTP.setTradeDecision("SELL");
        niftyLTP.setNiftyLTP(11800);
        niftyLTP.setMaxPainSP(11800);
        jobService.calculateLotSize(niftyLTP);
        assertEquals(0, niftyLTP.getLotSize());

        // Test case 4: Trade decision is BUY but NiftyLTP >= MaxPainSP
        niftyLTP.setTradeDecision("BUY");
        niftyLTP.setNiftyLTP(12000);
        niftyLTP.setMaxPainSP(11800);
        jobService.calculateLotSize(niftyLTP);
        assertEquals(0, niftyLTP.getLotSize());
    }
}