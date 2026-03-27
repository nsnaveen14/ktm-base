package com.trading.kalyani.KTManager.controller;

import com.trading.kalyani.KTManager.service.DailyJobService;
import com.trading.kalyani.KTManager.service.serviceImpl.KiteOrderService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;


@RestController
@CrossOrigin(value="*")
public class KiteAPIController {

    @Autowired
    KiteOrderService kiteOrderService;

    @Autowired
    DailyJobService dailyJobService;

    private static final Logger logger = LogManager.getLogger(KiteAPIController.class);

    /**
     * Check if ticker is connected
     * @return Map containing isTickerConnected status
     */
    @GetMapping("/isTickerConnected")
    public ResponseEntity<Map<String, Object>> isTickerConnected() {
        Map<String, Object> result = dailyJobService.getTickerConnectionStatus();
        return ResponseEntity.ok(result);
    }

    @GetMapping("/getEquityFundBalance")
    public ResponseEntity<Object> getAvailableMargins() {
        String enctoken = "enctoken 03tNiMsXeoyOWqaGKf+CK2Lkldlfk8Gz1Cr0b61MDK2eKcR9giAoAKiMo8mNpNg48xDLyF1sTJIiD+sUyqGvSDGYqOSpO3L7lzdpCk2I2gC2pkgh0astbQ==";
        Object response =kiteOrderService.getEquityFundBalance(enctoken);
        logger.info("Response from getAvailableMargins: {}", response);
        return ResponseEntity.ok(response);
    }

    /**
     * Get ticker provider data including tickerMapForJob, niftyLastPrice, and vixLastPrice
     * @return Map containing ticker provider data with all live tick information
     */
    @GetMapping("/getTickerProviderData")
    public ResponseEntity<Map<String, Object>> getTickerProviderData() {
        Map<String, Object> tickerData = dailyJobService.getTickerProviderData();
        logger.info("Response from getTickerProviderData: niftyLastPrice={}, vixLastPrice={}, tickerMapSize={}",
                tickerData.get("niftyLastPrice"),
                tickerData.get("vixLastPrice"),
                tickerData.get("tickerMapForJobSize"));
        return ResponseEntity.ok(tickerData);
    }

    /**
     * Get only NIFTY and VIX last prices
     * @return Map containing niftyLastPrice and vixLastPrice
     */
    @GetMapping("/getIndexPrices")
    public ResponseEntity<Map<String, Object>> getIndexPrices() {
        Map<String, Object> indexPrices = dailyJobService.getIndexPrices();
        logger.info("Response from getIndexPrices: niftyLastPrice={}, vixLastPrice={}",
                indexPrices.get("niftyLastPrice"),
                indexPrices.get("vixLastPrice"));
        return ResponseEntity.ok(indexPrices);
    }

    /**
     * Get tick data for a specific instrument token.
     * If the token is not already subscribed, it will be subscribed automatically.
     * @param instrumentToken The instrument token to get tick data for
     * @return Map containing tick data for the instrument
     */
    @GetMapping("/getTickData/{instrumentToken}")
    public ResponseEntity<Map<String, Object>> getTickDataByToken(@PathVariable Long instrumentToken) {
        Map<String, Object> tickData = dailyJobService.getTickDataByToken(instrumentToken);
        logger.info("Response from getTickDataByToken: instrumentToken={}, hasData={}, newlySubscribed={}",
                instrumentToken,
                tickData.get("hasData"),
                tickData.get("newlySubscribed"));
        return ResponseEntity.ok(tickData);
    }

}
