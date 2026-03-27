package com.trading.kalyani.KTManager.controller;

import com.trading.kalyani.KTManager.config.KiteConnectConfig;
import com.trading.kalyani.KTManager.model.CommonReqRes;
import com.trading.kalyani.KTManager.model.KiteModel;
import com.trading.kalyani.KTManager.model.OrderPlaceModel;
import com.trading.kalyani.KTManager.repository.TradeDecisionRepository;
import com.trading.kalyani.KTManager.service.TransactionService;
import com.zerodhatech.kiteconnect.KiteConnect;
import com.zerodhatech.kiteconnect.kitehttp.exceptions.KiteException;
import com.zerodhatech.models.Margin;
import com.zerodhatech.models.Order;
import com.zerodhatech.models.OrderParams;
import com.zerodhatech.models.Quote;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.trading.kalyani.KTManager.constants.ApplicationConstants.FAILURE;
import static com.trading.kalyani.KTManager.constants.ApplicationConstants.SUCCESS;

@RestController
@CrossOrigin(value="*")
public class TransactionController {

    @Autowired
    TransactionService transactionService;

    @Autowired
    KiteConnectConfig kiteConnectConfig;

    @Autowired
    TradeDecisionRepository tradeDecisionRepository;

    private static final Logger logger = LogManager.getLogger(TransactionController.class);

    @GetMapping("/getAvailableMargins")
    public ResponseEntity<Double> getAvailableMargins() {

        try {
            return new ResponseEntity<>(transactionService.getAvailableMargins(), HttpStatus.OK);
            }
        catch(Exception e){
            logger.error("Exception while getting available margins {}",e.getMessage());
            return new ResponseEntity<>(0.0, HttpStatus.UNAUTHORIZED);
        }

    }

    @PostMapping("/placeOrder")
    public ResponseEntity<Order> placeOrder(@RequestBody OrderPlaceModel orderPlaceModel) {

        try {
            Order order = transactionService.placeOrder(orderPlaceModel);
            return new ResponseEntity<>(order, HttpStatus.OK);
        } catch (Exception e) {
            Order order = new Order();
            order.statusMessage="Order Failed";
            return new ResponseEntity<>(order, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping("/placeOrderWebClient")
    public ResponseEntity<Order> placeOrderWebClient(@RequestBody OrderPlaceModel orderPlaceModel) {

        return new ResponseEntity<>(transactionService.placeOrderWebClient(orderPlaceModel),HttpStatus.OK);
    }

    @PostMapping("/exitMarket")
    public ResponseEntity<Integer> exitMarket() {

        try {
            Integer closedOrdersNum = transactionService.closeOrders();
            return new ResponseEntity<>(closedOrdersNum, HttpStatus.OK);
        } catch (Exception e) {
            logger.error("Exception while closing orders: {}", e.getMessage());
            return new ResponseEntity<>(-1, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/getTotalPositionPNL")
    public ResponseEntity<Double> getTotalPositionPNL() {

        try {
            Double totalPNL = transactionService.getTotalPositionPNL();
            return new ResponseEntity<>(totalPNL, HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<>(0.0, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping("/cancelAllOpenOrders")
    public ResponseEntity<CommonReqRes> cancelAllOpenOrders() {

        CommonReqRes response = new CommonReqRes();

        try {
            boolean cancelStatus = transactionService.cancelAllOpenOrders();
            if(cancelStatus) {
                response.setStatus(true);
                response.setMessage("All open orders cancelled successfully.");
                response.setType(SUCCESS);
            }
            else {
                response.setStatus(false);
                response.setMessage("No open orders to cancel.");
                response.setType(FAILURE);
            }
            logger.info("Response from cancelAllOpenOrders: {}", response);

            return new ResponseEntity<>(response, HttpStatus.OK);

        } catch (Exception e) {
            logger.error("Error cancelling open orders: {}", e.getMessage());
            response.setStatus(false);
            response.setMessage("Failed to cancel open orders.");
            response.setType(FAILURE);
            return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping("/getMarketQuote")
    public ResponseEntity<Map<String, Quote>> getMarketQuote(@RequestBody KiteModel kiteModel) {
            Map<String, Quote> response = new HashMap<>();

            response = transactionService.getMarketQuote(kiteModel.getInstrumentTokens());

            return new ResponseEntity<>(response, HttpStatus.OK);
        }


    @PostMapping("/updateAllOpenTrades")
    public ResponseEntity<Integer> updateAllOpenTrades(@RequestBody List<Integer> appJobConfigNums) {

        int recordsUpdated;

        try {
            if (appJobConfigNums.getFirst().equals(-1))
                recordsUpdated = tradeDecisionRepository.updateAllOpenTrades();
            else
                recordsUpdated = tradeDecisionRepository.updateAllOpenTradesByAppJobConfigNum(appJobConfigNums);

            logger.info("Number of records updated: {}",recordsUpdated);

            return new ResponseEntity<>(recordsUpdated, HttpStatus.OK);
        }

        catch(RuntimeException e)
        {
            recordsUpdated = -1;
            logger.error("Error during updating trade decision records: {}",e.getMessage());
            return new ResponseEntity<>(recordsUpdated,HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }



}