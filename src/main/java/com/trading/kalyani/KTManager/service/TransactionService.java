package com.trading.kalyani.KTManager.service;

import com.trading.kalyani.KTManager.model.OrderPlaceModel;
import com.zerodhatech.models.Order;
import com.zerodhatech.models.Quote;

import java.util.ArrayList;
import java.util.Map;


public interface TransactionService {

    Order placeOrder(OrderPlaceModel orderPlaceModel);

    Order placeOrderWebClient(OrderPlaceModel orderPlaceModel);

    int closeOrders();

    Double getAvailableMargins();

    Double getTotalPositionPNL();

    Boolean cancelAllOpenOrders();

    Map<String, Quote> getMarketQuote(ArrayList<Long> instrumentTokens);
}
