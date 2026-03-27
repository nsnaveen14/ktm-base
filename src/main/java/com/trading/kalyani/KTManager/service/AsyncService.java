package com.trading.kalyani.KTManager.service;

import com.trading.kalyani.KTManager.entity.IndexLTP;
import com.trading.kalyani.KTManager.entity.NiftyLTP;
import com.trading.kalyani.KTManager.model.AutoTradeParams;
import com.zerodhatech.models.Instrument;
import com.zerodhatech.models.User;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public interface AsyncService {

    CompletableFuture<Void> saveUserNInstrumentDataAsync(User userModel, List<Instrument> instruments);

    CompletableFuture<Void> sendEmailToAllUsersAsync(String subject, String body, String attachmentPath);

    CompletableFuture<Void> placeOrderAsync(IndexLTP indexLTP, AutoTradeParams autoTradeParams);

    CompletableFuture<Void> closeOrdersAsync(IndexLTP indexLTP);


}
