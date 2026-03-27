package com.trading.kalyani.KTManager.service;

import java.util.concurrent.CompletableFuture;

public interface AsyncDailyJobServices {

    CompletableFuture<Void> startJobByConfigNumberAsync(Integer appJobConfigNum);

    CompletableFuture<Void> stopJobByConfigNumberAsync(Integer appJobConfigNum);

    CompletableFuture<Void> saveOISnapshot(Integer appJobConfigNum);

}
