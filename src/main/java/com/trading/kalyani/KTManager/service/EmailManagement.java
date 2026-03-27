package com.trading.kalyani.KTManager.service;

import com.trading.kalyani.KTManager.entity.AppJobConfig;
import com.trading.kalyani.KTManager.entity.IndexLTP;
import com.trading.kalyani.KTManager.entity.JobIterationDetails;
import com.trading.kalyani.KTManager.entity.TradeDecisions;

public interface EmailManagement {

    void sendEmailForErrorInJob(Exception e, AppJobConfig appJobConfig, Integer retryCounter, JobIterationDetails jobIterationDetails);

    void sendEmailAsync(String fileName, AppJobConfig appJobConfig);

    void sendEmailOfTradeDecision(IndexLTP indexLTP, AppJobConfig appJobConfig);

    void sendEmailOfAutoTradeDecision(IndexLTP indexLTP, AppJobConfig appJobConfig, TradeDecisions lastTradeDecisions);

    void sendEmailOfPlacingAutoTradeOrder(TradeDecisions lastTradeDecisions, String instrumentToken);

    void sendEmailForClosingTrade(IndexLTP indexLTP, AppJobConfig appJobConfig, TradeDecisions tradeDecisions);

    void sendEmailForJobStopped(Integer appJobConfigNum);

    void sendEmailForRangeChange(IndexLTP indexLTP, IndexLTP prevItrIndexLTP, AppJobConfig appJobConfig);
}
