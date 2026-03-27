package com.trading.kalyani.KTManager.service;

import com.trading.kalyani.KTManager.entity.JobDetails;
import com.trading.kalyani.KTManager.entity.LTPTracker;
import com.trading.kalyani.KTManager.entity.MiniDelta;
import com.trading.kalyani.KTManager.entity.NiftyLTP;
import com.trading.kalyani.KTManager.model.AppJobConfigParams;
import com.trading.kalyani.KTManager.model.AutoTradeParams;
import com.trading.kalyani.KTManager.model.DeltaOICalculations;

import java.time.LocalDateTime;
import java.util.List;

public interface JobService {

    boolean saveOISnapshot(Integer appJobConfigNum);

    List<DeltaOICalculations> calculateOIDelta(JobDetails jobDetails);

    List<NiftyLTP> getDataForNiftyChart(Long startIndex);

    void loadBackUp();

    List<MiniDelta> getLatestMiniDelta();

    LocalDateTime getLastIterationTimestamp();

    LocalDateTime getLastTickerTS();

    List<NiftyLTP> getNiftyLTPDataAfterRequestedTime(LocalDateTime reqDateTime);

    AutoTradeParams setAutoTrade(AutoTradeParams autoTradeParams);

    AutoTradeParams getAutoTrade();

    void calculateStraddleFlags(NiftyLTP niftyLTP, LTPTracker ltpTracker);

    void calculateMaxPainLTP(NiftyLTP niftyLTP, LTPTracker ltpTracker);

    void calculateCloseFullFlag(NiftyLTP niftyLTP);

    void calculateLotSize(NiftyLTP niftyLTP);

    AutoTradeParams resetAutoTradeParams();

    boolean startKiteTicker();

    boolean stopKiteTicker();

    Boolean setAutoTradeEnabled(AppJobConfigParams appJobConfigParams);
}
