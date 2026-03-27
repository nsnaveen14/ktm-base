import { Injectable, Input } from '@angular/core';
import { HttpClient, HttpHeaders, HttpParams } from '@angular/common/http';
import { Observable, of, shareReplay } from 'rxjs';
import { UserModel } from '../models/userModel';
import { KiteModelInput } from '../models/kiteInputModel';
import { InstrumentEntity } from '../models/instrumentEntity';
import { LocalStorageService } from './localstorage.service';
import { NiftyLTP } from '../models/nifty-ltp';
import { MiniDelta } from '../models/minidelta';
import { OrderRequest } from '../models/orderRequest';
import { OrderResponse } from '../models/OrderResponse';
import { CommonReqRes } from '../models/CommonReqRes';
import { AutoTradeParams } from '../models/autoTradeParams';
import { AppJobConfig } from '../models/AppJobConfig';
import { IndexLTPModel } from '../models/indexLTPModel';
import { TradeDecisionModel } from '../models/TradeDecisionModel';
import { SwingPointData } from '../models/SwingPointData';
import { LTPTrackerConfig } from '../models/ltpTrackerConfig';
import { JobRunningDetails } from '../models/jobRunningDetails';
import { DailyJobPlanner } from '../models/dailyJobPlanner';
import {
  PredictedCandleStick,
  PredictionDeviation,
  TradeSetup,
  PredictionJobStatus,
  CorrectionFactors,
  DeviationSummary,
  TradeSetupPerformance,
  LiveTickData,
  EMAChartData,
  RollingChartData
} from '../models/analytics.model';

@Injectable({
  providedIn: 'root'
})
export class DataService {
  isLocal = false;
  portNo = 8084;
  baseUrl: string = "";
  tokenString: string = "";
  webSocketUrl: string = "/chat";
  private loginUrl = "/login";
  private accessTokenUrl = "/getAccessTokenFromDB";
  private getInstrumentDataUrl = "/getInstrumentsData";
  private startTickerConUrl = "/startKiteTicker";
  private stopTickerConUrl = "/stopKiteTicker";
  private isTickerConnectedUrl = "/isTickerConnected";
  private subscribeUrl = "/subscribeTicker";
  private unsubscribeUrl = "/unsubscribeTicker";
  private accessToken: string | any = '';
  private takeOISnapshot = "/startJobToCreateOISnapshot";
  private niftyLTP = "/getNiftyLTPForChart";
  private startJobForOICalculations = "/startJobForOICalculations";
  private stopJobForOICalculations = "/stopJobForOICalculations";
  private miniDelta = "/getMiniDeltaTable";
  private bhavCopyUrl = "/bhavcopy";
  private transferInstrumentsUrl = "/transferInstrumentData";
  private placeOrderUrl = "/placeOrder";
  private getNiftyLTPDataAfterTSUrl = "/getNiftyLTPDataAfterRequestedTime";
  private getAutoTradeParametersUrl = "/getAutoTradeParams";
  private setAutoTradeParams = "/setAutoTradeParams";
  private exitmarketUrl = "/exitMarket";
  private getAvailableMarginsUrl = "/getAvailableMargins";
  private uploadOISnapshotURL = "/uploadOISnapshotFile";
  private getJobRunningParametersUrl = "/getJobRunningParameters";
  private searchInstrumentsUrl = "/searchInstruments";
  private cancelOpenOrdersUrl = "/cancelAllOpenOrders";
  private getAppJobConfigDetailsUrl = "/getAppJobConfigDetails";
  private getIndexLTPDataByConfigNumUrl = "/getIndexLTPDataByConfigNum";
  private getTradeDecisionsByConfigNumUrl = "/getTradeDecisionsByConfigNum";
  private startJobUrl = "/startJob";
  private stopJobUrl = "/stopJob";
  private getMiniDeltaDataByAppJobConfigNumUrl = "/getMiniDeltaDataByAppJobConfigNum";
  private updateAllOpenTradesUrl = "/updateAllOpenTrades";
  private getSwingHighLowByConfigNumUrl = "/getSwingHighLowByConfigNum";
  private getLTPTrackerConfigUrl = "/getLTPTrackerConfig";
  private setLTPTrackerConfigUrl = "/setLTPTrackerConfig";
  private getJobRunningStatusByConfigNumUrl = "/getJobRunningStatusByConfigNum";
  private getDailyJobPlannerConfigUrl = "/getDailyJobPlannerConfig";
  private modifyDailyJobPlannerConfigUrl = "/modifyDailyJobPlannerConfig";

  // Analytics/Prediction API endpoints
  private predictionBaseUrl = "/api/prediction";
  private predictionGenerateUrl = this.predictionBaseUrl + "/generate";
  private predictionLatestUrl = this.predictionBaseUrl + "/latest";
  private predictionRecommendationUrl = this.predictionBaseUrl + "/recommendation";
  private predictionVerifyUrl = this.predictionBaseUrl + "/verify";
  private predictionAccuracyUrl = this.predictionBaseUrl + "/accuracy";
  private tradeSetupLatestUrl = this.predictionBaseUrl + "/trade-setup/latest";
  private tradeSetupTodayUrl = this.predictionBaseUrl + "/trade-setup/today";
  private tradeSetupPerformanceUrl = this.predictionBaseUrl + "/trade-setup/performance";
  private predictionJobStartUrl = this.predictionBaseUrl + "/job/start";
  private predictionJobStopUrl = this.predictionBaseUrl + "/job/stop";
  private predictionJobStatusUrl = this.predictionBaseUrl + "/job/status";
  private predictionJobExecuteUrl = this.predictionBaseUrl + "/job/execute";
  private deviationCalculateUrl = this.predictionBaseUrl + "/deviation/calculate";
  private deviationLatestUrl = this.predictionBaseUrl + "/deviation/latest";
  private deviationTodayUrl = this.predictionBaseUrl + "/deviation/today";
  private deviationCorrectionsUrl = this.predictionBaseUrl + "/deviation/corrections";
  private deviationSummaryUrl = this.predictionBaseUrl + "/deviation/summary";


  // Liquidity Sweep API endpoints
  private liquiditySweepBaseUrl = "/api/liquidity-sweep";
  private liquiditySweepLatestUrl = this.liquiditySweepBaseUrl + "/latest";
  private liquiditySweepLatestSetupUrl = this.liquiditySweepBaseUrl + "/latest-setup";
  private liquiditySweepTodayUrl = this.liquiditySweepBaseUrl + "/today";
  private liquiditySweepAnalyzeUrl = this.liquiditySweepBaseUrl + "/analyze";
  private liquiditySweepSignalUrl = this.liquiditySweepBaseUrl + "/signal";
  private liquiditySweepRecommendationUrl = this.liquiditySweepBaseUrl + "/recommendation";
  private liquiditySweepLevelsUrl = this.liquiditySweepBaseUrl + "/levels";
  private liquiditySweepWhaleActivityUrl = this.liquiditySweepBaseUrl + "/whale-activity";
  private liquiditySweepDashboardUrl = this.liquiditySweepBaseUrl + "/dashboard";
  private liquiditySweepConfigUrl = this.liquiditySweepBaseUrl + "/config";

  // Liquidity Zone Analysis API endpoints
  private liquidityZoneBaseUrl = "/api/liquidity";
  private liquidityZoneDashboardUrl = this.liquidityZoneBaseUrl + "/dashboard";
  private liquidityZoneAnalyzeUrl = this.liquidityZoneBaseUrl + "/analyze";
  private liquidityZoneAnalyzeAllUrl = this.liquidityZoneBaseUrl + "/analyze-all";
  private liquidityZoneTriggerAnalysisUrl = this.liquidityZoneBaseUrl + "/trigger-analysis";
  private liquidityZoneLatestUrl = this.liquidityZoneBaseUrl + "/latest";
  private liquidityZoneMultiTimeframeUrl = this.liquidityZoneBaseUrl + "/multi-timeframe";
  private liquidityZoneTodayUrl = this.liquidityZoneBaseUrl + "/today";
  private liquidityZoneActiveSetupsUrl = this.liquidityZoneBaseUrl + "/active-setups";
  private liquidityZoneChartDataUrl = this.liquidityZoneBaseUrl + "/chart-data";
  private liquidityZoneAllIndicesUrl = this.liquidityZoneBaseUrl + "/all-indices";

  // Internal Order Block (IOB) API endpoints
  private iobBaseUrl = "/api/iob";
  private iobScanUrl = this.iobBaseUrl + "/scan";
  private iobScanAllUrl = this.iobBaseUrl + "/scan-all";
  private iobFreshUrl = this.iobBaseUrl + "/fresh";
  private iobBullishUrl = this.iobBaseUrl + "/bullish";
  private iobBearishUrl = this.iobBaseUrl + "/bearish";
  private iobTodayUrl = this.iobBaseUrl + "/today";
  private iobTradableUrl = this.iobBaseUrl + "/tradable";
  private iobDashboardUrl = this.iobBaseUrl + "/dashboard";
  private iobAnalysisUrl = this.iobBaseUrl + "/analysis";
  private iobAllIndicesUrl = this.iobBaseUrl + "/all-indices";
  private iobTradeSetupUrl = this.iobBaseUrl + "/trade-setup";
  private iobExecuteTradeUrl = this.iobBaseUrl + "/execute-trade";
  private iobStatisticsUrl = this.iobBaseUrl + "/statistics";
  private iobCheckMitigationUrl = this.iobBaseUrl + "/check-mitigation";
  private iobMitigateUrl = this.iobBaseUrl + "/mitigate";

  // IOB Auto Trade API endpoints
  private iobAutoTradeBaseUrl = "/api/iob/auto-trade";
  private iobAutoTradeStatusUrl = this.iobAutoTradeBaseUrl + "/status";
  private iobAutoTradeEnableUrl = this.iobAutoTradeBaseUrl + "/enable";
  private iobAutoTradeDisableUrl = this.iobAutoTradeBaseUrl + "/disable";
  private iobAutoTradeConfigUrl = this.iobAutoTradeBaseUrl + "/config";
  private iobAutoTradeEnterUrl = this.iobAutoTradeBaseUrl + "/enter";
  private iobAutoTradeExitUrl = this.iobAutoTradeBaseUrl + "/exit";
  private iobAutoTradeExitAllUrl = this.iobAutoTradeBaseUrl + "/exit-all";
  private iobAutoTradeOpenUrl = this.iobAutoTradeBaseUrl + "/open-trades";
  private iobAutoTradeTodayUrl = this.iobAutoTradeBaseUrl + "/today";
  private iobAutoTradeHistoryUrl = this.iobAutoTradeBaseUrl + "/history";
  private iobAutoTradePerformanceUrl = this.iobAutoTradeBaseUrl + "/performance";
  private iobAutoTradeRiskUrl = this.iobAutoTradeBaseUrl + "/risk";
  private iobAutoTradeBacktestUrl = this.iobAutoTradeBaseUrl + "/backtest";
  private iobAutoTradeMTFUrl = this.iobAutoTradeBaseUrl + "/mtf";
  private iobAutoTradeHTFAlignedUrl = this.iobAutoTradeBaseUrl + "/htf-aligned";


  constructor(private httpClient: HttpClient, private localStorageService: LocalStorageService) {
    if (this.isLocal) {
      this.baseUrl = "http://localhost:" + this.portNo;
      this.webSocketUrl = "ws://localhost:" + this.portNo + "/chat";
    }

  }

  getAccessTokenFromDB() {
    return this.httpClient.get<UserModel>(this.baseUrl + this.accessTokenUrl);
  }

  getInstrumentData() {

    return this.httpClient.get<InstrumentEntity[]>(this.baseUrl + this.getInstrumentDataUrl);
  }

  startTickerConnection() {

    return this.httpClient.post<boolean>(this.baseUrl + this.startTickerConUrl, {});
  }

  stopTickerConnection() {
    return this.httpClient.post<boolean>(this.baseUrl + this.stopTickerConUrl, {});
  }

  subscribeInstrument(instrumentParamList: KiteModelInput | any) {
    return this.httpClient.post(this.baseUrl + this.subscribeUrl, instrumentParamList);
  }

  unsubscribeInstrument(instrumentParamList: KiteModelInput | any) {
    return this.httpClient.post(this.baseUrl + this.unsubscribeUrl, instrumentParamList);
  }


  getOISnapshot(params: number) {
    return this.httpClient.post<boolean>(this.baseUrl + this.takeOISnapshot, params);
  }


  getNiftyLTPForChart(startIndex: number) {
    let params = new HttpParams().set('startIndex', startIndex.toString());
    return this.httpClient.get<NiftyLTP[]>(this.baseUrl + this.niftyLTP, { params });
  }

  startJobForOICalculationsFN() {
    return this.httpClient.get(this.baseUrl + this.startJobForOICalculations);
  }

  stopJobForOICalculationsFN() {
    return this.httpClient.get(this.baseUrl + this.stopJobForOICalculations);
  }

  getMiniDeltaTable() {
    return this.httpClient.get<MiniDelta[]>(this.baseUrl + this.miniDelta);
  }

  uploadBhavcopy(file: File) {
    const formData = new FormData();
    formData.append('file', file, file.name);
    return this.httpClient.post<boolean>(this.baseUrl + this.bhavCopyUrl, formData);
  }

  uploadOISnapshot(file: File) {
    const formData = new FormData();
    formData.append('file', file, file.name);
    return this.httpClient.post<CommonReqRes>(this.baseUrl + this.uploadOISnapshotURL, formData);
  }

  transferInstruments() {
    return this.httpClient.post<Boolean>(this.baseUrl + this.transferInstrumentsUrl, null);
  }

  placeOrder(orderRequest: OrderRequest) {
    return this.httpClient.post<OrderResponse>(this.baseUrl + this.placeOrderUrl, orderRequest);
  }

  getNiftyLTPDataAfterTS(startTime: string) {
    let params = new HttpParams()
      .set('reqDateTime', startTime);
    return this.httpClient.get<NiftyLTP[]>(this.baseUrl + this.getNiftyLTPDataAfterTSUrl, { params });
  }

  getAutoTradeParameters() {
    return this.httpClient.get<AutoTradeParams>(this.baseUrl + this.getAutoTradeParametersUrl);
  }

  setAutoTradeParameters(autoTradeParams: AutoTradeParams) {
    return this.httpClient.post<AutoTradeParams>(this.baseUrl + this.setAutoTradeParams, autoTradeParams);
  }

  exitMarket() {
    return this.httpClient.post<number>(this.baseUrl + this.exitmarketUrl, {});
  }

  getJobRunningParameters() {
    return this.httpClient.get<Map<string, any>>(this.baseUrl + this.getJobRunningParametersUrl);
  }

  searchInstruments(searchString: string) {
    let params = new HttpParams().set('searchString', searchString);
    return this.httpClient.get<InstrumentEntity[]>(this.baseUrl + this.searchInstrumentsUrl, { params });
  }

  cancelOpenOrders() {
    return this.httpClient.post<CommonReqRes>(this.baseUrl + this.cancelOpenOrdersUrl, {});
  }

  getAppJobConfigDetails() {
    return this.httpClient.get<AppJobConfig[]>(this.baseUrl + this.getAppJobConfigDetailsUrl);
  }

  getIndexLTPDataByConfigNum(appJobConfigNum: number) {
    let params = new HttpParams().set('appJobConfigNum', appJobConfigNum.toString());
    return this.httpClient.get<IndexLTPModel[]>(this.baseUrl + this.getIndexLTPDataByConfigNumUrl, { params });
  }

  getTradeDecisionsByConfigNum(appJobConfigNum: number) {
    let params = new HttpParams().set('appJobConfigNum', appJobConfigNum.toString());
    return this.httpClient.get<TradeDecisionModel[]>(this.baseUrl + this.getTradeDecisionsByConfigNumUrl, { params });
  }

  startJob(appJobConfigNum: number) {

    return this.httpClient.post<boolean>(this.baseUrl + this.startJobUrl, appJobConfigNum);
  }

  stopJob(appJobConfigNum: number) {
    return this.httpClient.post<boolean>(this.baseUrl + this.stopJobUrl, appJobConfigNum);
  }

  getMiniDeltaDataByAppJobConfigNum(appJobConfigNum: number) {
    let params = new HttpParams().set('appJobConfigNum', appJobConfigNum.toString());
    return this.httpClient.get<MiniDelta[]>(this.baseUrl + this.getMiniDeltaDataByAppJobConfigNumUrl, { params });
  }

  updateAllOpenTrades(appJobConfigNums: number[]) {
    return this.httpClient.post<number>(this.baseUrl + this.updateAllOpenTradesUrl, appJobConfigNums);
  }

  getSwingHighLowByConfigNum(appJobConfigNum: number) {
    let params = new HttpParams().set('appJobConfigNum', appJobConfigNum.toString());
    return this.httpClient.get<SwingPointData>(this.baseUrl + this.getSwingHighLowByConfigNumUrl, { params });
  }

  getLTPTrackerConfig() {
    return this.httpClient.get<LTPTrackerConfig[]>(this.baseUrl + this.getLTPTrackerConfigUrl);
  }

  setLTPTrackerConfig(ltpTrackerConfig: LTPTrackerConfig) {
    return this.httpClient.post<LTPTrackerConfig>(this.baseUrl + this.setLTPTrackerConfigUrl, ltpTrackerConfig);
  }

  getJobRunningStatusByConfigNum(appJobConfigNum: number) {
    let params = new HttpParams().set('appJobConfigNum', appJobConfigNum.toString());
    return this.httpClient.get<JobRunningDetails[]>(this.baseUrl + this.getJobRunningStatusByConfigNumUrl, { params });
  }

  getDailyJobPlannerConfig() {
    return this.httpClient.get<DailyJobPlanner[]>(this.baseUrl + this.getDailyJobPlannerConfigUrl);
  }

  modifyDailyJobPlannerConfig(dailyJobPlanner: DailyJobPlanner) {
    return this.httpClient.post<DailyJobPlanner>(this.baseUrl + this.modifyDailyJobPlannerConfigUrl, dailyJobPlanner);
  }

  // ===================== Analytics/Prediction API Methods =====================

  // Prediction endpoints
  generatePredictions(appJobConfigNum: number) {
    return this.httpClient.post<PredictedCandleStick[]>(
      this.baseUrl + this.predictionGenerateUrl + '/' + appJobConfigNum, {}
    );
  }

  getLatestPredictions() {
    return this.httpClient.get<PredictedCandleStick[]>(this.baseUrl + this.predictionLatestUrl);
  }

  getTradeRecommendation() {
    return this.httpClient.get<{ recommendation: string }>(this.baseUrl + this.predictionRecommendationUrl);
  }

  verifyPredictions() {
    return this.httpClient.post<{ verifiedCount: number, sessionAccuracy: number }>(
      this.baseUrl + this.predictionVerifyUrl, {}
    );
  }

  getPredictionAccuracy() {
    return this.httpClient.get<{ accuracy: number }>(this.baseUrl + this.predictionAccuracyUrl);
  }

  // Trade Setup endpoints
  getLatestTradeSetup() {
    return this.httpClient.get<TradeSetup>(this.baseUrl + this.tradeSetupLatestUrl);
  }

  getTodayTradeSetups() {
    return this.httpClient.get<TradeSetup[]>(this.baseUrl + this.tradeSetupTodayUrl);
  }

  getTradeSetupPerformance() {
    return this.httpClient.get<TradeSetupPerformance>(this.baseUrl + this.tradeSetupPerformanceUrl);
  }

  // Prediction Job Control endpoints
  startPredictionJob() {
    return this.httpClient.post<{ status: string, message: string, isActive: boolean }>(
      this.baseUrl + this.predictionJobStartUrl, {}
    );
  }

  stopPredictionJob() {
    return this.httpClient.post<{ status: string, message: string, isActive: boolean }>(
      this.baseUrl + this.predictionJobStopUrl, {}
    );
  }

  getPredictionJobStatus() {
    return this.httpClient.get<PredictionJobStatus>(this.baseUrl + this.predictionJobStatusUrl);
  }

  executePredictionJobNow() {
    return this.httpClient.post<{ status: string, message: string, predictions: PredictedCandleStick[] }>(
      this.baseUrl + this.predictionJobExecuteUrl, {}
    );
  }

  // Deviation Tracking endpoints
  calculateDeviation() {
    return this.httpClient.post<PredictionDeviation>(this.baseUrl + this.deviationCalculateUrl, {});
  }

  getLatestDeviation() {
    return this.httpClient.get<PredictionDeviation>(this.baseUrl + this.deviationLatestUrl);
  }

  getTodayDeviations() {
    return this.httpClient.get<PredictionDeviation[]>(this.baseUrl + this.deviationTodayUrl);
  }

  getCorrectionFactors() {
    return this.httpClient.get<CorrectionFactors>(this.baseUrl + this.deviationCorrectionsUrl);
  }

  getDeviationSummary() {
    return this.httpClient.get<DeviationSummary>(this.baseUrl + this.deviationSummaryUrl);
  }

  // Live Tick Data endpoint
  /**
   * Return live tick data only during market hours.
   * When market is closed, returns an Observable of null so callers can handle it gracefully.
   */
  getLiveTickData(): Observable<any> {
    if (!this.isMarketOpen()) {
      return of(null);
    }
    return this.httpClient.get<any>(this.baseUrl + this.predictionBaseUrl + '/live-tick');
  }

  /**
   * Simple market hours check (defaults to Mon-Fri 09:15 - 15:30 local time).
   * This can be extended to read values from config or use server time if needed.
   */
  isMarketOpen(): boolean {
    try {
      const now = new Date();
      const day = now.getDay(); // 0=Sun,1=Mon...6=Sat
      if (day === 0 || day === 6) return false; // Weekend

      const openHour = 9, openMinute = 0;
      const closeHour = 15, closeMinute = 30;

      const open = new Date(now.getFullYear(), now.getMonth(), now.getDate(), openHour, openMinute, 0);
      const close = new Date(now.getFullYear(), now.getMonth(), now.getDate(), closeHour, closeMinute, 0);

      return now >= open && now <= close;
    } catch (e) {
      // On error, be conservative and return false to avoid calling live endpoints.
      return false;
    }
  }

  // EMA Chart Data endpoint
  getEMAChartData() {
    return this.httpClient.get<EMAChartData>(this.baseUrl + this.predictionBaseUrl + '/ema-chart');
  }

  // Rolling Chart Data endpoint (Predicted vs Actual with 30-min window)
  getRollingChartData() {
    return this.httpClient.get<RollingChartData>(this.baseUrl + this.predictionBaseUrl + '/rolling-chart');
  }

  // Check if ticker is connected
  isTickerConnected() {
    return this.httpClient.get<{ isTickerConnected: boolean, timestamp: string }>(this.baseUrl + this.isTickerConnectedUrl);
  }

  // ============= Simulated Trading Endpoints =============

  private tradingBaseUrl = '/api/trading';

  private tradingSummaryCache$: Observable<any> | null = null;
  private tradingSummaryCacheExpiry = 0;
  private readonly TRADING_CACHE_TTL = 30_000;

  // Get today's trading summary
  getTradingSummary() {
    if (this.tradingSummaryCache$ && Date.now() < this.tradingSummaryCacheExpiry) {
      return this.tradingSummaryCache$;
    }
    this.tradingSummaryCacheExpiry = Date.now() + this.TRADING_CACHE_TTL;
    this.tradingSummaryCache$ = this.httpClient.get<any>(this.baseUrl + this.tradingBaseUrl + '/summary/today').pipe(shareReplay(1));
    return this.tradingSummaryCache$;
  }

  // Get trading statistics
  getTradingStatistics() {
    return this.httpClient.get<any>(this.baseUrl + this.tradingBaseUrl + '/statistics');
  }

  // Get open trades
  getOpenTrades() {
    return this.httpClient.get<any[]>(this.baseUrl + this.tradingBaseUrl + '/open');
  }

  // Get today's trades
  getTodaysTrades() {
    return this.httpClient.get<any[]>(this.baseUrl + this.tradingBaseUrl + '/today');
  }

  // Discard (delete) selected trades from today's trades and ledger
  discardTrades(tradeIds: string[]) {
    // Backend should accept array of trade ids and remove them from ledger and recalculate P&L
    return this.httpClient.post<any>(this.baseUrl + this.tradingBaseUrl + '/discard', tradeIds);
  }

  // Get trading configuration
  getTradingConfig() {
    return this.httpClient.get<any>(this.baseUrl + this.tradingBaseUrl + '/config');
  }

  // Update trading configuration
  updateTradingConfig(config: any) {
    return this.httpClient.post<any>(this.baseUrl + this.tradingBaseUrl + '/config', config);
  }

  // Enable auto-trading
  enableAutoTrading() {
    return this.httpClient.post<any>(this.baseUrl + this.tradingBaseUrl + '/auto-trading/enable', {});
  }

  // Disable auto-trading
  disableAutoTrading() {
    return this.httpClient.post<any>(this.baseUrl + this.tradingBaseUrl + '/auto-trading/disable', {});
  }

  // Get auto-trading status
  getAutoTradingStatus() {
    return this.httpClient.get<{ autoTradingEnabled: boolean }>(this.baseUrl + this.tradingBaseUrl + '/auto-trading/status');
  }

  // Check for trade signals
  checkTradeSignals() {
    return this.httpClient.get<any>(this.baseUrl + this.tradingBaseUrl + '/signals/check');
  }

  // Trigger auto trade
  triggerAutoTrade() {
    return this.httpClient.post<any>(this.baseUrl + this.tradingBaseUrl + '/auto-trade', {});
  }

  // Manual buy trade
  manualBuyTrade(source?: string) {
    const params = source ? `?source=${source}` : '';
    return this.httpClient.post<any>(this.baseUrl + this.tradingBaseUrl + '/manual/buy' + params, {});
  }

  // Manual sell trade
  manualSellTrade(source?: string) {
    const params = source ? `?source=${source}` : '';
    return this.httpClient.post<any>(this.baseUrl + this.tradingBaseUrl + '/manual/sell' + params, {});
  }

  // Place trade based on liquidity analysis
  placeLiquidityTrade(tradeSignal: string, timeframe: string, instrumentToken?: number) {
    const params = new URLSearchParams();
    params.append('source', `LIQUIDITY_${timeframe.toUpperCase()}`);
    if (tradeSignal) params.append('tradeSignal', tradeSignal);
    if (instrumentToken) params.append('instrumentToken', instrumentToken.toString());

    const endpoint = tradeSignal === 'SHORT_COVER' ? '/manual/buy' : '/manual/sell';
    return this.httpClient.post<any>(this.baseUrl + this.tradingBaseUrl + endpoint + '?' + params.toString(), {});
  }

  // Monitor trades (check for exits)
  monitorTrades() {
    return this.httpClient.post<any>(this.baseUrl + this.tradingBaseUrl + '/monitor', {});
  }

  // Exit a specific trade
  exitTrade(tradeId: string, exitReason: string, exitPrice: number) {
    return this.httpClient.post<any>(
      this.baseUrl + this.tradingBaseUrl + `/exit/${tradeId}?exitReason=${exitReason}&exitPrice=${exitPrice}`,
      {}
    );
  }

  // Exit all open trades
  exitAllTrades(exitReason: string = 'MANUAL') {
    return this.httpClient.post<any>(
      this.baseUrl + this.tradingBaseUrl + `/exit-all?exitReason=${exitReason}`,
      {}
    );
  }

  // Get today's ledger
  getTodaysLedger() {
    return this.httpClient.get<any>(this.baseUrl + this.tradingBaseUrl + '/ledger/today');
  }

  // Update ledger
  updateLedger() {
    return this.httpClient.post<any>(this.baseUrl + this.tradingBaseUrl + '/ledger/update', {});
  }

  // ============= Performance Statistics API Methods =============

  // Get performance by period (daily, weekly, monthly)
  getPerformanceByPeriod(period: string) {
    return this.httpClient.get<any>(this.baseUrl + this.tradingBaseUrl + `/performance/${period}`);
  }

  // Get performance chart data
  getPerformanceChartData() {
    return this.httpClient.get<any>(this.baseUrl + this.tradingBaseUrl + '/performance/chart-data');
  }


  // ============= Liquidity Sweep API Methods =============

  /**
   * Get latest liquidity sweep analysis
   */
  getLatestLiquiditySweep(appJobConfigNum: number) {
    return this.httpClient.get<any>(this.baseUrl + this.liquiditySweepLatestUrl + `/${appJobConfigNum}`);
  }

  /**
   * Get latest valid trade setup from liquidity sweep
   */
  getLatestLiquiditySweepSetup(appJobConfigNum: number) {
    return this.httpClient.get<any>(this.baseUrl + this.liquiditySweepLatestSetupUrl + `/${appJobConfigNum}`);
  }

  /**
   * Get today's liquidity sweep analyses
   */
  getTodayLiquiditySweepAnalyses(appJobConfigNum: number) {
    return this.httpClient.get<any[]>(this.baseUrl + this.liquiditySweepTodayUrl + `/${appJobConfigNum}`);
  }

  /**
   * Run a fresh liquidity sweep analysis
   */
  runLiquiditySweepAnalysis(appJobConfigNum: number) {
    return this.httpClient.post<any>(this.baseUrl + this.liquiditySweepAnalyzeUrl + `/${appJobConfigNum}`, {});
  }

  /**
   * Check for liquidity sweep signal
   */
  checkLiquiditySweepSignal(appJobConfigNum: number) {
    return this.httpClient.get<any>(this.baseUrl + this.liquiditySweepSignalUrl + `/${appJobConfigNum}`);
  }

  /**
   * Get trade recommendation based on liquidity sweep
   */
  getLiquiditySweepRecommendation(appJobConfigNum: number) {
    return this.httpClient.get<any>(this.baseUrl + this.liquiditySweepRecommendationUrl + `/${appJobConfigNum}`);
  }

  /**
   * Get liquidity levels (BSL/SSL) for charting
   */
  getLiquidityLevels(appJobConfigNum: number) {
    return this.httpClient.get<any>(this.baseUrl + this.liquiditySweepLevelsUrl + `/${appJobConfigNum}`);
  }

  /**
   * Get whale activity indicators
   */
  getWhaleActivity(appJobConfigNum: number) {
    return this.httpClient.get<any>(this.baseUrl + this.liquiditySweepWhaleActivityUrl + `/${appJobConfigNum}`);
  }

  /**
   * Get comprehensive liquidity sweep dashboard data
   */
  getLiquiditySweepDashboard(appJobConfigNum: number) {
    return this.httpClient.get<any>(this.baseUrl + this.liquiditySweepDashboardUrl + `/${appJobConfigNum}`);
  }

  /**
   * Get liquidity sweep configuration
   */
  getLiquiditySweepConfig() {
    return this.httpClient.get<any>(this.baseUrl + this.liquiditySweepConfigUrl);
  }

  // ==================== Liquidity Zone Analysis Methods ====================

  /**
   * Get liquidity zone dashboard data for all indices and timeframes
   */
  getLiquidityZoneDashboard(): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.liquidityZoneDashboardUrl);
  }

  /**
   * Analyze liquidity zones for specific instrument and timeframe
   */
  analyzeLiquidityZones(instrumentToken: number, timeframe: string): Observable<any> {
    const params = new HttpParams()
      .set('instrumentToken', instrumentToken.toString())
      .set('timeframe', timeframe);
    return this.httpClient.post<any>(this.baseUrl + this.liquidityZoneAnalyzeUrl, null, { params });
  }

  /**
   * Analyze all indices (NIFTY) across all timeframes
   */
  analyzeLiquidityAllIndices(): Observable<any> {
    return this.httpClient.post<any>(this.baseUrl + this.liquidityZoneAnalyzeAllUrl, null);
  }

  /**
   * Trigger on-demand liquidity analysis (useful during non-market hours)
   */
  triggerLiquidityAnalysis(): Observable<any> {
    return this.httpClient.post<any>(this.baseUrl + this.liquidityZoneTriggerAnalysisUrl, null);
  }

  /**
   * Get latest analysis for specific instrument and timeframe
   */
  getLatestLiquidityAnalysis(instrumentToken: number, timeframe: string): Observable<any> {
    const params = new HttpParams()
      .set('instrumentToken', instrumentToken.toString())
      .set('timeframe', timeframe);
    return this.httpClient.get<any>(this.baseUrl + this.liquidityZoneLatestUrl, { params });
  }

  /**
   * Get multi-timeframe analysis for single instrument
   */
  getMultiTimeframeLiquidityAnalysis(instrumentToken: number): Observable<any> {
    const params = new HttpParams().set('instrumentToken', instrumentToken.toString());
    return this.httpClient.get<any>(this.baseUrl + this.liquidityZoneMultiTimeframeUrl, { params });
  }

  /**
   * Get today's analyses for an instrument
   */
  getTodaysLiquidityAnalyses(instrumentToken: number): Observable<any> {
    const params = new HttpParams().set('instrumentToken', instrumentToken.toString());
    return this.httpClient.get<any>(this.baseUrl + this.liquidityZoneTodayUrl, { params });
  }

  /**
   * Get active trade setups for all indices
   */
  getActiveLiquiditySetups(): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.liquidityZoneActiveSetupsUrl);
  }

  /**
   * Get chart data for visualization
   */
  getLiquidityChartData(instrumentToken: number, timeframe: string): Observable<any> {
    const params = new HttpParams()
      .set('instrumentToken', instrumentToken.toString())
      .set('timeframe', timeframe);
    return this.httpClient.get<any>(this.baseUrl + this.liquidityZoneChartDataUrl, { params });
  }

  /**
   * Get comprehensive data for NIFTY
   */
  getAllIndicesLiquidityData(): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.liquidityZoneAllIndicesUrl);
  }

  // ==================== Internal Order Block (IOB) Methods ====================

  /**
   * Scan for IOBs for a specific instrument
   */
  scanForIOBs(instrumentToken: number, timeframe: string = '5min'): Observable<any> {
    const params = new HttpParams().set('timeframe', timeframe);
    return this.httpClient.post<any>(this.baseUrl + this.iobScanUrl + '/' + instrumentToken, {}, { params });
  }

  /**
   * Scan all indices for IOBs
   */
  scanAllIndicesForIOBs(): Observable<any> {
    return this.httpClient.post<any>(this.baseUrl + this.iobScanAllUrl, {});
  }

  /**
   * Get fresh IOBs for an instrument
   */
  getFreshIOBs(instrumentToken: number): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.iobFreshUrl + '/' + instrumentToken);
  }

  /**
   * Get bullish IOBs
   */
  getBullishIOBs(instrumentToken: number): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.iobBullishUrl + '/' + instrumentToken);
  }

  /**
   * Get bearish IOBs
   */
  getBearishIOBs(instrumentToken: number): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.iobBearishUrl + '/' + instrumentToken);
  }

  /**
   * Get today's IOBs
   */
  getTodaysIOBs(instrumentToken: number): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.iobTodayUrl + '/' + instrumentToken);
  }

  /**
   * Get tradable IOBs
   */
  getTradableIOBs(instrumentToken: number): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.iobTradableUrl + '/' + instrumentToken);
  }

  /**
   * Get IOB dashboard
   */
  getIOBDashboard(): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.iobDashboardUrl);
  }

  /**
   * Get detailed IOB analysis for an instrument
   */
  getIOBDetailedAnalysis(instrumentToken: number): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.iobAnalysisUrl + '/' + instrumentToken);
  }

  private iobCache$: Observable<any> | null = null;
  private iobCacheExpiry = 0;
  private readonly IOB_CACHE_TTL = 30_000;

  /**
   * Get IOB data for all indices
   */
  getAllIndicesIOBData(): Observable<any> {
    if (this.iobCache$ && Date.now() < this.iobCacheExpiry) {
      return this.iobCache$;
    }
    this.iobCacheExpiry = Date.now() + this.IOB_CACHE_TTL;
    this.iobCache$ = this.httpClient.get<any>(this.baseUrl + this.iobAllIndicesUrl).pipe(shareReplay(1));
    return this.iobCache$;
  }

  /**
   * Get trade setup from IOB
   */
  getIOBTradeSetup(iobId: number): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.iobTradeSetupUrl + '/' + iobId);
  }

  /**
   * Execute trade based on IOB
   */
  executeIOBTrade(iobId: number): Observable<any> {
    return this.httpClient.post<any>(this.baseUrl + this.iobExecuteTradeUrl + '/' + iobId, {});
  }

  /**
   * Get IOB statistics
   */
  getIOBStatistics(instrumentToken: number): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.iobStatisticsUrl + '/' + instrumentToken);
  }

  /**
   * Check IOB mitigation
   */
  checkIOBMitigation(instrumentToken: number, currentPrice: number): Observable<any> {
    const params = new HttpParams()
      .set('instrumentToken', instrumentToken.toString())
      .set('currentPrice', currentPrice.toString());
    return this.httpClient.post<any>(this.baseUrl + this.iobCheckMitigationUrl, {}, { params });
  }

  /**
   * Mark IOB as mitigated
   */
  markIOBAsMitigated(iobId: number): Observable<any> {
    return this.httpClient.post<any>(this.baseUrl + this.iobMitigateUrl + '/' + iobId, {});
  }

  /**
   * Mark all fresh IOBs as mitigated for a given instrument
   */
  mitigateAllFreshIOBs(instrumentToken: number): Observable<any> {
    return this.httpClient.post<any>(this.baseUrl + this.iobMitigateUrl + '-all/' + instrumentToken, {});
  }

  /**
   * Mark multiple IOBs as completed (manually)
   */
  markIOBsAsCompleted(iobIds: number[]): Observable<any> {
    return this.httpClient.post<any>(this.baseUrl + this.iobBaseUrl + '/mark-completed', { iobIds });
  }

  // ==================== IOB Auto Trade API Methods ====================

  /**
   * Get auto-trade status and configuration
   */
  getIOBAutoTradeStatus(): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.iobAutoTradeStatusUrl);
  }

  /**
   * Enable IOB auto-trading
   */
  enableIOBAutoTrade(): Observable<any> {
    return this.httpClient.post<any>(this.baseUrl + this.iobAutoTradeEnableUrl, {});
  }

  /**
   * Disable IOB auto-trading
   */
  disableIOBAutoTrade(): Observable<any> {
    return this.httpClient.post<any>(this.baseUrl + this.iobAutoTradeDisableUrl, {});
  }

  /**
   * Update IOB auto-trade configuration
   */
  updateIOBAutoTradeConfig(config: any): Observable<any> {
    return this.httpClient.post<any>(this.baseUrl + this.iobAutoTradeConfigUrl, config);
  }

  /**
   * Manually enter a trade based on IOB
   */
  enterIOBTrade(iobId: number): Observable<any> {
    return this.httpClient.post<any>(this.baseUrl + this.iobAutoTradeEnterUrl + '/' + iobId, {});
  }

  /**
   * Exit a specific IOB trade
   */
  exitIOBTrade(tradeId: number, reason: string, exitPrice: number): Observable<any> {
    const params = new HttpParams()
      .set('reason', reason)
      .set('exitPrice', exitPrice.toString());
    return this.httpClient.post<any>(this.baseUrl + this.iobAutoTradeExitUrl + '/' + tradeId, {}, { params });
  }

  /**
   * Exit all open IOB trades
   */
  exitAllIOBTrades(reason: string = 'MANUAL_EXIT'): Observable<any> {
    const params = new HttpParams().set('reason', reason);
    return this.httpClient.post<any>(this.baseUrl + this.iobAutoTradeExitAllUrl, {}, { params });
  }

  /**
   * Get all open IOB trades
   */
  getOpenIOBTrades(): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.iobAutoTradeOpenUrl);
  }

  /**
   * Get today's IOB trades
   */
  getTodaysIOBTrades(): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.iobAutoTradeTodayUrl);
  }

  /**
   * Get IOB trade history
   */
  getIOBTradeHistory(startDate: string, endDate: string): Observable<any> {
    const params = new HttpParams()
      .set('startDate', startDate)
      .set('endDate', endDate);
    return this.httpClient.get<any>(this.baseUrl + this.iobAutoTradeHistoryUrl, { params });
  }

  /**
   * Get IOB trading performance statistics
   */
  getIOBPerformance(): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.iobAutoTradePerformanceUrl);
  }

  /**
   * Get IOB risk metrics
   */
  getIOBRiskMetrics(): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.iobAutoTradeRiskUrl);
  }

  /**
   * Run IOB backtest
   */
  runIOBBacktest(instrumentToken: number, startDate: string, endDate: string, timeframe: string = '5min'): Observable<any> {
    const params = new HttpParams()
      .set('instrumentToken', instrumentToken.toString())
      .set('startDate', startDate)
      .set('endDate', endDate)
      .set('timeframe', timeframe);
    return this.httpClient.post<any>(this.baseUrl + this.iobAutoTradeBacktestUrl, {}, { params });
  }

  /**
   * Get backtest results
   */
  getIOBBacktestResults(instrumentToken: number): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.iobAutoTradeBacktestUrl + '/results/' + instrumentToken);
  }

  /**
   * Clear backtest results
   */
  clearIOBBacktestResults(instrumentToken: number): Observable<any> {
    return this.httpClient.delete<any>(this.baseUrl + this.iobAutoTradeBacktestUrl + '/results/' + instrumentToken);
  }

  /**
   * Get Multi-Timeframe analysis for an instrument
   */
  getIOBMTFAnalysis(instrumentToken: number): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.iobAutoTradeMTFUrl + '/' + instrumentToken);
  }

  /**
   * Get MTF analysis for all indices
   */
  getIOBMTFAnalysisAll(): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.iobAutoTradeMTFUrl + '/all');
  }

  /**
   * Get HTF-aligned IOBs
   */
  getHTFAlignedIOBs(instrumentToken: number): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.iobAutoTradeHTFAlignedUrl + '/' + instrumentToken);
  }

  // ==================== Brahmastra Strategy API ====================

  private brahmastraBaseUrl = '/api/brahmastra';

  /**
   * Generate Brahmastra trading signals
   */
  generateBrahmastraSignals(request: any): Observable<any> {
    return this.httpClient.post<any>(this.baseUrl + this.brahmastraBaseUrl + '/signals/generate', request);
  }

  /**
   * Run Brahmastra backtest
   */
  runBrahmastraBacktest(request: any): Observable<any> {
    return this.httpClient.post<any>(this.baseUrl + this.brahmastraBaseUrl + '/backtest/run', request);
  }

  /**
   * Scan for live Brahmastra signals
   */
  scanBrahmastraLive(symbols: string = 'NIFTY'): Observable<any> {
    const params = new HttpParams().set('symbols', symbols);
    return this.httpClient.get<any>(this.baseUrl + this.brahmastraBaseUrl + '/scan/live', { params });
  }

  /**
   * Scan single symbol for Brahmastra signal
   */
  scanBrahmastraSymbol(symbol: string, timeframe: string = '5m'): Observable<any> {
    const params = new HttpParams().set('timeframe', timeframe);
    return this.httpClient.get<any>(this.baseUrl + this.brahmastraBaseUrl + '/scan/' + symbol, { params });
  }

  /**
   * Get Brahmastra dashboard summary
   */
  getBrahmastraDashboard(): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.brahmastraBaseUrl + '/dashboard/summary');
  }

  /**
   * Get Brahmastra symbol summary
   */
  getBrahmastraSymbolSummary(symbol: string): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.brahmastraBaseUrl + '/dashboard/symbol/' + symbol);
  }

  /**
   * Get active Brahmastra signals
   */
  getActiveBrahmastraSignals(): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.brahmastraBaseUrl + '/signals/active');
  }

  /**
   * Get Brahmastra signal history
   */
  getBrahmastraSignalHistory(symbol: string, fromDate: string, toDate: string): Observable<any> {
    const params = new HttpParams()
      .set('symbol', symbol)
      .set('fromDate', fromDate)
      .set('toDate', toDate);
    return this.httpClient.get<any>(this.baseUrl + this.brahmastraBaseUrl + '/signals/history', { params });
  }

  /**
   * Get Brahmastra signal by ID
   */
  getBrahmastraSignalById(id: number): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.brahmastraBaseUrl + '/signals/' + id);
  }

  /**
   * Update Brahmastra signal status
   */
  updateBrahmastraSignalStatus(id: number, status: string, exitPrice?: number, exitReason?: string): Observable<any> {
    let params = new HttpParams().set('status', status);
    if (exitPrice) params = params.set('exitPrice', exitPrice.toString());
    if (exitReason) params = params.set('exitReason', exitReason);
    return this.httpClient.put<any>(this.baseUrl + this.brahmastraBaseUrl + '/signals/' + id + '/status', null, { params });
  }

  /**
   * Get current PCR values
   */
  getBrahmastraPCR(): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.brahmastraBaseUrl + '/pcr/current');
  }

  /**
   * Get indicator metrics for a specific symbol
   */
  getBrahmastraIndicatorMetrics(symbol: string, timeframe: string = '5m', historyBars: number = 50): Observable<any> {
    const params = new HttpParams()
      .set('timeframe', timeframe)
      .set('historyBars', historyBars.toString());
    return this.httpClient.get<any>(this.baseUrl + this.brahmastraBaseUrl + '/indicators/' + symbol, { params });
  }

  /**
   * Get indicator metrics for all tracked symbols
   */
  getAllBrahmastraIndicatorMetrics(timeframe: string = '5m', historyBars: number = 50): Observable<any> {
    const params = new HttpParams()
      .set('timeframe', timeframe)
      .set('historyBars', historyBars.toString());
    return this.httpClient.get<any>(this.baseUrl + this.brahmastraBaseUrl + '/indicators/all', { params });
  }

  /**
   * Check Brahmastra service health
   */
  getBrahmastraHealth(): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.brahmastraBaseUrl + '/health');
  }

  // ==================== Brahmastra Option Chain Integration ====================

  /**
   * Get option chain metrics (Max Pain, OI Analysis) for a symbol
   */
  getBrahmastraOptionChainMetrics(symbol: string): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.brahmastraBaseUrl + '/option-chain/' + symbol);
  }

  /**
   * Get option chain metrics for all tracked symbols
   */
  getAllBrahmastraOptionChainMetrics(): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.brahmastraBaseUrl + '/option-chain/all');
  }

  /**
   * Check if option chain data confirms a trading signal
   */
  checkBrahmastraOptionChainConfirmation(symbol: string, signalType: string): Observable<any> {
    return this.httpClient.get<any>(this.baseUrl + this.brahmastraBaseUrl + '/option-chain/' + symbol + '/confirm/' + signalType);
  }

}


