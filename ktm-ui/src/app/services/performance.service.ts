import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';
import {
  PerformanceDashboard,
  PerformanceMetrics,
  TradeResult,
  BacktestResult,
  EquityCurvePoint,
  AutoTradingConfig,
  AutoTradingStats,
  AutoTradeOrder,
  OpenPosition,
  CompleteChartData,
  ChartCandle,
  IOBZone,
  SwingPoint,
  ChartLevel
} from '../models/performance.model';

@Injectable({
  providedIn: 'root'
})
export class PerformanceService {
  private baseUrl = '/api';

  constructor(private http: HttpClient) {}

  // ==================== Performance Dashboard ====================

  getPerformanceDashboard(): Observable<PerformanceDashboard> {
    return this.http.get<PerformanceDashboard>(`${this.baseUrl}/performance/dashboard`);
  }

  getAllTimeMetrics(): Observable<PerformanceMetrics> {
    return this.http.get<PerformanceMetrics>(`${this.baseUrl}/performance/metrics/all-time`);
  }

  getDailyMetrics(days: number = 30): Observable<PerformanceMetrics[]> {
    return this.http.get<PerformanceMetrics[]>(
      `${this.baseUrl}/performance/metrics/daily?days=${days}`
    );
  }

  calculateDailyMetrics(date: string): Observable<PerformanceMetrics> {
    return this.http.post<PerformanceMetrics>(
      `${this.baseUrl}/performance/metrics/calculate-daily?date=${date}`,
      {}
    );
  }

  recalculateMetrics(): Observable<any> {
    return this.http.post(`${this.baseUrl}/performance/metrics/recalculate`, {});
  }

  // ==================== Trade Results ====================

  getOpenTrades(): Observable<TradeResult[]> {
    return this.http.get<TradeResult[]>(`${this.baseUrl}/performance/trades/open`);
  }

  getRecentTrades(limit: number = 20): Observable<TradeResult[]> {
    return this.http.get<TradeResult[]>(
      `${this.baseUrl}/performance/trades/recent?limit=${limit}`
    );
  }

  getTradesInRange(startDate: string, endDate: string): Observable<TradeResult[]> {
    return this.http.get<TradeResult[]>(
      `${this.baseUrl}/performance/trades/range?startDate=${startDate}&endDate=${endDate}`
    );
  }

  getTradeByIOB(iobId: number): Observable<TradeResult> {
    return this.http.get<TradeResult>(`${this.baseUrl}/performance/trades/by-iob/${iobId}`);
  }

  closeTrade(tradeId: number, exitPrice: number, exitReason: string = 'MANUAL'): Observable<TradeResult> {
    return this.http.post<TradeResult>(
      `${this.baseUrl}/performance/trades/${tradeId}/close?exitPrice=${exitPrice}&exitReason=${exitReason}`,
      {}
    );
  }

  // ==================== Analysis ====================

  getEquityCurve(startDate: string, endDate: string): Observable<EquityCurvePoint[]> {
    return this.http.get<EquityCurvePoint[]>(
      `${this.baseUrl}/performance/analysis/equity-curve?startDate=${startDate}&endDate=${endDate}`
    );
  }

  getWinLossDistribution(): Observable<any> {
    return this.http.get(`${this.baseUrl}/performance/analysis/win-loss-distribution`);
  }

  getPerformanceByIOBType(): Observable<any> {
    return this.http.get(`${this.baseUrl}/performance/analysis/by-iob-type`);
  }

  getPerformanceByConfidence(): Observable<any> {
    return this.http.get(`${this.baseUrl}/performance/analysis/by-confidence`);
  }

  getPerformanceByTimeOfDay(): Observable<any> {
    return this.http.get(`${this.baseUrl}/performance/analysis/by-time-of-day`);
  }

  getDrawdownAnalysis(): Observable<any> {
    return this.http.get(`${this.baseUrl}/performance/analysis/drawdown`);
  }

  // ==================== Backtesting ====================

  runBacktest(
    instrumentToken: number,
    timeframe: string,
    startDate: string,
    endDate: string,
    parameters?: any
  ): Observable<BacktestResult> {
    const params = new HttpParams()
      .set('instrumentToken', instrumentToken.toString())
      .set('timeframe', timeframe)
      .set('startDate', startDate)
      .set('endDate', endDate);

    return this.http.post<BacktestResult>(
      `${this.baseUrl}/performance/backtest/run`,
      parameters || {},
      { params }
    );
  }

  runBacktestAll(startDate: string, endDate: string, parameters?: any): Observable<any> {
    const params = new HttpParams()
      .set('startDate', startDate)
      .set('endDate', endDate);

    return this.http.post(
      `${this.baseUrl}/performance/backtest/run-all`,
      parameters || {},
      { params }
    );
  }

  getBacktestResults(backtestId: string): Observable<BacktestResult> {
    return this.http.get<BacktestResult>(`${this.baseUrl}/performance/backtest/${backtestId}`);
  }

  getBacktestHistory(): Observable<any[]> {
    return this.http.get<any[]>(`${this.baseUrl}/performance/backtest/history`);
  }

  compareBacktests(backtestId1: string, backtestId2: string): Observable<any> {
    return this.http.get(
      `${this.baseUrl}/performance/backtest/compare?backtestId1=${backtestId1}&backtestId2=${backtestId2}`
    );
  }

  optimizeParameters(instrumentToken: number, startDate: string, endDate: string): Observable<any> {
    const params = new HttpParams()
      .set('instrumentToken', instrumentToken.toString())
      .set('startDate', startDate)
      .set('endDate', endDate);

    return this.http.post(`${this.baseUrl}/performance/backtest/optimize`, {}, { params });
  }

  // ==================== Auto Trading ====================

  getAutoTradingConfig(): Observable<AutoTradingConfig> {
    return this.http.get<AutoTradingConfig>(`${this.baseUrl}/auto-trade/config`);
  }

  updateAutoTradingConfig(config: Partial<AutoTradingConfig>): Observable<any> {
    return this.http.put(`${this.baseUrl}/auto-trade/config`, config);
  }

  enableAutoTrading(): Observable<any> {
    return this.http.post(`${this.baseUrl}/auto-trade/enable`, {});
  }

  disableAutoTrading(): Observable<any> {
    return this.http.post(`${this.baseUrl}/auto-trade/disable`, {});
  }

  getAutoTradingStatus(): Observable<AutoTradingStats> {
    return this.http.get<AutoTradingStats>(`${this.baseUrl}/auto-trade/status`);
  }

  checkEntryConditions(iobId: number, currentPrice: number): Observable<any> {
    return this.http.get(
      `${this.baseUrl}/auto-trade/check-entry/${iobId}?currentPrice=${currentPrice}`
    );
  }

  getReadyIOBs(instrumentToken: number, currentPrice: number): Observable<any> {
    return this.http.get(
      `${this.baseUrl}/auto-trade/ready-for-entry/${instrumentToken}?currentPrice=${currentPrice}`
    );
  }

  placeAutoOrder(iobId: number, entryPrice?: number, quantity?: number): Observable<any> {
    let url = `${this.baseUrl}/auto-trade/place-order/${iobId}`;
    const params: string[] = [];
    if (entryPrice) params.push(`entryPrice=${entryPrice}`);
    if (quantity) params.push(`quantity=${quantity}`);
    if (params.length > 0) url += '?' + params.join('&');
    return this.http.post(url, {});
  }

  cancelAutoOrder(orderId: string): Observable<any> {
    return this.http.post(`${this.baseUrl}/auto-trade/cancel-order/${orderId}`, {});
  }

  getOrderStatus(orderId: string): Observable<AutoTradeOrder> {
    return this.http.get<AutoTradeOrder>(`${this.baseUrl}/auto-trade/order/${orderId}`);
  }

  getPendingOrders(): Observable<AutoTradeOrder[]> {
    return this.http.get<AutoTradeOrder[]>(`${this.baseUrl}/auto-trade/pending-orders`);
  }

  getOpenPositions(): Observable<OpenPosition[]> {
    return this.http.get<OpenPosition[]>(`${this.baseUrl}/auto-trade/positions`);
  }

  updateTrailingStop(positionId: string, newStopLoss: number): Observable<any> {
    return this.http.post(
      `${this.baseUrl}/auto-trade/positions/${positionId}/trailing-stop?newStopLoss=${newStopLoss}`,
      {}
    );
  }

  bookPartialProfits(positionId: string, percentage: number, price: number): Observable<any> {
    return this.http.post(
      `${this.baseUrl}/auto-trade/positions/${positionId}/partial-exit?percentage=${percentage}&price=${price}`,
      {}
    );
  }

  closePosition(positionId: string, exitPrice: number, reason: string = 'MANUAL'): Observable<any> {
    return this.http.post(
      `${this.baseUrl}/auto-trade/positions/${positionId}/close?exitPrice=${exitPrice}&reason=${reason}`,
      {}
    );
  }

  closeAllPositions(instrumentToken?: number): Observable<any> {
    let url = `${this.baseUrl}/auto-trade/positions/close-all`;
    if (instrumentToken) url += `?instrumentToken=${instrumentToken}`;
    return this.http.post(url, {});
  }

  getTodaysAutoTrades(): Observable<AutoTradeOrder[]> {
    return this.http.get<AutoTradeOrder[]>(`${this.baseUrl}/auto-trade/today-trades`);
  }

  getActivityLog(limit: number = 50): Observable<any[]> {
    return this.http.get<any[]>(`${this.baseUrl}/auto-trade/activity-log?limit=${limit}`);
  }

  // ==================== Chart Data ====================

  getOHLCData(
    instrumentToken: number,
    interval: string = '5minute',
    fromDate?: string,
    toDate?: string
  ): Observable<any> {
    let params = new HttpParams().set('interval', interval);
    if (fromDate) params = params.set('fromDate', fromDate);
    if (toDate) params = params.set('toDate', toDate);

    return this.http.get(`${this.baseUrl}/chart/ohlc/${instrumentToken}`, { params });
  }

  getIOBZones(instrumentToken: number, includeExpired: boolean = false): Observable<any> {
    return this.http.get(
      `${this.baseUrl}/chart/iob-zones/${instrumentToken}?includeExpired=${includeExpired}`
    );
  }

  getSwingPoints(instrumentToken: number, interval: string = '5minute'): Observable<any> {
    return this.http.get(
      `${this.baseUrl}/chart/swing-points/${instrumentToken}?interval=${interval}`
    );
  }

  getCompleteChartData(
    instrumentToken: number,
    interval: string = '5minute',
    fromDate?: string,
    toDate?: string
  ): Observable<CompleteChartData> {
    let params = new HttpParams().set('interval', interval);
    if (fromDate) params = params.set('fromDate', fromDate);
    if (toDate) params = params.set('toDate', toDate);

    return this.http.get<CompleteChartData>(
      `${this.baseUrl}/chart/complete/${instrumentToken}`,
      { params }
    );
  }

  getTradeLevels(iobId: number): Observable<{ levels: ChartLevel[] }> {
    return this.http.get<{ levels: ChartLevel[] }>(
      `${this.baseUrl}/chart/trade-levels/${iobId}`
    );
  }

  getRealtimePrice(instrumentToken: number): Observable<any> {
    return this.http.get(`${this.baseUrl}/chart/realtime-price/${instrumentToken}`);
  }
}
