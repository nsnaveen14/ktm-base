import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable, Subject } from 'rxjs';
import {
  SignalRequest,
  BacktestRequest,
  SignalResponse,
  BacktestResponse,
  LiveScanResponse,
  DashboardResponse,
  PCRResponse,
  SignalDTO,
  LiveScanResult,
  IndicatorMetricsResponse,
  AllIndicatorMetricsResponse,
  OptionChainMetricsResponse,
  AllOptionChainMetricsResponse,
  OptionChainConfirmationResponse
} from '../models/brahmastra.model';
import { DataService } from './data.service';
import { WebsocketService } from './web-socket.service';

@Injectable({
  providedIn: 'root'
})
export class BrahmastraService {

  // Subject for WebSocket signals
  private signalSubject = new Subject<LiveScanResult>();
  public signalUpdates$ = this.signalSubject.asObservable();

  constructor(
    private dataService: DataService,
    private webSocketService: WebsocketService
  ) {
    // Subscribe to WebSocket Brahmastra signals
    this.webSocketService.getBrahmastraSignals().subscribe({
      next: (signal) => {
        this.signalSubject.next(signal);
      },
      error: (err) => console.error('Error receiving Brahmastra signal:', err)
    });
  }

  // ==================== Signal Generation ====================

  /**
   * Generate trading signals for a given symbol and timeframe.
   */
  generateSignals(request: SignalRequest): Observable<SignalResponse> {
    return this.dataService.generateBrahmastraSignals(request);
  }

  /**
   * Generate signals asynchronously (for large date ranges).
   */
  generateSignalsAsync(request: SignalRequest): Observable<SignalResponse> {
    return this.dataService.generateBrahmastraSignals(request);
  }

  // ==================== Backtesting ====================

  /**
   * Run backtest with specified parameters.
   */
  runBacktest(request: BacktestRequest): Observable<BacktestResponse> {
    return this.dataService.runBrahmastraBacktest(request);
  }

  /**
   * Run backtest asynchronously (for large date ranges).
   */
  runBacktestAsync(request: BacktestRequest): Observable<BacktestResponse> {
    return this.dataService.runBrahmastraBacktest(request);
  }

  // ==================== Live Scanning ====================

  /**
   * Scan multiple symbols for live signals.
   */
  scanLive(symbols: string = 'NIFTY'): Observable<LiveScanResponse> {
    return this.dataService.scanBrahmastraLive(symbols);
  }

  /**
   * Scan a single symbol.
   */
  scanSymbol(symbol: string, timeframe: string = '5m'): Observable<any> {
    return this.dataService.scanBrahmastraSymbol(symbol, timeframe);
  }

  // ==================== Dashboard ====================

  /**
   * Get aggregated dashboard summary.
   */
  getDashboardSummary(): Observable<DashboardResponse> {
    return this.dataService.getBrahmastraDashboard();
  }

  /**
   * Get symbol-specific summary.
   */
  getSymbolSummary(symbol: string): Observable<any> {
    return this.dataService.getBrahmastraSymbolSummary(symbol);
  }

  // ==================== Signal Management ====================

  /**
   * Get all active signals.
   */
  getActiveSignals(): Observable<any> {
    return this.dataService.getActiveBrahmastraSignals();
  }

  /**
   * Get signal history for a symbol.
   */
  getSignalHistory(symbol: string, fromDate: string, toDate: string): Observable<any> {
    return this.dataService.getBrahmastraSignalHistory(symbol, fromDate, toDate);
  }

  /**
   * Get signal by ID.
   */
  getSignalById(id: number): Observable<any> {
    return this.dataService.getBrahmastraSignalById(id);
  }

  /**
   * Update signal status.
   */
  updateSignalStatus(id: number, status: string, exitPrice?: number, exitReason?: string): Observable<any> {
    return this.dataService.updateBrahmastraSignalStatus(id, status, exitPrice, exitReason);
  }

  // ==================== PCR Integration ====================

  /**
   * Get current PCR values for all symbols.
   */
  getCurrentPCR(): Observable<PCRResponse> {
    return this.dataService.getBrahmastraPCR();
  }

  // ==================== Indicator Metrics ====================

  /**
   * Get real-time indicator metrics for a specific symbol.
   */
  getIndicatorMetrics(symbol: string, timeframe: string = '5m', historyBars: number = 50): Observable<IndicatorMetricsResponse> {
    return this.dataService.getBrahmastraIndicatorMetrics(symbol, timeframe, historyBars);
  }

  /**
   * Get indicator metrics for all tracked symbols.
   */
  getAllIndicatorMetrics(timeframe: string = '5m', historyBars: number = 50): Observable<AllIndicatorMetricsResponse> {
    return this.dataService.getAllBrahmastraIndicatorMetrics(timeframe, historyBars);
  }

  // ==================== Option Chain Integration ====================

  /**
   * Get option chain metrics (Max Pain, OI Analysis) for a symbol.
   */
  getOptionChainMetrics(symbol: string): Observable<OptionChainMetricsResponse> {
    return this.dataService.getBrahmastraOptionChainMetrics(symbol);
  }

  /**
   * Get option chain metrics for all tracked symbols.
   */
  getAllOptionChainMetrics(): Observable<AllOptionChainMetricsResponse> {
    return this.dataService.getAllBrahmastraOptionChainMetrics();
  }

  /**
   * Check if option chain data confirms a signal.
   */
  checkOptionChainConfirmation(symbol: string, signalType: string): Observable<OptionChainConfirmationResponse> {
    return this.dataService.checkBrahmastraOptionChainConfirmation(symbol, signalType);
  }

  // ==================== Health Check ====================

  /**
   * Check if Brahmastra service is running.
   */
  healthCheck(): Observable<any> {
    return this.dataService.getBrahmastraHealth();
  }

  // ==================== WebSocket Handling ====================

  /**
   * Handle incoming WebSocket signal.
   */
  handleWebSocketSignal(signal: LiveScanResult): void {
    this.signalSubject.next(signal);
  }

  // ==================== Utility Methods ====================

  /**
   * Format date for API request.
   */
  formatDate(date: Date): string {
    return date.toISOString().split('T')[0];
  }

  /**
   * Get default signal request.
   */
  getDefaultSignalRequest(): SignalRequest {
    const today = new Date();
    const weekAgo = new Date(today);
    weekAgo.setDate(weekAgo.getDate() - 7);

    return {
      symbol: 'NIFTY',
      timeframe: '5m',
      fromDate: this.formatDate(weekAgo),
      toDate: this.formatDate(today),
      usePCR: true,
      supertrendPeriod: 20,
      supertrendMultiplier: 2.0,
      macdFastPeriod: 12,
      macdSlowPeriod: 26,
      macdSignalPeriod: 9,
      vwapTolerance: 0.002
    };
  }

  /**
   * Get default backtest request.
   */
  getDefaultBacktestRequest(): BacktestRequest {
    const today = new Date();
    const monthAgo = new Date(today);
    monthAgo.setDate(monthAgo.getDate() - 30);

    return {
      symbol: 'NIFTY',
      timeframe: '5m',
      fromDate: this.formatDate(monthAgo),
      toDate: this.formatDate(today),
      usePCR: true,
      initialCapital: 100000,
      riskPerTrade: 1,
      supertrendPeriod: 20,
      supertrendMultiplier: 2.0,
      macdFastPeriod: 12,
      macdSlowPeriod: 26,
      macdSignalPeriod: 9,
      vwapTolerance: 0.002,
      useTrailingStop: false,
      usePartialProfits: true,
      partialProfitPercent: 50
    };
  }
}

