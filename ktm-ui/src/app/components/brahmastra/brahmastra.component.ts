import { Component, OnInit, OnDestroy, ViewChild, ElementRef, AfterViewInit, AfterViewChecked, ChangeDetectorRef, NgZone } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatTabsModule } from '@angular/material/tabs';
import { MatTableModule } from '@angular/material/table';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatDatepickerModule } from '@angular/material/datepicker';
import { MatNativeDateModule } from '@angular/material/core';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatChipsModule } from '@angular/material/chips';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatExpansionModule } from '@angular/material/expansion';
import { MatDividerModule } from '@angular/material/divider';
import { MatBadgeModule } from '@angular/material/badge';
import { MatSliderModule } from '@angular/material/slider';
import { MatSlideToggleModule } from '@angular/material/slide-toggle';
import { Subscription } from 'rxjs';

import { BrahmastraService } from '../../services/brahmastra.service';
import { WebsocketService } from '../../services/web-socket.service';
import { LiveTickComponent } from '../live-tick/live-tick.component';
import {
  SignalRequest,
  BacktestRequest,
  SignalDTO,
  BacktestResult,
  LiveScanResult,
  DashboardSummary,
  SymbolSummary,
  TradeLog,
  IndicatorMetrics
} from '../../models/brahmastra.model';

// Lightweight Charts for all charts
import {
  createChart,
  IChartApi,
  ISeriesApi,
  CandlestickData,
  LineStyle,
  CrosshairMode
} from 'lightweight-charts';


@Component({
  selector: 'app-brahmastra',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    ReactiveFormsModule,
    MatCardModule,
    MatButtonModule,
    MatIconModule,
    MatTabsModule,
    MatTableModule,
    MatFormFieldModule,
    MatInputModule,
    MatSelectModule,
    MatDatepickerModule,
    MatNativeDateModule,
    MatCheckboxModule,
    MatProgressSpinnerModule,
    MatChipsModule,
    MatTooltipModule,
    MatExpansionModule,
    MatDividerModule,
    MatBadgeModule,
    MatSliderModule,
    MatSlideToggleModule,
    LiveTickComponent
  ],
  templateUrl: './brahmastra.component.html',
  styleUrls: ['./brahmastra.component.css']
})
export class BrahmastraComponent implements OnInit, OnDestroy, AfterViewInit, AfterViewChecked {

  @ViewChild('equityChartContainer') equityChartRef!: ElementRef;
  @ViewChild('drawdownChartContainer') drawdownChartRef!: ElementRef;
  @ViewChild('signalChartContainer') signalChartContainer!: ElementRef;
  @ViewChild('priceSupertrendChartContainer') priceSupertrendChartContainer!: ElementRef;
  @ViewChild('macdChartContainer') macdChartContainer!: ElementRef;
  @ViewChild('vwapChartContainer') vwapChartContainer!: ElementRef;

  // Dashboard Data
  dashboardSummary: DashboardSummary | null = null;
  activeSignals: SignalDTO[] = [];
  liveScans: LiveScanResult[] = [];

  // Indicator Metrics
  indicatorMetrics: IndicatorMetrics[] = [];
  selectedSymbolMetrics: IndicatorMetrics | null = null;
  selectedMetricsSymbol: string = 'NIFTY';
  isLoadingMetrics = false;

  // Option Chain Data
  optionChainMetrics: any[] = [];
  selectedOptionChainMetrics: any = null;
  isLoadingOptionChain = false;

  // Dashboard Charts (Lightweight Charts)
  private priceSupertrendChart: IChartApi | null = null;
  private priceSupertrendCandleSeries: ISeriesApi<'Candlestick'> | null = null;
  private priceSupertrendLineSeries: ISeriesApi<'Line'> | null = null;

  private macdChartInstance: IChartApi | null = null;
  private macdLineSeries: ISeriesApi<'Line'> | null = null;
  private macdSignalLineSeries: ISeriesApi<'Line'> | null = null;
  private macdHistogramSeries: ISeriesApi<'Histogram'> | null = null;

  private vwapChartInstance: IChartApi | null = null;
  private vwapPriceSeries: ISeriesApi<'Line'> | null = null;
  private vwapLineSeries: ISeriesApi<'Line'> | null = null;
  private vwapUpperSeries: ISeriesApi<'Line'> | null = null;
  private vwapLowerSeries: ISeriesApi<'Line'> | null = null;

  // Interactive Signal Chart (Lightweight Charts)
  private signalChart: IChartApi | null = null;
  private candleSeries: ISeriesApi<'Candlestick'> | null = null;
  private supertrendLine: ISeriesApi<'Line'> | null = null;
  private vwapLine: ISeriesApi<'Line'> | null = null;
  private signalLevelLines: Map<string, any> = new Map();
  private signalMarkers: any[] = [];
  showSignalAnnotations = true;
  showTradeLevels = true;
  private signalChartInitialized = false;

  // Signal Generation
  signalRequest: SignalRequest;
  generatedSignals: SignalDTO[] = [];
  signalColumns = ['signalTime', 'signalType', 'entryPrice', 'stopLoss', 'target1', 'target2', 'riskRewardRatio', 'confidenceScore', 'status'];

  // Backtest
  backtestRequest: BacktestRequest;
  backtestResult: BacktestResult | null = null;
  tradeLogColumns = ['tradeNumber', 'signalType', 'entryTime', 'entryPrice', 'exitPrice', 'pnl', 'pnlPercent', 'exitReason', 'riskReward'];

  // PCR Data
  pcrData: any = null;

  // Loading States
  isLoadingDashboard = false;
  isLoadingSignals = false;
  isLoadingBacktest = false;
  isScanning = false;

  // Chart instances (Lightweight Charts)
  private equityChartInstance: IChartApi | null = null;
  private equityLineSeries: ISeriesApi<'Line'> | null = null;
  private drawdownChartInstance: IChartApi | null = null;
  private drawdownLineSeries: ISeriesApi<'Line'> | null = null;

  // Chart rendering state flags
  private indicatorChartsRendered = false;
  private indicatorChartsPending = false;
  private signalChartPending = false;
  private resizeObserver: ResizeObserver | null = null;
  private intersectionObserver: IntersectionObserver | null = null;

  // Live chart update
  private autoRefreshInterval: any = null;
  private priceSubscription: Subscription | null = null;
  private lastHistoryData: any[] = []; // cache for incremental updates
  isMarketOpen = false;
  lastRefreshTime: string = '';
  // Symbol options (Brahmastra is NIFTY-only for now)
  symbols = ['NIFTY'];
  timeframes = ['1m', '3m', '5m', '15m', '30m', '1h'];

  // WebSocket subscription
  private wsSubscription: Subscription | null = null;

  constructor(
    private brahmastraService: BrahmastraService,
    private webSocketService: WebsocketService,
    private cdr: ChangeDetectorRef,
    private ngZone: NgZone
  ) {
    this.signalRequest = this.brahmastraService.getDefaultSignalRequest();
    this.backtestRequest = this.brahmastraService.getDefaultBacktestRequest();
  }

  ngOnInit(): void {
    this.loadDashboard();
    this.loadPCRData();
    this.loadIndicatorMetrics();
    this.loadOptionChainMetrics();
    this.subscribeToWebSocket();
    this.subscribeToPriceUpdates();
    this.startAutoRefresh();
    this.checkMarketStatus();
  }

  ngAfterViewInit(): void {
    this.signalChartPending = true;
    setTimeout(() => this.tryInitAllCharts(), 1000);
    setTimeout(() => this.tryInitAllCharts(), 2000);
  }

  ngAfterViewChecked(): void {
    if (this.indicatorChartsPending && !this.indicatorChartsRendered) {
      this.tryRenderIndicatorCharts();
    }
    if (this.signalChartPending && !this.signalChartInitialized) {
      this.tryInitSignalChart();
    }
  }

  private tryInitAllCharts(): void {
    if (this.indicatorChartsPending && !this.indicatorChartsRendered) {
      this.tryRenderIndicatorCharts();
    }
    if (this.signalChartPending && !this.signalChartInitialized) {
      this.tryInitSignalChart();
    }
  }

  private tryRenderIndicatorCharts(): void {
    const container = this.priceSupertrendChartContainer?.nativeElement;
    if (!container) return;
    const rect = container.getBoundingClientRect();
    if (rect.width > 0 && rect.height > 0) {
      console.log('[Brahmastra] Indicator chart containers ready, width:', rect.width);
      this.renderIndicatorCharts();
    }
  }

  private tryInitSignalChart(): void {
    const container = this.signalChartContainer?.nativeElement;
    if (!container) return;
    const rect = container.getBoundingClientRect();
    if (rect.width > 0 && rect.height > 0) {
      console.log('[Brahmastra] Signal chart container ready, width:', rect.width);
      this.initSignalChart();
    }
  }

  ngOnDestroy(): void {
    if (this.wsSubscription) {
      this.wsSubscription.unsubscribe();
    }
    if (this.priceSubscription) {
      this.priceSubscription.unsubscribe();
    }
    if (this.autoRefreshInterval) {
      clearInterval(this.autoRefreshInterval);
    }
    if (this.resizeObserver) {
      this.resizeObserver.disconnect();
    }
    if (this.intersectionObserver) {
      this.intersectionObserver.disconnect();
    }
    // Cleanup signal chart
    this.destroySignalChart();
    if (this.equityChartInstance) {
      this.equityChartInstance.remove();
      this.equityChartInstance = null;
    }
    if (this.drawdownChartInstance) {
      this.drawdownChartInstance.remove();
      this.drawdownChartInstance = null;
    }
    // Destroy dashboard indicator charts
    this.destroyIndicatorCharts();
  }

  destroyIndicatorCharts(): void {
    if (this.priceSupertrendChart) { this.priceSupertrendChart.remove(); this.priceSupertrendChart = null; }
    if (this.macdChartInstance) { this.macdChartInstance.remove(); this.macdChartInstance = null; }
    if (this.vwapChartInstance) { this.vwapChartInstance.remove(); this.vwapChartInstance = null; }
  }

  private destroySignalChart(): void {
    if (this.signalChart) {
      this.signalChart.remove();
      this.signalChart = null;
    }
    this.candleSeries = null;
    this.supertrendLine = null;
    this.vwapLine = null;
    this.signalLevelLines.clear();
    this.signalMarkers = [];
    this.signalChartInitialized = false;
    this.signalChartPending = false;
  }

  // ==================== Dashboard ====================

  loadDashboard(): void {
    this.isLoadingDashboard = true;
    this.brahmastraService.getDashboardSummary().subscribe({
      next: (response) => {
        if (response.success) {
          this.dashboardSummary = response.summary;
          this.activeSignals = response.summary.liveSignals || [];
        }
        this.isLoadingDashboard = false;
      },
      error: (err) => {
        console.error('Error loading dashboard:', err);
        this.isLoadingDashboard = false;
      }
    });
  }

  loadPCRData(): void {
    this.brahmastraService.getCurrentPCR().subscribe({
      next: (response) => {
        if (response.success) {
          this.pcrData = response;
        }
      },
      error: (err) => console.error('Error loading PCR:', err)
    });
  }

  // ==================== Indicator Metrics ====================

  loadIndicatorMetrics(): void {
    // Destroy existing chart instances BEFORE hiding the DOM (isLoadingMetrics=true triggers *ngIf removal)
    this.destroyIndicatorCharts();
    this.destroySignalChart();
    this.isLoadingMetrics = true;
    this.brahmastraService.getAllIndicatorMetrics('5m', 50).subscribe({
      next: (response) => {
        if (response.success) {
          this.indicatorMetrics = response.metrics;
          // Set selected symbol metrics to NIFTY by default
          this.selectedSymbolMetrics = this.indicatorMetrics.find(m => m.symbol === this.selectedMetricsSymbol) || this.indicatorMetrics[0];
          // Mark charts as pending - ngAfterViewChecked will detect when containers are ready
          this.indicatorChartsRendered = false;
          this.indicatorChartsPending = true;
        }
        this.isLoadingMetrics = false;
        this.cdr.detectChanges();
        // Schedule multiple retry attempts to render charts after DOM is ready
        // The *ngIf becomes true after detectChanges, but containers may not have layout yet
        this.scheduleChartRendering();
      },
      error: (err) => {
        console.error('Error loading indicator metrics:', err);
        this.isLoadingMetrics = false;
      }
    });
  }

  private scheduleChartRendering(): void {
    // Try rendering indicator charts at multiple intervals to handle layout timing
    const delays = [100, 300, 500, 1000, 1500, 2000];
    delays.forEach(delay => {
      setTimeout(() => {
        if (!this.indicatorChartsRendered && this.selectedSymbolMetrics) {
          console.log(`[Brahmastra] Attempting indicator chart render at ${delay}ms`);
          this.renderIndicatorCharts();
        }
        if (!this.signalChartInitialized && this.selectedSymbolMetrics) {
          console.log(`[Brahmastra] Attempting signal chart init at ${delay}ms`);
          this.signalChartPending = true;
          this.initSignalChart();
        }
      }, delay);
    });
  }

  // ==================== Live Chart Updates ====================

  /**
   * Check if Indian stock market is currently open (9:15 AM - 3:30 PM IST, Mon-Fri).
   */
  private checkMarketStatus(): void {
    const now = new Date();
    const istOffset = 5.5 * 60 * 60 * 1000;
    const istNow = new Date(now.getTime() + (now.getTimezoneOffset() * 60 * 1000) + istOffset);
    const day = istNow.getDay();
    const hours = istNow.getHours();
    const minutes = istNow.getMinutes();
    const totalMinutes = hours * 60 + minutes;
    const marketOpen = 9 * 60 + 15;
    const marketClose = 15 * 60 + 30;
    this.isMarketOpen = day >= 1 && day <= 5 && totalMinutes >= marketOpen && totalMinutes <= marketClose;
  }

  /**
   * Start periodic auto-refresh of indicator data (every 30s during market hours).
   */
  private startAutoRefresh(): void {
    this.autoRefreshInterval = setInterval(() => {
      this.checkMarketStatus();
      if (this.isMarketOpen) {
        this.refreshIndicatorData();
      }
    }, 30000);
  }

  /**
   * Fetch fresh indicator data and update charts incrementally.
   */
  private refreshIndicatorData(): void {
    this.brahmastraService.getAllIndicatorMetrics('5m', 50).subscribe({
      next: (response) => {
        if (response.success && response.metrics) {
          this.indicatorMetrics = response.metrics;
          const newMetrics = response.metrics.find(m => m.symbol === this.selectedMetricsSymbol) || response.metrics[0];
          if (newMetrics && newMetrics.history && newMetrics.history.length > 0) {
            this.selectedSymbolMetrics = newMetrics;
            this.lastRefreshTime = new Date().toLocaleTimeString('en-IN');
            this.updateChartsIncrementally(newMetrics.history);
          }
        }
      },
      error: (err) => console.error('[Brahmastra] Error refreshing indicator data:', err)
    });
  }

  /**
   * Update existing charts with new data without destroying and recreating.
   */
  private updateChartsIncrementally(history: any[]): void {
    this.lastHistoryData = history;

    if (this.priceSupertrendChart && this.priceSupertrendCandleSeries) {
      const candleData: CandlestickData[] = history.map(h => ({
        time: this.toChartTime(h.timestamp),
        open: h.open || h.close, high: h.high || h.close,
        low: h.low || h.close, close: h.close
      }));
      this.priceSupertrendCandleSeries.setData(candleData);
      if (this.priceSupertrendLineSeries) {
        this.priceSupertrendLineSeries.setData(
          history.filter(h => h.supertrend != null).map(h => ({
            time: this.toChartTime(h.timestamp), value: h.supertrend,
            color: h.supertrendTrend === 'BULLISH' ? '#00ff88' : '#ff4444'
          }))
        );
      }
    } else if (this.priceSupertrendChartContainer?.nativeElement) {
      this.renderPriceSupertrendChart(history);
    }

    if (this.macdChartInstance && this.macdHistogramSeries) {
      this.macdHistogramSeries.setData(
        history.filter(h => h.macdHistogram != null).map(h => ({
          time: this.toChartTime(h.timestamp), value: h.macdHistogram,
          color: h.macdHistogram >= 0 ? 'rgba(0, 255, 136, 0.6)' : 'rgba(255, 68, 68, 0.6)'
        }))
      );
      if (this.macdLineSeries) {
        this.macdLineSeries.setData(history.filter(h => h.macdLine != null).map(h => ({
          time: this.toChartTime(h.timestamp), value: h.macdLine
        })));
      }
      if (this.macdSignalLineSeries) {
        this.macdSignalLineSeries.setData(history.filter(h => h.macdSignal != null).map(h => ({
          time: this.toChartTime(h.timestamp), value: h.macdSignal
        })));
      }
    } else if (this.macdChartContainer?.nativeElement) {
      this.renderMACDChart(history);
    }

    if (this.vwapChartInstance && this.vwapPriceSeries) {
      this.vwapPriceSeries.setData(history.filter(h => h.close != null).map(h => ({
        time: this.toChartTime(h.timestamp), value: h.close
      })));
      if (this.vwapLineSeries) {
        this.vwapLineSeries.setData(history.filter(h => h.vwap != null).map(h => ({
          time: this.toChartTime(h.timestamp), value: h.vwap
        })));
      }
      if (this.vwapUpperSeries) {
        this.vwapUpperSeries.setData(history.filter(h => h.vwap != null).map(h => ({
          time: this.toChartTime(h.timestamp), value: h.vwapUpperBand || h.vwap * 1.01
        })));
      }
      if (this.vwapLowerSeries) {
        this.vwapLowerSeries.setData(history.filter(h => h.vwap != null).map(h => ({
          time: this.toChartTime(h.timestamp), value: h.vwapLowerBand || h.vwap * 0.99
        })));
      }
    } else if (this.vwapChartContainer?.nativeElement) {
      this.renderVWAPChart(history);
    }

    if (this.signalChartInitialized) {
      this.updateSignalChart();
    }
    this.cdr.detectChanges();
  }

  /**
   * Subscribe to WebSocket price updates to update the latest candle in real-time.
   */
  private subscribeToPriceUpdates(): void {
    this.priceSubscription = this.webSocketService.getPriceUpdates().subscribe({
      next: (tick) => {
        if (!tick || !tick.prices) return;
        const priceData = (tick.prices as any)[this.selectedMetricsSymbol];
        if (!priceData) return;
        const livePrice = priceData.price;
        if (!livePrice || !this.lastHistoryData.length) return;

        const lastBar = this.lastHistoryData[this.lastHistoryData.length - 1];
        const lastTime = this.toChartTime(lastBar.timestamp);

        if (this.priceSupertrendCandleSeries) {
          this.priceSupertrendCandleSeries.update({
            time: lastTime,
            open: lastBar.open || lastBar.close,
            high: Math.max(lastBar.high || lastBar.close, livePrice),
            low: Math.min(lastBar.low || lastBar.close, livePrice),
            close: livePrice
          });
        }
        if (this.vwapPriceSeries) {
          this.vwapPriceSeries.update({ time: lastTime, value: livePrice });
        }
        if (this.selectedSymbolMetrics) {
          this.selectedSymbolMetrics.currentPrice = livePrice;
        }
      },
      error: (err) => console.error('[Brahmastra] WebSocket price error:', err)
    });
  }

  // ==================== Option Chain Metrics ====================

  loadOptionChainMetrics(): void {
    this.isLoadingOptionChain = true;
    this.brahmastraService.getAllOptionChainMetrics().subscribe({
      next: (response) => {
        if (response.success) {
          this.optionChainMetrics = response.metrics;
          // Set selected option chain metrics to NIFTY by default
          this.selectedOptionChainMetrics = this.optionChainMetrics.find(m => m.symbol === this.selectedMetricsSymbol) || this.optionChainMetrics[0];
        }
        this.isLoadingOptionChain = false;
      },
      error: (err) => {
        console.error('Error loading option chain metrics:', err);
        this.isLoadingOptionChain = false;
      }
    });
  }

  selectOptionChainSymbol(symbol: string): void {
    this.selectedOptionChainMetrics = this.optionChainMetrics.find(m => m.symbol === symbol) || null;
  }

  selectMetricsSymbol(symbol: string): void {
    this.selectedMetricsSymbol = symbol;
    this.selectedSymbolMetrics = this.indicatorMetrics.find(m => m.symbol === symbol) || null;
    this.selectedOptionChainMetrics = this.optionChainMetrics.find(m => m.symbol === symbol) || null;
    // Destroy existing charts and re-render
    this.destroyIndicatorCharts();
    this.destroySignalChart();
    this.indicatorChartsRendered = false;
    this.indicatorChartsPending = true;
    this.signalChartPending = true;
    this.cdr.detectChanges();
    // Fallback direct render after a delay
    setTimeout(() => {
      if (!this.indicatorChartsRendered) {
        this.renderIndicatorCharts();
      }
      if (!this.signalChartInitialized) {
        this.initSignalChart();
      }
    }, 300);
  }

  renderIndicatorCharts(): void {
    if (!this.selectedSymbolMetrics || !this.selectedSymbolMetrics.history || this.selectedSymbolMetrics.history.length === 0) {
      console.warn('[Brahmastra] No indicator metrics/history data to render charts');
      return;
    }

    // Verify at least one chart container exists and has dimensions before proceeding
    const container = this.priceSupertrendChartContainer?.nativeElement;
    if (!container) {
      console.warn('[Brahmastra] Chart containers not in DOM yet, skipping render');
      return;
    }
    const rect = container.getBoundingClientRect();
    if (rect.width === 0) {
      console.warn('[Brahmastra] Chart container has no width yet, skipping render');
      return;
    }

    const history = this.selectedSymbolMetrics.history;
    console.log('[Brahmastra] Rendering indicator charts with', history.length, 'data points');

    // Destroy any existing charts first
    this.destroyIndicatorCharts();
    this.indicatorChartsRendered = true;
    this.indicatorChartsPending = false;

    // Render all three charts with staggered delays
    setTimeout(() => this.renderPriceSupertrendChart(history), 100);
    setTimeout(() => this.renderMACDChart(history), 150);
    setTimeout(() => this.renderVWAPChart(history), 200);
  }

  private createLightweightChartOptions(container: HTMLElement, height: number = 250): any {
    const rect = container.getBoundingClientRect();
    const width = rect.width > 0 ? rect.width : container.clientWidth || 400;
    return {
      width: width,
      height: height,
      layout: {
        background: { color: '#1e222d' },
        textColor: '#d1d4dc'
      },
      grid: {
        vertLines: { color: '#2B2B43' },
        horzLines: { color: '#2B2B43' }
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: { color: '#758696', width: 1, style: LineStyle.Dashed },
        horzLine: { color: '#758696', width: 1, style: LineStyle.Dashed }
      },
      rightPriceScale: {
        borderColor: '#2B2B43',
        scaleMargins: { top: 0.1, bottom: 0.1 }
      },
      timeScale: {
        borderColor: '#2B2B43',
        timeVisible: true,
        secondsVisible: false
      }
    };
  }

  /**
   * Convert a timestamp (ISO string or array) to lightweight-charts time format.
   * Lightweight-charts displays time in UTC, so we add IST offset (+5:30)
   * to trick it into showing Indian market hours correctly.
   */
  private toChartTime(timestamp: any): any {
    const IST_OFFSET_SECONDS = 5 * 3600 + 30 * 60; // 5 hours 30 minutes in seconds

    let epochMs: number;

    if (Array.isArray(timestamp)) {
      // Java LocalDateTime serialized as array: [year, month, day, hour, minute, second]
      // Note: JavaScript months are 0-indexed, Java months are 1-indexed
      const [year, month, day, hour = 0, minute = 0, second = 0] = timestamp;
      // Create as UTC directly (the array values represent IST wall-clock time)
      epochMs = Date.UTC(year, month - 1, day, hour, minute, second);
      // Return as-is since we constructed it as UTC with IST values
      return Math.floor(epochMs / 1000) as any;
    } else if (typeof timestamp === 'string') {
      // ISO string like "2026-02-24T09:15:00" (no timezone = IST assumed)
      if (timestamp.endsWith('Z') || /[+-]\d{2}:\d{2}$/.test(timestamp)) {
        // Has timezone info — parse normally, then shift to IST display
        epochMs = new Date(timestamp).getTime();
        return Math.floor(epochMs / 1000 + IST_OFFSET_SECONDS) as any;
      } else {
        // No timezone info — treat values as IST wall-clock time
        // Create UTC date from the literal values so chart shows them directly
        const parts = timestamp.replace('T', '-').replace(/:/g, '-').split('-').map(Number);
        const [year, month, day, hour = 0, minute = 0, second = 0] = parts;
        epochMs = Date.UTC(year, month - 1, day, hour, minute, second);
        return Math.floor(epochMs / 1000) as any;
      }
    } else if (typeof timestamp === 'number') {
      // Already epoch seconds or milliseconds
      if (timestamp > 1e12) {
        return Math.floor(timestamp / 1000 + IST_OFFSET_SECONDS) as any;
      }
      return Math.floor(timestamp + IST_OFFSET_SECONDS) as any;
    }

    // Fallback
    return Math.floor(Date.now() / 1000) as any;
  }

  renderPriceSupertrendChart(history: any[], retryCount: number = 0): void {
    try {
      if (this.priceSupertrendChart) {
        this.priceSupertrendChart.remove();
        this.priceSupertrendChart = null;
      }

      const container = this.priceSupertrendChartContainer?.nativeElement;
      if (!container) {
        console.warn('[Brahmastra] Price chart container not found, retry:', retryCount);
        if (retryCount < 10) {
          setTimeout(() => this.renderPriceSupertrendChart(history, retryCount + 1), 300);
        }
        return;
      }
      const rect = container.getBoundingClientRect();
      if (rect.width === 0 || rect.height === 0) {
        console.warn('[Brahmastra] Price chart container has no dimensions, retry:', retryCount);
        if (retryCount < 10) {
          setTimeout(() => this.renderPriceSupertrendChart(history, retryCount + 1), 300);
        }
        return;
      }
      console.log('[Brahmastra] Creating Price & Supertrend chart, container:', rect.width, 'x', rect.height);

      this.priceSupertrendChart = createChart(container, this.createLightweightChartOptions(container));

      this.priceSupertrendCandleSeries = this.priceSupertrendChart.addCandlestickSeries({
        upColor: '#26a69a',
        downColor: '#ef5350',
        borderUpColor: '#26a69a',
        borderDownColor: '#ef5350',
        wickUpColor: '#26a69a',
        wickDownColor: '#ef5350'
      });

      const candleData: CandlestickData[] = history.map(h => ({
        time: this.toChartTime(h.timestamp),
        open: h.open || h.close,
        high: h.high || h.close,
        low: h.low || h.close,
        close: h.close
      }));
      this.priceSupertrendCandleSeries.setData(candleData);

      this.priceSupertrendLineSeries = this.priceSupertrendChart.addLineSeries({
        color: '#00d4ff',
        lineWidth: 2,
        title: 'Supertrend',
        lastValueVisible: true,
        priceLineVisible: false
      });

      const supertrendData = history
        .filter(h => h.supertrend != null)
        .map(h => ({
          time: this.toChartTime(h.timestamp),
          value: h.supertrend,
          color: h.supertrendTrend === 'BULLISH' ? '#00ff88' : '#ff4444'
        }));
      this.priceSupertrendLineSeries.setData(supertrendData);

      this.priceSupertrendChart.timeScale().fitContent();
    } catch (error) {
      console.error('[Brahmastra] Error rendering Price & Supertrend chart:', error);
    }
  }

  renderMACDChart(history: any[], retryCount: number = 0): void {
    try {
      if (this.macdChartInstance) {
        this.macdChartInstance.remove();
        this.macdChartInstance = null;
      }

      const container = this.macdChartContainer?.nativeElement;
      if (!container) {
        console.warn('[Brahmastra] MACD chart container not found, retry:', retryCount);
        if (retryCount < 10) {
          setTimeout(() => this.renderMACDChart(history, retryCount + 1), 300);
        }
        return;
      }
      const rect = container.getBoundingClientRect();
      if (rect.width === 0 || rect.height === 0) {
        console.warn('[Brahmastra] MACD chart container has no dimensions, retry:', retryCount);
        if (retryCount < 10) {
          setTimeout(() => this.renderMACDChart(history, retryCount + 1), 300);
        }
        return;
      }
      console.log('[Brahmastra] Creating MACD chart, container:', rect.width, 'x', rect.height);

    this.macdChartInstance = createChart(container, this.createLightweightChartOptions(container));

    this.macdHistogramSeries = this.macdChartInstance.addHistogramSeries({
      color: '#26a69a',
      priceFormat: { type: 'price', precision: 2, minMove: 0.01 },
      priceLineVisible: false,
      lastValueVisible: false
    });

    const histogramData = history
      .filter(h => h.macdHistogram != null)
      .map(h => ({
        time: this.toChartTime(h.timestamp),
        value: h.macdHistogram,
        color: h.macdHistogram >= 0 ? 'rgba(0, 255, 136, 0.6)' : 'rgba(255, 68, 68, 0.6)'
      }));
    this.macdHistogramSeries.setData(histogramData);

    this.macdLineSeries = this.macdChartInstance.addLineSeries({
      color: '#00d4ff',
      lineWidth: 2,
      title: 'MACD',
      lastValueVisible: true,
      priceLineVisible: false
    });

    const macdLineData = history
      .filter(h => h.macdLine != null)
      .map(h => ({
        time: this.toChartTime(h.timestamp),
        value: h.macdLine
      }));
    this.macdLineSeries.setData(macdLineData);

    this.macdSignalLineSeries = this.macdChartInstance.addLineSeries({
      color: '#ff9800',
      lineWidth: 2,
      title: 'Signal',
      lastValueVisible: true,
      priceLineVisible: false
    });

    const signalLineData = history
      .filter(h => h.macdSignal != null)
      .map(h => ({
        time: this.toChartTime(h.timestamp),
        value: h.macdSignal
      }));
    this.macdSignalLineSeries.setData(signalLineData);

    this.macdChartInstance.timeScale().fitContent();
    } catch (error) {
      console.error('[Brahmastra] Error rendering MACD chart:', error);
    }
  }

  renderVWAPChart(history: any[], retryCount: number = 0): void {
    try {
    if (this.vwapChartInstance) {
      this.vwapChartInstance.remove();
      this.vwapChartInstance = null;
    }

    const container = this.vwapChartContainer?.nativeElement;
    if (!container) {
      console.warn('[Brahmastra] VWAP chart container not found, retry:', retryCount);
      if (retryCount < 10) {
        setTimeout(() => this.renderVWAPChart(history, retryCount + 1), 300);
      }
      return;
    }
    const rect = container.getBoundingClientRect();
    if (rect.width === 0 || rect.height === 0) {
      console.warn('[Brahmastra] VWAP chart container has no dimensions, retry:', retryCount);
      if (retryCount < 10) {
        setTimeout(() => this.renderVWAPChart(history, retryCount + 1), 300);
      }
      return;
    }
    console.log('[Brahmastra] Creating VWAP chart, container:', rect.width, 'x', rect.height);

    this.vwapChartInstance = createChart(container, this.createLightweightChartOptions(container));

    this.vwapPriceSeries = this.vwapChartInstance.addLineSeries({
      color: '#ff9800',
      lineWidth: 2,
      title: 'Price',
      lastValueVisible: true,
      priceLineVisible: false
    });

    const priceData = history
      .filter(h => h.close != null)
      .map(h => ({
        time: this.toChartTime(h.timestamp),
        value: h.close
      }));
    this.vwapPriceSeries.setData(priceData);

    this.vwapLineSeries = this.vwapChartInstance.addLineSeries({
      color: '#9c27b0',
      lineWidth: 2,
      title: 'VWAP',
      lastValueVisible: true,
      priceLineVisible: false
    });

    const vwapData = history
      .filter(h => h.vwap != null)
      .map(h => ({
        time: this.toChartTime(h.timestamp),
        value: h.vwap
      }));
    this.vwapLineSeries.setData(vwapData);

    this.vwapUpperSeries = this.vwapChartInstance.addLineSeries({
      color: 'rgba(156, 39, 176, 0.4)',
      lineWidth: 1,
      lineStyle: LineStyle.Dashed,
      title: 'Upper',
      lastValueVisible: false,
      priceLineVisible: false
    });

    const upperData = history
      .filter(h => h.vwap != null)
      .map(h => ({
        time: this.toChartTime(h.timestamp),
        value: h.vwapUpperBand || h.vwap * 1.01
      }));
    this.vwapUpperSeries.setData(upperData);

    this.vwapLowerSeries = this.vwapChartInstance.addLineSeries({
      color: 'rgba(156, 39, 176, 0.4)',
      lineWidth: 1,
      lineStyle: LineStyle.Dashed,
      title: 'Lower',
      lastValueVisible: false,
      priceLineVisible: false
    });

    const lowerData = history
      .filter(h => h.vwap != null)
      .map(h => ({
        time: this.toChartTime(h.timestamp),
        value: h.vwapLowerBand || h.vwap * 0.99
      }));
    this.vwapLowerSeries.setData(lowerData);

    this.vwapChartInstance.timeScale().fitContent();
    } catch (error) {
      console.error('[Brahmastra] Error rendering VWAP chart:', error);
    }
  }


  // ==================== Interactive Signal Chart (Lightweight Charts) ====================

  private initSignalChart(): void {
    if (this.signalChartInitialized) {
      return;
    }
    if (!this.signalChartContainer?.nativeElement) {
      console.warn('[Brahmastra] Signal chart: container not available yet');
      return;
    }

    const container = this.signalChartContainer.nativeElement;
    const signalRect = container.getBoundingClientRect();
    const chartWidth = signalRect.width > 0 ? signalRect.width : container.clientWidth > 0 ? container.clientWidth : container.offsetWidth > 0 ? container.offsetWidth : 0;
    if (chartWidth === 0) {
      console.warn('[Brahmastra] Signal chart container width=0, will retry');
      return;
    }
    console.log('[Brahmastra] Initializing interactive signal chart, width:', chartWidth);

    this.signalChart = createChart(container, {
      width: chartWidth,
      height: 450,
      layout: {
        background: { color: '#1e222d' },
        textColor: '#d1d4dc'
      },
      grid: {
        vertLines: { color: '#2B2B43' },
        horzLines: { color: '#2B2B43' }
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: {
          color: '#758696',
          width: 1,
          style: LineStyle.Dashed,
          labelBackgroundColor: '#2B2B43'
        },
        horzLine: {
          color: '#758696',
          width: 1,
          style: LineStyle.Dashed,
          labelBackgroundColor: '#2B2B43'
        }
      },
      rightPriceScale: {
        borderColor: '#2B2B43',
        scaleMargins: { top: 0.1, bottom: 0.1 }
      },
      timeScale: {
        borderColor: '#2B2B43',
        timeVisible: true,
        secondsVisible: false
      }
    });

    // Add candlestick series
    this.candleSeries = this.signalChart.addCandlestickSeries({
      upColor: '#26a69a',
      downColor: '#ef5350',
      borderUpColor: '#26a69a',
      borderDownColor: '#ef5350',
      wickUpColor: '#26a69a',
      wickDownColor: '#ef5350'
    });

    // Add Supertrend line series
    this.supertrendLine = this.signalChart.addLineSeries({
      color: '#00d4ff',
      lineWidth: 2,
      title: 'Supertrend'
    });

    // Add VWAP line series
    this.vwapLine = this.signalChart.addLineSeries({
      color: '#9c27b0',
      lineWidth: 2,
      lineStyle: LineStyle.Dotted,
      title: 'VWAP'
    });

    this.signalChartInitialized = true;
    this.signalChartPending = false;

    // Load initial data
    this.updateSignalChart();
  }

  updateSignalChart(): void {
    if (!this.signalChartInitialized || !this.selectedSymbolMetrics?.history) return;

    const history = this.selectedSymbolMetrics.history;
    if (!history || history.length === 0) return;

    // Convert to candlestick data
    const candleData: CandlestickData[] = history.map(h => ({
      time: this.toChartTime(h.timestamp),
      open: h.open,
      high: h.high,
      low: h.low,
      close: h.close
    }));

    // Supertrend data
    const supertrendData = history
      .filter(h => h.supertrend != null)
      .map(h => ({
        time: this.toChartTime(h.timestamp),
        value: h.supertrend,
        color: h.supertrendTrend === 'BULLISH' ? '#00ff88' : '#ff4444'
      }));

    // VWAP data
    const vwapData = history
      .filter(h => h.vwap != null)
      .map(h => ({
        time: this.toChartTime(h.timestamp),
        value: h.vwap
      }));

    // Update chart data
    if (this.candleSeries) {
      this.candleSeries.setData(candleData);
    }

    if (this.supertrendLine && supertrendData.length > 0) {
      this.supertrendLine.setData(supertrendData);
    }

    if (this.vwapLine && vwapData.length > 0) {
      this.vwapLine.setData(vwapData);
    }

    // Clear existing annotations
    this.clearSignalAnnotations();

    // Add signal markers and annotations
    if (this.showSignalAnnotations) {
      this.addSignalMarkers(history);
    }

    // Add trade levels for active signals
    if (this.showTradeLevels && this.activeSignals.length > 0) {
      this.addTradeLevelAnnotations();
    }

    // Fit content
    this.signalChart?.timeScale().fitContent();
  }

  private addSignalMarkers(history: any[]): void {
    if (!this.candleSeries) return;

    const markers: any[] = [];

    // Find MACD crossovers and signal points
    for (let i = 1; i < history.length; i++) {
      const curr = history[i];
      const prev = history[i - 1];

      // Check for bullish crossover (MACD crosses above signal)
      if (prev.macdLine <= prev.macdSignal && curr.macdLine > curr.macdSignal) {
        if (curr.supertrendTrend === 'BULLISH') {
          markers.push({
            time: this.toChartTime(curr.timestamp),
            position: 'belowBar',
            color: '#00ff88',
            shape: 'arrowUp',
            text: 'BUY',
            size: 2
          });
        }
      }

      // Check for bearish crossover (MACD crosses below signal)
      if (prev.macdLine >= prev.macdSignal && curr.macdLine < curr.macdSignal) {
        if (curr.supertrendTrend === 'BEARISH') {
          markers.push({
            time: this.toChartTime(curr.timestamp),
            position: 'aboveBar',
            color: '#ff4444',
            shape: 'arrowDown',
            text: 'SELL',
            size: 2
          });
        }
      }

      // Mark Supertrend trend changes
      if (prev.supertrendTrend !== curr.supertrendTrend && curr.supertrendTrend) {
        markers.push({
          time: this.toChartTime(curr.timestamp),
          position: curr.supertrendTrend === 'BULLISH' ? 'belowBar' : 'aboveBar',
          color: curr.supertrendTrend === 'BULLISH' ? '#4CAF50' : '#F44336',
          shape: 'circle',
          text: 'ST',
          size: 1
        });
      }
    }

    markers.sort((a, b) => a.time - b.time);
    this.signalMarkers = markers;
    this.candleSeries.setMarkers(markers);
  }

  private addTradeLevelAnnotations(): void {
    if (!this.candleSeries) return;

    const symbolSignals = this.activeSignals.filter(
      s => s.symbol?.toUpperCase() === this.selectedMetricsSymbol.toUpperCase()
    );

    if (symbolSignals.length === 0) return;

    const latestSignal = symbolSignals[0];

    if (latestSignal.entryPrice) {
      const entryLine = this.candleSeries.createPriceLine({
        price: latestSignal.entryPrice,
        color: '#2196F3',
        lineWidth: 2,
        lineStyle: LineStyle.Solid,
        axisLabelVisible: true,
        title: `Entry: ${latestSignal.entryPrice.toFixed(2)}`
      });
      this.signalLevelLines.set('entry', entryLine);
    }

    if (latestSignal.stopLoss) {
      const slLine = this.candleSeries.createPriceLine({
        price: latestSignal.stopLoss,
        color: '#F44336',
        lineWidth: 2,
        lineStyle: LineStyle.Dashed,
        axisLabelVisible: true,
        title: `SL: ${latestSignal.stopLoss.toFixed(2)}`
      });
      this.signalLevelLines.set('stopLoss', slLine);
    }

    if (latestSignal.target1) {
      const t1Line = this.candleSeries.createPriceLine({
        price: latestSignal.target1,
        color: '#4CAF50',
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        axisLabelVisible: true,
        title: `T1: ${latestSignal.target1.toFixed(2)}`
      });
      this.signalLevelLines.set('target1', t1Line);
    }

    if (latestSignal.target2) {
      const t2Line = this.candleSeries.createPriceLine({
        price: latestSignal.target2,
        color: '#8BC34A',
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        axisLabelVisible: true,
        title: `T2: ${latestSignal.target2.toFixed(2)}`
      });
      this.signalLevelLines.set('target2', t2Line);
    }

    if (latestSignal.target3) {
      const t3Line = this.candleSeries.createPriceLine({
        price: latestSignal.target3,
        color: '#CDDC39',
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        axisLabelVisible: true,
        title: `T3: ${latestSignal.target3.toFixed(2)}`
      });
      this.signalLevelLines.set('target3', t3Line);
    }

    if (this.selectedSymbolMetrics?.currentPrice) {
      const ltpLine = this.candleSeries.createPriceLine({
        price: this.selectedSymbolMetrics.currentPrice,
        color: '#FF9800',
        lineWidth: 2,
        lineStyle: LineStyle.Solid,
        axisLabelVisible: true,
        title: `LTP: ${this.selectedSymbolMetrics.currentPrice.toFixed(2)}`
      });
      this.signalLevelLines.set('ltp', ltpLine);
    }
  }

  private clearSignalAnnotations(): void {
    this.signalLevelLines.forEach((line) => {
      if (this.candleSeries) {
        this.candleSeries.removePriceLine(line);
      }
    });
    this.signalLevelLines.clear();

    if (this.candleSeries) {
      this.candleSeries.setMarkers([]);
    }
    this.signalMarkers = [];
  }

  toggleSignalAnnotations(): void {
    this.showSignalAnnotations = !this.showSignalAnnotations;
    this.updateSignalChart();
  }

  toggleTradeLevels(): void {
    this.showTradeLevels = !this.showTradeLevels;
    this.updateSignalChart();
  }

  onSignalChartSymbolChange(): void {
    this.updateSignalChart();
  }

  refreshDashboard(): void {
    this.loadDashboard();
    this.loadPCRData();
    this.loadIndicatorMetrics();
    this.loadOptionChainMetrics();
    this.scanAllSymbols();
  }

  // ==================== Signal Generation ====================

  generateSignals(): void {
    this.isLoadingSignals = true;
    this.generatedSignals = [];

    this.brahmastraService.generateSignals(this.signalRequest).subscribe({
      next: (response) => {
        if (response.success) {
          this.generatedSignals = response.signals;
        }
        this.isLoadingSignals = false;
      },
      error: (err) => {
        console.error('Error generating signals:', err);
        this.isLoadingSignals = false;
      }
    });
  }

  // ==================== Live Scanning ====================

  scanAllSymbols(): void {
    this.isScanning = true;
    this.brahmastraService.scanLive().subscribe({
      next: (response) => {
        if (response.success) {
          this.liveScans = response.results;
        }
        this.isScanning = false;
      },
      error: (err) => {
        console.error('Error scanning:', err);
        this.isScanning = false;
      }
    });
  }

  scanSymbol(symbol: string): void {
    this.brahmastraService.scanSymbol(symbol, '5m').subscribe({
      next: (response) => {
        if (response.success) {
          const index = this.liveScans.findIndex(s => s.symbol === symbol);
          if (index >= 0) {
            this.liveScans[index] = response.result;
          } else {
            this.liveScans.push(response.result);
          }
        }
      },
      error: (err) => console.error(`Error scanning ${symbol}:`, err)
    });
  }

  // ==================== Backtesting ====================

  runBacktest(): void {
    this.isLoadingBacktest = true;
    this.backtestResult = null;

    this.brahmastraService.runBacktest(this.backtestRequest).subscribe({
      next: (response) => {
        if (response.success) {
          this.backtestResult = response.result;
          setTimeout(() => this.renderCharts(), 100);
        }
        this.isLoadingBacktest = false;
      },
      error: (err) => {
        console.error('Error running backtest:', err);
        this.isLoadingBacktest = false;
      }
    });
  }

  // ==================== Charts ====================

  renderCharts(): void {
    if (!this.backtestResult) return;

    this.renderEquityChart();
    this.renderDrawdownChart();
  }

  renderEquityChart(retryCount: number = 0): void {
    if (this.equityChartInstance) {
      this.equityChartInstance.remove();
      this.equityChartInstance = null;
    }

    const container = this.equityChartRef?.nativeElement;
    if (!container || !this.backtestResult?.equityCurve) return;
    const rect = container.getBoundingClientRect();
    if (rect.width === 0 || rect.height === 0) {
      if (retryCount < 10) {
        setTimeout(() => this.renderEquityChart(retryCount + 1), 300);
      }
      return;
    }

    this.equityChartInstance = createChart(container, this.createLightweightChartOptions(container, 300));

    this.equityLineSeries = this.equityChartInstance.addLineSeries({
      color: '#4CAF50',
      lineWidth: 2,
      title: 'Equity',
      lastValueVisible: true,
      priceLineVisible: false
    });

    const data = this.backtestResult.equityCurve.map(p => ({
      time: this.toChartTime(p.timestamp),
      value: p.equity
    }));
    this.equityLineSeries.setData(data);

    // Add area fill
    const areaSeries = this.equityChartInstance.addAreaSeries({
      topColor: 'rgba(76, 175, 80, 0.3)',
      bottomColor: 'rgba(76, 175, 80, 0.05)',
      lineColor: 'rgba(76, 175, 80, 0)',
      lineWidth: 1,
      lastValueVisible: false,
      priceLineVisible: false
    });
    areaSeries.setData(data);

    this.equityChartInstance.timeScale().fitContent();
  }

  renderDrawdownChart(retryCount: number = 0): void {
    if (this.drawdownChartInstance) {
      this.drawdownChartInstance.remove();
      this.drawdownChartInstance = null;
    }

    const container = this.drawdownChartRef?.nativeElement;
    if (!container || !this.backtestResult?.drawdownCurve) return;
    const rect = container.getBoundingClientRect();
    if (rect.width === 0 || rect.height === 0) {
      if (retryCount < 10) {
        setTimeout(() => this.renderDrawdownChart(retryCount + 1), 300);
      }
      return;
    }

    this.drawdownChartInstance = createChart(container, this.createLightweightChartOptions(container, 300));

    this.drawdownLineSeries = this.drawdownChartInstance.addLineSeries({
      color: '#f44336',
      lineWidth: 2,
      title: 'Drawdown %',
      lastValueVisible: true,
      priceLineVisible: false
    });

    const data = this.backtestResult.drawdownCurve.map(p => ({
      time: this.toChartTime(p.timestamp),
      value: -p.drawdownPercent
    }));
    this.drawdownLineSeries.setData(data);

    // Add area fill
    const areaSeries = this.drawdownChartInstance.addAreaSeries({
      topColor: 'rgba(244, 67, 54, 0.05)',
      bottomColor: 'rgba(244, 67, 54, 0.3)',
      lineColor: 'rgba(244, 67, 54, 0)',
      lineWidth: 1,
      lastValueVisible: false,
      priceLineVisible: false,
      invertFilledArea: true
    });
    areaSeries.setData(data);

    this.drawdownChartInstance.timeScale().fitContent();
  }

  // ==================== WebSocket ====================

  subscribeToWebSocket(): void {
    this.wsSubscription = this.brahmastraService.signalUpdates$.subscribe({
      next: (signal) => {
        this.handleNewSignal(signal);
      }
    });
  }

  handleNewSignal(signal: LiveScanResult): void {
    if (signal.isNewSignal) {
      // Show notification or alert
      console.log('New Brahmastra Signal:', signal);

      // Update live scans
      const index = this.liveScans.findIndex(s => s.symbol === signal.symbol);
      if (index >= 0) {
        this.liveScans[index] = signal;
      } else {
        this.liveScans.unshift(signal);
      }

      // Refresh dashboard
      this.loadDashboard();
    }
  }

  // ==================== Utility Methods ====================

  getSignalClass(signalType: string): string {
    return signalType === 'BUY' ? 'signal-buy' : signalType === 'SELL' ? 'signal-sell' : '';
  }

  getBiasClass(bias: string): string {
    return bias === 'BULLISH' ? 'bias-bullish' : bias === 'BEARISH' ? 'bias-bearish' : 'bias-neutral';
  }

  getStatusClass(status: string): string {
    switch (status) {
      case 'HEALTHY': return 'status-healthy';
      case 'CAUTION': return 'status-caution';
      case 'PAUSE': return 'status-pause';
      default: return '';
    }
  }

  getSignalStatusClass(status: string): string {
    switch (status?.toUpperCase()) {
      case 'ACTIVE':
      case 'OPEN':
        return 'status-active';
      case 'CLOSED':
      case 'COMPLETED':
        return 'status-closed';
      case 'STOPPED':
      case 'STOP_OUT':
        return 'status-stopped';
      default:
        return 'status-active';
    }
  }

  formatCurrency(value: number): string {
    return new Intl.NumberFormat('en-IN', {
      style: 'currency',
      currency: 'INR',
      minimumFractionDigits: 2
    }).format(value);
  }

  formatPercent(value: number): string {
    return `${value >= 0 ? '+' : ''}${value.toFixed(2)}%`;
  }

  formatTime(timestamp: string): string {
    return new Date(timestamp).toLocaleString('en-IN');
  }

  exportBacktestToCSV(): void {
    if (!this.backtestResult?.tradeLog) return;

    const headers = ['Trade #', 'Type', 'Entry Time', 'Exit Time', 'Entry Price', 'Exit Price', 'P&L', 'P&L %', 'Exit Reason', 'R:R'];
    const rows = this.backtestResult.tradeLog.map(t => [
      t.tradeNumber,
      t.signalType,
      t.entryTime,
      t.exitTime,
      t.entryPrice,
      t.exitPrice,
      t.pnl.toFixed(2),
      t.pnlPercent.toFixed(2),
      t.exitReason,
      t.riskReward.toFixed(2)
    ]);

    const csvContent = [headers, ...rows].map(row => row.join(',')).join('\n');
    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `brahmastra_backtest_${this.backtestResult.symbol}_${new Date().toISOString().split('T')[0]}.csv`;
    link.click();
  }
}

