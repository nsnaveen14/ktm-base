import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatButtonModule } from '@angular/material/button';
import { MatCardModule } from '@angular/material/card';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatSlideToggleModule } from '@angular/material/slide-toggle';
import { MatExpansionModule } from '@angular/material/expansion';
import { MatTabsModule } from '@angular/material/tabs';
import { MatChipsModule } from '@angular/material/chips';
import { MatBadgeModule } from '@angular/material/badge';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatDividerModule } from '@angular/material/divider';
import { MatTableModule } from '@angular/material/table';
import { interval, Subscription } from 'rxjs';
import { DataService } from '../../services/data.service';
import { TickerService } from '../../services/ticker.service';
import { TelegramService } from '../../services/telegram.service';
import {
  PredictedCandleStick,
  PredictionDeviation,
  TradeSetup,
  PredictionJobStatus,
  DeviationSummary,
  TradeSetupPerformance,
  CorrectionFactors,
  LiveTickData,
  EMAChartData,
  RollingChartData
} from '../../models/analytics.model';
import * as Highcharts from 'highcharts';
import { HighchartsChartModule } from 'highcharts-angular';
import { LiveTickComponent } from '../live-tick/live-tick.component';

@Component({
  selector: 'app-analytics',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatButtonModule,
    MatCardModule,
    MatIconModule,
    MatProgressSpinnerModule,
    MatSlideToggleModule,
    MatExpansionModule,
    MatTabsModule,
    MatChipsModule,
    MatBadgeModule,
    MatTooltipModule,
    MatDividerModule,
    MatTableModule,
    HighchartsChartModule,
    LiveTickComponent
  ],
  templateUrl: './analytics.component.html',
  styleUrl: './analytics.component.css'
})
export class AnalyticsComponent implements OnInit, OnDestroy {
  // Expose Math to template
  Math = Math;

  // Highcharts configuration
  Highcharts: typeof Highcharts = Highcharts;
  chartOptions: Highcharts.Options = {};
  updateFlag = false;
  chartRef: Highcharts.Chart | null = null;

  // EMA Chart configuration
  emaChartOptions: Highcharts.Options = {};
  emaUpdateFlag = false;
  emaChartRef: Highcharts.Chart | null = null;
  emaChartData: EMAChartData | null = null;

  // EMA Chart callback to get chart reference
  emaChartCallback: Highcharts.ChartCallbackFunction = (chart) => { this.emaChartRef = chart; };

  // Chart callback to get chart reference
  chartCallback: Highcharts.ChartCallbackFunction = (chart) => { this.chartRef = chart; };

  // Job status
  isJobActive = false;
  isLoading = false;
  jobStatus: PredictionJobStatus | null = null;

  // Data
  latestPredictions: PredictedCandleStick[] = [];
  todayDeviations: PredictionDeviation[] = [];
  latestTradeSetup: TradeSetup | null = null;
  todayTradeSetups: TradeSetup[] = [];
  tradeSetupPerformance: TradeSetupPerformance | null = null;
  deviationSummary: DeviationSummary | null = null;
  correctionFactors: CorrectionFactors | null = null;
  tradeRecommendation: string = 'NEUTRAL';
  liveTickData: LiveTickData | null = null;
  isTickerConnected: boolean = false;

  // Chart data
  predictedData: number[][] = [];
  actualData: number[][] = [];
  rollingChartData: RollingChartData | null = null;

  // Deviation Alert properties
  showDeviationAlert: boolean = false;
  deviationAlertMessage: string = '';
  deviationAlertType: 'warning' | 'danger' = 'warning';
  lastDeviationAlertTime: number = 0;
  deviationThreshold: number = 10; // Points threshold for alert
  private alertAudio: HTMLAudioElement | null = null;
  private alertTimeout: any = null;

  // Telegram notification settings
  telegramAlertsEnabled: boolean = true;
  private lastTelegramAlertTime: number = 0;
  private telegramAlertCooldownMs: number = 120000; // 2 minutes between Telegram alerts

  // Auto-refresh subscription
  private refreshSubscription: Subscription | null = null;
  private chartRefreshSubscription: Subscription | null = null;
  private liveTickRefreshSubscription: Subscription | null = null;

  // Table columns
  tradeSetupColumns = ['direction', 'entry', 'target1', 'stopLoss', 'rr', 'confidence', 'status'];
  deviationColumns = ['batchId', 'avgDeviation', 'directionAccuracy', 'bias', 'verified'];

  // Ticker service subscriptions
  private tickerConnSub: Subscription | null = null;
  private tickerLiveSub: Subscription | null = null;
  private tickerJobSub: Subscription | null = null;

  constructor(
    private dataService: DataService,
    private tickerService: TickerService,
    private telegramService: TelegramService
  ) {}

  ngOnInit(): void {
    this.initializeChart();
    this.initializeEMAChart();
    this.initializeAlertAudio();
    this.loadInitialData();

  // Start and subscribe to ticker service
    this.tickerService.start();
    this.tickerConnSub = this.tickerService.isTickerConnected$.subscribe(v => {
      this.isTickerConnected = v;
      // Auto-start job when ticker is connected and market is open
      this.autoStartJobIfEligible();
    });
    this.tickerLiveSub = this.tickerService.liveTick$.subscribe(v => {
      if (v) {
        this.liveTickData = {
          niftyLTP: v.niftyLTP || 0,
          niftyChange: v.niftyChange || 0,
          niftyChangePercent: v.niftyChangePercent || 0,
          niftyOpen: v.niftyOpen || 0,
          niftyHigh: v.niftyHigh || 0,
          niftyLow: v.niftyLow || 0,
          vixValue: v.vixValue || 0,
          atmStrike: v.atmStrike || 0,
          atmCELTP: v.atmCELTP || 0,
          atmPELTP: v.atmPELTP || 0,
          atmCEChange: v.atmCEChange || 0,
          atmPEChange: v.atmPEChange || 0,
          atmCESymbol: v.atmCESymbol || '',
          atmPESymbol: v.atmPESymbol || '',
          cePeDiff: v.cePeDiff || 0,
          straddlePremium: v.straddlePremium || 0,
          syntheticFuture: v.syntheticFuture || 0,
          sentiment: v.sentiment || 'NEUTRAL',
          timestamp: v.timestamp || new Date().toISOString(),
          isLive: v.isLive || false,
          tickerMapSize: v.tickerMapSize || 0,
          error: v.error
        } as LiveTickData;
      } else {
        this.liveTickData = null;
      }
    });
    this.tickerJobSub = this.tickerService.jobStatus$.subscribe(s => {
      this.jobStatus = s;
      if (s) this.isJobActive = !!s.isActive;
      // Auto-start job when we know the job status
      this.autoStartJobIfEligible();
    });

    this.startAutoRefresh();
  }

  ngOnDestroy(): void {
    this.stopAutoRefresh();
    this.cleanupAlert();
    if (this.tickerConnSub) this.tickerConnSub.unsubscribe();
    if (this.tickerLiveSub) this.tickerLiveSub.unsubscribe();
    if (this.tickerJobSub) this.tickerJobSub.unsubscribe();
  }

  initializeAlertAudio(): void {
    // Initialize audio element for alert sound
    this.alertAudio = new Audio('alert.mp3');
    this.alertAudio.volume = 0.7;
  }

  cleanupAlert(): void {
    if (this.alertTimeout) clearTimeout(this.alertTimeout);
    this.showDeviationAlert = false;
  }

  initializeChart(): void {
    // Set timezone offset for IST (UTC+5:30 = 330 minutes)
    const istOffset = 5.5 * 60 * 60 * 1000; // 5.5 hours in milliseconds

    this.chartOptions = {
      chart: { type: 'line', height: 350, backgroundColor: '#1e1e1e', animation: true },
      time: { timezoneOffset: -330, useUTC: false },
      title: { text: 'Predicted vs Actual Candle Close Prices', style: { color: '#ffffff' } },
      subtitle: { text: 'Rolling 30-min window (25 min actual + 5 min predicted)', style: { color: '#888888', fontSize: '11px' } },
      xAxis: { type: 'datetime', labels: { style: { color: '#cccccc' }, format: '{value:%H:%M}' }, lineColor: '#444444', tickColor: '#444444', tickInterval: 5 * 60 * 1000, title: { text: 'Time (IST)', style: { color: '#888888' } } },
      yAxis: { title: { text: 'Price', style: { color: '#cccccc' } }, labels: { style: { color: '#cccccc' } }, gridLineColor: '#333333' },
      legend: { itemStyle: { color: '#cccccc' } },
      tooltip: { shared: true, xDateFormat: '%H:%M:%S', backgroundColor: '#2d2d2d', style: { color: '#ffffff' } },
      plotOptions: { line: { marker: { enabled: true, radius: 4 }, lineWidth: 2 } },
      series: [
        { name: 'Predicted Close', type: 'line', color: '#4CAF50', data: [], marker: { symbol: 'circle' } },
        { name: 'Actual Close', type: 'line', color: '#2196F3', data: [], marker: { symbol: 'diamond' }, dashStyle: 'Solid' }
      ],
      credits: { enabled: false }
    };
  }

  initializeEMAChart(): void {
    this.emaChartOptions = {
      chart: { type: 'line', height: 280, backgroundColor: '#1e1e1e', animation: true },
      time: { timezoneOffset: -330, useUTC: false },
      title: { text: '9-21-50 EMA Crossover', style: { color: '#ffffff', fontSize: '14px' } },
      xAxis: { type: 'datetime', labels: { style: { color: '#cccccc' }, format: '{value:%H:%M}' }, lineColor: '#444444', tickColor: '#444444', min: this.getMarketOpenTime(), tickInterval: 5 * 60 * 1000, title: { text: 'Time (IST)', style: { color: '#888888', fontSize: '10px' } } },
      yAxis: { title: { text: 'Price', style: { color: '#cccccc', fontSize: '10px' } }, labels: { style: { color: '#cccccc' } }, gridLineColor: '#333333' },
      legend: { itemStyle: { color: '#cccccc' } },
      tooltip: { shared: true, xDateFormat: '%H:%M:%S', backgroundColor: '#2d2d2d', style: { color: '#ffffff' } },
      plotOptions: { line: { marker: { enabled: false }, lineWidth: 2 } },
      series: [
        { name: 'Price', type: 'line', color: '#888888', data: [], lineWidth: 1, dashStyle: 'Dot', marker: { enabled: false } },
        { name: '9 EMA', type: 'line', color: '#FF9800', data: [], lineWidth: 2, marker: { enabled: false } },
        { name: '21 EMA', type: 'line', color: '#2196F3', data: [], lineWidth: 2, marker: { enabled: false } },
        { name: '50 EMA', type: 'line', color: '#9C27B0', data: [], lineWidth: 2, marker: { enabled: false } }
      ],
      credits: { enabled: false }
    };
  }

  // Get market open time for today (9:15 AM IST)
  getMarketOpenTime(): number {
    const now = new Date();
    const marketOpen = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 9, 15, 0, 0);
    return marketOpen.getTime();
  }

  loadInitialData(): void {
    this.isLoading = true;

    // job status/predictions/EMA/rolling/deviation/trade setups
    this.loadJobStatus();
    this.loadPredictions();
    this.loadRollingChartData();
    this.loadDeviations();
    this.loadTradeSetups();
    this.loadTradeRecommendation();
    this.loadEMAChartData();

    this.isLoading = false;
  }

  loadJobStatus(): void {
    this.dataService.getPredictionJobStatus().subscribe({
      next: (status) => {
        this.jobStatus = status;
        this.isJobActive = status.isActive;
        // Update ticker connection status from job stats as well
        if (status.isTickerConnected !== undefined) {
          this.isTickerConnected = status.isTickerConnected;
        }
      },
      error: (err) => {
        console.error('Error loading job status:', err);
        // Server is down or unreachable - reset all status
        this.isTickerConnected = false;
        this.isJobActive = false;
        if (this.jobStatus) {
          this.jobStatus.isActive = false;
          this.jobStatus.isTickerConnected = false;
          this.jobStatus.isWithinMarketHours = false;
        }
      }
    });
  }

  loadPredictions(): void {
    // First verify past predictions to get actual close prices
    this.dataService.verifyPredictions().subscribe({
      next: (result) => {
        console.log('Verified predictions:', result.verifiedCount, 'Session accuracy:', result.sessionAccuracy);

        // Now load the updated predictions with actual data
        this.dataService.getLatestPredictions().subscribe({
          next: (predictions) => {
            this.latestPredictions = predictions;
            console.log('Loaded predictions:', predictions.length);

            // Log how many have actual data
            const withActual = predictions.filter(p => p.actualClosePrice && p.actualClosePrice > 0);
            console.log('Predictions with actual data:', withActual.length);

            // Note: Chart is now updated by loadRollingChartData(), not here
          },
          error: (err) => console.error('Error loading predictions:', err)
        });
      },
      error: (err) => {
        console.error('Error verifying predictions:', err);
        // Still try to load predictions even if verification fails
        this.dataService.getLatestPredictions().subscribe({
          next: (predictions) => {
            this.latestPredictions = predictions;
            // Note: Chart is now updated by loadRollingChartData(), not here
          },
          error: (err2) => console.error('Error loading predictions:', err2)
        });
      }
    });
  }

  loadDeviations(): void {
    this.dataService.getTodayDeviations().subscribe({
      next: (deviations) => {
        this.todayDeviations = deviations;
      },
      error: (err) => console.error('Error loading deviations:', err)
    });

    this.dataService.getDeviationSummary().subscribe({
      next: (summary) => {
        this.deviationSummary = summary;
      },
      error: (err) => console.error('Error loading deviation summary:', err)
    });

    this.dataService.getCorrectionFactors().subscribe({
      next: (factors) => {
        this.correctionFactors = factors;
      },
      error: (err) => console.error('Error loading correction factors:', err)
    });
  }

  loadTradeSetups(): void {
    this.dataService.getLatestTradeSetup().subscribe({
      next: (setup) => {
        this.latestTradeSetup = setup;
      },
      error: (err) => {
        // 404 is expected if no setup exists
        if (err.status !== 404) {
          console.error('Error loading latest trade setup:', err);
        }
      }
    });

    this.dataService.getTodayTradeSetups().subscribe({
      next: (setups) => {
        this.todayTradeSetups = setups;
      },
      error: (err) => console.error('Error loading today trade setups:', err)
    });

    this.dataService.getTradeSetupPerformance().subscribe({
      next: (performance) => {
        this.tradeSetupPerformance = performance;
      },
      error: (err) => console.error('Error loading trade setup performance:', err)
    });
  }

  loadTradeRecommendation(): void {
    this.dataService.getTradeRecommendation().subscribe({
      next: (response) => {
        this.tradeRecommendation = response.recommendation;
      },
      error: (err) => console.error('Error loading trade recommendation:', err)
    });
  }

  loadEMAChartData(): void {
    this.dataService.getEMAChartData().subscribe({
      next: (data) => {
        this.emaChartData = data;
        this.updateEMAChart();
      },
      error: (err) => console.error('Error loading EMA chart data:', err)
    });
  }

  loadRollingChartData(): void {
    this.dataService.getRollingChartData().subscribe({
      next: (data) => {
        this.rollingChartData = data;
        this.updateRollingChart();
      },
      error: (err) => console.error('Error loading rolling chart data:', err)
    });
  }

  updateRollingChart(): void {
    if (!this.rollingChartData) return;

    // Convert series data to number arrays
    this.predictedData = (this.rollingChartData.predictedSeries || []).map(point => [point[0], point[1]] as number[]);
    this.actualData = (this.rollingChartData.actualSeries || []).map(point => [point[0], point[1]] as number[]);

    console.log('Rolling Chart Data Received:');
    console.log('  - Actual data points:', this.actualData.length);
    console.log('  - Predicted data points:', this.predictedData.length);

    // Check for large deviations and trigger alert
    this.checkDeviationAlert();

    // Update chart
    if (this.chartRef) {
      // Update series data directly
      if (this.chartRef.series[0]) {
        this.chartRef.series[0].setData(this.predictedData, false);
      }
      if (this.chartRef.series[1]) {
        this.chartRef.series[1].setData(this.actualData, false);
      }

      // Set x-axis extremes for 30-minute rolling window
      if (this.rollingChartData.windowStart && this.rollingChartData.windowEnd) {
        this.chartRef.xAxis[0].setExtremes(
          this.rollingChartData.windowStart,
          this.rollingChartData.windowEnd,
          false
        );
      }

      // Force Y-axis to auto-scale based on both series
      if (this.chartRef.yAxis[0]) {
        this.chartRef.yAxis[0].setExtremes(undefined, undefined, false);
      }

      // Redraw the chart
      this.chartRef.redraw();
    } else {
      // Fallback: Update chart options
      if (this.chartOptions.series) {
        (this.chartOptions.series[0] as Highcharts.SeriesLineOptions).data = this.predictedData;
        (this.chartOptions.series[1] as Highcharts.SeriesLineOptions).data = this.actualData;
      }

      // Update x-axis range
      if (this.chartOptions.xAxis && this.rollingChartData.windowStart && this.rollingChartData.windowEnd) {
        (this.chartOptions.xAxis as Highcharts.XAxisOptions).min = this.rollingChartData.windowStart;
        (this.chartOptions.xAxis as Highcharts.XAxisOptions).max = this.rollingChartData.windowEnd;
      }

      this.updateFlag = true;
    }
  }

  /**
   * Check for deviations between actual and predicted values.
   * Triggers an alert if deviation exceeds threshold.
   */
  checkDeviationAlert(): void {
    if (this.actualData.length === 0 || this.predictedData.length === 0) {
      return;
    }

    // Find matching timestamps and calculate deviations
    const actualMap = new Map<number, number>();
    this.actualData.forEach(point => {
      actualMap.set(point[0], point[1]);
    });

    let maxDeviation = 0;
    let maxDeviationTime = 0;
    let actualValue = 0;
    let predictedValue = 0;

    for (const predPoint of this.predictedData) {
      const timestamp = predPoint[0];
      const predValue = predPoint[1];

      // Look for actual value at the same timestamp (with 1-minute tolerance)
      let matchedActual: number | undefined;
      for (const [actTime, actValue] of actualMap) {
        if (Math.abs(actTime - timestamp) < 60000) { // Within 1 minute
          matchedActual = actValue;
          break;
        }
      }

      if (matchedActual !== undefined) {
        const deviation = Math.abs(matchedActual - predValue);
        if (deviation > maxDeviation) {
          maxDeviation = deviation;
          maxDeviationTime = timestamp;
          actualValue = matchedActual;
          predictedValue = predValue;
        }
      }
    }

    // Check if deviation exceeds threshold
    if (maxDeviation >= this.deviationThreshold) {
      // Avoid alerting too frequently (cooldown of 60 seconds)
      const now = Date.now();
      if (now - this.lastDeviationAlertTime > 60000) {
        this.triggerDeviationAlert(maxDeviation, actualValue, predictedValue, maxDeviationTime);
        this.lastDeviationAlertTime = now;
      }
    }
  }

  /**
   * Trigger the deviation alert with popup and sound.
   */
  triggerDeviationAlert(deviation: number, actual: number, predicted: number, timestamp: number): void {
    const time = new Date(timestamp).toLocaleTimeString('en-IN', {
      hour: '2-digit',
      minute: '2-digit',
      hour12: false
    });

    const direction = actual > predicted ? 'ABOVE' : 'BELOW';

    this.deviationAlertMessage = `⚠️ Large Deviation Detected at ${time}!\n` +
      `Actual: ${actual.toFixed(2)} | Predicted: ${predicted.toFixed(2)}\n` +
      `Deviation: ${deviation.toFixed(2)} points ${direction} prediction`;

    this.deviationAlertType = deviation >= 20 ? 'danger' : 'warning';
    this.showDeviationAlert = true;

    // Send Telegram notification with cooldown
    this.sendTelegramDeviationAlert(deviation, actual, predicted, time);

    // Sound alert disabled - showing visual notification only
    // this.playAlertSound();

    // Auto-close after 8 seconds
    if (this.alertTimeout) {
      clearTimeout(this.alertTimeout);
    }
    this.alertTimeout = setTimeout(() => {
      this.showDeviationAlert = false;
    }, 8000);

    console.log('Deviation Alert Triggered:', this.deviationAlertMessage);
  }

  /**
   * Send deviation alert to Telegram with cooldown to prevent spam.
   */
  sendTelegramDeviationAlert(deviation: number, actual: number, predicted: number, time: string): void {
    if (!this.telegramAlertsEnabled) {
      return;
    }

    const now = Date.now();
    if (now - this.lastTelegramAlertTime < this.telegramAlertCooldownMs) {
      console.log('Telegram alert skipped - cooldown active');
      return;
    }

    this.telegramService.alertDeviation(deviation, actual, predicted, time).subscribe({
      next: (response) => {
        if (response.success) {
          console.log('Telegram deviation alert sent successfully');
          this.lastTelegramAlertTime = now;
        } else {
          console.warn('Telegram alert failed:', response.error);
        }
      },
      error: (err) => {
        console.error('Error sending Telegram alert:', err);
      }
    });
  }

  /**
   * Play alert sound.
   */
  playAlertSound(): void {
    if (this.alertAudio) {
      this.alertAudio.currentTime = 0;
      this.alertAudio.play().catch(err => {
        console.warn('Could not play alert sound:', err);
      });
    }
  }

  /**
   * Manually dismiss the alert.
   */
  dismissAlert(): void {
    this.showDeviationAlert = false;
    if (this.alertTimeout) {
      clearTimeout(this.alertTimeout);
    }
  }

  updateEMAChart(): void {
    if (!this.emaChartData) return;

    // Update EMA chart series
    if (this.emaChartRef) {
      // Update Price series
      if (this.emaChartData.priceSeries && this.emaChartRef.series[0]) {
        this.emaChartRef.series[0].setData(this.emaChartData.priceSeries, false);
      }
      // Update 9 EMA series
      if (this.emaChartData.ema9 && this.emaChartRef.series[1]) {
        this.emaChartRef.series[1].setData(this.emaChartData.ema9, false);
      }
      // Update 21 EMA series
      if (this.emaChartData.ema21 && this.emaChartRef.series[2]) {
        this.emaChartRef.series[2].setData(this.emaChartData.ema21, false);
      }
      // Update 50 EMA series
      if (this.emaChartData.ema50 && this.emaChartRef.series[3]) {
        this.emaChartRef.series[3].setData(this.emaChartData.ema50, false);
      }
      this.emaChartRef.redraw();
    } else {
      // Fallback: Update via options
      const series = this.emaChartOptions.series as Highcharts.SeriesOptionsType[];
      if (series && series.length >= 4) {
        (series[0] as any).data = this.emaChartData.priceSeries || [];
        (series[1] as any).data = this.emaChartData.ema9 || [];
        (series[2] as any).data = this.emaChartData.ema21 || [];
        (series[3] as any).data = this.emaChartData.ema50 || [];
      }
      this.emaUpdateFlag = true;
    }
  }

  updateChartData(): void {
    this.predictedData = []; this.actualData = [];
    const sortedPredictions = [...this.latestPredictions].sort((a, b) => new Date(a.candleStartTime).getTime() - new Date(b.candleStartTime).getTime());
    for (const pred of sortedPredictions) {
      let timestamp: number;
      if (typeof pred.candleStartTime === 'string') timestamp = new Date(pred.candleStartTime).getTime();
      else if (Array.isArray(pred.candleStartTime)) { const [year, month, day, hour, minute, second] = pred.candleStartTime as unknown as number[]; timestamp = new Date(year, month - 1, day, hour, minute, second || 0).getTime(); }
      else continue;
      if (pred.closePrice !== null && pred.closePrice !== undefined) this.predictedData.push([timestamp, pred.closePrice]);
      if (pred.actualClosePrice !== null && pred.actualClosePrice !== undefined && pred.actualClosePrice > 0) this.actualData.push([timestamp, pred.actualClosePrice]);
    }
    if (this.chartRef) {
      if (this.chartRef.series[0]) this.chartRef.series[0].setData(this.predictedData, false);
      if (this.chartRef.series[1]) this.chartRef.series[1].setData(this.actualData, false);
      if (this.predictedData.length > 0) { const minTime = Math.min(...this.predictedData.map(d => d[0])); const maxTime = Math.max(...this.predictedData.map(d => d[0])); this.chartRef.xAxis[0].setExtremes(minTime - 60000, maxTime + 60000, false); }
      this.chartRef.redraw();
    } else {
      if (this.chartOptions.series) { (this.chartOptions.series[0] as Highcharts.SeriesLineOptions).data = this.predictedData; (this.chartOptions.series[1] as Highcharts.SeriesLineOptions).data = this.actualData; }
      if (this.predictedData.length > 0) { const minTime = Math.min(...this.predictedData.map(d => d[0])); const maxTime = Math.max(...this.predictedData.map(d => d[0])); if (this.chartOptions.xAxis) { (this.chartOptions.xAxis as Highcharts.XAxisOptions).min = minTime - 60000; (this.chartOptions.xAxis as Highcharts.XAxisOptions).max = maxTime + 60000; } }
      this.updateFlag = false; setTimeout(() => this.updateFlag = true, 0);
    }
  }

  startAutoRefresh(): void {
    // Ticker is polled by TickerService; ensure it's started
    this.tickerService.start();

    // Periodic refreshes for job status and other analytical data
    this.refreshSubscription = interval(30000).subscribe(() => {
      this.loadJobStatus();
      this.loadDeviations();
      this.loadTradeSetups();
      this.loadTradeRecommendation();
    });

    this.chartRefreshSubscription = interval(30000).subscribe(() => {
      console.log('Auto-refreshing chart data...');
      this.loadPredictions();
      this.loadRollingChartData();
      this.loadEMAChartData();
    });
  }

  stopAutoRefresh(): void { if (this.refreshSubscription) this.refreshSubscription.unsubscribe(); if (this.chartRefreshSubscription) this.chartRefreshSubscription.unsubscribe(); if (this.liveTickRefreshSubscription) this.liveTickRefreshSubscription.unsubscribe(); }

  // Job Control Methods
  toggleJob(): void { if (this.isJobActive) this.stopJob(); else this.startJob(); }

  /**
   * Auto-start the prediction job if:
   * 1. Ticker is connected
   * 2. Market is open
   * 3. Job is not already active
   */
  autoStartJobIfEligible(): void {
    if (this.isTickerConnected && this.dataService.isMarketOpen() && !this.isJobActive && !this.isLoading) {
      console.log('Auto-starting Candle Prediction job - ticker connected and market is open');
      this.startJob();
    }
  }

  startJob(): void { this.isLoading = true; this.dataService.startPredictionJob().subscribe({ next: (response) => { this.isJobActive = response.isActive; this.loadJobStatus(); this.isLoading = false; }, error: (err) => { console.error('Error starting job:', err); this.isLoading = false; } }); }
  stopJob(): void { this.isLoading = true; this.dataService.stopPredictionJob().subscribe({ next: (response) => { this.isJobActive = response.isActive; this.loadJobStatus(); this.isLoading = false; }, error: (err) => { console.error('Error stopping job:', err); this.isLoading = false; } }); }
  executePredictionNow(): void { this.isLoading = true; this.dataService.executePredictionJobNow().subscribe({ next: (response) => { if (response.predictions) { this.latestPredictions = response.predictions; this.loadRollingChartData(); } this.isLoading = false; }, error: (err) => { console.error('Error executing prediction:', err); this.isLoading = false; } }); }
  verifyPredictionsNow(): void { this.isLoading = true; this.dataService.verifyPredictions().subscribe({ next: (result) => { this.loadPredictions(); this.loadDeviations(); this.loadJobStatus(); this.isLoading = false; }, error: (err) => { console.error('Error verifying predictions:', err); this.isLoading = false; } }); }
  calculateDeviationNow(): void { this.isLoading = true; this.dataService.calculateDeviation().subscribe({ next: (result: any) => { if (result && result.message) console.log('Deviation message:', result.message); this.loadDeviations(); this.isLoading = false; }, error: (err) => { console.error('Error calculating deviation:', err); this.isLoading = false; } }); }
  refreshAll(): void { this.loadInitialData(); }

  // Utility methods
  getDirectionClass(direction: string | null | undefined): string { if (!direction) return 'neutral'; const dir = direction.toUpperCase(); switch (dir) { case 'BULLISH': case 'BUY': case 'STRONG_BULLISH': return 'bullish'; case 'BEARISH': case 'SELL': case 'STRONG_BEARISH': return 'bearish'; case 'NONE': case 'NEUTRAL': default: return 'neutral'; } }
  getSignalClass(signal: string | null | undefined): string { if (!signal) return 'neutral'; const s = signal.toUpperCase(); switch (s) { case 'BUY': return 'signal-buy'; case 'SELL': return 'signal-sell'; case 'HOLD': default: return 'signal-hold'; } }
  getSignalIcon(signal: string | null | undefined): string { if (!signal) return 'pause'; const s = signal.toUpperCase(); switch (s) { case 'BUY': return 'arrow_upward'; case 'SELL': return 'arrow_downward'; case 'HOLD': default: return 'pause'; } }
  getBiasIcon(bias: string | null | undefined): string { if (!bias) return 'trending_flat'; const b = bias.toUpperCase(); switch (b) { case 'BULLISH': case 'BUY': return 'trending_up'; case 'BEARISH': case 'SELL': return 'trending_down'; default: return 'trending_flat'; } }
  formatNumber(value: number | undefined | null, decimals: number = 2): string { if (value === undefined || value === null) return '-'; return value.toFixed(decimals); }
  formatPercent(value: number | undefined | null): string { if (value === undefined || value === null) return '-'; return value.toFixed(2) + '%'; }
  formatTime(dateStr: string | undefined | null): string { if (!dateStr) return '-'; return new Date(dateStr).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' }); }

  trackByPrediction(_: number, pred: PredictedCandleStick): number { return pred.predictionSequence; }
  trackBySetup(_: number, setup: TradeSetup): number { return setup.id; }
  trackByDeviation(_: number, dev: PredictionDeviation): string { return dev.batchId; }
}
