import { Component, OnInit, OnDestroy, ViewChild, AfterViewInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatTableModule, MatTableDataSource } from '@angular/material/table';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatTabsModule } from '@angular/material/tabs';
import { MatChipsModule } from '@angular/material/chips';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatSlideToggleModule } from '@angular/material/slide-toggle';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatDatepickerModule } from '@angular/material/datepicker';
import { MatNativeDateModule } from '@angular/material/core';
import { MatExpansionModule } from '@angular/material/expansion';
import { MatDividerModule } from '@angular/material/divider';
import { MatPaginatorModule, MatPaginator } from '@angular/material/paginator';
import { DataService } from '../../services/data.service';
import { interval, Subscription } from 'rxjs';
import * as XLSX from 'xlsx';
import {
  IOBTradeResult,
  IOBAutoTradeConfig,
  IOBTradingSummary,
  IOBPerformanceStats,
  IOBBacktestResult,
  IOBRiskMetrics,
  IOBMTFAnalysis
} from '../../models/iob.model';

@Component({
  selector: 'app-iob-performance',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatCardModule,
    MatTableModule,
    MatButtonModule,
    MatIconModule,
    MatTabsModule,
    MatChipsModule,
    MatProgressSpinnerModule,
    MatTooltipModule,
    MatSlideToggleModule,
    MatFormFieldModule,
    MatInputModule,
    MatSelectModule,
    MatDatepickerModule,
    MatNativeDateModule,
    MatExpansionModule,
    MatDividerModule,
    MatPaginatorModule
  ],
  templateUrl: './iob-performance.component.html',
  styleUrls: ['./iob-performance.component.css']
})
export class IobPerformanceComponent implements OnInit, OnDestroy, AfterViewInit {

  // Paginator for backtest trades - using setter to handle dynamic binding
  private _backtestPaginator!: MatPaginator;

  @ViewChild('backtestPaginator')
  set backtestPaginator(paginator: MatPaginator) {
    this._backtestPaginator = paginator;
    if (paginator && this.backtestTradesDataSource) {
      // Use setTimeout to avoid ExpressionChangedAfterItHasBeenCheckedError
      setTimeout(() => {
        this.backtestTradesDataSource.paginator = paginator;
      });
    }
  }

  get backtestPaginator(): MatPaginator {
    return this._backtestPaginator;
  }

  ngAfterViewInit(): void {
    // Ensure paginator is connected after view initializes
    if (this._backtestPaginator && this.backtestTradesDataSource) {
      this.backtestTradesDataSource.paginator = this._backtestPaginator;
    }
  }

  // Loading states
  isLoading = false;
  isRunningBacktest = false;

  // Data
  autoTradeConfig: IOBAutoTradeConfig | null = null;
  todaySummary: IOBTradingSummary | null = null;
  performanceStats: IOBPerformanceStats | null = null;
  riskMetrics: IOBRiskMetrics | null = null;
  openTrades: IOBTradeResult[] = [];
  todaysTrades: IOBTradeResult[] = [];
  backtestResult: IOBBacktestResult | null = null;
  mtfAnalysis: IOBMTFAnalysis | null = null;

  // Backtest trades data source for pagination
  backtestTradesDataSource = new MatTableDataSource<IOBTradeResult>([]);

  // Backtest form
  backtestInstrument = 256265;
  backtestStartDate: Date = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
  backtestEndDate: Date = new Date();
  backtestTimeframe = '5min';

  // Config form
  configMinConfidence = 85; // Minimum 85% confidence for auto-trading
  configMaxDistance = 0.5;
  configMaxTrades = 3;
  configDailyLoss = 5000;
  configRiskPerTrade = 1000;
  configRequireFVG = false;
  configRequireHTF = false;

  // Table columns
  tradeColumns = [
    'tradeId', 'instrumentName', 'tradeDirection', 'entryPrice', 'exitPrice',
    'pointsCaptured', 'achievedRR', 'status', 'exitReason', 'netPnl'
  ];

  // Backtest table columns (with time, confidence, zone, and targets)
  backtestColumns = [
    'index', 'entryTime', 'iobType', 'tradeDirection', 'confidence',
    'zone', 'entryPrice', 'stopLoss', 'targets', 'exitPrice', 'pointsCaptured', 'achievedRR', 'result', 'exitReason'
  ];

  private refreshSub?: Subscription;

  constructor(private dataService: DataService) {}

  ngOnInit(): void {
    this.loadAllData();
    this.startAutoRefresh();
  }

  ngOnDestroy(): void {
    this.stopAutoRefresh();
  }

  loadAllData(): void {
    this.isLoading = true;
    this.loadAutoTradeStatus();
    this.loadTodaySummary();
    this.loadPerformanceStats();
    this.loadRiskMetrics();
    this.loadOpenTrades();
    this.loadMTFAnalysis();
  }

  loadAutoTradeStatus(): void {
    this.dataService.getIOBAutoTradeStatus().subscribe({
      next: (data) => {
        this.autoTradeConfig = data;
        this.updateConfigForm();
        this.isLoading = false;
      },
      error: (err) => {
        console.error('Error loading auto-trade status:', err);
        this.isLoading = false;
      }
    });
  }

  loadTodaySummary(): void {
    this.dataService.getTodaysIOBTrades().subscribe({
      next: (data) => {
        this.todaySummary = data;
        this.todaysTrades = data.trades || [];
      },
      error: (err) => console.error('Error loading today summary:', err)
    });
  }

  loadPerformanceStats(): void {
    this.dataService.getIOBPerformance().subscribe({
      next: (data) => {
        this.performanceStats = data;
      },
      error: (err) => console.error('Error loading performance:', err)
    });
  }

  loadRiskMetrics(): void {
    this.dataService.getIOBRiskMetrics().subscribe({
      next: (data) => {
        this.riskMetrics = data;
      },
      error: (err) => console.error('Error loading risk metrics:', err)
    });
  }

  loadOpenTrades(): void {
    this.dataService.getOpenIOBTrades().subscribe({
      next: (data) => {
        this.openTrades = data.trades || [];
      },
      error: (err) => console.error('Error loading open trades:', err)
    });
  }

  loadMTFAnalysis(): void {
    this.dataService.getIOBMTFAnalysisAll().subscribe({
      next: (data) => {
        this.mtfAnalysis = data.NIFTY;
      },
      error: (err) => console.error('Error loading MTF analysis:', err)
    });
  }

  startAutoRefresh(): void {
    this.refreshSub = interval(30000).subscribe(() => {
      this.loadTodaySummary();
      this.loadOpenTrades();
      this.loadRiskMetrics();
    });
  }

  stopAutoRefresh(): void {
    if (this.refreshSub) {
      this.refreshSub.unsubscribe();
    }
  }

  // Auto Trade Controls

  toggleAutoTrading(): void {
    if (this.autoTradeConfig?.autoTradingEnabled) {
      this.dataService.disableIOBAutoTrade().subscribe({
        next: () => {
          this.loadAutoTradeStatus();
        },
        error: (err) => console.error('Error disabling auto-trade:', err)
      });
    } else {
      this.dataService.enableIOBAutoTrade().subscribe({
        next: () => {
          this.loadAutoTradeStatus();
        },
        error: (err) => console.error('Error enabling auto-trade:', err)
      });
    }
  }

  updateConfigForm(): void {
    if (this.autoTradeConfig) {
      this.configMinConfidence = this.autoTradeConfig.minConfidence;
      this.configMaxDistance = this.autoTradeConfig.maxZoneDistancePercent;
      this.configMaxTrades = this.autoTradeConfig.maxOpenTrades;
      this.configDailyLoss = this.autoTradeConfig.dailyLossLimit;
      this.configRiskPerTrade = this.autoTradeConfig.riskPerTrade;
      this.configRequireFVG = this.autoTradeConfig.requireFVG;
      this.configRequireHTF = this.autoTradeConfig.requireHTFAlignment;
    }
  }

  saveConfig(): void {
    const config = {
      minConfidence: this.configMinConfidence,
      maxZoneDistancePercent: this.configMaxDistance,
      maxOpenTrades: this.configMaxTrades,
      dailyLossLimit: this.configDailyLoss,
      riskPerTrade: this.configRiskPerTrade,
      requireFVG: this.configRequireFVG,
      requireHTFAlignment: this.configRequireHTF
    };

    this.dataService.updateIOBAutoTradeConfig(config).subscribe({
      next: () => {
        this.loadAutoTradeStatus();
        alert('Configuration saved successfully!');
      },
      error: (err) => {
        console.error('Error saving config:', err);
        alert('Failed to save configuration');
      }
    });
  }

  // Trade Management

  exitAllTrades(): void {
    if (!confirm('Exit all open trades?')) return;

    this.dataService.exitAllIOBTrades('MANUAL_EXIT').subscribe({
      next: (data) => {
        alert(`Exited ${data.exitedCount} trades`);
        this.loadOpenTrades();
        this.loadTodaySummary();
      },
      error: (err) => {
        console.error('Error exiting trades:', err);
        alert('Failed to exit trades');
      }
    });
  }

  // Backtesting

  runBacktest(): void {
    this.isRunningBacktest = true;
    this.backtestResult = null;
    this.backtestTradesDataSource.data = [];

    const startDate = this.formatDate(this.backtestStartDate);
    const endDate = this.formatDate(this.backtestEndDate);

    this.dataService.runIOBBacktest(
      this.backtestInstrument,
      startDate,
      endDate,
      this.backtestTimeframe
    ).subscribe({
      next: (data) => {
        this.backtestResult = data;
        this.backtestTradesDataSource.data = data.trades || [];
        this.isRunningBacktest = false;

        // Need to wait for the DOM to render the paginator (since it's inside *ngIf)
        // then reconnect the paginator to the data source
        setTimeout(() => {
          if (this._backtestPaginator) {
            this.backtestTradesDataSource.paginator = this._backtestPaginator;
          }
        }, 100);
      },
      error: (err) => {
        console.error('Error running backtest:', err);
        this.isRunningBacktest = false;
        alert('Backtest failed: ' + err.message);
      }
    });
  }

  // Helpers

  formatDateTime(dateString: string | undefined): string {
    if (!dateString) return '--';
    try {
      const date = new Date(dateString);
      return date.toLocaleString('en-IN', {
        day: '2-digit',
        month: 'short',
        hour: '2-digit',
        minute: '2-digit'
      });
    } catch {
      return dateString;
    }
  }

  formatDate(date: Date): string {
    return date.toISOString().split('T')[0];
  }

  formatNumber(value: number | undefined | null, decimals: number = 2): string {
    if (value === undefined || value === null) return '--';
    return value.toFixed(decimals);
  }

  formatPercent(value: number | undefined | null): string {
    if (value === undefined || value === null) return '--';
    return `${value.toFixed(1)}%`;
  }

  formatCurrency(value: number | undefined | null): string {
    if (value === undefined || value === null) return '--';
    return `₹${value.toFixed(2)}`;
  }

  getStatusClass(status: string | undefined): string {
    if (!status) return '';
    switch (status) {
      case 'OPEN': return 'status-open';
      case 'CLOSED': return 'status-closed';
      case 'PENDING': return 'status-pending';
      default: return '';
    }
  }

  getResultClass(isWinner: boolean | undefined): string {
    if (isWinner === undefined || isWinner === null) return '';
    return isWinner ? 'result-win' : 'result-loss';
  }

  getDirectionClass(direction: string | undefined): string {
    if (!direction) return '';
    return direction === 'LONG' ? 'direction-long' : 'direction-short';
  }

  getBiasClass(bias: string | undefined): string {
    if (!bias) return '';
    switch (bias) {
      case 'BULLISH': return 'bias-bullish';
      case 'BEARISH': return 'bias-bearish';
      default: return 'bias-neutral';
    }
  }

  // Export backtest results to Excel
  exportToExcel(): void {
    if (!this.backtestResult || !this.backtestResult.trades || this.backtestResult.trades.length === 0) {
      alert('No backtest results to export');
      return;
    }

    // Prepare summary data
    const summaryData = [
      ['IOB Backtest Results Summary'],
      [''],
      ['Parameter', 'Value'],
      ['Instrument', 'NIFTY'],
      ['Start Date', this.formatDate(this.backtestStartDate)],
      ['End Date', this.formatDate(this.backtestEndDate)],
      ['Timeframe', this.backtestTimeframe],
      [''],
      ['Metric', 'Value'],
      ['Total Trades', this.backtestResult.totalTrades],
      ['Wins', this.backtestResult.wins],
      ['Losses', this.backtestResult.losses],
      ['Win Rate', `${this.backtestResult.winRate?.toFixed(1)}%`],
      ['Average RR', this.backtestResult.averageRR?.toFixed(2)],
      ['Total P&L', `₹${this.backtestResult.totalPnl?.toFixed(2)}`],
      ['Total Points', this.backtestResult.totalPointsCaptured?.toFixed(2)],
      ['']
    ];

    // Prepare trades data with headers
    const tradesData = [
      ['Trade Details'],
      ['#', 'Entry Time', 'IOB Type', 'Direction', 'Confidence %', 'Zone Low', 'Zone High',
       'Entry Price', 'Stop Loss', 'Target 1', 'Target 2', 'Target 3', 'Exit Price',
       'Points Captured', 'RR', 'Result', 'Exit Reason', 'P&L']
    ];

    // Add each trade
    this.backtestResult.trades.forEach((trade, index) => {
      tradesData.push([
        (index + 1).toString(),
        trade.entryTime || '--',
        trade.iobType || '--',
        trade.tradeDirection || '--',
        trade.signalConfidence?.toFixed(1) || '--',
        trade.zoneLow?.toFixed(2) || '--',
        trade.zoneHigh?.toFixed(2) || '--',
        trade.actualEntry?.toFixed(2) || '--',
        trade.actualStopLoss?.toFixed(2) || '--',
        trade.target1?.toFixed(2) || '--',
        trade.target2?.toFixed(2) || '--',
        trade.target3?.toFixed(2) || '--',
        trade.exitPrice?.toFixed(2) || '--',
        trade.pointsCaptured?.toFixed(2) || '--',
        trade.achievedRR?.toFixed(2) || '--',
        trade.isWinner ? 'WIN' : 'LOSS',
        trade.exitReason || '--',
        trade.netPnl?.toFixed(2) || '--'
      ]);
    });

    // Create workbook and worksheets
    const workbook = XLSX.utils.book_new();

    // Create summary worksheet
    const summaryWs = XLSX.utils.aoa_to_sheet(summaryData);
    // Set column widths
    summaryWs['!cols'] = [{ wch: 20 }, { wch: 25 }];
    XLSX.utils.book_append_sheet(workbook, summaryWs, 'Summary');

    // Create trades worksheet
    const tradesWs = XLSX.utils.aoa_to_sheet(tradesData);
    // Set column widths for trades
    tradesWs['!cols'] = [
      { wch: 5 },   // #
      { wch: 20 },  // Entry Time
      { wch: 15 },  // IOB Type
      { wch: 10 },  // Direction
      { wch: 12 },  // Confidence
      { wch: 12 },  // Zone Low
      { wch: 12 },  // Zone High
      { wch: 12 },  // Entry Price
      { wch: 12 },  // Stop Loss
      { wch: 12 },  // Target 1
      { wch: 12 },  // Target 2
      { wch: 12 },  // Target 3
      { wch: 12 },  // Exit Price
      { wch: 15 },  // Points Captured
      { wch: 8 },   // RR
      { wch: 8 },   // Result
      { wch: 15 },  // Exit Reason
      { wch: 12 }   // P&L
    ];
    XLSX.utils.book_append_sheet(workbook, tradesWs, 'Trades');

    // Generate filename with date
    const instrument = this.backtestInstrument === 256265 ? 'NIFTY' : 'SENSEX';
    const filename = `IOB_Backtest_${instrument}_${this.formatDate(this.backtestStartDate)}_to_${this.formatDate(this.backtestEndDate)}.xlsx`;

    // Write and download
    XLSX.writeFile(workbook, filename);
  }
}
