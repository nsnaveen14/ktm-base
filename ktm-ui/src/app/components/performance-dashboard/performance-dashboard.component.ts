import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatTableModule } from '@angular/material/table';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatTabsModule } from '@angular/material/tabs';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatChipsModule } from '@angular/material/chips';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatSelectModule } from '@angular/material/select';
import { MatInputModule } from '@angular/material/input';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatDatepickerModule } from '@angular/material/datepicker';
import { MatNativeDateModule } from '@angular/material/core';
import { MatExpansionModule } from '@angular/material/expansion';
import { MatDividerModule } from '@angular/material/divider';
import { MatSnackBar, MatSnackBarModule } from '@angular/material/snack-bar';
import { Subscription } from 'rxjs';
import { PerformanceService } from '../../services/performance.service';
import {
  PerformanceDashboard,
  PerformanceMetrics,
  TradeResult,
  BacktestResult
} from '../../models/performance.model';

@Component({
  selector: 'app-performance-dashboard',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatCardModule,
    MatTableModule,
    MatButtonModule,
    MatIconModule,
    MatTabsModule,
    MatProgressSpinnerModule,
    MatChipsModule,
    MatTooltipModule,
    MatSelectModule,
    MatInputModule,
    MatFormFieldModule,
    MatDatepickerModule,
    MatNativeDateModule,
    MatExpansionModule,
    MatDividerModule,
    MatSnackBarModule
  ],
  templateUrl: './performance-dashboard.component.html',
  styleUrls: ['./performance-dashboard.component.css']
})
export class PerformanceDashboardComponent implements OnInit, OnDestroy {
  isLoading = false;

  // Dashboard data
  dashboard: PerformanceDashboard | null = null;
  allTimeMetrics: PerformanceMetrics | null = null;
  recentTrades: TradeResult[] = [];
  openTrades: TradeResult[] = [];

  // Backtest
  isRunningBacktest = false;
  backtestResult: BacktestResult | null = null;
  backtestHistory: any[] = [];

  // Backtest form
  backtestInstrument = 256265; // NIFTY
  backtestStartDate: Date | null = null;
  backtestEndDate: Date | null = null;

  // Analysis data
  performanceByIOBType: any = null;
  performanceByConfidence: any = null;
  drawdownAnalysis: any = null;

  // Table columns
  tradeColumns = [
    'tradeId', 'instrumentName', 'direction', 'entryPrice', 'exitPrice',
    'netPnl', 'pnlPercent', 'outcome', 'exitReason', 'duration'
  ];

  private subscriptions: Subscription[] = [];

  constructor(
    private performanceService: PerformanceService,
    private snackBar: MatSnackBar
  ) {}

  ngOnInit(): void {
    this.loadDashboard();
    this.loadBacktestHistory();
    this.loadAnalysisData();
  }

  ngOnDestroy(): void {
    this.subscriptions.forEach(sub => sub.unsubscribe());
  }

  // ==================== Data Loading ====================

  loadDashboard(): void {
    this.isLoading = true;

    this.performanceService.getPerformanceDashboard().subscribe({
      next: (data) => {
        this.dashboard = data;
        this.allTimeMetrics = data.allTimeMetrics;
        this.recentTrades = data.recentTrades || [];
        this.openTrades = data.openTrades || [];
        this.isLoading = false;
      },
      error: (err) => {
        console.error('Error loading dashboard:', err);
        this.showNotification('Failed to load dashboard', 'error');
        this.isLoading = false;
      }
    });
  }

  loadBacktestHistory(): void {
    this.performanceService.getBacktestHistory().subscribe({
      next: (history) => {
        this.backtestHistory = history;
      },
      error: (err) => {
        console.error('Error loading backtest history:', err);
      }
    });
  }

  loadAnalysisData(): void {
    this.performanceService.getPerformanceByIOBType().subscribe({
      next: (data) => this.performanceByIOBType = data,
      error: (err) => console.error('Error loading IOB type analysis:', err)
    });

    this.performanceService.getPerformanceByConfidence().subscribe({
      next: (data) => this.performanceByConfidence = data,
      error: (err) => console.error('Error loading confidence analysis:', err)
    });

    this.performanceService.getDrawdownAnalysis().subscribe({
      next: (data) => this.drawdownAnalysis = data,
      error: (err) => console.error('Error loading drawdown analysis:', err)
    });
  }

  // ==================== Backtest ====================

  runBacktest(): void {
    if (!this.backtestStartDate || !this.backtestEndDate) {
      this.showNotification('Please select date range', 'error');
      return;
    }

    this.isRunningBacktest = true;

    const startDate = this.formatDateTime(this.backtestStartDate);
    const endDate = this.formatDateTime(this.backtestEndDate);

    this.performanceService.runBacktest(
      this.backtestInstrument,
      '5minute',
      startDate,
      endDate
    ).subscribe({
      next: (result) => {
        this.backtestResult = result;
        this.isRunningBacktest = false;
        this.loadBacktestHistory();
        this.showNotification('Backtest completed', 'success');
      },
      error: (err) => {
        console.error('Error running backtest:', err);
        this.showNotification('Backtest failed', 'error');
        this.isRunningBacktest = false;
      }
    });
  }

  viewBacktestResult(backtestId: string): void {
    this.performanceService.getBacktestResults(backtestId).subscribe({
      next: (result) => {
        this.backtestResult = result;
      },
      error: (err) => {
        console.error('Error loading backtest result:', err);
        this.showNotification('Failed to load backtest result', 'error');
      }
    });
  }

  // ==================== Actions ====================

  recalculateMetrics(): void {
    this.isLoading = true;
    this.performanceService.recalculateMetrics().subscribe({
      next: () => {
        this.showNotification('Metrics recalculated', 'success');
        this.loadDashboard();
      },
      error: (err) => {
        console.error('Error recalculating metrics:', err);
        this.showNotification('Failed to recalculate metrics', 'error');
        this.isLoading = false;
      }
    });
  }

  closeTrade(trade: TradeResult): void {
    const exitPrice = prompt('Enter exit price:');
    if (!exitPrice) return;

    this.performanceService.closeTrade(trade.id, parseFloat(exitPrice), 'MANUAL').subscribe({
      next: () => {
        this.showNotification('Trade closed', 'success');
        this.loadDashboard();
      },
      error: (err) => {
        console.error('Error closing trade:', err);
        this.showNotification('Failed to close trade', 'error');
      }
    });
  }

  // ==================== Formatting Helpers ====================

  formatNumber(value: number | undefined | null, decimals: number = 2): string {
    if (value === undefined || value === null) return '--';
    return value.toFixed(decimals);
  }

  formatPercent(value: number | undefined | null): string {
    if (value === undefined || value === null) return '--';
    return value.toFixed(2) + '%';
  }

  formatCurrency(value: number | undefined | null): string {
    if (value === undefined || value === null) return '--';
    return '₹' + value.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }

  formatDuration(minutes: number | undefined | null): string {
    if (!minutes) return '--';
    if (minutes < 60) return `${minutes}m`;
    const hours = Math.floor(minutes / 60);
    const mins = minutes % 60;
    return `${hours}h ${mins}m`;
  }

  formatDateTime(date: Date): string {
    return date.toISOString();
  }

  // ==================== UI Helpers ====================

  getOutcomeClass(outcome: string): string {
    switch (outcome) {
      case 'WIN': return 'outcome-win';
      case 'LOSS': return 'outcome-loss';
      case 'BREAKEVEN': return 'outcome-breakeven';
      default: return '';
    }
  }

  getPnlClass(pnl: number | undefined | null): string {
    if (!pnl) return '';
    return pnl >= 0 ? 'pnl-positive' : 'pnl-negative';
  }

  getDirectionClass(direction: string): string {
    return direction === 'LONG' ? 'direction-long' : 'direction-short';
  }

  private showNotification(message: string, type: 'success' | 'error'): void {
    this.snackBar.open(message, 'Close', {
      duration: 3000,
      horizontalPosition: 'right',
      verticalPosition: 'top',
      panelClass: type === 'error' ? ['error-snackbar'] : ['success-snackbar']
    });
  }
}
