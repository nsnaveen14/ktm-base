import { Component, OnInit, OnDestroy, Inject, ChangeDetectionStrategy, ChangeDetectorRef } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatButtonModule } from '@angular/material/button';
import { MatCardModule } from '@angular/material/card';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatSlideToggleModule } from '@angular/material/slide-toggle';
import { MatChipsModule } from '@angular/material/chips';
import { MatBadgeModule } from '@angular/material/badge';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatDividerModule } from '@angular/material/divider';
import { MatTableModule } from '@angular/material/table';
import { MatInputModule } from '@angular/material/input';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatTabsModule } from '@angular/material/tabs';
import { MatExpansionModule } from '@angular/material/expansion';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatSnackBar, MatSnackBarModule } from '@angular/material/snack-bar';
import { MatDialog, MatDialogModule, MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { interval, Subscription } from 'rxjs';
import { DataService } from '../../services/data.service';
import { LiveTickComponent } from '../live-tick/live-tick.component';
import { PerformanceChartsComponent } from './performance-charts/performance-charts.component';
import { SimulatedTrade, TradingSummary, TradingConfig, TradingLedger } from '../../models/analytics.model';

@Component({
  selector: 'app-trading',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatButtonModule,
    MatCardModule,
    MatIconModule,
    MatProgressSpinnerModule,
    MatSlideToggleModule,
    MatChipsModule,
    MatBadgeModule,
    MatTooltipModule,
    MatDividerModule,
    MatTableModule,
    MatInputModule,
    MatFormFieldModule,
    MatSelectModule,
    MatTabsModule,
    MatExpansionModule,
    MatCheckboxModule,
    MatSnackBarModule,
    MatDialogModule,
    LiveTickComponent,
    PerformanceChartsComponent
  ],
  templateUrl: './trading.component.html',
  styleUrls: ['./trading.component.css'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TradingComponent implements OnInit, OnDestroy {

  // Trading state
  isAutoTradingEnabled = false;
  isLoading = false;

  // Data
  tradingSummary: TradingSummary | null = null;
  openTrades: SimulatedTrade[] = [];
  todaysTrades: SimulatedTrade[] = [];
  // Control whether the Today's Trades expansion panel is expanded
  todaysTradesExpanded = false;
  tradingConfig: TradingConfig | null = null;
  todaysLedger: TradingLedger | null = null;
  currentSignal: any = null;

  // Selection state for discarding trades
  selectedTodaysTradeIds: Set<string> = new Set<string>();
  selectAllTodayTrades = false;

  // Config edit mode
  isEditingConfig = false;
  editConfig: Partial<TradingConfig> = {};

  // Auto-refresh
  private refreshSubscription: Subscription | null = null;
  private monitorSubscription: Subscription | null = null;

  // Table columns
  openTradesColumns = ['tradeId', 'optionType', 'strike', 'entry', 'current', 'pnl', 'target', 'sl', 'actions'];
  closedTradesColumns = ['time', 'tradeId', 'type', 'option', 'entry', 'exit', 'pnl', 'reason'];

  constructor(
    private dataService: DataService,
    private snackBar: MatSnackBar,
    private dialog: MatDialog,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.loadInitialData();
    this.startAutoRefresh();
  }

  ngOnDestroy(): void {
    this.stopAutoRefresh();
  }

  loadInitialData(): void {
    this.loadTradingSummary();
    this.loadOpenTrades();
    this.loadTodaysTrades();
    this.loadTradingConfig();
    this.loadTodaysLedger();
    this.checkSignals();
  }

  loadTradingSummary(): void {
    this.dataService.getTradingSummary().subscribe({
      next: (summary) => {
        this.tradingSummary = summary;
        this.isAutoTradingEnabled = summary.autoTradingEnabled;
        this.cdr.markForCheck();
      },
      error: (err) => console.error('Error loading trading summary:', err)
    });
  }

  loadOpenTrades(): void {
    this.dataService.getOpenTrades().subscribe({
      next: (trades) => {
        this.openTrades = trades;
        this.cdr.markForCheck();
      },
      error: (err) => console.error('Error loading open trades:', err)
    });
  }

  loadTodaysTrades(): void {
    this.dataService.getTodaysTrades().subscribe({
      next: (trades) => {
        this.todaysTrades = trades;
        this.selectedTodaysTradeIds.clear();
        this.selectAllTodayTrades = false;
        this.cdr.markForCheck();
      },
      error: (err) => console.error('Error loading today\'s trades:', err)
    });
  }

  loadTradingConfig(): void {
    this.dataService.getTradingConfig().subscribe({
      next: (config) => {
        this.tradingConfig = config;
        this.cdr.markForCheck();
      },
      error: (err) => console.error('Error loading trading config:', err)
    });
  }

  loadTodaysLedger(): void {
    this.dataService.getTodaysLedger().subscribe({
      next: (ledger) => {
        this.todaysLedger = ledger;
        this.cdr.markForCheck();
      },
      error: (err) => console.error('Error loading ledger:', err)
    });
  }

  checkSignals(): void {
    this.dataService.checkTradeSignals().subscribe({
      next: (signal) => {
        this.currentSignal = signal;
        this.cdr.markForCheck();
      },
      error: (err) => console.error('Error checking signals:', err)
    });
  }

  // ============= Trading Controls =============

  toggleAutoTrading(): void {
    this.isLoading = true;
    const action = this.isAutoTradingEnabled ?
      this.dataService.disableAutoTrading() :
      this.dataService.enableAutoTrading();

    action.subscribe({
      next: (response) => {
        this.isAutoTradingEnabled = response.autoTradingEnabled;
        this.isLoading = false;
        this.cdr.markForCheck();
      },
      error: (err) => {
        console.error('Error toggling auto-trading:', err);
        this.isLoading = false;
        this.cdr.markForCheck();
      }
    });
  }

  manualBuy(): void {
    this.isLoading = true;
    this.dataService.manualBuyTrade('MANUAL').subscribe({
      next: (response) => {
        if (response.success) {
          this.loadOpenTrades();
          this.loadTodaysTrades();
          this.loadTradingSummary();
        }
        this.isLoading = false;
        this.cdr.markForCheck();
      },
      error: (err) => {
        console.error('Error placing buy trade:', err);
        this.isLoading = false;
        this.cdr.markForCheck();
      }
    });
  }

  manualSell(): void {
    this.isLoading = true;
    this.dataService.manualSellTrade('MANUAL').subscribe({
      next: (response) => {
        if (response.success) {
          this.loadOpenTrades();
          this.loadTodaysTrades();
          this.loadTradingSummary();
        }
        this.isLoading = false;
        this.cdr.markForCheck();
      },
      error: (err) => {
        console.error('Error placing sell trade:', err);
        this.isLoading = false;
        this.cdr.markForCheck();
      }
    });
  }

  triggerAutoTrade(): void {
    this.isLoading = true;
    this.dataService.triggerAutoTrade().subscribe({
      next: (response) => {
        if (response.tradePlaced) {
          this.loadOpenTrades();
          this.loadTodaysTrades();
          this.loadTradingSummary();
        }
        this.isLoading = false;
        this.cdr.markForCheck();
      },
      error: (err) => {
        console.error('Error triggering auto-trade:', err);
        this.isLoading = false;
        this.cdr.markForCheck();
      }
    });
  }

  exitTrade(trade: SimulatedTrade): void {
    // Get current price from the trade's unrealized P&L calculation
    const exitPrice = trade.entryPrice; // This should be updated with current price
    this.dataService.exitTrade(trade.tradeId, 'MANUAL', exitPrice).subscribe({
      next: (response) => {
        if (response.success) {
          this.loadOpenTrades();
          this.loadTodaysTrades();
          this.loadTradingSummary();
          this.loadTodaysLedger();
        }
      },
      error: (err) => console.error('Error exiting trade:', err)
    });
  }

  exitAllTrades(): void {
    const dialogRef = this.dialog.open(ConfirmDialogComponent, {
      width: 'auto',
      panelClass: 'custom-dialog-container',
      data: {
        title: 'Exit All Trades',
        message: `Are you sure you want to exit all ${this.openTrades.length} open trade(s)? This action cannot be undone.`,
        confirmText: 'Exit All',
        cancelText: 'Cancel',
        confirmColor: 'warn'
      }
    });

    dialogRef.afterClosed().subscribe(result => {
      if (result) {
        this.isLoading = true;
        this.cdr.markForCheck();
        this.dataService.exitAllTrades('MANUAL').subscribe({
          next: (response) => {
            this.loadOpenTrades();
            this.loadTodaysTrades();
            this.loadTradingSummary();
            this.loadTodaysLedger();
            this.isLoading = false;
            this.cdr.markForCheck();
            this.snackBar.open(`Successfully exited all trades`, 'Close', {
              duration: 3000,
              panelClass: ['success-snackbar']
            });
          },
          error: (err) => {
            console.error('Error exiting all trades:', err);
            this.isLoading = false;
            this.cdr.markForCheck();
            this.snackBar.open('Failed to exit trades', 'Close', {
              duration: 3000,
              panelClass: ['error-snackbar']
            });
          }
        });
      }
    });
  }

  monitorTrades(): void {
    this.dataService.monitorTrades().subscribe({
      next: (response) => {
        if (response.exitedCount > 0) {
          this.loadOpenTrades();
          this.loadTodaysTrades();
          this.loadTradingSummary();
          this.loadTodaysLedger();
        }
      },
      error: (err) => console.error('Error monitoring trades:', err)
    });
  }


  // ============= Configuration =============

  startEditConfig(): void {
    if (this.tradingConfig) {
      this.editConfig = { ...this.tradingConfig };
      this.isEditingConfig = true;
    }
  }

  cancelEditConfig(): void {
    this.isEditingConfig = false;
    this.editConfig = {};
  }

  saveConfig(): void {
    this.isLoading = true;
    this.dataService.updateTradingConfig(this.editConfig).subscribe({
      next: (response) => {
        this.tradingConfig = response.config;
        this.isEditingConfig = false;
        this.isLoading = false;
        this.cdr.markForCheck();
      },
      error: (err) => {
        console.error('Error saving config:', err);
        this.isLoading = false;
        this.cdr.markForCheck();
      }
    });
  }

  // ============= Auto Refresh =============

  startAutoRefresh(): void {
    // Refresh data every 30 seconds
    this.refreshSubscription = interval(30000).subscribe(() => {
      this.loadTradingSummary();
      this.loadOpenTrades();
      this.checkSignals();
    });

    // Monitor trades every 10 seconds (check for target/SL hits)
    this.monitorSubscription = interval(10000).subscribe(() => {
      if (this.openTrades.length > 0) {
        this.monitorTrades();
      }
    });
  }

  stopAutoRefresh(): void {
    if (this.refreshSubscription) {
      this.refreshSubscription.unsubscribe();
    }
    if (this.monitorSubscription) {
      this.monitorSubscription.unsubscribe();
    }
  }

  refreshAll(): void {
    this.loadInitialData();
  }

  // ============= Utility Methods =============

  formatNumber(value: number | undefined | null, decimals: number = 2): string {
    if (value === undefined || value === null) return '-';
    return value.toFixed(decimals);
  }

  formatPercent(value: number | undefined | null): string {
    if (value === undefined || value === null) return '-';
    return value.toFixed(2) + '%';
  }

  formatCurrency(value: number | undefined | null): string {
    if (value === undefined || value === null) return '-';
    const prefix = value >= 0 ? '+₹' : '-₹';
    return prefix + Math.abs(value).toFixed(2);
  }

  formatTime(timestamp: string | undefined): string {
    if (!timestamp) return '-';
    const date = new Date(timestamp);
    return date.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' });
  }

  getPnlClass(pnl: number | undefined | null): string {
    if (pnl === undefined || pnl === null) return '';
    return pnl >= 0 ? 'positive' : 'negative';
  }

  getSignalClass(signal: string | undefined): string {
    if (!signal) return '';
    if (signal === 'BUY' || signal === 'BULLISH') return 'bullish';
    if (signal === 'SELL' || signal === 'BEARISH') return 'bearish';
    return 'neutral';
  }

  getStatusClass(status: string): string {
    switch (status) {
      case 'OPEN': return 'status-open';
      case 'CLOSED': return 'status-closed';
      case 'CANCELLED': return 'status-cancelled';
      default: return '';
    }
  }

  getExitReasonIcon(reason: string | undefined): string {
    switch (reason) {
      case 'TARGET_HIT': return 'emoji_events';
      case 'STOPLOSS_HIT': return 'warning';
      case 'TRAILING_SL': return 'trending_down';
      case 'TIME_EXIT': return 'schedule';
      case 'MANUAL': return 'touch_app';
      default: return 'help';
    }
  }

  toggleSelectTrade(tradeId: string, checked: boolean) {
    if (checked) this.selectedTodaysTradeIds.add(tradeId);
    else this.selectedTodaysTradeIds.delete(tradeId);
    // update selectAll flag
    this.selectAllTodayTrades = this.todaysTrades.length > 0 && this.selectedTodaysTradeIds.size === this.todaysTrades.length;
  }

  toggleSelectAllTodayTrades(checked: boolean) {
    this.selectAllTodayTrades = checked;
    this.selectedTodaysTradeIds.clear();
    if (checked) {
      for (const t of this.todaysTrades) {
        if (t.tradeId) this.selectedTodaysTradeIds.add(t.tradeId);
      }
    }
  }

  discardSelectedTrades(): void {
    // Use snackbar for confirmation instead of native confirm
    if (this.selectedTodaysTradeIds.size === 0) {
      this.snackBar.open('No trades selected to discard', 'OK', { duration: 3000 });
      return;
    }

    const count = this.selectedTodaysTradeIds.size;
    const ids = Array.from(this.selectedTodaysTradeIds);

    const sb = this.snackBar.open(`Discard ${count} selected trade(s)?`, 'Confirm', { duration: 10000 });
    const actionSub = sb.onAction().subscribe(() => {
      actionSub.unsubscribe();
      this.isLoading = true;
      this.cdr.markForCheck();
      this.dataService.discardTrades(ids).subscribe({
        next: (res) => {
          try {
            const discardedCount = res && res.discardedCount ? res.discardedCount : ids.length;
            const confirmedIds = (res && res.discardedTrades) ? res.discardedTrades.map((t: any) => t.tradeId) : ids;
            const idSet = new Set(confirmedIds);
            this.todaysTrades = this.todaysTrades.filter(t => !idSet.has(t.tradeId));
            this.selectedTodaysTradeIds.clear();
            this.selectAllTodayTrades = false;
            this.loadTradingSummary();
            this.loadTodaysLedger();
            this.loadTodaysTrades();
            this.isLoading = false;
            this.cdr.markForCheck();
            this.snackBar.open(`${discardedCount} trade(s) discarded`, 'OK', { duration: 4000 });
          } catch (e) {
            console.error('Error processing discard response', e);
            this.isLoading = false;
            this.cdr.markForCheck();
            this.snackBar.open('Discard completed but response parsing failed', 'OK', { duration: 4000 });
          }
        },
        error: (err) => {
          console.error('Error discarding trades:', err);
          this.isLoading = false;
          this.cdr.markForCheck();
          const msg = err && err.error && (err.error.message || err.error.error) ? (err.error.message || err.error.error) : (err.message || 'Failed to discard trades');
          this.snackBar.open(msg, 'OK', { duration: 6000 });
        }
      });
    });
    // If snackbar expires without action, do nothing; unsubbing is not strictly necessary here but safe cleanup handled by actionSub when triggered
  }
}

// Confirm Dialog Component
@Component({
  selector: 'app-confirm-dialog',
  standalone: true,
  imports: [
    CommonModule,
    MatButtonModule,
    MatDialogModule,
    MatIconModule
  ],
  template: `
    <div class="confirm-dialog-container">
      <div class="dialog-icon">
        <mat-icon>warning_amber</mat-icon>
      </div>
      <h2 class="dialog-title">{{data.title}}</h2>
      <p class="dialog-message">{{data.message}}</p>
      <div class="dialog-actions">
        <button mat-stroked-button class="cancel-btn" (click)="onCancel()">
          {{data.cancelText || 'Cancel'}}
        </button>
        <button mat-raised-button class="confirm-btn" (click)="onConfirm()">
          <mat-icon>exit_to_app</mat-icon>
          {{data.confirmText || 'Confirm'}}
        </button>
      </div>
    </div>
  `,
  styles: [`
    :host {
      display: block;
    }

    .confirm-dialog-container {
      padding: 24px;
      text-align: center;
      background: #1e1e2f;
      border-radius: 12px;
      min-width: 320px;
      max-width: 400px;
    }

    .dialog-icon {
      width: 64px;
      height: 64px;
      margin: 0 auto 16px;
      background: rgba(244, 67, 54, 0.15);
      border-radius: 50%;
      display: flex;
      align-items: center;
      justify-content: center;
    }

    .dialog-icon mat-icon {
      font-size: 36px;
      width: 36px;
      height: 36px;
      color: #ff5252;
    }

    .dialog-title {
      margin: 0 0 12px 0;
      font-size: 20px;
      font-weight: 600;
      color: #ffffff;
    }

    .dialog-message {
      margin: 0 0 24px 0;
      font-size: 14px;
      color: #aaaaaa;
      line-height: 1.5;
    }

    .dialog-actions {
      display: flex;
      gap: 12px;
      justify-content: center;
    }

    .cancel-btn {
      flex: 1;
      padding: 10px 20px;
      border-radius: 8px;
      color: #aaaaaa !important;
      border-color: #444 !important;
    }

    .cancel-btn:hover {
      background: rgba(255, 255, 255, 0.05) !important;
    }

    .confirm-btn {
      flex: 1;
      padding: 10px 20px;
      border-radius: 8px;
      background: linear-gradient(135deg, #f44336 0%, #d32f2f 100%) !important;
      color: white !important;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 6px;
    }

    .confirm-btn mat-icon {
      font-size: 18px;
      width: 18px;
      height: 18px;
    }

    .confirm-btn:hover {
      background: linear-gradient(135deg, #e53935 0%, #c62828 100%) !important;
    }
  `]
})
export class ConfirmDialogComponent {
  constructor(
    public dialogRef: MatDialogRef<ConfirmDialogComponent>,
    @Inject(MAT_DIALOG_DATA) public data: any
  ) {}

  onCancel(): void {
    this.dialogRef.close(false);
  }

  onConfirm(): void {
    this.dialogRef.close(true);
  }
}

