import { Component, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatCardModule } from '@angular/material/card';
import { MatTableModule } from '@angular/material/table';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatTabsModule } from '@angular/material/tabs';
import { MatChipsModule } from '@angular/material/chips';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatDividerModule } from '@angular/material/divider';
import { DataService } from '../../services/data.service';
import { interval, Subscription } from 'rxjs';
import { LiquidityZoneAnalysis, AllIndicesData, MultiTimeframeAnalysis } from '../../models/liquidity.model';
import { LiveTickComponent } from '../live-tick/live-tick.component';

@Component({
  selector: 'app-liquidity-analysis',
  standalone: true,
  imports: [
    CommonModule,
    MatCardModule,
    MatTableModule,
    MatButtonModule,
    MatIconModule,
    MatTabsModule,
    MatChipsModule,
    MatProgressSpinnerModule,
    MatTooltipModule,
    MatDividerModule,
    LiveTickComponent
  ],
  templateUrl: './liquidity-analysis.component.html',
  styleUrls: ['./liquidity-analysis.component.css']
})
export class LiquidityAnalysisComponent implements OnInit, OnDestroy {

  isLoading = false;
  isPlacingTrade = false;
  autoRefresh = true;
  refreshInterval = 30000; // 30 seconds

  niftyData: MultiTimeframeAnalysis | null = null;

  selectedInstrument = 'NIFTY';
  selectedTimeframe: '5min' | '15min' | '1hour' = '5min';

  niftyToken = 256265;

  timeframes = ['5min', '15min', '1hour'];

  displayedColumns = [
    'timeframe',
    'currentPrice',
    'buySideLiquidity',
    'sellSideLiquidity',
    'grabbedLevel',
    'tradeSignal',
    'entryPrice',
    'stopLoss',
    'targets',
    'riskReward',
    'confidence',
    'status'
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

    this.dataService.getAllIndicesLiquidityData().subscribe({
      next: (data: any) => {
        console.log('Liquidity data received:', data);
        this.niftyData = data.NIFTY;
        this.isLoading = false;
      },
      error: (err) => {
        console.error('Error loading liquidity data:', err);
        this.isLoading = false;
      }
    });
  }

  analyzeAll(): void {
    this.isLoading = true;

    this.dataService.triggerLiquidityAnalysis().subscribe({
      next: (data) => {
        console.log('Analysis triggered:', data);
        // Reload data after analysis
        setTimeout(() => {
          this.loadAllData();
        }, 2000); // Wait 2 seconds for analysis to complete
      },
      error: (err) => {
        console.error('Error triggering liquidity analysis:', err);
        this.isLoading = false;
      }
    });
  }

  refreshData(): void {
    this.loadAllData();
  }

  startAutoRefresh(): void {
    if (this.autoRefresh) {
      this.refreshSub = interval(this.refreshInterval).subscribe(() => {
        this.loadAllData();
      });
    }
  }

  stopAutoRefresh(): void {
    if (this.refreshSub) {
      this.refreshSub.unsubscribe();
    }
  }

  toggleAutoRefresh(): void {
    this.autoRefresh = !this.autoRefresh;
    if (this.autoRefresh) {
      this.startAutoRefresh();
    } else {
      this.stopAutoRefresh();
    }
  }

  selectInstrument(instrument: string): void {
    this.selectedInstrument = instrument;
  }

  selectTimeframe(index: number): void {
    const timeframeMap: { [key: number]: '5min' | '15min' | '1hour' } = {
      0: '5min',
      1: '15min',
      2: '1hour'
    };
    this.selectedTimeframe = timeframeMap[index] || '5min';
  }

  getSelectedData(): MultiTimeframeAnalysis | null {
    return this.niftyData;
  }

  getAnalysisForTimeframe(timeframe: string): LiquidityZoneAnalysis | null {
    const data = this.getSelectedData();
    if (!data || !data.timeframes) return null;

    return (data.timeframes as any)[timeframe] || null;
  }

  getAllAnalyses(): LiquidityZoneAnalysis[] {
    const data = this.getSelectedData();
    if (!data || !data.timeframes) return [];

    const analyses: LiquidityZoneAnalysis[] = [];
    for (const tf of this.timeframes) {
      const analysis = (data.timeframes as any)[tf];
      if (analysis) {
        analyses.push(analysis);
      }
    }
    return analyses;
  }

  formatNumber(value: number | undefined | null, decimals: number = 2): string {
    if (value === undefined || value === null || value === 0) return '--';
    return value.toFixed(decimals);
  }

  formatPercent(value: number | undefined | null): string {
    if (value === undefined || value === null || value === 0) return '--';
    return `${value.toFixed(1)}%`;
  }

  getSignalClass(signal: string | undefined): string {
    if (!signal) return 'signal-neutral';

    if (signal === 'LONG_UNWIND') return 'signal-sell';
    if (signal === 'SHORT_COVER') return 'signal-buy';
    return 'signal-neutral';
  }

  getSignalIcon(signal: string | undefined): string {
    if (!signal) return 'remove';
    if (signal === 'LONG_UNWIND') return 'trending_down';
    if (signal === 'SHORT_COVER') return 'trending_up';
    return 'remove';
  }

  getMarketStructureClass(structure: string): string {
    if (!structure) return '';

    if (structure === 'BULLISH') return 'structure-bullish';
    if (structure === 'BEARISH') return 'structure-bearish';
    return 'structure-ranging';
  }

  getGrabTypeText(analysis: LiquidityZoneAnalysis | null): string {
    if (!analysis) return 'No Data';
    if (analysis.buySideGrabbed) return 'Buy-Side Grabbed';
    if (analysis.sellSideGrabbed) return 'Sell-Side Grabbed';
    return 'No Grab';
  }

  getGrabTypeClass(analysis: LiquidityZoneAnalysis | null): string {
    if (!analysis) return 'grab-none';
    if (analysis.buySideGrabbed) return 'grab-buy-side';
    if (analysis.sellSideGrabbed) return 'grab-sell-side';
    return 'grab-none';
  }

  hasValidSetup(analysis: LiquidityZoneAnalysis | null): boolean {
    return analysis !== null && analysis !== undefined && analysis.isValidSetup === true;
  }

  getLiquidityLevels(analysis: LiquidityZoneAnalysis | null, side: 'buy' | 'sell'): string {
    if (!analysis) return '--';

    const levels: string[] = [];
    if (side === 'buy') {
      if (analysis.buySideLiquidity1) levels.push(analysis.buySideLiquidity1.toFixed(2));
      if (analysis.buySideLiquidity2) levels.push(analysis.buySideLiquidity2.toFixed(2));
      if (analysis.buySideLiquidity3) levels.push(analysis.buySideLiquidity3.toFixed(2));
    } else {
      if (analysis.sellSideLiquidity1) levels.push(analysis.sellSideLiquidity1.toFixed(2));
      if (analysis.sellSideLiquidity2) levels.push(analysis.sellSideLiquidity2.toFixed(2));
      if (analysis.sellSideLiquidity3) levels.push(analysis.sellSideLiquidity3.toFixed(2));
    }

    return levels.length > 0 ? levels.join(', ') : '--';
  }

  getTargets(analysis: LiquidityZoneAnalysis | null): string {
    if (!analysis) return '--';

    const targets: string[] = [];
    if (analysis.target1) targets.push(analysis.target1.toFixed(2));
    if (analysis.target2) targets.push(analysis.target2.toFixed(2));
    if (analysis.target3) targets.push(analysis.target3.toFixed(2));

    return targets.length > 0 ? targets.join(' / ') : '--';
  }

  getConfidenceClass(confidence: number | undefined | null): string {
    if (!confidence) return 'confidence-low';
    if (confidence >= 75) return 'confidence-high';
    if (confidence >= 50) return 'confidence-medium';
    return 'confidence-low';
  }

  getStatusText(analysis: LiquidityZoneAnalysis | null): string {
    if (!analysis) return 'No Data';
    if (analysis.isValidSetup) return 'Valid Setup';
    return 'No Setup';
  }

  getStatusClass(analysis: LiquidityZoneAnalysis | null): string {
    if (!analysis) return 'status-none';
    if (analysis.isValidSetup) return 'status-valid';
    return 'status-invalid';
  }

  /**
   * Place an automated trade based on liquidity grab setup
   */
  placeLiquidityTrade(analysis: LiquidityZoneAnalysis): void {
    if (!analysis || !analysis.tradeSignal || !analysis.timeframe) {
      console.error('Invalid analysis data for trade');
      return;
    }

    this.isPlacingTrade = true;

    this.dataService.placeLiquidityTrade(
      analysis.tradeSignal,
      analysis.timeframe,
      analysis.instrumentToken
    ).subscribe({
      next: (response) => {
        console.log('Trade placed successfully:', response);
        this.isPlacingTrade = false;
        // Optionally show a success message or refresh data
        this.loadAllData();
      },
      error: (err) => {
        console.error('Error placing liquidity trade:', err);
        this.isPlacingTrade = false;
      }
    });
  }
}

