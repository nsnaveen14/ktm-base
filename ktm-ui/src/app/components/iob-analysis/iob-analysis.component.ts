import { Component, OnInit, OnDestroy, ChangeDetectionStrategy, ChangeDetectorRef } from '@angular/core';
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
import { MatBadgeModule } from '@angular/material/badge';
import { MatSnackBar, MatSnackBarModule } from '@angular/material/snack-bar';
import { MatSlideToggleModule } from '@angular/material/slide-toggle';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { FormsModule } from '@angular/forms';
import { DataService } from '../../services/data.service';
import { interval, Subscription } from 'rxjs';
import { InternalOrderBlock, IOBDetailedAnalysis } from '../../models/iob.model';
import { LiveTickComponent } from '../live-tick/live-tick.component';
import Swal from 'sweetalert2';

@Component({
  selector: 'app-iob-analysis',
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
    MatBadgeModule,
    MatSnackBarModule,
    MatSlideToggleModule,
    MatCheckboxModule,
    FormsModule,
    LiveTickComponent
  ],
  templateUrl: './iob-analysis.component.html',
  styleUrls: ['./iob-analysis.component.css'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class IobAnalysisComponent implements OnInit, OnDestroy {

  isLoading = false;
  isScanning = false;
  isExecutingTrade = false;
  isMitigatingAll = false;
  isMarkingCompleted = false;
  autoRefresh = true;
  refreshInterval = 60000; // 60 seconds - IOB scanning interval

  // Toggle to show/hide completed IOBs (default: hide)
  showCompletedIOBs = false;

  // Toggle to show/hide mitigated IOBs (default: hide)
  showMitigatedIOBs = false;

  // Displayed IOBs for the table (updates when toggle or data changes)
  displayedIOBs: InternalOrderBlock[] = [];

  // Completed IOBs for the new section
  completedIOBsData: InternalOrderBlock[] = [];

  // Multi-select for marking as completed
  selectedIOBs: Set<number> = new Set();

  // Signal count for UI
  newSignalCount = 0;

  niftyData: IOBDetailedAnalysis | null = null;

  selectedInstrument = 'NIFTY';

  niftyToken = 256265;

  displayedColumns = [
    'select',
    'obType',
    'obCandleTime',
    'detectionTimestamp',
    'zone',
    'currentPrice',
    'distanceToZone',
    'targetStatus',
    'hasFvg',
    'tradeDirection',
    'entryPrice',
    'stopLoss',
    'targets',
    'riskReward',
    'confidence',
    'status',
    'actions'
  ];

  // Columns for completed IOBs section
  completedDisplayedColumns = [
    'obType',
    'obCandleTime',
    'zone',
    'tradeDirection',
    'entryPrice',
    'stopLoss',
    'targets',
    'confidence',
    'outcome',
    'estimatedPnL'
  ];

  private refreshSub?: Subscription;

  constructor(
    private dataService: DataService,
    private snackBar: MatSnackBar,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.loadAllData();
    this.startAutoRefresh();
  }

  ngOnDestroy(): void {
    this.stopAutoRefresh();
  }

  // ==================== Helper Methods ====================

  private showNotification(message: string, action: string): void {
    this.snackBar.open(message, action, {
      duration: 5000,
      horizontalPosition: 'right',
      verticalPosition: 'top',
      panelClass: ['iob-notification']
    });
  }


  // Clear new signal count when user views
  clearNewSignalCount(): void {
    this.newSignalCount = 0;
  }

  loadAllData(): void {
    this.isLoading = true;

    this.dataService.getAllIndicesIOBData().subscribe({
      next: (data: any) => {
        console.log('IOB Analysis - Raw data received:', data);
        console.log('IOB Analysis - NIFTY counts:', {
          activeCount: data.NIFTY?.activeCount,
          mitigatedCount: data.NIFTY?.mitigatedCount,
          completedCount: data.NIFTY?.completedCount,
          activeIOBsLength: data.NIFTY?.activeIOBs?.length,
          mitigatedIOBsLength: data.NIFTY?.mitigatedIOBs?.length,
          completedIOBsLength: data.NIFTY?.completedIOBs?.length
        });
        this.niftyData = data.NIFTY;
        this.updateDisplayedIOBs();
        this.isLoading = false;
        this.cdr.markForCheck();
      },
      error: (err) => {
        console.error('Error loading IOB data:', err);
        this.isLoading = false;
        this.cdr.markForCheck();
      }
    });
  }

  scanAll(): void {
    this.isScanning = true;

    this.dataService.scanAllIndicesForIOBs().subscribe({
      next: (data) => {
        console.log('IOB scan completed:', data);
        setTimeout(() => {
          this.loadAllData();
          this.isScanning = false;
          this.cdr.markForCheck();
        }, 2000);
      },
      error: (err) => {
        console.error('Error scanning for IOBs:', err);
        this.isScanning = false;
        this.cdr.markForCheck();
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
    this.updateDisplayedIOBs();
  }

  getSelectedData(): IOBDetailedAnalysis | null {
    return this.niftyData;
  }

  getAllIOBs(): InternalOrderBlock[] {
    const data = this.getSelectedData();
    if (!data) return [];

    // Get active IOBs (always shown) - these are FRESH or TRADED status only
    // Fallback to fresh IOBs if activeIOBs is not available
    let activeIOBs: InternalOrderBlock[] = data.activeIOBs || [];

    // If activeIOBs is empty, fallback to combining fresh bullish and bearish IOBs
    if (activeIOBs.length === 0) {
      const freshBullish = data.freshBullishIOBs || [];
      const freshBearish = data.freshBearishIOBs || [];
      activeIOBs = [...freshBullish, ...freshBearish];
    }

    // Get mitigated IOBs (only shown if toggle is on)
    const mitigatedIOBs: InternalOrderBlock[] = this.showMitigatedIOBs ? (data.mitigatedIOBs || []) : [];

    // Get completed IOBs (only shown if toggle is on)
    const completedIOBs: InternalOrderBlock[] = this.showCompletedIOBs ? (data.completedIOBs || []) : [];

    // Combine based on toggles
    const allIOBs = [...activeIOBs, ...mitigatedIOBs, ...completedIOBs];

    // Sort by OB Candle Time (descending - latest first)
    return allIOBs.sort((a, b) => {
      const dateA = a.obCandleTime ? new Date(a.obCandleTime).getTime() : 0;
      const dateB = b.obCandleTime ? new Date(b.obCandleTime).getTime() : 0;
      return dateB - dateA;
    });
  }

  // Update the displayed IOBs list
  updateDisplayedIOBs(): void {
    this.displayedIOBs = this.getAllIOBs();
  }

  // Toggle show/hide mitigated IOBs
  toggleShowMitigated(): void {
    this.showMitigatedIOBs = !this.showMitigatedIOBs;
    this.updateDisplayedIOBs();
  }

  // Called when showMitigatedIOBs changes via ngModel
  onShowMitigatedChange(): void {
    this.updateDisplayedIOBs();
  }

  // Toggle show/hide completed IOBs
  toggleShowCompleted(): void {
    this.showCompletedIOBs = !this.showCompletedIOBs;
    this.updateDisplayedIOBs();
  }

  // Called when showCompletedIOBs changes via ngModel
  onShowCompletedChange(): void {
    this.updateDisplayedIOBs();
  }

  // Get active IOB count
  getActiveCount(): number {
    const data = this.getSelectedData();
    return data?.activeCount || 0;
  }

  // Get mitigated IOB count
  getMitigatedCount(): number {
    const data = this.getSelectedData();
    return data?.mitigatedCount || 0;
  }

  // Get completed IOB count
  getCompletedCount(): number {
    const data = this.getSelectedData();
    return data?.completedCount || 0;
  }

  // Check if an IOB is completed (target 3 hit or stop loss hit)
  isCompleted(iob: InternalOrderBlock): boolean {
    return iob.target3AlertSent === true ||
           iob.status === 'STOPPED' ||
           iob.status === 'COMPLETED';
  }

  // Get target status display
  getTargetStatusDisplay(iob: InternalOrderBlock): string {
    const targets: string[] = [];
    if (iob.target1AlertSent) targets.push('T1 ✓');
    if (iob.target2AlertSent) targets.push('T2 ✓');
    if (iob.target3AlertSent) targets.push('T3 ✓');
    if (iob.status === 'STOPPED') return '🛑 SL Hit';
    if (targets.length === 0) return 'Pending';
    return targets.join(' ');
  }

  // Get target status class
  getTargetStatusClass(iob: InternalOrderBlock): string {
    if (iob.status === 'STOPPED') return 'status-stopped';
    if (iob.target3AlertSent) return 'status-completed';
    if (iob.target2AlertSent) return 'status-t2-hit';
    if (iob.target1AlertSent) return 'status-t1-hit';
    return 'status-pending';
  }

  // ==================== Multi-Select Methods ====================

  // Toggle selection of a single IOB
  toggleSelection(iob: InternalOrderBlock): void {
    if (iob.id) {
      if (this.selectedIOBs.has(iob.id)) {
        this.selectedIOBs.delete(iob.id);
      } else {
        this.selectedIOBs.add(iob.id);
      }
    }
  }

  // Check if an IOB is selected
  isSelected(iob: InternalOrderBlock): boolean {
    return iob.id ? this.selectedIOBs.has(iob.id) : false;
  }

  // Check if all displayed IOBs are selected
  isAllSelected(): boolean {
    const selectableIOBs = this.displayedIOBs.filter(iob => !this.isCompleted(iob));
    return selectableIOBs.length > 0 && selectableIOBs.every(iob => iob.id && this.selectedIOBs.has(iob.id));
  }

  // Check if some but not all are selected
  isIndeterminate(): boolean {
    const selectableIOBs = this.displayedIOBs.filter(iob => !this.isCompleted(iob));
    const selectedCount = selectableIOBs.filter(iob => iob.id && this.selectedIOBs.has(iob.id)).length;
    return selectedCount > 0 && selectedCount < selectableIOBs.length;
  }

  // Toggle select all
  toggleSelectAll(): void {
    const selectableIOBs = this.displayedIOBs.filter(iob => !this.isCompleted(iob));
    if (this.isAllSelected()) {
      // Deselect all
      selectableIOBs.forEach(iob => {
        if (iob.id) this.selectedIOBs.delete(iob.id);
      });
    } else {
      // Select all
      selectableIOBs.forEach(iob => {
        if (iob.id) this.selectedIOBs.add(iob.id);
      });
    }
  }

  // Clear all selections
  clearSelection(): void {
    this.selectedIOBs.clear();
  }

  // Get count of selected IOBs
  getSelectedCount(): number {
    return this.selectedIOBs.size;
  }

  // Mark selected IOBs as completed
  markSelectedAsCompleted(): void {
    if (this.selectedIOBs.size === 0) {
      this.showNotification('No IOBs selected', 'Close');
      return;
    }

    Swal.fire({
      title: 'Mark as Completed?',
      html: `Are you sure you want to mark <strong>${this.selectedIOBs.size}</strong> IOB(s) as completed?`,
      icon: 'question',
      showCancelButton: true,
      confirmButtonText: 'Yes, Mark Completed',
      cancelButtonText: 'Cancel',
      background: '#1a1a2e',
      color: '#e0e0e0',
      confirmButtonColor: '#4caf50',
      cancelButtonColor: '#666'
    }).then((result) => {
      if (result.isConfirmed) {
        this.isMarkingCompleted = true;
        const iobIds = Array.from(this.selectedIOBs);

        // Call API to mark as completed
        this.dataService.markIOBsAsCompleted(iobIds).subscribe({
          next: (response) => {
            console.log('IOBs marked as completed:', response);
            this.showNotification(`${iobIds.length} IOB(s) marked as completed`, 'Success');
            this.clearSelection();
            this.loadAllData();
            this.isMarkingCompleted = false;
            this.cdr.markForCheck();
          },
          error: (err) => {
            console.error('Error marking IOBs as completed:', err);
            this.showNotification('Failed to mark IOBs as completed', 'Error');
            this.isMarkingCompleted = false;
            this.cdr.markForCheck();
          }
        });
      }
    });
  }

  getTradableIOBs(): InternalOrderBlock[] {
    const data = this.getSelectedData();
    return data?.tradableIOBs || [];
  }

  formatNumber(value: number | undefined | null, decimals: number = 2): string {
    if (value === undefined || value === null || value === 0) return '--';
    return value.toFixed(decimals);
  }

  formatPercent(value: number | undefined | null): string {
    if (value === undefined || value === null || value === 0) return '--';
    return `${value.toFixed(1)}%`;
  }

  formatDateTime(value: string | undefined | null): string {
    if (!value) return '--';
    try {
      const date = new Date(value);
      return date.toLocaleString('en-IN', {
        month: 'short',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit'
      });
    } catch {
      return value;
    }
  }

  getOBTypeClass(obType: string | undefined): string {
    if (!obType) return '';
    return obType === 'BULLISH_IOB' ? 'ob-bullish' : 'ob-bearish';
  }

  getOBTypeIcon(obType: string | undefined): string {
    if (!obType) return 'help';
    return obType === 'BULLISH_IOB' ? 'trending_up' : 'trending_down';
  }

  getDirectionClass(direction: string | undefined): string {
    if (!direction) return '';
    return direction === 'LONG' ? 'direction-long' : 'direction-short';
  }

  getStatusClass(status: string | undefined): string {
    if (!status) return '';
    switch (status) {
      case 'FRESH': return 'status-fresh';
      case 'MITIGATED': return 'status-mitigated';
      case 'TRADED': return 'status-traded';
      case 'EXPIRED': return 'status-expired';
      default: return '';
    }
  }

  getConfidenceClass(confidence: number | undefined): string {
    if (!confidence) return 'confidence-low';
    if (confidence >= 75) return 'confidence-high';
    if (confidence >= 50) return 'confidence-medium';
    return 'confidence-low';
  }

  getZoneDisplay(iob: InternalOrderBlock): string {
    if (!iob.zoneHigh || !iob.zoneLow) return '--';
    return `${this.formatNumber(iob.zoneLow)} - ${this.formatNumber(iob.zoneHigh)}`;
  }

  getTargetsDisplay(iob: InternalOrderBlock): string {
    const targets: string[] = [];
    if (iob.target1) targets.push(this.formatNumber(iob.target1));
    if (iob.target2) targets.push(this.formatNumber(iob.target2));
    if (iob.target3) targets.push(this.formatNumber(iob.target3));
    return targets.length > 0 ? targets.join(' / ') : '--';
  }

  executeTrade(iob: InternalOrderBlock): void {
    if (!iob.id) return;

    this.isExecutingTrade = true;

    this.dataService.executeIOBTrade(iob.id).subscribe({
      next: (result) => {
        console.log('Trade executed:', result);
        if (result.success) {
          Swal.fire({
            icon: 'success',
            title: 'Trade Executed!',
            html: `
              <div style="text-align: left; font-family: 'Roboto Mono', monospace;">
                <p><strong>Trade ID:</strong> ${result.tradeId}</p>
                <p><strong>Direction:</strong> ${result.direction}</p>
              </div>
            `,
            background: '#1a1a2e',
            color: '#e0e0e0',
            confirmButtonColor: '#00d4ff'
          });
          this.loadAllData();
        } else {
          Swal.fire({
            icon: 'error',
            title: 'Trade Execution Failed',
            text: result.message,
            background: '#1a1a2e',
            color: '#e0e0e0'
          });
        }
        this.isExecutingTrade = false;
        this.cdr.markForCheck();
      },
      error: (err) => {
        console.error('Error executing trade:', err);
        Swal.fire({
          icon: 'error',
          title: 'Error',
          text: 'Error executing trade',
          background: '#1a1a2e',
          color: '#e0e0e0'
        });
        this.isExecutingTrade = false;
        this.cdr.markForCheck();
      }
    });
  }

  viewTradeSetup(iob: InternalOrderBlock): void {
    if (!iob.id) return;

    this.dataService.getIOBTradeSetup(iob.id).subscribe({
      next: (setup) => {
        console.log('Trade setup:', setup);

        const directionIcon = setup.direction === 'LONG' ? '📈' : '📉';
        const directionColor = setup.direction === 'LONG' ? '#00ff88' : '#ff4444';

        // Enhanced FVG display using IOB data
        let fvgStatusHtml = '';
        if (iob.hasFvg) {
          const fvgColor = iob.fvgValid ? '#00ff88' : '#ff9800';
          const fvgIcon = iob.fvgValid ? '✅' : '❌';
          const fvgLabel = iob.fvgValid ? 'Valid' : 'Invalid';
          const fvgScore = iob.fvgValidationScore ? Math.round(iob.fvgValidationScore) : 0;
          fvgStatusHtml = `<span style="color: ${fvgColor};">${fvgIcon} ${fvgLabel} (${fvgScore}%)</span>`;
        } else {
          fvgStatusHtml = '<span style="color: #888;">✗ Absent</span>';
        }

        // Build FVG factors breakdown
        let fvgFactorsHtml = '';
        if (iob.hasFvg) {
          const factors = [
            { label: 'Unmitigated', value: iob.fvgUnmitigated },
            { label: 'Candle Reaction', value: iob.fvgCandleReactionValid },
            { label: 'S/R Confluence', value: iob.fvgSrConfluence },
            { label: 'Gann Box', value: iob.fvgGannBoxValid },
            { label: 'BOS Confirmed', value: iob.fvgBosConfirmed },
          ];
          const factorItems = factors.map(f =>
            `<span style="color: ${f.value ? '#00ff88' : '#ff4444'}; margin-right: 8px;">${f.value ? '✅' : '❌'} ${f.label}</span>`
          ).join('');
          const priorityText = iob.fvgPriority && iob.fvgPriority > 0 ? `<span style="color: #ffd700; margin-right: 8px;">🏆 Priority #${iob.fvgPriority}</span>` : '';
          fvgFactorsHtml = `
            <div style="margin-top: 16px; background: rgba(0,212,255,0.08); padding: 12px; border-radius: 8px; border-left: 3px solid ${iob.fvgValid ? '#00ff88' : '#ff9800'};">
              <div style="color: ${iob.fvgValid ? '#00ff88' : '#ff9800'}; font-weight: bold; margin-bottom: 8px;">📊 FVG 6-Factor Validation</div>
              <div style="display: flex; flex-wrap: wrap; gap: 4px; font-size: 12px;">
                ${factorItems}
                ${priorityText}
              </div>
              <div style="margin-top: 8px; color: #888; font-size: 11px;">
                FVG Zone: ₹${iob.fvgLow?.toFixed(2)} - ₹${iob.fvgHigh?.toFixed(2)}
              </div>
            </div>`;
        }

        Swal.fire({
          title: `${directionIcon} IOB Trade Setup`,
          html: `
            <div style="text-align: left; font-family: 'Roboto Mono', monospace; font-size: 14px; line-height: 1.8;">
              <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 16px;">
                <div style="background: rgba(0,212,255,0.1); padding: 12px; border-radius: 8px; border-left: 3px solid ${directionColor};">
                  <div style="color: #888; font-size: 12px;">Direction</div>
                  <div style="color: ${directionColor}; font-weight: bold; font-size: 18px;">${setup.direction}</div>
                </div>
                <div style="background: rgba(0,212,255,0.1); padding: 12px; border-radius: 8px;">
                  <div style="color: #888; font-size: 12px;">Confidence</div>
                  <div style="color: #00d4ff; font-weight: bold; font-size: 18px;">${setup.confidence?.toFixed(1)}%</div>
                </div>
              </div>

              <div style="margin-top: 16px; background: rgba(0,0,0,0.3); padding: 16px; border-radius: 8px;">
                <div style="margin-bottom: 12px;">
                  <span style="color: #888;">Entry Zone:</span>
                  <span style="color: #e0e0e0; font-weight: 500;">${setup.entryZone?.low?.toFixed(2)} - ${setup.entryZone?.high?.toFixed(2)}</span>
                </div>
                <div style="margin-bottom: 12px;">
                  <span style="color: #888;">Entry Price:</span>
                  <span style="color: #00d4ff; font-weight: bold;">${setup.entryPrice?.toFixed(2)}</span>
                </div>
                <div style="margin-bottom: 12px;">
                  <span style="color: #888;">Stop Loss:</span>
                  <span style="color: #ff4444; font-weight: bold;">${setup.stopLoss?.toFixed(2)}</span>
                </div>
              </div>

              <div style="margin-top: 16px; background: rgba(0,255,136,0.1); padding: 16px; border-radius: 8px;">
                <div style="color: #00ff88; font-weight: bold; margin-bottom: 8px;">🎯 Targets</div>
                <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px;">
                  <div style="text-align: center; padding: 8px; background: rgba(0,0,0,0.2); border-radius: 4px;">
                    <div style="color: #888; font-size: 11px;">T1</div>
                    <div style="color: #00ff88; font-weight: bold;">${setup.target1?.toFixed(2)}</div>
                  </div>
                  <div style="text-align: center; padding: 8px; background: rgba(0,0,0,0.2); border-radius: 4px;">
                    <div style="color: #888; font-size: 11px;">T2</div>
                    <div style="color: #00ff88; font-weight: bold;">${setup.target2?.toFixed(2)}</div>
                  </div>
                  <div style="text-align: center; padding: 8px; background: rgba(0,0,0,0.2); border-radius: 4px;">
                    <div style="color: #888; font-size: 11px;">T3</div>
                    <div style="color: #00ff88; font-weight: bold;">${setup.target3?.toFixed(2)}</div>
                  </div>
                </div>
              </div>

              <div style="margin-top: 16px; display: grid; grid-template-columns: 1fr 1fr; gap: 12px;">
                <div style="background: rgba(0,0,0,0.2); padding: 10px; border-radius: 6px; text-align: center;">
                  <div style="color: #888; font-size: 11px;">Risk-Reward</div>
                  <div style="color: #ffaa00; font-weight: bold; font-size: 16px;">1:${setup.riskRewardRatio?.toFixed(2)}</div>
                </div>
                <div style="background: rgba(0,0,0,0.2); padding: 10px; border-radius: 6px; text-align: center;">
                  <div style="color: #888; font-size: 11px;">FVG</div>
                  <div style="font-size: 16px;">${fvgStatusHtml}</div>
                </div>
              </div>

              ${fvgFactorsHtml}

              ${setup.validationNotes ? `
              <div style="margin-top: 16px; background: rgba(255,170,0,0.1); padding: 12px; border-radius: 8px; border-left: 3px solid #ffaa00;">
                <div style="color: #ffaa00; font-size: 12px; margin-bottom: 4px;">📝 Notes</div>
                <div style="color: #e0e0e0;">${setup.validationNotes}</div>
              </div>
              ` : ''}
            </div>
          `,
          background: '#1a1a2e',
          color: '#e0e0e0',
          showCloseButton: true,
          showConfirmButton: false,
          width: '500px',
          customClass: {
            popup: 'iob-swal-popup'
          }
        });
      },
      error: (err) => {
        console.error('Error fetching trade setup:', err);
        Swal.fire({
          icon: 'error',
          title: 'Error',
          text: 'Failed to fetch trade setup details',
          background: '#1a1a2e',
          color: '#e0e0e0'
        });
      }
    });
  }

  // ==================== FVG Validation Display Methods ====================

  /**
   * Get FVG validation badge text
   */
  getFvgDisplayText(iob: InternalOrderBlock): string {
    if (!iob.hasFvg) return 'No FVG';
    if (iob.fvgValid === true) {
      const score = iob.fvgValidationScore ? Math.round(iob.fvgValidationScore) : 0;
      return `Valid ${score}%`;
    }
    if (iob.fvgValid === false) {
      const score = iob.fvgValidationScore ? Math.round(iob.fvgValidationScore) : 0;
      return `Invalid ${score}%`;
    }
    return 'FVG ✓';
  }

  /**
   * Get FVG validation CSS class
   */
  getFvgClass(iob: InternalOrderBlock): string {
    if (!iob.hasFvg) return 'fvg-absent';
    if (iob.fvgValid === true) return 'fvg-valid';
    if (iob.fvgValid === false) return 'fvg-invalid';
    return 'fvg-present';
  }

  /**
   * Get FVG validation tooltip with factor breakdown
   */
  getFvgTooltip(iob: InternalOrderBlock): string {
    if (!iob.hasFvg) return 'No Fair Value Gap detected';
    if (iob.fvgValidationDetails) return iob.fvgValidationDetails;

    const parts: string[] = [];
    parts.push(`FVG Zone: ${this.formatNumber(iob.fvgLow)} - ${this.formatNumber(iob.fvgHigh)}`);
    if (iob.fvgValidationScore !== undefined) {
      parts.push(`Score: ${Math.round(iob.fvgValidationScore)}%`);
    }
    if (iob.fvgPriority && iob.fvgPriority > 0) {
      parts.push(`Priority: #${iob.fvgPriority}`);
    }
    const factors: string[] = [];
    if (iob.fvgUnmitigated !== undefined) factors.push(`Unmitigated: ${iob.fvgUnmitigated ? '✅' : '❌'}`);
    if (iob.fvgCandleReactionValid !== undefined) factors.push(`Candle Reaction: ${iob.fvgCandleReactionValid ? '✅' : '❌'}`);
    if (iob.fvgSrConfluence !== undefined) factors.push(`S/R Confluence: ${iob.fvgSrConfluence ? '✅' : '❌'}`);
    if (iob.fvgGannBoxValid !== undefined) factors.push(`Gann Box: ${iob.fvgGannBoxValid ? '✅' : '❌'}`);
    if (iob.fvgBosConfirmed !== undefined) factors.push(`BOS: ${iob.fvgBosConfirmed ? '✅' : '❌'}`);
    if (factors.length > 0) parts.push(factors.join(' | '));
    return parts.join('\n');
  }

  /**
   * Get FVG icon
   */
  getFvgIcon(iob: InternalOrderBlock): string {
    if (!iob.hasFvg) return 'remove_circle_outline';
    if (iob.fvgValid === true) return 'verified';
    if (iob.fvgValid === false) return 'warning';
    return 'check_circle';
  }

  /**
   * Count of valid FVG IOBs
   */
  getValidFvgCount(): number {
    return this.displayedIOBs.filter(iob => iob.fvgValid === true).length;
  }

  getBullishCount(): number {
    const data = this.getSelectedData();
    return data?.freshBullishIOBs?.length || 0;
  }

  getBearishCount(): number {
    const data = this.getSelectedData();
    return data?.freshBearishIOBs?.length || 0;
  }

  getTradableCount(): number {
    const data = this.getSelectedData();
    return data?.tradableIOBs?.length || 0;
  }

  isNearZone(distancePercent: number | undefined): boolean {
    if (distancePercent === undefined || distancePercent === null) return false;
    return Math.abs(distancePercent) < 0.5;
  }

  /**
   * Manually mark an IOB as mitigated
   */
  markAsMitigated(iob: InternalOrderBlock): void {
    if (!iob.id) return;

    Swal.fire({
      title: 'Mark as Mitigated?',
      html: `
        <div style="text-align: left; font-family: 'Roboto Mono', monospace; font-size: 14px;">
          <p>Are you sure you want to mark this IOB as mitigated?</p>
          <div style="margin-top: 12px; background: rgba(0,0,0,0.3); padding: 12px; border-radius: 8px;">
            <p><strong>Type:</strong> ${iob.obType === 'BULLISH_IOB' ? 'Bullish' : 'Bearish'}</p>
            <p><strong>Zone:</strong> ${this.getZoneDisplay(iob)}</p>
            <p><strong>OB Time:</strong> ${this.formatDateTime(iob.obCandleTime)}</p>
          </div>
          <p style="margin-top: 12px; color: #ffaa00; font-size: 12px;">
            ⚠️ This action cannot be undone.
          </p>
        </div>
      `,
      icon: 'question',
      showCancelButton: true,
      confirmButtonText: 'Yes, Mark Mitigated',
      cancelButtonText: 'Cancel',
      background: '#1a1a2e',
      color: '#e0e0e0',
      confirmButtonColor: '#ff9800',
      cancelButtonColor: '#666'
    }).then((result) => {
      if (result.isConfirmed) {
        this.dataService.markIOBAsMitigated(iob.id!).subscribe({
          next: (response) => {
            console.log('IOB marked as mitigated:', response);
            if (response.success) {
              Swal.fire({
                icon: 'success',
                title: 'IOB Mitigated',
                text: 'The IOB has been marked as mitigated.',
                background: '#1a1a2e',
                color: '#e0e0e0',
                confirmButtonColor: '#00d4ff',
                timer: 2000,
                showConfirmButton: false
              });
              this.loadAllData();
            } else {
              Swal.fire({
                icon: 'error',
                title: 'Error',
                text: response.error || 'Failed to mark IOB as mitigated',
                background: '#1a1a2e',
                color: '#e0e0e0'
              });
            }
            this.cdr.markForCheck();
          },
          error: (err) => {
            console.error('Error marking IOB as mitigated:', err);
            Swal.fire({
              icon: 'error',
              title: 'Error',
              text: 'Failed to mark IOB as mitigated. Please try again.',
              background: '#1a1a2e',
              color: '#e0e0e0'
            });
            this.cdr.markForCheck();
          }
        });
      }
    });
  }

  /**
   * Check if an IOB can be manually mitigated
   */
  canMitigate(iob: InternalOrderBlock): boolean {
    return iob.status === 'FRESH';
  }

  /**
   * Get count of fresh IOBs for current instrument
   */
  getFreshCount(): number {
    const iobs = this.getAllIOBs();
    return iobs.filter(iob => iob.status === 'FRESH').length;
  }

  // ==================== Completed IOBs Section Methods ====================

  /**
   * Get completed IOBs for the dedicated section
   */
  getCompletedIOBsForSection(): InternalOrderBlock[] {
    const data = this.getSelectedData();
    if (!data) return [];

    const completedIOBs = data.completedIOBs || [];

    // Sort by OB Candle Time (descending - latest first)
    return completedIOBs.sort((a, b) => {
      const dateA = a.obCandleTime ? new Date(a.obCandleTime).getTime() : 0;
      const dateB = b.obCandleTime ? new Date(b.obCandleTime).getTime() : 0;
      return dateB - dateA;
    });
  }

  /**
   * Get the final outcome of a completed IOB
   */
  getOutcomeDisplay(iob: InternalOrderBlock): string {
    if (iob.status === 'STOPPED') return '🛑 Stop Loss Hit';
    if (iob.target3AlertSent) return '🏆 All Targets Hit (T1, T2, T3)';
    if (iob.target2AlertSent) return '🎯 T2 Hit (Partial Win)';
    if (iob.target1AlertSent) return '✅ T1 Hit (Partial Win)';
    if (iob.status === 'COMPLETED') return '✔️ Completed';
    return '❓ Unknown';
  }

  /**
   * Get outcome class for styling
   */
  getOutcomeClass(iob: InternalOrderBlock): string {
    if (iob.status === 'STOPPED') return 'outcome-loss';
    if (iob.target3AlertSent) return 'outcome-full-win';
    if (iob.target2AlertSent || iob.target1AlertSent) return 'outcome-partial-win';
    return 'outcome-neutral';
  }

  /**
   * Calculate estimated P&L based on entry and targets/SL hit
   */
  getEstimatedPnL(iob: InternalOrderBlock): string {
    if (!iob.entryPrice || !iob.stopLoss) return '--';

    const direction = iob.tradeDirection === 'LONG' ? 1 : -1;
    let exitPrice = 0;

    if (iob.status === 'STOPPED') {
      exitPrice = iob.stopLoss;
    } else if (iob.target3AlertSent && iob.target3) {
      exitPrice = iob.target3;
    } else if (iob.target2AlertSent && iob.target2) {
      exitPrice = iob.target2;
    } else if (iob.target1AlertSent && iob.target1) {
      exitPrice = iob.target1;
    } else {
      return '--';
    }

    const points = (exitPrice - iob.entryPrice) * direction;
    return points >= 0 ? `+${points.toFixed(2)}` : points.toFixed(2);
  }

  /**
   * Get P&L class for styling
   */
  getPnLClass(iob: InternalOrderBlock): string {
    if (iob.status === 'STOPPED') return 'pnl-negative';
    if (iob.target1AlertSent || iob.target2AlertSent || iob.target3AlertSent) return 'pnl-positive';
    return '';
  }

  /**
   * Get summary statistics for completed IOBs
   */
  getCompletedStats(): { total: number; wins: number; losses: number; winRate: number } {
    const completed = this.getCompletedIOBsForSection();
    const total = completed.length;
    const wins = completed.filter(iob =>
      iob.target1AlertSent || iob.target2AlertSent || iob.target3AlertSent
    ).length;
    const losses = completed.filter(iob => iob.status === 'STOPPED').length;
    const winRate = total > 0 ? (wins / total) * 100 : 0;

    return { total, wins, losses, winRate };
  }

  /**
   * Mitigate all fresh IOBs for the current instrument
   */
  mitigateAllFresh(): void {
    const freshCount = this.getFreshCount();
    if (freshCount === 0) {
      this.showNotification('No fresh IOBs to mitigate', 'Info');
      return;
    }

    const instrumentToken = this.niftyToken;

    Swal.fire({
      title: 'Mitigate All Fresh IOBs?',
      html: `
        <div style="text-align: left; font-family: 'Roboto Mono', monospace; font-size: 14px;">
          <p>Are you sure you want to mark <strong>${freshCount}</strong> fresh IOBs as mitigated?</p>
          <div style="margin-top: 12px; background: rgba(0,0,0,0.3); padding: 12px; border-radius: 8px;">
            <p><strong>Instrument:</strong> ${this.selectedInstrument}</p>
            <p><strong>Fresh IOBs:</strong> ${freshCount}</p>
          </div>
          <p style="margin-top: 12px; color: #ff6b6b; font-size: 12px;">
            ⚠️ This action cannot be undone. All fresh IOBs will be marked as mitigated.
          </p>
        </div>
      `,
      icon: 'warning',
      showCancelButton: true,
      confirmButtonText: 'Yes, Mitigate All',
      cancelButtonText: 'Cancel',
      background: '#1a1a2e',
      color: '#e0e0e0',
      confirmButtonColor: '#f44336',
      cancelButtonColor: '#666'
    }).then((result) => {
      if (result.isConfirmed) {
        this.isMitigatingAll = true;
        this.dataService.mitigateAllFreshIOBs(instrumentToken).subscribe({
          next: (response) => {
            this.isMitigatingAll = false;
            console.log('All fresh IOBs mitigated:', response);
            if (response.success) {
              Swal.fire({
                icon: 'success',
                title: 'All IOBs Mitigated',
                text: `Successfully mitigated ${response.mitigatedCount} fresh IOBs.`,
                background: '#1a1a2e',
                color: '#e0e0e0',
                confirmButtonColor: '#00d4ff',
                timer: 3000,
                showConfirmButton: false
              });
              this.loadAllData();
            } else {
              Swal.fire({
                icon: 'error',
                title: 'Error',
                text: response.error || 'Failed to mitigate IOBs',
                background: '#1a1a2e',
                color: '#e0e0e0'
              });
            }
            this.cdr.markForCheck();
          },
          error: (err) => {
            this.isMitigatingAll = false;
            console.error('Error mitigating all fresh IOBs:', err);
            Swal.fire({
              icon: 'error',
              title: 'Error',
              text: 'Failed to mitigate IOBs. Please try again.',
              background: '#1a1a2e',
              color: '#e0e0e0'
            });
            this.cdr.markForCheck();
          }
        });
      }
    });
  }
}
