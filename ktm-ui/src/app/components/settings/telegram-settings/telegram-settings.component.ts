import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatSlideToggleModule } from '@angular/material/slide-toggle';
import { MatButtonModule } from '@angular/material/button';
import { MatCardModule } from '@angular/material/card';
import { MatIconModule } from '@angular/material/icon';
import { MatExpansionModule } from '@angular/material/expansion';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatSnackBar, MatSnackBarModule } from '@angular/material/snack-bar';
import { MatDividerModule } from '@angular/material/divider';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatChipsModule } from '@angular/material/chips';
import { TelegramService } from '../../../services/telegram.service';
import { TelegramSettings, TelegramValidationResult } from '../../../models/telegram.model';

@Component({
  selector: 'app-telegram-settings',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatSlideToggleModule,
    MatButtonModule,
    MatCardModule,
    MatIconModule,
    MatExpansionModule,
    MatFormFieldModule,
    MatInputModule,
    MatProgressSpinnerModule,
    MatSnackBarModule,
    MatDividerModule,
    MatTooltipModule,
    MatChipsModule
  ],
  templateUrl: './telegram-settings.component.html',
  styleUrls: ['./telegram-settings.component.css']
})
export class TelegramSettingsComponent implements OnInit {
  settings: TelegramSettings | null = null;
  validationResult: TelegramValidationResult | null = null;
  isLoading = false;
  isSaving = false;
  isValidating = false;
  isTesting = false;
  testResult: { success: boolean; message: string } | null = null;

  constructor(
    private telegramService: TelegramService,
    private snackBar: MatSnackBar
  ) {}

  ngOnInit(): void {
    this.loadSettings();
  }

  loadSettings(): void {
    this.isLoading = true;
    this.telegramService.getSettings().subscribe({
      next: (settings) => {
        this.settings = settings;
        this.isLoading = false;
      },
      error: (err) => {
        console.error('Error loading Telegram settings:', err);
        this.snackBar.open('Failed to load Telegram settings', 'Close', { duration: 3000 });
        this.isLoading = false;
      }
    });
  }

  saveSettings(): void {
    if (!this.settings) return;

    this.isSaving = true;
    this.telegramService.updateSettings(this.settings).subscribe({
      next: (updated) => {
        this.settings = updated;
        this.snackBar.open('Settings saved successfully!', 'Close', { duration: 3000 });
        this.isSaving = false;
      },
      error: (err) => {
        console.error('Error saving settings:', err);
        this.snackBar.open('Failed to save settings', 'Close', { duration: 3000 });
        this.isSaving = false;
      }
    });
  }

  validateConfiguration(): void {
    this.isValidating = true;
    this.validationResult = null;
    this.telegramService.validateConfiguration().subscribe({
      next: (result) => {
        this.validationResult = result;
        this.isValidating = false;
        if (result.valid) {
          this.snackBar.open(`Bot validated: @${result.botUsername}`, 'Close', { duration: 3000 });
        } else {
          this.snackBar.open(`Validation failed: ${result.error}`, 'Close', { duration: 5000 });
        }
      },
      error: (err) => {
        console.error('Error validating:', err);
        this.snackBar.open('Validation failed', 'Close', { duration: 3000 });
        this.isValidating = false;
      }
    });
  }

  sendTestNotification(): void {
    this.isTesting = true;
    this.testResult = null;
    this.telegramService.sendTestNotification().subscribe({
      next: (response) => {
        this.testResult = {
          success: response.success,
          message: response.success ? 'Test message sent successfully!' : (response.error || 'Failed to send')
        };
        this.isTesting = false;
        if (response.success) {
          this.snackBar.open('Test notification sent!', 'Close', { duration: 3000 });
        } else {
          this.snackBar.open(`Failed: ${response.error}`, 'Close', { duration: 5000 });
        }
      },
      error: (err) => {
        console.error('Error sending test:', err);
        this.testResult = { success: false, message: 'Failed to send test notification' };
        this.isTesting = false;
      }
    });
  }

  // Toggle all sub-alerts when main category is toggled
  onTradeAlertsToggle(): void {
    if (this.settings?.tradeAlerts) {
      const enabled = this.settings.tradeAlerts.enabled;
      if (!enabled) {
        // Optionally disable all sub-alerts when main is disabled
      }
    }
  }

  onPredictionAlertsToggle(): void {
    // Similar logic if needed
  }

  onSystemAlertsToggle(): void {
    // Similar logic if needed
  }

  onPatternAlertsToggle(): void {
    // Similar logic if needed
  }

  onMarketAlertsToggle(): void {
    // Similar logic if needed
  }

  // Count enabled sub-alerts for display
  getEnabledTradeAlertCount(): number {
    if (!this.settings?.tradeAlerts) return 0;
    const ta = this.settings.tradeAlerts;
    let count = 0;
    if (ta.iobAlerts) count++;
    if (ta.iobMitigationAlerts) count++;
    if (ta.tradeSetupAlerts) count++;
    if (ta.tradeDecisionAlerts) count++;
    if (ta.liquidityZoneAlerts) count++;
    if (ta.brahmastraAlerts) count++;
    return count;
  }

  getEnabledPredictionAlertCount(): number {
    if (!this.settings?.predictionAlerts) return 0;
    const pa = this.settings.predictionAlerts;
    let count = 0;
    if (pa.candlePredictionAlerts) count++;
    if (pa.trendChangeAlerts) count++;
    if (pa.targetHitAlerts) count++;
    return count;
  }

  getEnabledSystemAlertCount(): number {
    if (!this.settings?.systemAlerts) return 0;
    const sa = this.settings.systemAlerts;
    let count = 0;
    if (sa.tickerConnectionAlerts) count++;
    if (sa.jobStatusAlerts) count++;
    if (sa.errorAlerts) count++;
    return count;
  }

  getEnabledPatternAlertCount(): number {
    if (!this.settings?.patternAlerts) return 0;
    const pa = this.settings.patternAlerts;
    let count = 0;
    if (pa.bullishPatternAlerts) count++;
    if (pa.bearishPatternAlerts) count++;
    return count;
  }

  getEnabledMarketAlertCount(): number {
    if (!this.settings?.marketAlerts) return 0;
    const ma = this.settings.marketAlerts;
    let count = 0;
    if (ma.marketOpenCloseAlerts) count++;
    if (ma.significantMoveAlerts) count++;
    return count;
  }
}
