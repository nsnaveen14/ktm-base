import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatSlideToggleModule } from '@angular/material/slide-toggle';
import { MatButtonModule } from '@angular/material/button';
import { LTPTrackerConfigComponent } from './ltptracker-config/ltptracker-config.component';
import { DailyJobPlannerComponent } from './daily-job-planner/daily-job-planner.component';
import { JobRunningStatusComponent } from './job-running-status/job-running-status.component';
import { TelegramSettingsComponent } from './telegram-settings/telegram-settings.component';
import { AutoTradingConfigComponent } from '../auto-trading-config/auto-trading-config.component';

@Component({
  selector: 'app-settings',
  imports: [

    DailyJobPlannerComponent,
    JobRunningStatusComponent,
    TelegramSettingsComponent,
    AutoTradingConfigComponent
  ],
  templateUrl: './settings.component.html',
  styleUrl: './settings.component.css'
})
export class SettingsComponent {

}
