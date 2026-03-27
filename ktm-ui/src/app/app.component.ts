import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterOutlet,RouterLink } from '@angular/router';
import { AppHeaderComponent } from './components/common/app-header/app-header.component';
import { MatButtonModule } from '@angular/material/button';
import { MatTabsModule } from '@angular/material/tabs';
import { MultiSegmentsComponent } from './components/multi-segments/multi-segments.component';
import { SettingsComponent } from './components/settings/settings.component';
import { AnalyticsComponent } from './components/analytics/analytics.component';
import { TradingComponent } from './components/trading/trading.component';
import { MatCardModule } from '@angular/material/card';
import { MatIconModule } from '@angular/material/icon';
import { MatChipsModule } from '@angular/material/chips';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatDividerModule } from '@angular/material/divider';
import { MatTableModule } from '@angular/material/table';
import { MatInputModule } from '@angular/material/input';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSlideToggleModule } from '@angular/material/slide-toggle';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatExpansionModule } from '@angular/material/expansion';
import { MatBadgeModule } from '@angular/material/badge';
import { MatSelectModule } from '@angular/material/select';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { LiquidityAnalysisComponent } from './components/liquidity-analysis/liquidity-analysis.component';
import { OrderBlocksContainerComponent } from './components/order-blocks-container/order-blocks-container.component';
import { BrahmastraComponent } from './components/brahmastra/brahmastra.component';

@Component({
  selector: 'app-root',
  imports: [CommonModule, RouterOutlet, AppHeaderComponent, MatButtonModule, MatTabsModule, MultiSegmentsComponent, SettingsComponent, AnalyticsComponent, TradingComponent,
    MatCardModule, MatIconModule, MatChipsModule, MatTooltipModule, MatDividerModule, MatTableModule, MatInputModule, MatFormFieldModule, MatSlideToggleModule, MatProgressSpinnerModule, MatExpansionModule, MatBadgeModule, MatSelectModule, MatCheckboxModule
  , LiquidityAnalysisComponent, OrderBlocksContainerComponent, BrahmastraComponent
  ],
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css'],
  standalone: true
})
export class AppComponent {

  showSettings = true;

  onTabChange(index: number) {
    // index is the selected tab index
    console.log('Tab changed to:', index);
    // You can add your logic here
    if (index === 7) {
      this.showSettings = false;
      Promise.resolve().then(() => this.showSettings = true);
    }

  }

}
