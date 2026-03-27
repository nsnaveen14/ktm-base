import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatTabsModule } from '@angular/material/tabs';
import { IobAnalysisComponent } from '../iob-analysis/iob-analysis.component';
import { IobChartComponent } from '../iob-chart/iob-chart.component';
import { IobPerformanceComponent } from '../iob-performance/iob-performance.component';
import { IobTradesComponent } from '../iob-trades/iob-trades.component';

@Component({
  selector: 'app-order-blocks-container',
  standalone: true,
  imports: [
    CommonModule,
    MatTabsModule,
    IobAnalysisComponent,
    IobChartComponent,
    IobPerformanceComponent,
    IobTradesComponent
  ],
  templateUrl: './order-blocks-container.component.html',
  styleUrls: ['./order-blocks-container.component.css']
})
export class OrderBlocksContainerComponent {

}
