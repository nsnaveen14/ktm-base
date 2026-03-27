import { Component, Input, OnInit, ViewChild, AfterViewInit } from '@angular/core';
import { AppJobConfig } from '../../../models/AppJobConfig';
import { TradeDecisionModel } from '../../../models/TradeDecisionModel';
import { DataService } from '../../../services/data.service';
import { CommonModule } from '@angular/common';
import { MatTable, MatTableModule, MatTableDataSource } from '@angular/material/table';
import {MatPaginator, MatPaginatorModule} from '@angular/material/paginator';
import { MatSelectModule } from '@angular/material/select';
import { MatFormFieldModule } from '@angular/material/form-field';
import { FormControl, ReactiveFormsModule } from '@angular/forms';
import { IndexOHLC } from '../../../models/IndexOHLC';


  @Component({
  selector: 'app-summary-tab',
  imports: [CommonModule, MatTableModule, MatPaginatorModule, MatSelectModule, MatFormFieldModule, ReactiveFormsModule],
  templateUrl: './summary-tab.component.html',
  styleUrl: './summary-tab.component.css'
})
export class SummaryTabComponent implements OnInit, AfterViewInit {

  @Input() selectedConfigFromMultiSegment: AppJobConfig | any;
  @Input() indexOHLCData: { [key: number]: IndexOHLC } = {}; // Indexed by appJobConfigNum
  tradeDecisions: TradeDecisionModel[] = [];
  filteredTradeDecisions: TradeDecisionModel[] = [];
  tradeDecisionsPaginatorDataSource = new MatTableDataSource<TradeDecisionModel>();
  displayedColumns: string[] = [
    'appJobConfigNum',
    'appJobConfigName',
    'tradeDecisionTS',
    'tradeDecision',
    'tradeDecisionType',
    'entryIndexLTP',
    'targetIndexLTP',
    'stopLossIndexLTP',
    'indexLTP',
    'status',
    'trade_decision_result',
    'trade_decision_result_ts',
    'swingTarget',
    'swingTaken',
    'confirmationTaken'
  ];
  defaultAppJobConfigNum: number = 0; // Default value if none selected
  appJobConfigDetails: AppJobConfig[] | any;
  // Filter properties
  configNumFilter = new FormControl<number[]>([]);
  tradeDecisionTypeFilter = new FormControl<string[]>([]);
  availableConfigNums: number[]|any = [];
  availableTradeDecisionTypes: string[] = [];
  playAudio: boolean = true;
  audio = new Audio('alert.mp3');
  ltpData: any[] = [];

  @ViewChild(MatPaginator) paginator!: MatPaginator;

  constructor(private dataService: DataService) {}

  ngOnInit(): void {
    this.clearFilter();
    this.defaultAppJobConfigNum = this.selectedConfigFromMultiSegment?.appJobConfigNum || 0;
    this.getLatestTradeDecisions(this.selectedConfigFromMultiSegment.appJobConfigNum);
    // Subscribe to filter changes
    this.configNumFilter.valueChanges.subscribe(() => {
      this.applyFilter();
    });
    this.tradeDecisionTypeFilter.valueChanges.subscribe(() => {
      this.applyFilter();
    });

    // Subscribe to LTP updates
    console.log('Received indexOHLCData in SummaryTabComponent:', this.indexOHLCData);
  }

  resetView(): void {
    this.clearFilter();
    this.getLatestTradeDecisions(0);
  }

  ngAfterViewInit(): void {
    this.tradeDecisionsPaginatorDataSource.paginator = this.paginator;
  }

  getLatestTradeDecisions(appJobConfigNum: number): void {
    this.dataService.getTradeDecisionsByConfigNum(appJobConfigNum).subscribe(
      (response) => {
        this.tradeDecisions = response;
        this.filteredTradeDecisions = [...this.tradeDecisions];
        this.tradeDecisionsPaginatorDataSource.data = this.filteredTradeDecisions;

        // Extract unique config numbers for filter options
        this.availableConfigNums = [...new Set(this.tradeDecisions.map(td => td.appJobConfig?.appJobConfigNum))]
          .filter((num): num is number => typeof num === 'number')
          .sort((a, b) => a - b);

        // Extract unique trade decision types for filter options
        this.availableTradeDecisionTypes = [...new Set(this.tradeDecisions.map(td => td.tradeDecisionType))]
          .filter((type): type is string => typeof type === 'string' && type.trim() !== '')
          .sort();

        if (this.paginator) {
          this.tradeDecisionsPaginatorDataSource.paginator = this.paginator;
        }
      }
    );
  }

  applyFilter(): void {
    const selectedConfigNums = this.configNumFilter.value;
    const selectedTradeDecisionTypes = this.tradeDecisionTypeFilter.value;

    let filtered = [...this.tradeDecisions];

    // Apply config number filter
    if (selectedConfigNums && selectedConfigNums.length > 0) {
      filtered = filtered.filter(td =>
        td.appJobConfig?.appJobConfigNum !== undefined && selectedConfigNums.includes(td.appJobConfig?.appJobConfigNum)
      );
    }

    // Apply trade decision type filter
    if (selectedTradeDecisionTypes && selectedTradeDecisionTypes.length > 0) {
      filtered = filtered.filter(td =>
        td.tradeDecisionType && selectedTradeDecisionTypes.includes(td.tradeDecisionType)
      );
    }

    this.filteredTradeDecisions = filtered;
    this.tradeDecisionsPaginatorDataSource.data = this.filteredTradeDecisions;

    // Reset paginator to first page
    if (this.paginator) {
      this.paginator.firstPage();
    }
  }

  clearFilter(): void {
    this.configNumFilter.setValue([]);
    this.tradeDecisionTypeFilter.setValue([]);
  }

  addNewTradeDecisionMessage(message: TradeDecisionModel): void {
    // console.log('Adding new TradeDecision message:', message);

    this.tradeDecisions.unshift(message);

    if(this.configNumFilter.value?.length === 0 && this.tradeDecisionTypeFilter.value?.length === 0) {
      this.tradeDecisionsPaginatorDataSource.data = this.tradeDecisions;
    } else {
      let shouldInclude = true;

      // Check config number filter
      if (this.configNumFilter.value && this.configNumFilter.value.length > 0) {
        shouldInclude = shouldInclude &&
          message.appJobConfig?.appJobConfigNum !== undefined &&
          this.configNumFilter.value.includes(message.appJobConfig.appJobConfigNum);
      }

      // Check trade decision type filter
      if (this.tradeDecisionTypeFilter.value && this.tradeDecisionTypeFilter.value.length > 0) {
        shouldInclude = shouldInclude &&
          message.tradeDecisionType !== undefined &&
          this.tradeDecisionTypeFilter.value.includes(message.tradeDecisionType);
      }

      if (shouldInclude) {
        this.filteredTradeDecisions.unshift(message);
      }

      this.tradeDecisionsPaginatorDataSource.data = this.filteredTradeDecisions;
    }

    // Refresh paginator
    if (this.paginator) {
      this.tradeDecisionsPaginatorDataSource.paginator = this.paginator;
    }

     if(this.playAudio) {
        // Play sound alert
         this.audio.play();
     }

  }

  updateAllOpenTrades(): void {
      const selectedConfigNums = this.configNumFilter.value;

      let appJobConfigNums: number[] = [];
      if(selectedConfigNums && selectedConfigNums.length > 0) {
        appJobConfigNums = [...selectedConfigNums];
      }
      else
        appJobConfigNums.push(-1); // Indicate all configs

      console.log('Updating all open trades for config nums:', appJobConfigNums);

      this.dataService.updateAllOpenTrades(appJobConfigNums).pipe().subscribe(
        (response) => {
          console.log('Update All Open Trades response:', response);
          if(response>0) {
             this.ngOnInit();
          }
        }
      );
    }

  }




