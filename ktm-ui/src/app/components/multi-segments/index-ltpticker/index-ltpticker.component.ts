import { Component, Input, OnInit, OnChanges, SimpleChanges, ViewChild, AfterViewInit } from '@angular/core';
import { AppJobConfig } from '../../../models/AppJobConfig';
import { CommonModule } from '@angular/common';
import { DataService } from '../../../services/data.service';
import { IndexLTPModel } from '../../../models/indexLTPModel';
import { MatTable,MatTableModule,MatTableDataSource} from '@angular/material/table';
import {MatPaginator, MatPaginatorModule} from '@angular/material/paginator';
import { MiniDelta } from '../../../models/minidelta';
import { TradeFilterService } from '../../../services/trade-filter.service';


@Component({
  selector: 'app-index-ltpticker',
  imports: [CommonModule, MatTableModule, MatPaginatorModule],
  templateUrl: './index-ltpticker.component.html',
  styleUrl: './index-ltpticker.component.css'
})
export class IndexLtptickerComponent implements OnInit, OnChanges, AfterViewInit {
  @Input() selectedConfigFromMultiSegment: AppJobConfig | any;
  indexLTPData: { [key: number]: IndexLTPModel[] } = {}; // Indexed by appJobConfigNum

  displayedColumns: string[] = [

    'appJobConfigNum',
    'indexTS',
    'indexLTP',
    'meanStrikePCR',
    'meanRateOI',
    'combiRate',
    'support',
    'resistance',
    'range',
    'tradeDecision',
    'maxPainSP',
    'maxPainSPSecond'

  ];

  indexLTPPaginatorDataSource = new MatTableDataSource<IndexLTPModel>();
  @ViewChild(MatPaginator) paginator!: MatPaginator;

  private filterTradeDecisions: boolean = false;
  private allIndexLTPData: IndexLTPModel[] = []; // Store all data

  constructor(private dataService: DataService,private tradeFilterService: TradeFilterService) {}


  ngOnInit(): void {
        
    // Subscribe to changes
  this.tradeFilterService.filterTradeDecisions$.subscribe(value => {
    this.filterTradeDecisions = value;
  });

    if (this.selectedConfigFromMultiSegment) {
      this.getIndexLTPData(this.selectedConfigFromMultiSegment.appJobConfigNum);
       }

       
  }

  ngAfterViewInit(): void {
    this.indexLTPPaginatorDataSource.paginator = this.paginator;
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (
      changes['selectedConfigFromMultiSegment'] &&
      !changes['selectedConfigFromMultiSegment'].firstChange
    ) {
      this.getIndexLTPData(this.selectedConfigFromMultiSegment.appJobConfigNum);

    }

 }

  getIndexLTPData(appJobConfigNum: number): void {
    
    this.dataService.getIndexLTPDataByConfigNum(appJobConfigNum).subscribe(
      (response: IndexLTPModel[]) => {
        this.indexLTPData[appJobConfigNum] = response;
        this.indexLTPPaginatorDataSource.data = this.indexLTPData[appJobConfigNum];

        this.allIndexLTPData = this.indexLTPData[appJobConfigNum];
        // Set paginator after data is set and view is initialized
        if (this.paginator) {
          this.indexLTPPaginatorDataSource.paginator = this.paginator;
        }

        if(this.filterTradeDecisions) {
          this.applyTradeDecisionFilter(this.filterTradeDecisions);
        }
      }
    );
  }



  get currentIndexLTPData(): IndexLTPModel[] {
    return this.selectedConfigFromMultiSegment && this.indexLTPData[this.selectedConfigFromMultiSegment.appJobConfigNum]
      ? this.indexLTPData[this.selectedConfigFromMultiSegment.appJobConfigNum]
      : [];
  }

  addNewIndexLTPMessage(message: IndexLTPModel): void {
    console.log('Adding new IndexLTP message:', message);
    const appJobConfigNum = message.appJobConfigNum;
    if (appJobConfigNum === undefined) {
      // Optionally handle the error or just return
      return;
    }

    // Initialize array if it doesn't exist
    if (!this.indexLTPData[appJobConfigNum]) {
      this.indexLTPData[appJobConfigNum] = [];
    }

    // Add new message to the beginning of the array (latest first)
    this.indexLTPData[appJobConfigNum].unshift(message);

      // Update the data source if this message belongs to the currently selected config
    if (this.selectedConfigFromMultiSegment &&
        this.selectedConfigFromMultiSegment.appJobConfigNum === appJobConfigNum) {
      this.indexLTPPaginatorDataSource.data = this.indexLTPData[appJobConfigNum];
      // Refresh paginator
      if (this.paginator) {
        this.indexLTPPaginatorDataSource.paginator = this.paginator;
      }
    }

    // Store in complete dataset
    this.allIndexLTPData.push(message);
    
    // Apply filter and update paginator
    this.refreshDataSource();

  }

  

  applyTradeDecisionFilter(enableFilter: boolean): void {
    this.filterTradeDecisions = enableFilter;
    this.tradeFilterService.setFilterTradeDecisions(this.filterTradeDecisions);
    this.refreshDataSource();
  }

  private refreshDataSource(): void {
    let filteredData = this.allIndexLTPData;
    
    if (this.filterTradeDecisions) {
      filteredData = this.allIndexLTPData.filter(
        item => item.tradeDecision === 'BUY' || item.tradeDecision === 'SELL'
      );
      
    }
    this.indexLTPPaginatorDataSource.data = filteredData;
    // Set paginator after data is set and view is initialized
    if (this.paginator) {
      this.indexLTPPaginatorDataSource.paginator = this.paginator;
    }
  }

}
