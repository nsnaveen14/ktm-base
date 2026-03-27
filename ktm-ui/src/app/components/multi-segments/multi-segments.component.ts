import { Component, OnInit, AfterViewInit, ElementRef, ViewChild, OnDestroy } from '@angular/core';
import { DataService } from '../../services/data.service';
import { WebsocketService } from '../../services/web-socket.service';
import { CommonReqRes } from '../../models/CommonReqRes';
import { IndexOHLC } from '../../models/IndexOHLC';
import { MessageService } from '../../services/message.service';
import { AppJobConfig } from '../../models/AppJobConfig';
import { CommonModule } from '@angular/common';
import { MatTabsModule } from '@angular/material/tabs';
import { SummaryTabComponent } from './summary-tab/summary-tab.component';
import { IndexLtptickerComponent } from './index-ltpticker/index-ltpticker.component';
import { MinideltaTableConfignumComponent } from './minidelta-table-confignum/minidelta-table-confignum.component';
import { IndexLTPModel } from '../../models/indexLTPModel';
import { MiniDelta } from '../../models/minidelta';
import { FormsModule } from '@angular/forms';
import { MatSlideToggleModule } from '@angular/material/slide-toggle';
import { TradeFilterService } from '../../services/trade-filter.service';
import { SwingCandleModalComponent } from '../swing-candle-modal/swing-candle-modal.component';
import { SwingPointData } from '../../models/SwingPointData';
import { NgbModal } from '@ng-bootstrap/ng-bootstrap';
import { IndexLTPChartComponent } from './index-ltpchart/index-ltpchart.component';
import { PriceTick } from '../../models/performance.model';



@Component({
  selector: 'app-multi-segments',
  imports: [CommonModule, MatTabsModule, SummaryTabComponent, IndexLtptickerComponent, MinideltaTableConfignumComponent, FormsModule, MatSlideToggleModule],
  templateUrl: './multi-segments.component.html',
  styleUrl: './multi-segments.component.css'
})
export class MultiSegmentsComponent implements OnInit, OnDestroy {

  appJobConfigDetails: AppJobConfig[] | any;
  selectedAppJobConfigNum: number = 0; // Default selection
  selectedConfig: AppJobConfig | undefined;
  selectedTabIndex: number = 0; // Default tab index
  indexLTPMessageForChild: any;
  indexOHLCData: { [key: number]: IndexOHLC } = {}; // Indexed by appJobConfigNum
  currentIndexOHLCData: IndexOHLC | any;

  showOnlyTradeDecisions: boolean = false; // Add this property

  @ViewChild('indexLtptickerRef') indexLtptickerComponent!: IndexLtptickerComponent;
  @ViewChild('deltaTableRef') deltaTableComponent!: MinideltaTableConfignumComponent;
  @ViewChild('summaryTabTradeDecisionsRef') summaryTabTradeDecisionsComponent!: SummaryTabComponent;
  @ViewChild('indexLtpChartRef') indexLtpChartComponent!: IndexLTPChartComponent;
  @ViewChild(SwingCandleModalComponent) swingCandleModal!: SwingCandleModalComponent;
  swingCandleData: SwingPointData | null = null;
  currentIndexLTPDataForChart: IndexLTPModel[] = [];

  constructor(private dataService: DataService, private webSocketService: WebsocketService, private messageService: MessageService, private tradeFilterService: TradeFilterService, private modalService: NgbModal) {

  }

  toggleTradeDecisionFilter(): void {

    this.showOnlyTradeDecisions = !this.showOnlyTradeDecisions;
    this.tradeFilterService.setFilterTradeDecisions(this.showOnlyTradeDecisions);
    if (this.indexLtptickerComponent) {
      this.indexLtptickerComponent.applyTradeDecisionFilter(this.showOnlyTradeDecisions);
    }
  }

  ngOnInit(): void {

    // Subscribe to changes
    this.tradeFilterService.filterTradeDecisions$.subscribe(value => {
      this.showOnlyTradeDecisions = value;
    });
    this.getAppJobConfigDetails();
    this.updateSelectedConfig(); // Initialize the selected configuration

    this.webSocketService.listenToMultiSegmentTopics(
      (indexOHLCMessage) => {
        // Handle incoming IndexOHLC messages here
        console.log('Received IndexOHLC message in multisegment component:', indexOHLCMessage);

        // You can add your logic to process the message as needed
        const appJobConfigNum = indexOHLCMessage.appJobConfigNum;
        this.indexOHLCData[appJobConfigNum] = indexOHLCMessage;
        if (this.selectedAppJobConfigNum === appJobConfigNum)
          this.currentIndexOHLCData = this.indexOHLCData[this.selectedAppJobConfigNum];
      },
      (commonMessage) => {
        console.log('Common Message:', commonMessage);
        // Handle commonMessage here
        //  this.handleCommonMessageForJobParams(commonMessage);

        this.messageService.sendMessage(commonMessage);
      },
      (indexLTPMessage) => {
        console.log('Received IndexLTP message in multisegment component:', indexLTPMessage);
        // Handle incoming IndexLTP messages here
        // You can add your logic to process the message as needed
        this.handleIndexLTPMessage(indexLTPMessage);
      },
      (miniDeltaMessage) => {
        console.log('Received MiniDelta message in multisegment component:', miniDeltaMessage);
        // Handle incoming MiniDelta messages here
        this.handleMiniDeltaMessage(miniDeltaMessage);

      },
      (tradeDecisionMessage) => {
        console.log('Received TradeDecision message in multisegment component:', tradeDecisionMessage);
        // Handle incoming TradeDecision messages here
        this.handleTradeDecisionMessage(tradeDecisionMessage);
      },
      (priceTick: PriceTick) => {
        console.log('Price update received:', priceTick);
        //  this.handlePriceUpdate(priceTick);
        this.webSocketService.handlePriceUpdate(priceTick);
      },
      (iobZoneTouchAlert: any) => {
        console.log('IOB Zone Touch alert received:', iobZoneTouchAlert);
        this.webSocketService.handleIOBZoneTouchAlert(iobZoneTouchAlert);
      },
      (iobMitigationAlert: any) => {
        console.log('IOB Mitigation alert received:', iobMitigationAlert);
        this.webSocketService.handleIOBMitigationAlert(iobMitigationAlert);
      },
      (iobNewOrderAlert: any) => {
        console.log('IOB New Order alert received:', iobNewOrderAlert);
        this.webSocketService.handleIOBNewOrder(iobNewOrderAlert);
      },
      (autoTradeAlert: any) => {
        console.log('Auto Trade alert received:', autoTradeAlert);
        this.webSocketService.handleAutoTrade(autoTradeAlert);
      }
    );

  }

  handleIndexLTPMessage(indexLTPMessage: IndexLTPModel): void {
    // Pass the message to the IndexLtptickerComponent if it's available
    if (this.indexLtptickerComponent) {
      this.indexLtptickerComponent.addNewIndexLTPMessage(indexLTPMessage);
    }
  }
  handleMiniDeltaMessage(miniDeltaMessage: MiniDelta[]): void {
    if (this.deltaTableComponent) {
      this.deltaTableComponent.addNewMiniDeltaMessage(miniDeltaMessage);
    }
  }
  handleTradeDecisionMessage(tradeDecisionMessage: any): void {
    // Implement handling of TradeDecision messages if needed
    if (this.summaryTabTradeDecisionsComponent) {
      //  this.summaryTabTradeDecisionsComponent.addNewTradeDecisionMessage(tradeDecisionMessage);
      this.summaryTabTradeDecisionsComponent.resetView();
    }
  }

  ngOnDestroy(): void {
    // Unsubscribe from WebSocket topics to avoid receiving further messages
    this.webSocketService.ngOnDestroy();

    console.log('MultiSegmentsComponent destroyed and resources cleaned up.');
  }

  getAppJobConfigDetails() {

    this.dataService.getAppJobConfigDetails().subscribe(res => {

      this.appJobConfigDetails = res;

      console.log("App Job Config Details:", this.appJobConfigDetails);
      this.updateSelectedConfig(); // Update the selected configuration after fetching data
    });
  }

  onTabChange(index: number): void {
    console.log('Selected Tab Index:', index);
    this.selectedTabIndex = index;
    this.selectedAppJobConfigNum = this.appJobConfigDetails[index].appJobConfigNum;
    this.updateSelectedConfig();
    this.currentIndexOHLCData = this.indexOHLCData[this.selectedAppJobConfigNum];
  }

  updateSelectedConfig(): void {
    this.selectedConfig = this.appJobConfigDetails?.find(
      (config: AppJobConfig) => config.appJobConfigNum === this.selectedAppJobConfigNum
    );
    console.log('Selected Config:', this.selectedConfig);
  }

  startJob(): void {
    if (this.selectedConfig) {
      this.dataService.startJob(this.selectedConfig.appJobConfigNum).pipe().subscribe(
        (response) => {
          console.log('Job started successfully:', response);
          // Optionally, you can show a success message to the user
          //  this.messageService.sendMessage(new CommonReqRes(false,"Job Started Successfully",0,null,"success"));
        },
        (error) => {
          console.error('Error starting job:', error);
          // Optionally, you can show an error message to the user'
          this.messageService.sendMessage(new CommonReqRes(false, "Error in starting job", 0, null, "error"));
        }
      );
    }
  }



  stopJob(): void {
    if (this.selectedConfig) {
      this.dataService.stopJob(this.selectedConfig.appJobConfigNum).pipe().subscribe(
        (response) => {
          console.log('Job stopped successfully:', response);
          // Optionally, you can show a success message to the user
          this.messageService.sendMessage(new CommonReqRes(false, "Job Stopped Successfully", 0, null, "error"));
        },
        (error) => {
          console.error('Error stopping job:', error);
          // Optionally, you can show an error message to the user
          this.messageService.sendMessage(new CommonReqRes(false, "Error in stopping job", 0, null, "error"));
        }
      );
    }
  }

  getSwingCandleDataTest(): void {

    if (this.selectedConfig) {
      this.dataService.getSwingHighLowByConfigNum(this.selectedConfig.appJobConfigNum).subscribe(data => {
        console.log('Swing Candle Data Test:', data);
      });
    }

  }

  getSwingCandleData() {
    if (this.selectedConfig) {
      this.dataService.getSwingHighLowByConfigNum(this.selectedConfig.appJobConfigNum).subscribe({
        next: (data: SwingPointData) => {
          this.openSwingCandleModal(data);
        },
        error: (error) => {
          console.error('Error fetching swing candle data:', error);
        }
      });
    }
  }

  openSwingCandleModal(data: SwingPointData) {
    const modalRef = this.modalService.open(SwingCandleModalComponent, { size: 'xl' });
    modalRef.componentInstance.swingCandleData = data;
  }

  exitMarket() {
    this.dataService.exitMarket().subscribe(res => {
      let commonMessage: CommonReqRes | any;
      if (res > 0)
        commonMessage = new CommonReqRes(false, "All orders are closed", res, null, "success");

      else
        commonMessage = new CommonReqRes(false, "Error occured during exit market operation.", res, null, "error");


      this.messageService.sendMessage(commonMessage);
    });
  }

  cancelOpenOrders() {
    this.dataService.cancelOpenOrders().subscribe(res => {
      this.messageService.sendMessage(res);

    });
  }




}
