import { Component, Output, OnInit } from '@angular/core';
import { CallordersComponent } from './callorders/callorders.component';
import { PutordersComponent } from './putorders/putorders.component';
import { MyordersComponent } from './myorders/myorders.component';
import { MypositionsComponent } from './mypositions/mypositions.component';
import { InstrumentMiniTableSource } from './instrumentMiniTableSource';
import { DataService } from '../../services/data.service';
import { InstrumentEntity } from '../../models/instrumentEntity';
import { MatTableDataSource } from '@angular/material/table';
import { WebsocketService } from '../../services/web-socket.service';

@Component({
  selector: 'app-orders',
  imports: [CallordersComponent,PutordersComponent,MyordersComponent,MypositionsComponent],
  templateUrl: './orders.component.html',
  styleUrl: './orders.component.css'
})
export class OrdersComponent implements OnInit {

  callTableSource: InstrumentMiniTableSource[] = [];
  callTableDataSource =new MatTableDataSource<InstrumentMiniTableSource>();
  putTableSource: InstrumentMiniTableSource[] = [];
  putTableDataSource =new MatTableDataSource<InstrumentMiniTableSource>();
  allInstrumentsData: InstrumentEntity[]=[];
  
  constructor(private dataService: DataService,private webSocketService: WebsocketService) { }

  ngOnInit() {
    this.loadInstrumentsData();
    console.log("this.webSocketService.kiteWSUrl");
    console.log(this.webSocketService.kiteWSUrl);
  }
  
  loadInstrumentsData() {

    this.dataService.getInstrumentData().pipe().subscribe(data => {
      this.allInstrumentsData = data;
     
   this.putTableSource =[];
   this.callTableSource =[];
   this.allInstrumentsData.map(i => {
      
      let instrumentMiniTableSource = new InstrumentMiniTableSource();
      instrumentMiniTableSource.id = i.id;
      instrumentMiniTableSource.instrument_token = i.instrument.instrument_token;
      instrumentMiniTableSource.tradingsymbol = i.instrument.tradingsymbol;
      instrumentMiniTableSource.lot_size = i.instrument.lot_size;
      instrumentMiniTableSource.exchange = i.instrument.exchange;
      instrumentMiniTableSource.last_price = i.instrument.last_price;
      instrumentMiniTableSource.strike = i.instrument.strike;
      if(i.instrument.instrument_type == 'CE'){
        this.callTableSource.push(instrumentMiniTableSource);
      }else{
        this.putTableSource.push(instrumentMiniTableSource);
      }
   
    });

    this.callTableDataSource.data = this.callTableSource;
    this.putTableDataSource.data = this.putTableSource;
     
  });
  }

  placeOrder(row: any){
    console.log(row);
    console.log("Order Placed")
  }

}
