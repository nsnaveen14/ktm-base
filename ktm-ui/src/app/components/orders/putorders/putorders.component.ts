import { ChangeDetectionStrategy,Component, Input, OnInit, OnChanges, SimpleChanges, AfterViewInit,ViewChild  } from '@angular/core';
import { InstrumentMiniTableSource } from '../instrumentMiniTableSource';
import { MatTableDataSource,MatTableModule } from '@angular/material/table';
import {MatPaginator, MatPaginatorModule} from '@angular/material/paginator';
import {MatFormFieldModule} from '@angular/material/form-field';
import {MatInputModule} from '@angular/material/input';
import {MatCheckboxModule} from '@angular/material/checkbox'; 
import { MatSelect } from '@angular/material/select'; 
import {SelectionModel} from '@angular/cdk/collections';
import { MatDialog } from '@angular/material/dialog';
import { PlaceorderdialogueComponent } from '../placeorderdialogue/placeorderdialogue.component';
import { OrderRequest } from '../../../models/orderRequest';
import { DataService } from '../../../services/data.service';

@Component({
  selector: 'app-putorders',
  imports: [MatTableModule,MatPaginator, MatPaginatorModule,MatFormFieldModule, MatInputModule,MatCheckboxModule],
  templateUrl: './putorders.component.html',
  styleUrl: './putorders.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class PutordersComponent {
  @ViewChild(MatPaginator) paginator: MatPaginator|any;

  @Input() putTableDataSource =new MatTableDataSource<InstrumentMiniTableSource>();
  putTableColumns: string[] = ['select','strike','instrument_token','tradingsymbol','last_price'];
  selection = new SelectionModel<InstrumentMiniTableSource>(true, []);
  successMessage: string | null = null;
  errorMessage: string | null = null;
  constructor(public dialog: MatDialog,private dataService: DataService) { 
    
  }

  ngOnInit(): void {
    
  }
  
  ngAfterViewInit() {
    this.putTableDataSource.paginator = this.paginator;
  }

  applyFilter(event: Event) {
      const filterValue = (event.target as HTMLInputElement).value;
      this.putTableDataSource.filter = filterValue.trim().toUpperCase();
  
    }
  
     /** Whether the number of selected elements matches the total number of rows. */
     isAllSelected() {
      const numSelected = this.selection.selected.length;
      const numRows = this.putTableDataSource.data.length;
      return numSelected === numRows;
    }
  
     /** Selects all rows if they are not all selected; otherwise clear selection. */
     toggleAllRows() {
      if (this.isAllSelected()) {
        this.selection.clear();
        return;
      }
  
      this.selection.select(...this.putTableDataSource.data);
    }
  
    /** The label for the checkbox on the passed row */
    checkboxLabel(row?: InstrumentMiniTableSource): string {
      if (!row) {
        return `${this.isAllSelected() ? 'deselect' : 'select'} all`;
      }
      return `${this.selection.isSelected(row) ? 'deselect' : 'select'} row ${(row.id as number) + 1}`;
    }

    openDialog(row: any): void {
      const dialogRef = this.dialog.open(PlaceorderdialogueComponent, {
        width: '600px',
        height: '400px',
        data: row
      });
  
      dialogRef.afterClosed().subscribe(result => {
        console.log(`Dialog result: ${result}`);
        if(result){
          this.placeOrder(row, result.orderType, result.quantity);
          
        }
      });
    }

    placeOrder(row: any, orderType: string, quantity: number){
          console.log(row);
          
          let orderRequest: OrderRequest = {
            tradingSymbol: row.tradingsymbol,
            exchange: row.exchange,
            transaction_type: orderType,
            order_type: "MARKET",
            quantity: quantity*row.lot_size,
            product: "NRML",
            price: row.last_price,
            trigger_price: row.last_price,
            validity: "DAY"
          }
    
          this.dataService.placeOrder(orderRequest).subscribe({
            next: (v) => {console.log(v);
              this.successMessage = 'Transaction executed successfully';
              
            },
            error: (e) => {console.error(e);
              if (e.status === 500) {
                
                this.errorMessage = 'Internal Server Error. Please try again later.';
              } else {
                this.errorMessage = 'Transaction failed. Please try again.';
              }
                       
            },
            complete: () => {console.info('complete')} 
        });
          
        }
    


}
