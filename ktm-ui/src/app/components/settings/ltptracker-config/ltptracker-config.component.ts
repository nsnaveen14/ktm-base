import { Component,ViewChild } from '@angular/core';
import { DataService } from '../../../services/data.service';
import { LTPTrackerConfig } from '../../../models/ltpTrackerConfig';
import { MatTable, MatTableModule, MatTableDataSource } from '@angular/material/table';
import {MatPaginator, MatPaginatorModule} from '@angular/material/paginator';
import { MatSelectModule } from '@angular/material/select';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatIconModule } from '@angular/material/icon';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MessageService } from '../../../services/message.service';
import { CommonReqRes } from '../../../models/CommonReqRes';

@Component({
  selector: 'app-ltptracker-config',
  imports: [
    CommonModule, MatTableModule, MatPaginatorModule, MatSelectModule,
    FormsModule, MatIconModule, MatTooltipModule
  ],
  templateUrl: './ltptracker-config.component.html',
  styleUrl: './ltptracker-config.component.css'
})
export class LTPTrackerConfigComponent {

  ltpTrackerConfigList:LTPTrackerConfig[] = []; 
  ltpTrackerPaginatorDataSource = new MatTableDataSource<LTPTrackerConfig>();
  @ViewChild(MatPaginator) paginator!: MatPaginator;
  displayedColumns: string[] = [
  'AppJobConfigNum',
  'Call StrikePrice',
  'Put StrikePrice',
  'Last Modified'
  ];
  editingRow: LTPTrackerConfig | null = null;
  editingColumn: string | null = null;

  constructor(private dataService: DataService, private messageService: MessageService) {}

  ngOnInit(): void {
    this.fetchLTPTrackerConfigs();
  }

  fetchLTPTrackerConfigs(): void {
    this.dataService.getLTPTrackerConfig().pipe().subscribe(response=>
     {
      this.ltpTrackerPaginatorDataSource.data = response;
      if (this.paginator) {
        this.ltpTrackerPaginatorDataSource.paginator = this.paginator;
      }
     }
    );
  }

  startEdit(row: LTPTrackerConfig, column: string) {
    this.editingRow = row;
    this.editingColumn = column;
  }

  saveEdit(row: LTPTrackerConfig, column: string, newValue: any) {
    (row as any)[column] = newValue;
    this.editingRow = null;
    this.editingColumn = null;
    this.dataService.setLTPTrackerConfig(row).subscribe({
      next: () => {
        this.messageService.sendMessage(
          new CommonReqRes(false, "LTP Tracker configuration updated successfully.", 0, null, "success")
        );
      },
      error: (err) => {
        this.messageService.sendMessage(
          new CommonReqRes(true, "Failed to update LTP Tracker configuration.", 0, err, "error")
        );
      }
    });
  }

}
