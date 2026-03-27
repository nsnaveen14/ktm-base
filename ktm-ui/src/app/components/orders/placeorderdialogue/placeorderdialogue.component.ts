import { ChangeDetectionStrategy,Component, Inject } from '@angular/core';
import { MAT_DIALOG_DATA,MatDialogModule } from '@angular/material/dialog';
import { MatButtonModule } from '@angular/material/button';
import {MatRadioModule} from '@angular/material/radio';
import { FormsModule } from '@angular/forms';

@Component({
  selector: 'app-placeorderdialogue',
  imports: [MatButtonModule,MatDialogModule,MatRadioModule,FormsModule],
  templateUrl: './placeorderdialogue.component.html',
  styleUrl: './placeorderdialogue.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class PlaceorderdialogueComponent {

  orderType: string = 'BUY';
  quantity: number = 1;

  constructor(@Inject(MAT_DIALOG_DATA) public data: any) {}
}
