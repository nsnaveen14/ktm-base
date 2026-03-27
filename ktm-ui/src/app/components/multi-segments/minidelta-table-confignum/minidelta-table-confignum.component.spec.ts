import { ComponentFixture, TestBed } from '@angular/core/testing';

import { MinideltaTableConfignumComponent } from './minidelta-table-confignum.component';

describe('MinideltaTableConfignumComponent', () => {
  let component: MinideltaTableConfignumComponent;
  let fixture: ComponentFixture<MinideltaTableConfignumComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [MinideltaTableConfignumComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(MinideltaTableConfignumComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
