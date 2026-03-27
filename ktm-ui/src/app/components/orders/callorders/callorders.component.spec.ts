import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CallordersComponent } from './callorders.component';

describe('CallordersComponent', () => {
  let component: CallordersComponent;
  let fixture: ComponentFixture<CallordersComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [CallordersComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(CallordersComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
