import { ComponentFixture, TestBed } from '@angular/core/testing';

import { IndexLTPChartComponent } from './index-ltpchart.component';

describe('IndexLTPChartComponent', () => {
  let component: IndexLTPChartComponent;
  let fixture: ComponentFixture<IndexLTPChartComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [IndexLTPChartComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(IndexLTPChartComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
