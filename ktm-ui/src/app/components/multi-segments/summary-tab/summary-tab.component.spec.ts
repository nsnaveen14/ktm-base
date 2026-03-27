import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SummaryTabComponent } from './summary-tab.component';

describe('SummaryTabComponent', () => {
  let component: SummaryTabComponent;
  let fixture: ComponentFixture<SummaryTabComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [SummaryTabComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(SummaryTabComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
