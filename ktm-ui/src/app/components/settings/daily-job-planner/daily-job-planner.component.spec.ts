import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DailyJobPlannerComponent } from './daily-job-planner.component';

describe('DailyJobPlannerComponent', () => {
  let component: DailyJobPlannerComponent;
  let fixture: ComponentFixture<DailyJobPlannerComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [DailyJobPlannerComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(DailyJobPlannerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
