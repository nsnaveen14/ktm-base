import { ComponentFixture, TestBed } from '@angular/core/testing';

import { JobRunningStatusComponent } from './job-running-status.component';

describe('JobRunningStatusComponent', () => {
  let component: JobRunningStatusComponent;
  let fixture: ComponentFixture<JobRunningStatusComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [JobRunningStatusComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(JobRunningStatusComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
