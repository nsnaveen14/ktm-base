import { ComponentFixture, TestBed } from '@angular/core/testing';

import { LTPTrackerConfigComponent } from './ltptracker-config.component';

describe('LTPTrackerConfigComponent', () => {
  let component: LTPTrackerConfigComponent;
  let fixture: ComponentFixture<LTPTrackerConfigComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [LTPTrackerConfigComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(LTPTrackerConfigComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
