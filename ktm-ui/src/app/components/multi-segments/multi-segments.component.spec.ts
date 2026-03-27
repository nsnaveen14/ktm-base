import { ComponentFixture, TestBed } from '@angular/core/testing';

import { MultiSegmentsComponent } from './multi-segments.component';

describe('MultiSegmentsComponent', () => {
  let component: MultiSegmentsComponent;
  let fixture: ComponentFixture<MultiSegmentsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [MultiSegmentsComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(MultiSegmentsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
