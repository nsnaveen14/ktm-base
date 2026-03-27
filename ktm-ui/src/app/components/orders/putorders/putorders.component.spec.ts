import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PutordersComponent } from './putorders.component';

describe('PutordersComponent', () => {
  let component: PutordersComponent;
  let fixture: ComponentFixture<PutordersComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [PutordersComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(PutordersComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
