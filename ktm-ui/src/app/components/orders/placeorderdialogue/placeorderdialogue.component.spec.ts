import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PlaceorderdialogueComponent } from './placeorderdialogue.component';

describe('PlaceorderdialogueComponent', () => {
  let component: PlaceorderdialogueComponent;
  let fixture: ComponentFixture<PlaceorderdialogueComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [PlaceorderdialogueComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(PlaceorderdialogueComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
