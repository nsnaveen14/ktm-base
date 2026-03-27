import { ComponentFixture, TestBed } from '@angular/core/testing';

import { MypositionsComponent } from './mypositions.component';

describe('MypositionsComponent', () => {
  let component: MypositionsComponent;
  let fixture: ComponentFixture<MypositionsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [MypositionsComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(MypositionsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
