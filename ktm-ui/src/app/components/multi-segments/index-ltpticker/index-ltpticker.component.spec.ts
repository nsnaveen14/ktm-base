import { ComponentFixture, TestBed } from '@angular/core/testing';

import { IndexLtptickerComponent } from './index-ltpticker.component';

describe('IndexLtptickerComponent', () => {
  let component: IndexLtptickerComponent;
  let fixture: ComponentFixture<IndexLtptickerComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [IndexLtptickerComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(IndexLtptickerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
