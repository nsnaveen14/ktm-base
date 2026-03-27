package com.trading.kalyani.KTManager.entity;

import com.zerodhatech.models.Instrument;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Getter
@Setter
@Table(name="Instrument")
@AllArgsConstructor
@NoArgsConstructor
public class InstrumentEntity {

    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    private Long id;

    @Embedded
    private Instrument instrument;

}
