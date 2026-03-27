package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Getter
@Setter
@Table(name="DeltaOIEntity")
@AllArgsConstructor
@NoArgsConstructor
public class DeltaOIEntity {

    @Id
    @GeneratedValue(strategy= GenerationType.SEQUENCE)
    private Long id;

    public String strikePrice;

    public Double rateOI,strikePCR,callOI,putOI;

    public Double callOIChange,putOIChange;

    @ManyToOne
    @JoinColumn(name = "jobIterationId", referencedColumnName = "id", nullable = false)
    public JobIterationDetails jobIterationDetails;


}
