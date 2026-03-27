package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.LocalDateTime;

@Entity
@Getter
@Setter
@Table(name="MiniDelta")
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class MiniDelta {
    @Id
    @GeneratedValue(strategy= GenerationType.AUTO)
    private Long id;

    private Integer appJobConfigNum;

    private LocalDateTime deltaInstant;

    private String strikePrice;

    private Double rateOI,strikePCR,callOI,putOI,callOIChange,putOIChange;

    @ManyToOne
    @JoinColumn(name = "jobIterationId", referencedColumnName = "id", nullable = false)
    public JobIterationDetails jobIterationDetails;

    @Transient
    private String rateOIColor,strikePCRColor,callOIColor,putOIColor,callOIChangeColor,putOIChangeColor;

}
