package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.*;

import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Entity
public class LTPTrackerConfig {


    @Id
    private Integer appJobConfigNum;

    private LocalDateTime configSTP;

    private String atmStrikePriceCE;

    private String atmStrikePricePE;

}
