package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Entity
@Table(name="CandleStick")
public class CandleStick {

    @Id
    @GeneratedValue(strategy= GenerationType.SEQUENCE)
    private Long id;

    Long instrumentToken;

    Double openPrice;
    Double highPrice;
    Double lowPrice;
    Double closePrice;

    LocalDateTime candleStartTime;
    LocalDateTime candleEndTime;

}
