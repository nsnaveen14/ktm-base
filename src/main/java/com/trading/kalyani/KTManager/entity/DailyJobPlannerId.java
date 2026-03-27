package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;
import java.io.Serializable;
import java.time.LocalDate;

@Embeddable
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class DailyJobPlannerId implements Serializable {
    private LocalDate jobDate;
    private Integer appJobConfigNum;
}

