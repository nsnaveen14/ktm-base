package com.trading.kalyani.KTManager.entity;


import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Entity
@Table(name = "DailyJobPlanner")
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class DailyJobPlanner {

    @EmbeddedId
    private DailyJobPlannerId id;

    private LocalDate jobForExpiryDate;

    private boolean isJobRequired;

    private LocalDateTime lastModified;

}
