package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Entity
@Getter
@Setter
@Table(name="OrderBook")
@AllArgsConstructor
@NoArgsConstructor
@ToString

public class OrderBook {

    @Id
    private String orderId;

    private LocalDateTime orderPlacedSTP;

    private String userId;

    private Integer orderTargetPrice;

    private Integer orderStopLossPrice;

    private String orderStatus;

    @OneToOne
    @JoinColumn(name = "jobIterationId", referencedColumnName = "id", nullable = false)
    private JobIterationDetails jobIterationDetails;


}
