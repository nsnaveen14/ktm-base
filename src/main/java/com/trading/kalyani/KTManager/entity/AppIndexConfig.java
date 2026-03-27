package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Getter
@Setter
@Table(name="AppIndexConfig")
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class AppIndexConfig {

    @Id
    private Integer indexId;

    private String indexName;

    private long instrumentToken;

    private String snapshotExpression;

    private String jobStartExpression;

    private String jobEndExpression;

    private boolean isActive;

    private String strikePriceName;

    private String strikePriceSegment;

    private Integer tFactor;

    private Integer priceGap;

    private Integer defaultThreshold;

    private Integer atmRange;


}
