package com.trading.kalyani.KTManager.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import lombok.*;

import java.util.Date;

@Entity
@Getter
@Setter
@Table(name="OISnapshot")
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class OISnapshotEntity {

    @Id
    public Long instrument_token;

    private Double oi;

    private Date tickTimestamp;

    public String tradingsymbol;

    public String name;

    @Version
    private Integer version;

}
