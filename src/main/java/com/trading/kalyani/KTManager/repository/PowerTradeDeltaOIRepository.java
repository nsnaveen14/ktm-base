package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.NiftyLTP;
import com.trading.kalyani.KTManager.entity.PowerTradeDeltaOI;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface PowerTradeDeltaOIRepository extends CrudRepository<PowerTradeDeltaOI,Long> {

    @Query(value = "SELECT * FROM power_trade_deltaoi WHERE NIFTYTS > :reqDateTime ORDER BY ID DESC", nativeQuery = true)
    List<PowerTradeDeltaOI> getDataAfterSpecifiedTS(@Param("reqDateTime") LocalDateTime reqDateTime);

}
