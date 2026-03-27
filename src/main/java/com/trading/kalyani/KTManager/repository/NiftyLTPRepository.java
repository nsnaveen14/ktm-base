package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.NiftyLTP;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface NiftyLTPRepository extends CrudRepository<NiftyLTP, Long> {

    @Query(value = "SELECT * FROM NIFTYLTP WHERE ID > :startIndex ORDER BY ID ASC LIMIT 10", nativeQuery = true)
    List<NiftyLTP> getDataForChart(@Param("startIndex") Long startIndex);

    @Query(value = "SELECT * FROM NIFTYLTP ORDER BY ID ASC", nativeQuery = true)
    List<NiftyLTP> getAllDataForChart();

    @Query(value = "SELECT * FROM NIFTYLTP WHERE NIFTYTS > :reqDateTime ORDER BY ID DESC", nativeQuery = true)
    List<NiftyLTP> getDataAfterSpecifiedTS(@Param("reqDateTime") LocalDateTime reqDateTime);

    @Query(value = "SELECT * FROM NIFTYLTP WHERE ID = (SELECT MAX(ID) FROM NIFTYLTP)", nativeQuery = true)
    NiftyLTP getLatestNiftyLTP();

    @Query(value = " SELECT * FROM niftyltp where trade_decision in ('BUY','SELL') order by niftyts desc limit 5", nativeQuery = true)
    List<NiftyLTP> getNiftyLTPTestData();

}
