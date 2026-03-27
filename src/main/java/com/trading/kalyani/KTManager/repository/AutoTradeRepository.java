package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.model.AutoTradeParams;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface AutoTradeRepository extends CrudRepository<AutoTradeParams,Long> {

    @Query(value = "SELECT * FROM AUTO_TRADE_PARAMS WHERE job_iteration_id = :jobITID ", nativeQuery = true)
    AutoTradeParams findLatestAutoTradeParamsByJobIterationID(Long jobITID);

}
