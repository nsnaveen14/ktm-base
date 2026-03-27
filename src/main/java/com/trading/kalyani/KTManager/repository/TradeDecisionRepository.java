package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.TradeDecisions;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

@Repository
public interface TradeDecisionRepository extends CrudRepository<TradeDecisions,Long> {

    @Query(value="SELECT * FROM trade_decisions ORDER BY id DESC limit 1", nativeQuery = true)
    Optional<TradeDecisions> findLatestTradeDecision();

    @Query(value="SELECT * FROM trade_decisions WHERE trade_decision=:type and trade_decisionts >= :tradingDate ORDER BY id DESC", nativeQuery = true)
    List<TradeDecisions> findTradeDecisionsByTypeAndDate(String type, LocalDate tradingDate);

    @Query(value = "SELECT * FROM trade_decisions WHERE app_job_config_num = :appJobConfigNum ORDER BY id DESC LIMIT 500", nativeQuery = true)
    List<TradeDecisions> findLatestTradesByAppJobConfigNum(Integer appJobConfigNum);

    @Query(value = "SELECT * FROM trade_decisions ORDER BY id DESC LIMIT 500", nativeQuery = true)
    List<TradeDecisions> findLatestTradesForAllIndex();

    @Query(value="SELECT * FROM trade_decisions WHERE app_job_config_num = :appJobConfigNum ORDER BY id DESC limit 1", nativeQuery = true)
    Optional<TradeDecisions> findLatestTradeDecisionByConfigNum(Integer appJobConfigNum);

    @Transactional
    @Modifying(clearAutomatically = true)
    @Query(value="UPDATE trade_decisions SET STATUS = 'COMPLETE', TRADE_DECISION_RESULT = 'LAPSED', TRADE_DECISION_RESULT_TS = CURRENT_TIMESTAMP WHERE STATUS = 'OPEN'",nativeQuery = true)
    int updateAllOpenTrades();

    @Transactional
    @Modifying(clearAutomatically = true)
    @Query(value="UPDATE trade_decisions SET STATUS = 'COMPLETE', TRADE_DECISION_RESULT = 'LAPSED', TRADE_DECISION_RESULT_TS = CURRENT_TIMESTAMP WHERE app_job_config_num IN (:appJobConfigNums) AND STATUS = 'OPEN'",nativeQuery = true)
    int updateAllOpenTradesByAppJobConfigNum(@Param("appJobConfigNums") List<Integer> appJobConfigNums);

}
