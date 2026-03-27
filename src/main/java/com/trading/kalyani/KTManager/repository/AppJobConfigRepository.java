package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.AppJobConfig;
import org.springframework.data.repository.CrudRepository;

public interface AppJobConfigRepository extends CrudRepository<AppJobConfig,Integer> {

    Iterable<AppJobConfig> findAllByOrderByAppJobConfigNumAsc();
}
