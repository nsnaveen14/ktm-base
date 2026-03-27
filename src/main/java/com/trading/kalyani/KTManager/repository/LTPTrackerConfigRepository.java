package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.LTPTrackerConfig;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface LTPTrackerConfigRepository extends CrudRepository<LTPTrackerConfig, Integer> {
}
