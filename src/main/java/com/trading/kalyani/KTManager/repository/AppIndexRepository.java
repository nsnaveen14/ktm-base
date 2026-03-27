package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.AppIndexConfig;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface AppIndexRepository extends CrudRepository<AppIndexConfig,Long> {


    @Query(value = """
            SELECT * FROM APP_INDEX_CONFIG WHERE is_active = TRUE ORDER BY INDEX_ID ASC
            """,nativeQuery = true)
    List<AppIndexConfig> findActiveAppIndices();
}
