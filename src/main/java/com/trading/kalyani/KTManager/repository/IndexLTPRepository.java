package com.trading.kalyani.KTManager.repository;

import com.trading.kalyani.KTManager.entity.IndexLTP;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface IndexLTPRepository extends CrudRepository<IndexLTP, Long> {

    @Query(value = "SELECT * FROM indexLTP WHERE display=true ORDER BY id DESC LIMIT 2000", nativeQuery = true)
    List<IndexLTP> findLatestIndexDataForAllIndex();

    @Query(value = "SELECT * FROM indexLTP WHERE app_job_config_num = :appJobConfigNum and display=true ORDER BY id DESC LIMIT 1000", nativeQuery = true)
    List<IndexLTP> findLatestIndexDataByAppJobConfigNum(@Param("appJobConfigNum") Integer appJobConfigNum);

    @Query(value = "SELECT * FROM indexLTP WHERE app_job_config_num = :appJobConfigNum ORDER BY id DESC LIMIT 5000", nativeQuery = true)
    List<IndexLTP> findLast5000IndexDataByAppJobConfigNum(@Param("appJobConfigNum") Integer appJobConfigNum);

    // Derived query to fetch the IndexLTP by the associated JobIterationDetails id
    Optional<IndexLTP> findByJobIterationDetails_Id(Long jobIterationId);

    // Find latest IndexLTP by appJobConfigNum (for Brahmastra PCR lookup)
    IndexLTP findFirstByAppJobConfigNumOrderByIndexTSDesc(Integer appJobConfigNum);
}
