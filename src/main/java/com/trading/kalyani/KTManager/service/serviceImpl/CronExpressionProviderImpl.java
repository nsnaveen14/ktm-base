package com.trading.kalyani.KTManager.service.serviceImpl;

import com.trading.kalyani.KTManager.entity.AppIndexConfig;
import com.trading.kalyani.KTManager.repository.AppIndexRepository;
import com.trading.kalyani.KTManager.service.CronExpressionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

@Service
public class CronExpressionProviderImpl implements CronExpressionProvider {

    private static final Logger logger = LoggerFactory.getLogger(CronExpressionProviderImpl.class);

    @Autowired
    AppIndexRepository appIndexRepository;

    @Override
    public String getCronExpressionForSnapshot(Integer indexId) {
        String expr = findConfig(indexId).getSnapshotExpression();
        if (expr == null) {
            logger.warn("snapshotExpression is null for indexId={}", indexId);
        }
        return expr;
    }

    @Override
    public String getCronExpressionForStartJob(Integer indexId) {
        String expr = findConfig(indexId).getJobStartExpression();
        if (expr == null) {
            logger.warn("jobStartExpression is null for indexId={}", indexId);
        }
        return expr;
    }

    @Override
    public String getCronExpressionForEndJob(Integer indexId) {
        String expr = findConfig(indexId).getJobEndExpression();
        if (expr == null) {
            logger.warn("jobEndExpression is null for indexId={}", indexId);
        }
        return expr;
    }

    /**
     * Fetch AppIndexConfig by indexId, throwing a clear exception if the record is missing.
     * Result is cached — all three getCronExpression* calls for the same indexId share one DB hit.
     */
    @Cacheable(value = "appIndexConfig", key = "#indexId")
    public AppIndexConfig findConfig(Integer indexId) {
        logger.debug("Loading AppIndexConfig from DB for indexId={}", indexId);
        return appIndexRepository.findById(indexId.longValue())
                .orElseThrow(() -> new IllegalArgumentException(
                        "No AppIndexConfig found for indexId=" + indexId));
    }

    /**
     * Evict the cached config for a specific index (call after updating cron expressions in DB).
     */
    @CacheEvict(value = "appIndexConfig", key = "#indexId")
    public void evictConfig(Integer indexId) {
        logger.info("Evicted appIndexConfig cache for indexId={}", indexId);
    }

    /**
     * Evict all cached configs (call after a bulk update).
     */
    @CacheEvict(value = "appIndexConfig", allEntries = true)
    public void evictAllConfigs() {
        logger.info("Evicted all appIndexConfig cache entries");
    }
}
