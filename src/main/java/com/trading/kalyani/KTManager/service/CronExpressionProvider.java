package com.trading.kalyani.KTManager.service;

public interface CronExpressionProvider {

    String getCronExpressionForSnapshot(Integer indexId);

    String getCronExpressionForStartJob(Integer indexId);

    String getCronExpressionForEndJob(Integer indexId);

}
