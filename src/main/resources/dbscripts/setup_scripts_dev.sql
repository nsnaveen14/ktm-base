INSERT INTO realtime_dev.app_index_config (index_id,is_active,instrument_token,index_name,job_end_expression,job_start_expression,snapshot_expression,strike_price_name,strike_price_segment,t_factor,price_gap,default_threshold,atm_range) VALUES
	 (1,true,256265,'NIFTY 50','1 30 15 * * *','1 15 9 * * *','1 10 9 * * *','NIFTY','NFO-OPT',3,50,25,1000),
	 (2,true,265,'SENSEX','1 30 15 * * *','1 15 9 * * *','1 10 9 * * *','SENSEX','BFO-OPT',2,100,15,2000),
	 (3,true,260105,'NIFTY BANK','1 30 15 * * *','1 15 9 * * *','1 10 9 * * *','BANKNIFTY','NFO-OPT',2,100,15,2000),
	 (4,true,292361,'CRUDE OIL','','','','CRUDEOIL','MCX-OPT',1,NULL,NULL,NULL),
	 (5,true,293385,'ENERGY','','','',NULL,NULL,1,NULL,NULL,NULL);


INSERT INTO realtime_dev.job_type (job_type_code,job_type,job_iteration_delay_seconds) VALUES
	 (1,'Current_Week',2),
	 (2,'Next_Week',2),
	 (3,'Monthly',5);

INSERT INTO realtime_dev.app_job_config (index_id,job_type_code,is_active,auto_trade_enabled) VALUES
	 (1,1,true,true),
	 (1,2,true,false),
	 (1,3,true,false),
	 (2,1,true,false),
	 (2,2,true,false),
	 (2,3,true,false),
	 (3,1,true,false);

INSERT INTO realtime_dev.ltptracker_config
(app_job_config_num, atm_strike_pricece, atm_strike_pricepe, configstp) VALUES
(1, '26200', '26100', current_timestamp),
(2, '26200', '26100', current_timestamp),
(3, '26200', '26100', current_timestamp),
(4, '86000', '85900', current_timestamp),
(5, '86000', '85900', current_timestamp),
(6, '86000', '85900', current_timestamp),
(7, '60000', '59000', current_timestamp)

-- realtime.predicted_candle_stick definition

-- Drop table

-- DROP TABLE realtime.predicted_candle_stick;

CREATE TABLE realtime.predicted_candle_stick (
	id int8 NOT NULL,
	actual_close_price float8 NULL,
	candle_end_time timestamp(6) NULL,
	candle_start_time timestamp(6) NULL,
	close_price float8 NULL,
	confidence_score float8 NULL,
	high_price float8 NULL,
	instrument_token int8 NULL,
	low_price float8 NULL,
	max_pain_strike int4 NULL,
	open_price float8 NULL,
	pcr_at_prediction float8 NULL,
	predicted_volatility float8 NULL,
	prediction_accuracy float8 NULL,
	prediction_basis varchar(255) NULL,
	prediction_generated_at timestamp(6) NULL,
	prediction_sequence int4 NULL,
	resistance_level float8 NULL,
	support_level float8 NULL,
	trend_direction varchar(255) NULL,
	verified bool NULL,
	CONSTRAINT predicted_candle_stick_pkey PRIMARY KEY (id)
);

CREATE SEQUENCE predicted_candle_stick_seq START 1 INCREMENT 1;
