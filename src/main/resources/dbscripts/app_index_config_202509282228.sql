INSERT INTO realtime.app_index_config (index_id,is_active,instrument_token,index_name,job_end_expression,job_start_expression,snapshot_expression,strike_price_name,strike_price_segment,t_factor,price_gap,default_threshold,atm_range) VALUES
	 (1,true,256265,'NIFTY 50','1 30 15 * * *','1 15 9 * * *','1 10 9 * * *','NIFTY','NFO-OPT',3,50,25,1000),
	 (2,true,265,'SENSEX','1 30 15 * * *','1 15 9 * * *','1 10 9 * * *','SENSEX','BFO-OPT',2,100,15,2000),
	 (3,true,260105,'NIFTY BANK','1 30 15 * * *','1 15 9 * * *','1 10 9 * * *','BANKNIFTY','NFO-OPT',2,100,15,2000),
	 (4,true,292361,'CRUDE OIL','','','','CRUDEOIL','MCX-OPT',1,NULL,NULL,NULL),
	 (5,true,293385,'ENERGY','','','',NULL,NULL,1,NULL,NULL,NULL);
