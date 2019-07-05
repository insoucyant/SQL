--**************************************************************************************
-- This code finds the best models for each month for the period 1990 - 2010.
-- Based on the top models and regression on that it does a prediction for each month from 2011-2018.
--
--
--
--
--**************************************************************************************

--********************************************************************************************************
--***********************************		models_data3		  ********************************************
-- FLOW 		models_data --> models_data3
--********************************************************************************************************
DROP TABLE IF EXISTS models_data3;
CREATE TABLE models_data3
AS
SELECT 
forecasted_year_month
	, (actual_price)
	, model 
	, variables 
	, (forecast)
	, (error)
	, (absolute_percentage_error)
FROM 
models_data;

--select * from models_data3;
--select distinct(substring(forecasted_year_month,1,4)) from models_data3 order by 1;
--********************************************************************************************************
--***********************************		BENCHMARK		  ********************************************
-- FLOW 		models_data --> BENCHMARK
--********************************************************************************************************
DROP TABLE IF EXISTS benchmark;
CREATE TABLE benchmark
AS
SELECT forecasted_year_month, actual_price, model, variables, forecast,error, absolute_percentage_error AS MAPE
 FROM models_data WHERE variables = 'calculated_benchmark';
--*******************************
--SELECT * FROM benchmark;
--********************************************************************************************************
--***********************************		BASE		  ********************************************
-- FLOW models_data3 + benchmark --> BASE
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************

DROP TABLE IF EXISTS BASE;
CREATE TABLE BASE
AS
SELECT A.*,B.actual_price as bcm_act_pr, B.forecast as bcm_fcst, B.mape as bcm_mape FROM 
models_data3 A
LEFT JOIN
benchmark B
ON
A.forecasted_year_month = B.forecasted_year_month
ORDER BY 1,3,4;
--SELECT * FROM BASE;
DELETE FROM BASE WHERE variables = 'calculated_benchmark';
ALTER TABLE BASE ADD COLUMN mdl_btr REAL;
UPDATE BASE
SET 
mdl_btr = (bcm_mape-absolute_percentage_error)/bcm_mape;

ALTER TABLE BASE ADD COLUMN FLG INTEGER;
UPDATE BASE SET 
FLG = CASE WHEN absolute_percentage_error > bcm_mape THEN 0 ELSE 1 END;
ALTER TABLE BASE ADD COLUMN FLG20 INTEGER;

UPDATE BASE SET 
FLG20 = CASE WHEN MDL_BTR < 0.2 THEN 0 ELSE 1 END;
ALTER TABLE BASE ADD COLUMN mnth varchar(10);
UPDATE BASE SET 
mnth = substring(forecasted_year_month,6,7);
ALTER TABLE BASE ADD COLUMN raey varchar(10);
UPDATE BASE SET 
raey = substring(forecasted_year_month,1,4);
-- SELECT * FROM BASE LIMIT 20;
--********************************************************************************************************
--********************************************************************************************************
--***********************************		trn_bs		  ********************************************
-- FLOW models_data3 + benchmark --> BASE --> trn_bs
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************
DROP TABLE IF EXISTS trn_bs;
CREATE TABLE trn_bs
AS
SELECT *
FROM
BASE
where substring(forecasted_year_month,1,3) in ('199')
or
substring(forecasted_year_month,1,4) in ('2010');
-- SELECT * FROM trn_bs LIMIT 20;
-- SELECT DISTINCT raey from trn_bs order by 1;
--********************************************************************************************************
--********************************************************************************************************
--***********************************		tst_bs		  ********************************************
-- FLOW models_data3 + benchmark --> BASE --> tst_bs
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************
DROP TABLE IF EXISTS tst_bs;
CREATE TABLE tst_bs
AS
SELECT *
FROM
BASE
where substring(forecasted_year_month,1,3) in ('201');
DELETE FROM tst_bs
WHERE
substring(forecasted_year_month,1,4) in ('2010');
-- SELECT * FROM tst_bs LIMIT 20;
--SELECT DISTINCT raey from tst_bs order by 1;
--********************************************************************************************************
--********************************************************************************************************
--***********************************		bst_mdls		  ********************************************
-- FLOW models_data3 + benchmark --> BASE --> tst_bs --> bst_mdls
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************
DROP TABLE IF EXISTS bst_mdls;
CREATE TABLE bst_mdls
AS
SELECT model, variables, mnth,
SUM(flg) as NUM1, COUNT(flg) as NUM01
FROM tst_bs
GROUP BY model, variables, mnth
ORDER BY 1,2,3;
ALTER TABLE bst_mdls ADD COLUMN PTGE REAL;
UPDATE bst_mdls 
SET PTGE = ROUND((NUM1::numeric/NUM01),2);
SELECT * FROM bst_mdls;

