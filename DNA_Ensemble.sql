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
forecasted_year_month AS FYM
	, (actual_price)
	, model 
	, variables 
	, (forecast)
	, (error)
	, (absolute_percentage_error)
FROM 
models_data;

--select * from models_data3;
--select distinct(substring(FYM,1,4)) from models_data3 order by 1;
--********************************************************************************************************
--***********************************		BENCHMARK		  ********************************************
-- FLOW 		models_data --> BENCHMARK
--********************************************************************************************************
DROP TABLE IF EXISTS benchmark;
CREATE TABLE benchmark
AS
SELECT forecasted_year_month as FYM, actual_price, model, variables, forecast,error, absolute_percentage_error AS MAPE
 FROM models_data WHERE variables = 'calculated_benchmark';
--*******************************
--SELECT * FROM benchmark;
--********************************************************************************************************
--***********************************		BASE		  ********************************************
-- FLOW models_data3 + benchmark --> BASE
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************

DROP TABLE IF EXISTS BASE;  --- The benchmark has been removed from rows ro become a column and then all the details. 
CREATE TABLE BASE
AS
SELECT A.*,B.actual_price as bcm_act_pr, B.forecast as bcm_fcst, B.mape as bcm_mape FROM 
models_data3 A
LEFT JOIN
benchmark B
ON
A.FYM = B.FYM
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
mnth = substring(FYM,6,7);
ALTER TABLE BASE ADD COLUMN raey varchar(10);
UPDATE BASE SET 
raey = substring(FYM,1,4);
-- SELECT * FROM BASE LIMIT 20;
--********************************************************************************************************
--********************************************************************************************************
--***********************************		scrd_mdls		  ********************************************
-- FLOW models_data3 + benchmark --> BASE --> scrd_mdls
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************
DROP TABLE IF EXISTS scrd_mdls;  -- Giving a score (percentage of time better than bcmrk by 20%) -- Scored Models
CREATE TABLE scrd_mdls
AS
SELECT model, variables, mnth,
SUM(flg20) as NUM1, COUNT(flg20) as NUM01
FROM BASE
GROUP BY model, variables, mnth
ORDER BY 1,2,3;
ALTER TABLE scrd_mdls ADD COLUMN PTGE REAL;
UPDATE scrd_mdls 
SET PTGE = ROUND((NUM1::numeric/NUM01),2);
SELECT * FROM scrd_mdls;

--********************************************************************************************************
--********************************************************************************************************
--***********************************		mdls_rnkd		  ********************************************
-- FLOW models_data3 + benchmark --> BASE --> trn_bs --> scrd_mdls --> mdls_rnkd
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************

DROP TABLE IF EXISTS mdls_rnkd;  -- Models are ranked and lower ranks are removed
CREATE TABLE mdls_rnkd
AS
SELECT mnth, model, variables, ptge,
RANK() OVER(
	PARTITION BY mnth
	ORDER BY ptge DESC
) knar
FROM bst_mdls
;
DELETE FROM mdls_rnkd where knar > 10;
SELECT * FROM mdls_rnkd; 


--********************************************************************************************************
--********************************************************************************************************
--***********************************		rankd_dtls		  ********************************************
-- FLOW models_data3 + benchmark --> BASE --> trn_bs --> scrd_mdls --> mdls_rnkd --> rankd_dtls
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************
DROP TABLE IF EXISTS tst1;
CREATE TABLE tst1
AS
SELECT A.FYM , A.actual_price, A.model, A.variables, A.forecast, A.absolute_percentage_error AS ape
,A.raey, B.rank, B.mnth
FROM 
BASE A
INNER JOIN
mdls_rnkd B 
ON 
substring(A.FYM,6,7) = B.mnth
AND
A.model = B.model
AND
A.variables = B.variables
ORDER BY 9,1;

SELECT * FROM tst1; 
