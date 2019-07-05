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

DROP TABLE IF EXISTS mdls_rnkd;  -- Models are ranked and lower ranks are removed #Models Ranked
CREATE TABLE mdls_rnkd
AS
SELECT mnth, model, variables, ptge,
RANK() OVER(
	PARTITION BY mnth
	ORDER BY ptge DESC
) knar
FROM bst_mdls
;
--DELETE FROM mdls_rnkd where knar > 10;  -- Culled here 
SELECT * FROM mdls_rnkd; 


--********************************************************************************************************
--********************************************************************************************************
--***********************************		rankd_dtls	[BASE 2]	  ********************************************
-- FLOW models_data3 + benchmark --> BASE --> trn_bs --> scrd_mdls --> mdls_rnkd --> rankd_dtls
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************
DROP TABLE IF EXISTS rankd_dtls;  -- Why would there be NULLS in forecast? If so, is there an error in the join/flow?
-- There are such cases in BASE table where forecast is NULL
CREATE TABLE rankd_dtls
AS
SELECT A.FYM , A.actual_price, A.model, A.variables, A.forecast, A.absolute_percentage_error AS ape
,A.raey, B.knar, B.mnth
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

SELECT * FROM rankd_dtls; 
--********************************************************************************************************
--*****************************						DNA					**********************************
--********************************************************************************************************
-- Read csv
DROP TABLE IF EXISTS DNA;
CREATE TABLE DNA ( 
 TimeWindow INTEGER,
 YEARMONTH VARCHAR(20), 
 Corn_MeanReversed REAL,
 Soy_MeanReversed REAL,
 NCOMM_OI_Percent REAL, 
 MatchingTimeframe VARCHAR(20), 
 Corn_MeanReversed_Matched REAL,
 Soy_MeanReversed_Matched REAL,
 NCOMM_OI_Percent_Matched REAL
)
-----------------------------------------------------
COPY DNA(TimeWindow, YEARMONTH, Corn_MeanReversed, Soy_MeanReversed, 
NCOMM_OI_Percent, MatchingTimeframe, Corn_MeanReversed_Matched, Soy_MeanReversed_Matched, NCOMM_OI_Percent_Matched) 
FROM 'D:\Sumit\WORK\Eltsen\Data\InputFiles\DNA_Ensemble\DNA.csv' DELIMITER ',' CSV HEADER;


ALTER TABLE DNA 
 DROP COLUMN IF EXISTS TimeWindow
,DROP COLUMN IF EXISTS Corn_MeanReversed
,DROP COLUMN IF EXISTS Soy_MeanReversed
,DROP COLUMN IF EXISTS NCOMM_OI_Percent
,DROP COLUMN IF EXISTS Corn_MeanReversed_Matched
,DROP COLUMN IF EXISTS Soy_MeanReversed_Matched
,DROP COLUMN IF EXISTS NCOMM_OI_Percent_Matched
;


SELECT * FROM DNA LIMIT 10;


--********************************************************************************************************
--*****************************						MTF_Details			**********************************
--********************************************************************************************************
-- Matched Time Frame Details
-- rankd_dtls + fym   --> MTF_Details

DROP TABLE IF EXISTS MTF_Details;
CREATE TABLE MTF_Details
AS
SELECT fym, actual_price, model, variables, forecast, absolute_percentage_error as APE FROM BASE
where fym in (Select MatchingTimeframe::text from DNA) ;


SELECT * FROM MTF_Details order by 1;
SELECT fym, count(*) FROM MTF_Details group by fym order by 1;
SELECT * FROM MTF_Details;

--********************************************************************************************************
--*****************************						MTF_Dtls_rnk 		**********************************
--********************************************************************************************************
-- Additional Ranking based on APE
-- rankd_dtls + fym   --> MTF_Details --> MTF_Dtls_rnk
-- Selecting only the top 10
-- This will be used again to pick models for original time frame.

DROP TABLE IF EXISTS MTF_Dtls_rnk;  -- Models are ranked and lower ranks are removed #Models Ranked
CREATE TABLE MTF_Dtls_rnk
AS
SELECT fym, model, variables, actual_price, forecast, ape,
RANK() OVER(
	PARTITION BY fym
	ORDER BY ape
) knar2
FROM MTF_Details
;
 
SELECT * FROM MTF_Dtls_rnk;
Alter TABLE MTF_Dtls_rnk
DROP COLUMN ape;
DELETE FROM MTF_Dtls_rnk where knar2 > 10;  -- Culled here 
SELECT * FROM MTF_Dtls_rnk;


--********************************************************************************************************
--*****************************						MTF_Details_rnk_aap			**********************************
--********************************************************************************************************
-- Additional Ranking based on APE
-- rankd_dtls + fym   --> MTF_Details --> MTF_Dtls_rnk --> MTF_Details_rnk_aap
-- Selecting only the top 10
-- This table will be used again to 
--********************************************************************************************************

DROP TABLE IF EXISTS MTF_Details_rnk_aap;
CREATE TABLE MTF_Details_rnk_aap
AS
SELECT
   fym,
   model,
   variables,
   forecast,
   knar2,
   AVG(actual_price) OVER (
      PARTITION BY fym
   )aap
FROM
MTF_Dtls_rnk;
SELECT * FROM MTF_Details_rnk_aap order by 1, 5; 

--********************************************************************************************************
--********************************************************************************************************
--*****************************						FYM_MTF_Dtls		**********************************
--********************************************************************************************************
-- rankd_dtls + fym   --> MTF_Details --> MTF_Dtls_rnk --> MTF_Details_rnk_aap --> FYM_MTF_Dtls
-- Selecting only the top 10
-- This table will be used again to 
--********************************************************************************************************
DROP TABLE IF EXISTS FYM_MTF_Dtls;
CREATE TABLE FYM_MTF_Dtls
AS
SELECT A.*,B.model, B.variables, B.forecast as MTF_fcst, B.aap as MTF_aap, B.knar2
From 
DNA A 
LEFT JOIN 
MTF_Details_rnk_aap B 
ON 
A.matchingtimeframe =  B.fym
order by 1,2;
SELECT * FROM FYM_MTF_Dtls order by 1,2, 7; 


--********************************************************************************************************
--********************************************************************************************************
--*****************************						FYM_MTF_Dtls2		**********************************
--********************************************************************************************************
-- FYM_MTF_Dtls + rankd_dtls [BASE2] --> FYM_MTF_Dtls2
-- rankd_dtls + fym   --> MTF_Details --> MTF_Dtls_rnk --> MTF_Details_rnk_aap --> FYM_MTF_Dtls
--********************************************************************************************************
DROP TABLE IF EXISTS FYM_MTF_Dtls2;
CREATE TABLE FYM_MTF_Dtls2
AS
SELECT A.*, B.actual_price as FYM_AP, B.forecast as FYM_fcst
FROM 
FYM_MTF_Dtls A
LEFT JOIN 
rankd_dtls B
ON 
A.yearmonth = B.fym
AND 
A.model = B.model
AND
A.variables = B.variables
order by 1,2,7;
SELECT * FROM FYM_MTF_Dtls2 order by 1,2, 7; 

--********************************************************************************************************
--********************************************************************************************************
--*****************************						FYM_MTF_Dtls3		**********************************
--********************************************************************************************************
-- FYM_MTF_Dtls + rankd_dtls [BASE2] --> FYM_MTF_Dtls2 --> FYM_MTF_Dtls3 
-- rankd_dtls + fym   --> MTF_Details --> MTF_Dtls_rnk --> MTF_Details_rnk_aap --> FYM_MTF_Dtls
--********************************************************************************************************
-- Averaging of forecast actual price is done here
--********************************************************************************************************