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
 
 
 COPY benchmark TO 'D:\Sumit\WORK\Eltsen\Data\SQL\ensemble\bchmrk.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;
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
-- 2010
--********************************************************************************************************
--********************************************************************************************************
-- SELECT * FROM BASE LIMIT 20;
DROP TABLE IF EXISTS trn_bs2010;
CREATE TABLE trn_bs2010
AS
SELECT *
FROM
BASE
where substring(fym,1,3) in ('199')
or
substring(fym,1,4) in ('2010');
SELECT * FROM trn_bs2010 order by 1;
--********************************************************************************************************
--********************************************************************************************************
--***********************************		scrd_mdls2010		  ********************************************
-- FLOW models_data3 + benchmark --> BASE --> trn_bs2010 --> scrd_mdls2010
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************
DROP TABLE IF EXISTS scrd_mdls2010;  -- Giving a score (percentage of time better than bcmrk by 20%) -- Scored Models
CREATE TABLE scrd_mdls2010
AS
SELECT model, variables, mnth,
SUM(flg20) as NUM1, COUNT(flg20) as NUM01
FROM trn_bs2010
GROUP BY model, variables, mnth
ORDER BY 1,2,3;
ALTER TABLE scrd_mdls2010 ADD COLUMN PTGE REAL;
UPDATE scrd_mdls2010 
SET PTGE = ROUND((NUM1::numeric/NUM01),2);
SELECT * FROM scrd_mdls2010;

--********************************************************************************************************
--********************************************************************************************************
--***********************************		mdls_rnkd20X		  ********************************************
-- FLOW models_data3 + benchmark --> BASE --> trn_bs --> trn_bs2010 --> scrd_mdls2010 --> mdls_rnkd
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************

DROP TABLE IF EXISTS mdls_rnkd20X;  -- Models are ranked and lower ranks are removed #Models Ranked
CREATE TABLE mdls_rnkd20X
AS
SELECT mnth, model, variables, ptge,
RANK() OVER(
	PARTITION BY mnth
	ORDER BY ptge DESC
) knar
FROM scrd_mdls2010
;
DELETE FROM mdls_rnkd20X where knar > 50;  -- Culled here 
SELECT * FROM mdls_rnkd20X; 
--********************************************************************************************************
--DROP TABLE IF EXISTS BASE20XI;  
--CREATE TABLE BASE20XI
--AS
--	SELECT * FROM BASE where substring( fym, 1,4) = '2011';
-- SELECT * FROM BASE20XI order by 1; 
-- SELECT distinct(fym) FROM BASE20XI order by 1;
--********************************************************************************************************
DROP TABLE IF EXISTS frcst20XI;  
CREATE TABLE frcst20XI
AS
SELECT A.mnth, A.model, A.variables,B.fym, B.forecast, B.actual_price, B.bcm_fcst  
FROM 
mdls_rnkd20X A
LEFT JOIN 
(SELECT * FROM BASE where substring( fym, 1,4) = '2011') B
ON 
A.mnth = B.mnth
AND
A.model = B.model
AND
A.variables = B.variables
ORDER BY 1,4;
SELECT * FROM frcst20XI ORDER BY 4; 
--********************************************************************************************************
DROP TABLE IF EXISTS frcst20XI_smmry;  
CREATE TABLE frcst20XI_smmry
AS
SELECT mnth, avg(forecast) as fcst_avg, avg(actual_price) as avgap, avg(bcm_fcst) as bchmrk
from 
frcst20XI
GROUP BY mnth
ORDER BY 1
;

ALTER TABLE frcst20XI_smmry 
ADD COLUMN fcst_mape REAL,
ADD COLUMN bchmrk_mape REAL,
ADD COLUMN flg INTEGER
;
UPDATE frcst20XI_smmry SET fcst_mape = (abs(avgap-fcst_avg)/avgap);
UPDATE frcst20XI_smmry SET bchmrk_mape = (abs(avgap-bchmrk)/avgap);
UPDATE frcst20XI_smmry SET flg = CASE WHEN bchmrk_mape < fcst_mape THEN 0 ELSE 1 END;
SELECT * FROM frcst20XI_smmry ORDER BY 1; 





-- 2011  2011 2011  2011 2011  2011 2011  2011 2011  2011 2011  2011 2011  2011 2011  2011 2011  2011
--********************************************************************************************************
--********************************************************************************************************
-- SELECT * FROM BASE LIMIT 20;
DROP TABLE IF EXISTS trn_bs2011;
CREATE TABLE trn_bs2011
AS
SELECT *
FROM
BASE
where substring(fym,1,3) in ('199')
or
substring(fym,1,4) in ('2010', '2011');
SELECT * FROM trn_bs2011 order by 1;
SELECT distinct(substring(fym,1,4)) FROM trn_bs2011 order by 1;
--********************************************************************************************************
--********************************************************************************************************
--***********************************		scrd_mdls2011		  ********************************************
-- FLOW models_data3 + benchmark --> BASE --> trn_bs2011 --> scrd_mdls2011
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************
DROP TABLE IF EXISTS scrd_mdls2011;  -- Giving a score (percentage of time better than bcmrk by 20%) -- Scored Models
CREATE TABLE scrd_mdls2011
AS
SELECT model, variables, mnth,
SUM(flg20) as NUM1, COUNT(flg20) as NUM01
FROM trn_bs2011
GROUP BY model, variables, mnth
ORDER BY 1,2,3;
ALTER TABLE scrd_mdls2011 ADD COLUMN PTGE REAL;
UPDATE scrd_mdls2011 
SET PTGE = ROUND((NUM1::numeric/NUM01),2);
SELECT * FROM scrd_mdls2011;

--********************************************************************************************************
--********************************************************************************************************
--***********************************		mdls_rnkd20XI		  ********************************************
-- FLOW models_data3 + benchmark --> BASE --> trn_bs --> trn_bs2011 --> scrd_mdls2011 --> mdls_rnkd
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************

DROP TABLE IF EXISTS mdls_rnkd20XI;  -- Models are ranked and lower ranks are removed #Models Ranked
CREATE TABLE mdls_rnkd20XI
AS
SELECT mnth, model, variables, ptge,
RANK() OVER(
	PARTITION BY mnth
	ORDER BY ptge DESC
) knar
FROM scrd_mdls2011
;
DELETE FROM mdls_rnkd20XI where knar > 50;  -- Culled here 
SELECT * FROM mdls_rnkd20XI; 
--********************************************************************************************************
--********************************************************************************************************
DROP TABLE IF EXISTS frcst20XII;  
CREATE TABLE frcst20XII
AS
SELECT A.mnth, A.model, A.variables,B.fym, B.forecast, B.actual_price, B.bcm_fcst  
FROM 
mdls_rnkd20XI A
LEFT JOIN 
(SELECT * FROM BASE where substring( fym, 1,4) = '2012') B
ON 
A.mnth = B.mnth
AND
A.model = B.model
AND
A.variables = B.variables
ORDER BY 1,4;
SELECT * FROM frcst20XII ORDER BY 4; 
--********************************************************************************************************
DROP TABLE IF EXISTS frcst20XII_smmry;  
CREATE TABLE frcst20XII_smmry
AS
SELECT mnth, avg(forecast) as fcst_avg, avg(actual_price) as avgap, avg(bcm_fcst) as bchmrk
from 
frcst20XII
GROUP BY mnth
ORDER BY 1
;

ALTER TABLE frcst20XII_smmry 
ADD COLUMN fcst_mape REAL,
ADD COLUMN bchmrk_mape REAL,
ADD COLUMN flg INTEGER
;
UPDATE frcst20XII_smmry SET fcst_mape = (abs(avgap-fcst_avg)/avgap);
UPDATE frcst20XII_smmry SET bchmrk_mape = (abs(avgap-bchmrk)/avgap);
UPDATE frcst20XII_smmry SET flg = CASE WHEN bchmrk_mape < fcst_mape THEN 0 ELSE 1 END;
SELECT * FROM frcst20XII_smmry ORDER BY 1; 






--  2012  2012     2012  2012     2012  2012     2012  2012     2012  2012     2012  2012     2012  2012    
--********************************************************************************************************
--********************************************************************************************************
-- SELECT * FROM BASE LIMIT 20;
DROP TABLE IF EXISTS trn_bs2012;
CREATE TABLE trn_bs2012
AS
SELECT *
FROM
BASE
where substring(fym,1,3) in ('199')
or
substring(fym,1,4) in ('2010', '2011','2012');
SELECT * FROM trn_bs2012 order by 1;
SELECT distinct(substring(fym,1,4)) FROM trn_bs2012 order by 1;
--********************************************************************************************************
--********************************************************************************************************
--***********************************		scrd_mdls2012		  ********************************************
-- FLOW models_data3 + benchmark --> BASE --> trn_bs2012 --> scrd_mdls2012
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************
DROP TABLE IF EXISTS scrd_mdls2012;  -- Giving a score (percentage of time better than bcmrk by 20%) -- Scored Models
CREATE TABLE scrd_mdls2012
AS
SELECT model, variables, mnth,
SUM(flg20) as NUM1, COUNT(flg20) as NUM01
FROM trn_bs2012
GROUP BY model, variables, mnth
ORDER BY 1,2,3;
ALTER TABLE scrd_mdls2012 ADD COLUMN PTGE REAL;
UPDATE scrd_mdls2012 
SET PTGE = ROUND((NUM1::numeric/NUM01),2);
SELECT * FROM scrd_mdls2012;

--********************************************************************************************************
--********************************************************************************************************
--***********************************		mdls_rnkd20XII		  ********************************************
-- FLOW models_data3 + benchmark --> BASE --> trn_bs --> trn_bs2012 --> scrd_mdls2012 --> mdls_rnkd
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************

DROP TABLE IF EXISTS mdls_rnkd20XII;  -- Models are ranked and lower ranks are removed #Models Ranked
CREATE TABLE mdls_rnkd20XII
AS
SELECT mnth, model, variables, ptge,
RANK() OVER(
	PARTITION BY mnth
	ORDER BY ptge DESC
) knar
FROM scrd_mdls2012
;
DELETE FROM mdls_rnkd20XII where knar > 50;  -- Culled here 
SELECT * FROM mdls_rnkd20XII; 
--********************************************************************************************************
--********************************************************************************************************
DROP TABLE IF EXISTS frcst20XIII;  
CREATE TABLE frcst20XIII
AS
SELECT A.mnth, A.model, A.variables,B.fym, B.forecast, B.actual_price, B.bcm_fcst  
FROM 
mdls_rnkd20XII A
LEFT JOIN 
(SELECT * FROM BASE where substring( fym, 1,4) = '2013') B
ON 
A.mnth = B.mnth
AND
A.model = B.model
AND
A.variables = B.variables
ORDER BY 1,4;
SELECT * FROM frcst20XIII ORDER BY 4; 
--********************************************************************************************************
DROP TABLE IF EXISTS frcst20XIII_smmry;  
CREATE TABLE frcst20XIII_smmry
AS
SELECT mnth, avg(forecast) as fcst_avg, avg(actual_price) as avgap, avg(bcm_fcst) as bchmrk
from 
frcst20XIII
GROUP BY mnth
ORDER BY 1
;

ALTER TABLE frcst20XIII_smmry 
ADD COLUMN fcst_mape REAL,
ADD COLUMN bchmrk_mape REAL,
ADD COLUMN flg INTEGER
;
UPDATE frcst20XIII_smmry SET fcst_mape = (abs(avgap-fcst_avg)/avgap);
UPDATE frcst20XIII_smmry SET bchmrk_mape = (abs(avgap-bchmrk)/avgap);
UPDATE frcst20XIII_smmry SET flg = CASE WHEN bchmrk_mape < fcst_mape THEN 0 ELSE 1 END;
SELECT * FROM frcst20XIII_smmry ORDER BY 1; 







--  2013  2013     2013  2013  2013     2013  2013  2013     2013  2013  2013     2013  2013  2013   2013  
--********************************************************************************************************
--********************************************************************************************************
-- SELECT * FROM BASE LIMIT 20;
DROP TABLE IF EXISTS trn_bs2013;
CREATE TABLE trn_bs2013
AS
SELECT *
FROM
BASE
where substring(fym,1,3) in ('199')
or
substring(fym,1,4) in ('2010', '2011','2012','2013');
SELECT * FROM trn_bs2013 order by 1;
SELECT distinct(substring(fym,1,4)) FROM trn_bs2013 order by 1;
--********************************************************************************************************
--********************************************************************************************************
--***********************************		scrd_mdls2013		  ********************************************
-- FLOW models_data3 + benchmark --> BASE --> trn_bs2013 --> scrd_mdls2013
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************
DROP TABLE IF EXISTS scrd_mdls2013;  -- Giving a score (percentage of time better than bcmrk by 20%) -- Scored Models
CREATE TABLE scrd_mdls2013
AS
SELECT model, variables, mnth,
SUM(flg20) as NUM1, COUNT(flg20) as NUM01
FROM trn_bs2013
GROUP BY model, variables, mnth
ORDER BY 1,2,3;
ALTER TABLE scrd_mdls2013 ADD COLUMN PTGE REAL;
UPDATE scrd_mdls2013 
SET PTGE = ROUND((NUM1::numeric/NUM01),2);
SELECT * FROM scrd_mdls2013;

--********************************************************************************************************
--********************************************************************************************************
--***********************************		mdls_rnkd20XIII		  ********************************************
-- FLOW models_data3 + benchmark --> BASE --> trn_bs --> trn_bs2013 --> scrd_mdls2013 --> mdls_rnkd
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************

DROP TABLE IF EXISTS mdls_rnkd20XIII;  -- Models are ranked and lower ranks are removed #Models Ranked
CREATE TABLE mdls_rnkd20XIII
AS
SELECT mnth, model, variables, ptge,
RANK() OVER(
	PARTITION BY mnth
	ORDER BY ptge DESC
) knar
FROM scrd_mdls2013
;
DELETE FROM mdls_rnkd20XIII where knar > 50;  -- Culled here 
SELECT * FROM mdls_rnkd20XIII; 
--********************************************************************************************************
--********************************************************************************************************
DROP TABLE IF EXISTS frcst20XIV;  
CREATE TABLE frcst20XIV
AS
SELECT A.mnth, A.model, A.variables,B.fym, B.forecast, B.actual_price, B.bcm_fcst  
FROM 
mdls_rnkd20XIII A
LEFT JOIN 
(SELECT * FROM BASE where substring( fym, 1,4) = '2014') B
ON 
A.mnth = B.mnth
AND
A.model = B.model
AND
A.variables = B.variables
ORDER BY 1,4;
SELECT * FROM frcst20XIV ORDER BY 4; 
--********************************************************************************************************
DROP TABLE IF EXISTS frcst20XIV_smmry;  
CREATE TABLE frcst20XIV_smmry
AS
SELECT mnth, avg(forecast) as fcst_avg, avg(actual_price) as avgap, avg(bcm_fcst) as bchmrk
from 
frcst20XIV
GROUP BY mnth
ORDER BY 1
;

ALTER TABLE frcst20XIV_smmry 
ADD COLUMN fcst_mape REAL,
ADD COLUMN bchmrk_mape REAL,
ADD COLUMN flg INTEGER
;
UPDATE frcst20XIV_smmry SET fcst_mape = (abs(avgap-fcst_avg)/avgap);
UPDATE frcst20XIV_smmry SET bchmrk_mape = (abs(avgap-bchmrk)/avgap);
UPDATE frcst20XIV_smmry SET flg = CASE WHEN bchmrk_mape < fcst_mape THEN 0 ELSE 1 END;
SELECT * FROM frcst20XIV_smmry ORDER BY 1; 








--  2014  2014  2014  2014  2014  2014  2014  2014  2014  2014  2014  2014  2014  2014  2014  2014  2014    
--********************************************************************************************************
--********************************************************************************************************
-- SELECT * FROM BASE LIMIT 20;
DROP TABLE IF EXISTS trn_bs2014;
CREATE TABLE trn_bs2014
AS
SELECT *
FROM
BASE
where substring(fym,1,3) in ('199')
or
substring(fym,1,4) in ('2010', '2011','2012','2013','2014');
SELECT * FROM trn_bs2014 order by 1;
SELECT distinct(substring(fym,1,4)) FROM trn_bs2014 order by 1;
--********************************************************************************************************
--********************************************************************************************************
--***********************************		scrd_mdls2014		  ********************************************
-- FLOW models_data3 + benchmark --> BASE --> trn_bs2014 --> scrd_mdls2014
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************
DROP TABLE IF EXISTS scrd_mdls2014;  -- Giving a score (percentage of time better than bcmrk by 20%) -- Scored Models
CREATE TABLE scrd_mdls2014
AS
SELECT model, variables, mnth,
SUM(flg20) as NUM1, COUNT(flg20) as NUM01
FROM trn_bs2014
GROUP BY model, variables, mnth
ORDER BY 1,2,3;
ALTER TABLE scrd_mdls2014 ADD COLUMN PTGE REAL;
UPDATE scrd_mdls2014 
SET PTGE = ROUND((NUM1::numeric/NUM01),2);
SELECT * FROM scrd_mdls2014;

--********************************************************************************************************
--********************************************************************************************************
--***********************************		mdls_rnkd20XIV		  ********************************************
-- FLOW models_data3 + benchmark --> BASE --> trn_bs --> trn_bs2014 --> scrd_mdls2014 --> mdls_rnkd
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************

DROP TABLE IF EXISTS mdls_rnkd20XIV;  -- Models are ranked and lower ranks are removed #Models Ranked
CREATE TABLE mdls_rnkd20XIV
AS
SELECT mnth, model, variables, ptge,
RANK() OVER(
	PARTITION BY mnth
	ORDER BY ptge DESC
) knar
FROM scrd_mdls2014
;
DELETE FROM mdls_rnkd20XIV where knar > 50;  -- Culled here 
SELECT * FROM mdls_rnkd20XIV; 
--********************************************************************************************************
--********************************************************************************************************
DROP TABLE IF EXISTS frcst20XV;  
CREATE TABLE frcst20XV
AS
SELECT A.mnth, A.model, A.variables,B.fym, B.forecast, B.actual_price, B.bcm_fcst  
FROM 
mdls_rnkd20XIV A
LEFT JOIN 
(SELECT * FROM BASE where substring( fym, 1,4) = '2015') B
ON 
A.mnth = B.mnth
AND
A.model = B.model
AND
A.variables = B.variables
ORDER BY 1,4;
SELECT * FROM frcst20XV ORDER BY 4; 
--********************************************************************************************************
DROP TABLE IF EXISTS frcst20XV_smmry;  
CREATE TABLE frcst20XV_smmry
AS
SELECT mnth, avg(forecast) as fcst_avg, avg(actual_price) as avgap, avg(bcm_fcst) as bchmrk
from 
frcst20XV
GROUP BY mnth
ORDER BY 1
;

ALTER TABLE frcst20XV_smmry 
ADD COLUMN fcst_mape REAL,
ADD COLUMN bchmrk_mape REAL,
ADD COLUMN flg INTEGER
;
UPDATE frcst20XV_smmry SET fcst_mape = (abs(avgap-fcst_avg)/avgap);
UPDATE frcst20XV_smmry SET bchmrk_mape = (abs(avgap-bchmrk)/avgap);
UPDATE frcst20XV_smmry SET flg = CASE WHEN bchmrk_mape < fcst_mape THEN 0 ELSE 1 END;
SELECT * FROM frcst20XV_smmry ORDER BY 1; 













--  2015  2015  2015  2015  2015  2015  2015  2015  2015  2015  2015  2015  2015  2015  2015  2015  2015    
--********************************************************************************************************
--********************************************************************************************************
-- SELECT * FROM BASE LIMIT 20;
DROP TABLE IF EXISTS trn_bs2015;
CREATE TABLE trn_bs2015
AS
SELECT *
FROM
BASE
where substring(fym,1,3) in ('199')
or
substring(fym,1,4) in ('2010', '2011','2012','2013','2014','2015');
SELECT * FROM trn_bs2015 order by 1;
SELECT distinct(substring(fym,1,4)) FROM trn_bs2015 order by 1;
--********************************************************************************************************
--********************************************************************************************************
--***********************************		scrd_mdls2015		  ********************************************
-- FLOW models_data3 + benchmark --> BASE --> trn_bs2015 --> scrd_mdls2015
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************
DROP TABLE IF EXISTS scrd_mdls2015;  -- Giving a score (percentage of time better than bcmrk by 20%) -- Scored Models
CREATE TABLE scrd_mdls2015
AS
SELECT model, variables, mnth,
SUM(flg20) as NUM1, COUNT(flg20) as NUM01
FROM trn_bs2015
GROUP BY model, variables, mnth
ORDER BY 1,2,3;
ALTER TABLE scrd_mdls2015 ADD COLUMN PTGE REAL;
UPDATE scrd_mdls2015 
SET PTGE = ROUND((NUM1::numeric/NUM01),2);
SELECT * FROM scrd_mdls2015;

--********************************************************************************************************
--********************************************************************************************************
--***********************************		mdls_rnkd20XV		  ********************************************
-- FLOW models_data3 + benchmark --> BASE --> trn_bs --> trn_bs2015 --> scrd_mdls2015 --> mdls_rnkd
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************

DROP TABLE IF EXISTS mdls_rnkd20XV;  -- Models are ranked and lower ranks are removed #Models Ranked
CREATE TABLE mdls_rnkd20XV
AS
SELECT mnth, model, variables, ptge,
RANK() OVER(
	PARTITION BY mnth
	ORDER BY ptge DESC
) knar
FROM scrd_mdls2015
;
DELETE FROM mdls_rnkd20XV where knar > 50;  -- Culled here 
DELETE FROM mdls_rnkd20XV where ptge < .30;  -- Culled here 
SELECT * FROM mdls_rnkd20XV; 
--********************************************************************************************************
--********************************************************************************************************
DROP TABLE IF EXISTS frcst20XVI;  
CREATE TABLE frcst20XVI
AS
SELECT A.mnth, A.model, A.variables,B.fym, B.forecast, B.actual_price, B.bcm_fcst  
FROM 
mdls_rnkd20XV A
LEFT JOIN 
(SELECT * FROM BASE where substring( fym, 1,4) = '2016') B
ON 
A.mnth = B.mnth
AND
A.model = B.model
AND
A.variables = B.variables
ORDER BY 1,4;
SELECT * FROM frcst20XVI ORDER BY 4; 
--********************************************************************************************************
DROP TABLE IF EXISTS frcst20XVI_smmry;  
CREATE TABLE frcst20XVI_smmry
AS
SELECT mnth, avg(forecast) as fcst_avg, avg(actual_price) as avgap, avg(bcm_fcst) as bchmrk
from 
frcst20XVI
GROUP BY mnth
ORDER BY 1
;

ALTER TABLE frcst20XVI_smmry 
ADD COLUMN fcst_mape REAL,
ADD COLUMN bchmrk_mape REAL,
ADD COLUMN flg INTEGER
;
UPDATE frcst20XVI_smmry SET fcst_mape = (abs(avgap-fcst_avg)/avgap);
UPDATE frcst20XVI_smmry SET bchmrk_mape = (abs(avgap-bchmrk)/avgap);
UPDATE frcst20XVI_smmry SET flg = CASE WHEN bchmrk_mape < fcst_mape THEN 0 ELSE 1 END;
SELECT * FROM frcst20XVI_smmry ORDER BY 1; 




--  2016  2016  2016  2016  2016  2016  2016  2016  2016  2016  2016  2016  2016  2016  2016  2016  2016  
--********************************************************************************************************
--********************************************************************************************************
-- SELECT * FROM BASE LIMIT 20;
DROP TABLE IF EXISTS trn_bs2016;
CREATE TABLE trn_bs2016
AS
SELECT *
FROM
BASE
where substring(fym,1,3) in ('199')
or
substring(fym,1,4) in ('2010', '2011','2012','2013','2014','2015','2016');
SELECT * FROM trn_bs2016 order by 1;
SELECT distinct(substring(fym,1,4)) FROM trn_bs2016 order by 1;
--********************************************************************************************************
--********************************************************************************************************
--***********************************		scrd_mdls2016		  ********************************************
-- FLOW models_data3 + benchmark --> BASE --> trn_bs2016 --> scrd_mdls2016
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************
DROP TABLE IF EXISTS scrd_mdls2016;  -- Giving a score (percentage of time better than bcmrk by 20%) -- Scored Models
CREATE TABLE scrd_mdls2016
AS
SELECT model, variables, mnth,
SUM(flg20) as NUM1, COUNT(flg20) as NUM01
FROM trn_bs2016
GROUP BY model, variables, mnth
ORDER BY 1,2,3;
ALTER TABLE scrd_mdls2016 ADD COLUMN PTGE REAL;
UPDATE scrd_mdls2016 
SET PTGE = ROUND((NUM1::numeric/NUM01),2);
SELECT * FROM scrd_mdls2016;

--********************************************************************************************************
--********************************************************************************************************
--***********************************		mdls_rnkd20XVI		  ********************************************
-- FLOW models_data3 + benchmark --> BASE --> trn_bs --> trn_bs2016 --> scrd_mdls2016 --> mdls_rnkd
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************

DROP TABLE IF EXISTS mdls_rnkd20XVI;  -- Models are ranked and lower ranks are removed #Models Ranked
CREATE TABLE mdls_rnkd20XVI
AS
SELECT mnth, model, variables, ptge,
RANK() OVER(
	PARTITION BY mnth
	ORDER BY ptge DESC
) knar
FROM scrd_mdls2016
;
DELETE FROM mdls_rnkd20XVI where knar > 50;  -- Culled here 
DELETE FROM mdls_rnkd20XVI where ptge < .30;  -- Culled here 
SELECT * FROM mdls_rnkd20XVI; 
--********************************************************************************************************
--********************************************************************************************************
DROP TABLE IF EXISTS frcst20XVII;  
CREATE TABLE frcst20XVII
AS
SELECT A.mnth, A.model, A.variables,B.fym, B.forecast, B.actual_price, B.bcm_fcst  
FROM 
mdls_rnkd20XVI A
LEFT JOIN 
(SELECT * FROM BASE where substring( fym, 1,4) = '2017') B
ON 
A.mnth = B.mnth
AND
A.model = B.model
AND
A.variables = B.variables
ORDER BY 1,4;
SELECT * FROM frcst20XVII ORDER BY 4; 
--********************************************************************************************************
DROP TABLE IF EXISTS frcst20XVII_smmry;  
CREATE TABLE frcst20XVII_smmry
AS
SELECT mnth, avg(forecast) as fcst_avg, avg(actual_price) as avgap, avg(bcm_fcst) as bchmrk
from 
frcst20XVII
GROUP BY mnth
ORDER BY 1
;

ALTER TABLE frcst20XVII_smmry 
ADD COLUMN fcst_mape REAL,
ADD COLUMN bchmrk_mape REAL,
ADD COLUMN flg INTEGER
;
UPDATE frcst20XVII_smmry SET fcst_mape = (abs(avgap-fcst_avg)/avgap);
UPDATE frcst20XVII_smmry SET bchmrk_mape = (abs(avgap-bchmrk)/avgap);
UPDATE frcst20XVII_smmry SET flg = CASE WHEN bchmrk_mape < fcst_mape THEN 0 ELSE 1 END;
SELECT * FROM frcst20XVII_smmry ORDER BY 1; 


--  2018  2018    2018  2018    2018  2018    2018  2018    2018  2018    2018  2018    2018  2018  
--********************************************************************************************************
--********************************************************************************************************
-- SELECT * FROM BASE LIMIT 20;
DROP TABLE IF EXISTS trn_bs2016;
CREATE TABLE trn_bs2016
AS
SELECT *
FROM
BASE
where substring(fym,1,3) in ('199')
or
substring(fym,1,4) in ('2010', '2011','2012','2013','2014','2015','2016');
SELECT * FROM trn_bs2016 order by 1;
SELECT distinct(substring(fym,1,4)) FROM trn_bs2016 order by 1;
--********************************************************************************************************
--********************************************************************************************************
--***********************************		scrd_mdls2016		  ********************************************
-- FLOW models_data3 + benchmark --> BASE --> trn_bs2016 --> scrd_mdls2016
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************
DROP TABLE IF EXISTS scrd_mdls2016;  -- Giving a score (percentage of time better than bcmrk by 20%) -- Scored Models
CREATE TABLE scrd_mdls2016
AS
SELECT model, variables, mnth,
SUM(flg20) as NUM1, COUNT(flg20) as NUM01
FROM trn_bs2016
GROUP BY model, variables, mnth
ORDER BY 1,2,3;
ALTER TABLE scrd_mdls2016 ADD COLUMN PTGE REAL;
UPDATE scrd_mdls2016 
SET PTGE = ROUND((NUM1::numeric/NUM01),2);
SELECT * FROM scrd_mdls2016;

--********************************************************************************************************
--********************************************************************************************************
--***********************************		mdls_rnkd20XVI		  ********************************************
-- FLOW models_data3 + benchmark --> BASE --> trn_bs --> trn_bs2016 --> scrd_mdls2016 --> mdls_rnkd
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************

DROP TABLE IF EXISTS mdls_rnkd20XVI;  -- Models are ranked and lower ranks are removed #Models Ranked
CREATE TABLE mdls_rnkd20XVI
AS
SELECT mnth, model, variables, ptge,
RANK() OVER(
	PARTITION BY mnth
	ORDER BY ptge DESC
) knar
FROM scrd_mdls2016
;
DELETE FROM mdls_rnkd20XVI where knar > 50;  -- Culled here 
DELETE FROM mdls_rnkd20XVI where ptge < .30;  -- Culled here 
SELECT * FROM mdls_rnkd20XVI; 
--********************************************************************************************************
--********************************************************************************************************
DROP TABLE IF EXISTS frcst20XVII;  
CREATE TABLE frcst20XVII
AS
SELECT A.mnth, A.model, A.variables,B.fym, B.forecast, B.actual_price, B.bcm_fcst  
FROM 
mdls_rnkd20XVI A
LEFT JOIN 
(SELECT * FROM BASE where substring( fym, 1,4) = '2017') B
ON 
A.mnth = B.mnth
AND
A.model = B.model
AND
A.variables = B.variables
ORDER BY 1,4;
SELECT * FROM frcst20XVII ORDER BY 4; 
--********************************************************************************************************
DROP TABLE IF EXISTS frcst20XVII_smmry;  
CREATE TABLE frcst20XVII_smmry
AS
SELECT mnth, avg(forecast) as fcst_avg, avg(actual_price) as avgap, avg(bcm_fcst) as bchmrk
from 
frcst20XVII
GROUP BY mnth
ORDER BY 1
;

ALTER TABLE frcst20XVII_smmry 
ADD COLUMN fcst_mape REAL,
ADD COLUMN bchmrk_mape REAL,
ADD COLUMN flg INTEGER
;
UPDATE frcst20XVII_smmry SET fcst_mape = (abs(avgap-fcst_avg)/avgap);
UPDATE frcst20XVII_smmry SET bchmrk_mape = (abs(avgap-bchmrk)/avgap);
UPDATE frcst20XVII_smmry SET flg = CASE WHEN bchmrk_mape < fcst_mape THEN 0 ELSE 1 END;
SELECT * FROM frcst20XVII_smmry ORDER BY 1; 


































