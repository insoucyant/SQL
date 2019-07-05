--**************************************************************************************
-- This code finds the best models for August 2019.
--
--
--
--
--
--**************************************************************************************

--**************************************************************************************
SELECT version();
--********************************************************************************************************
--***********************************		models_data3		  ********************************************
-- FLOW 		full_dump --> models_data3
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
	,error
FROM 
full_dump;
--SELECT * FROM full_dump LIMIT 20;
--SELECT * FROM models_data3 LIMIT 20;
--SELECT distinct(substring(FYM,1,4)) FROM models_data3 order by 1;
--********************************************************************************************************
--***********************************		BENCHMARK		  ********************************************
-- FLOW 		full_dump --> BENCHMARK --> bchmrk2
--********************************************************************************************************
DROP TABLE IF EXISTS benchmark;
CREATE TABLE benchmark
AS
SELECT forecasted_year_month as FYM, actual_price, model, variables, forecast,error
FROM full_dump WHERE variables = 'calculated_benchmark';
--********************************************************************************************************
ALTER TABLE benchmark ADD COLUMN ape REAL;
UPDATE benchmark
SET 
ape = abs(error)/actual_price;
--********************************************************************************************************
ALTER TABLE benchmark ADD COLUMN mnth varchar(10);
UPDATE benchmark SET 
mnth = substring(FYM,6,7);
--********************************************************************************************************
ALTER TABLE benchmark ADD COLUMN raey varchar(10);
UPDATE benchmark SET 
raey = substring(FYM,1,4);
--********************************************************************************************************
DROP TABLE IF EXISTS bchmrk2;
CREATE TABLE bchmrk2 
AS
SELECT ad.mnth, ad.raey, ad.fym,ad.actual_price, ad.model, ad.variables, ad.forecast ,ad.ape,  -- This code is simply awesome. 
       AVG(ad.ape)
            OVER(ORDER BY ad.mnth, ad.raey,ad.model, ad.variables ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS mape
FROM benchmark ad  ;
SELECT * FROM bchmrk2;
--0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
COPY bchmrk2 TO 'D:\Sumit\WORK\Elsten\Data\June23\July1\bchmrk2.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;
--0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
--********************************************************************************************************
--***********************************		BASE		  ********************************************
-- FLOW models_data3 + bchmrk2 --> BASEA --> BASE --> T2011
-- FLOW full_dump --> BENCHMARK --> bchmrk2
-- FLOW full_dump --> models_data3
--********************************************************************************************************
DROP TABLE IF EXISTS BASEA;  --- The benchmark has been removed from rows ro become a column and then all the details. 
CREATE TABLE BASEA
AS
SELECT A.*,B.actual_price as bcm_act_pr, B.forecast as bcm_fcst, B.ape as bcm_ape, B.mape as bcm_mape FROM 
models_data3 A
LEFT JOIN
bchmrk2 B
ON
A.FYM = B.FYM
ORDER BY 1,3,4;
--********************************************************************************************************
DELETE FROM BASEA WHERE variables = 'calculated_benchmark';
ALTER TABLE BASEA ADD COLUMN mnth varchar(10);
UPDATE BASEA SET 
mnth = substring(FYM,6,7);
--********************************************************************************************************
ALTER TABLE BASEA ADD COLUMN raey varchar(10);
UPDATE BASEA SET 
raey = substring(FYM,1,4);
--********************************************************************************************************
ALTER TABLE BASEA ADD COLUMN ape REAL;
UPDATE BASEA
SET 
ape = abs(error)/actual_price;
--SELECT * FROM BASEA;
--********************************************************************************************************
MEDIAN TO BE DONE
--********************************************************************************************************
--********************************************************************************************************
DROP TABLE IF EXISTS BASE;  
CREATE TABLE BASE
AS
SELECT ad.mnth, ad.raey, ad.fym,ad.actual_price, ad.model, ad.variables, ad.forecast ,ad.error,  -- This code is simply awesome. 
       ad.bcm_act_pr, ad.bcm_fcst, ad.bcm_ape,ad.bcm_mape,ad.ape ,AVG(ad.ape)
            OVER(ORDER BY ad.mnth, ad.raey,ad.model, ad.variables ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS mape
FROM BASEA ad  ;
--SELECT * FROM BASE LIMIT 20;
--********************************************************************************************************
ALTER TABLE BASE ADD COLUMN mdl_btr_ape REAL;
UPDATE BASE
SET 
mdl_btr_ape = (bcm_ape-ape)/bcm_ape;
--********************************************************************************************************
ALTER TABLE BASE ADD COLUMN FLG_ape INTEGER;
UPDATE BASE SET 
FLG_ape = CASE WHEN ape > bcm_ape THEN 0 ELSE 1 END;
--********************************************************************************************************
ALTER TABLE BASE ADD COLUMN FLG20_ape INTEGER;
UPDATE BASE SET 
FLG20_ape = CASE WHEN mdl_btr_ape < 0.2 THEN 0 ELSE 1 END;
--********************************************************************************************************
ALTER TABLE BASE ADD COLUMN mdl_btr REAL;
UPDATE BASE
SET 
mdl_btr = (bcm_mape-mape)/bcm_mape;
--********************************************************************************************************
ALTER TABLE BASE ADD COLUMN FLG INTEGER;
UPDATE BASE SET 
FLG = CASE WHEN mape > bcm_mape THEN 0 ELSE 1 END;
--********************************************************************************************************
ALTER TABLE BASE ADD COLUMN FLG20 INTEGER;
UPDATE BASE SET 
FLG20 = CASE WHEN mdl_btr < 0.2 THEN 0 ELSE 1 END;
--********************************************************************************************************
--SELECT * FROM BASE;
--********************************************************************************************************
--********************************************************************************************************
-- BASE --> T2011
--********************************************************************************************************
DROP TABLE IF EXISTS T2011;  --- Picking data from 2011 onwards. 
CREATE TABLE T2011
AS
SELECT * FROM base where raey > '2010';
SELECT DISTINCT raey from T2011 order by 1;
--********************************************************************************************************
--********************************************************************************************************
-- Table scrd_mdls2019aug has for each model calculation of how many times it beat bnchmrk and by T20
-- Also What percentage of time it did so since year 2011 included. 
-- This table contains data for all months and not just August.
--********************************************************************************************************
-- FLOW models_data3 + bchmrk2 --> BASEA --> BASE --> T2011 --> scrd_mdls2019aug --> VAR2019aug
-- FLOW full_dump --> BENCHMARK --> bchmrk2
-- FLOW full_dump --> models_data3
-- FLOW T2011 --> scrd_mdls2019aug --> VAR2019aug
-- scrd_mdls2019aug has nothing to do with August. It is a summarised table. 
--********************************************************************************************************
DROP TABLE IF EXISTS scrd_mdls2019aug;  -- Giving a score (percentage of time better than bcmrk by 20%) -- Scored Models
CREATE TABLE scrd_mdls2019aug
AS
SELECT model, variables, mnth,
SUM(flg20) as TNUM1, COUNT(flg20) as TNUM01,
SUM(flg) as NUM1, COUNT(flg) as NUM01
FROM T2011
GROUP BY model, variables, mnth
ORDER BY 1,2,3;
ALTER TABLE scrd_mdls2019aug ADD COLUMN PTGE20 REAL;
ALTER TABLE scrd_mdls2019aug ADD COLUMN PTGE REAL;
UPDATE scrd_mdls2019aug 
SET PTGE = ROUND((NUM1::numeric/NUM01),2);
UPDATE scrd_mdls2019aug 
SET PTGE20 = ROUND((TNUM1::numeric/TNUM01),2);
--SELECT * FROM scrd_mdls2019aug;
SELECT * FROM scrd_mdls2019aug where mnth = '08' order by 9 DESC;
--********************************************************************************************************
-- The following VAR/GBM tables picks the best model. Need to be modified to rank instead of deleting based on cut off.
--********************************************************************************************************
DROP TABLE IF EXISTS VAR2019aug;  -- 
CREATE TABLE VAR2019aug
AS
SELECT * FROM scrd_mdls2019aug where mnth = '08' and model = 'VAR' order by 9 DESC, 8 DESC;
delete from VAR2019aug where ptge < 0.5;
delete from VAR2019aug where ptge20 < 0.3;
SELECT * FROM VAR2019aug;
--corn_future_1
----oil_price+soy_price+stu_ratio_old+corn_future_1+corn_future_3+corn_future_4+corn_future_5+corn_future_6
----oil_price+soy_price+stu_ratio_new+corn_future_1+corn_future_3+corn_future_4+corn_future_5+corn_future_6
----oil_price+soy_price+stu_ratio_new+corn_future_1+corn_future_3+corn_future_4+corn_future_5+corn_future_6+corn_future_7
--********************************************************************************************************
DROP TABLE IF EXISTS GBM2019aug;  -- 
CREATE TABLE GBM2019aug
AS
SELECT * FROM scrd_mdls2019aug where mnth = '08' and model = 'GBM' order by 9 DESC, 8 DESC;
delete from GBM2019aug where ptge < 0.6;
delete from GBM2019aug where ptge20 < 0.5;
SELECT * FROM GBM2019aug;
--corn_price+soy_price+corn_future_1+corn_future_3+stu_ratio_old+ethanol_production
--corn_price+soy_price+corn_future_3+stu_ratio_old+ethanol_production
--********************************************************************************************************
DROP TABLE IF EXISTS VAR2019jul;  -- 
CREATE TABLE VAR2019jul
AS
SELECT * FROM scrd_mdls2019aug where mnth = '07' and model = 'VAR' order by 9 DESC, 8 DESC;
delete from VAR2019jul where ptge < 0.5;
delete from VAR2019jul where ptge20 < 0.3;
SELECT * FROM VAR2019jul;
--corn_future_1+corn_future_3
--corn_future_1
--oil_price+soy_price+stu_ratio_old+ethanol_production+corn_future_1+corn_future_3+corn_future_4+corn_future_5+corn_future_6+corn_future_7
--********************************************************************************************************
DROP TABLE IF EXISTS GBM2019jul;  -- 
CREATE TABLE GBM2019jul
AS
SELECT * FROM scrd_mdls2019aug where mnth = '08' and model = 'GBM' order by 9 DESC, 8 DESC;
delete from GBM2019jul where ptge < 0.6;
delete from GBM2019jul where ptge20 < 0.5;
SELECT * FROM GBM2019jul;
--corn_price+soy_price+corn_future_1+corn_future_3+stu_ratio_old+ethanol_production
--corn_price+soy_price+corn_future_3+stu_ratio_old+ethanol_production
--********************************************************************************************************
DROP TABLE IF EXISTS VAR2019sep;  -- 
CREATE TABLE VAR2019sep
AS
SELECT * FROM scrd_mdls2019aug where mnth = '09' and model = 'VAR' order by 9 DESC, 8 DESC;
delete from VAR2019sep where ptge < 0.5;
delete from VAR2019sep where ptge20 < 0.2;
SELECT * FROM VAR2019sep;
--corn_future_1
--oil_price+soy_price+stu_ratio_old+ethanol_production+corn_future_1+corn_future_3+corn_future_4+corn_future_5+corn_future_6+corn_future_7
--oil_price+soy_price+stu_ratio_old+ethanol_production+corn_future_1+corn_future_3+corn_future_4+corn_future_5+corn_future_6
--********************************************************************************************************
DROP TABLE IF EXISTS GBM2019sep;  -- 
CREATE TABLE GBM2019sep
AS
SELECT * FROM scrd_mdls2019aug where mnth = '08' and model = 'GBM' order by 9 DESC, 8 DESC;
delete from GBM2019sep where ptge < 0.6;
delete from GBM2019sep where ptge20 < 0.5;
SELECT * FROM GBM2019sep;
--corn_price+soy_price+corn_future_1+corn_future_3+stu_ratio_old+ethanol_production
--corn_price+soy_price+corn_future_3+stu_ratio_old+ethanol_production
--********************************************************************************************************
--********************************************************************************************************
-- Longi (Longitudinal) implies that we have the data for same month across the years.
-- One Longi table has data for one month only. In this case the data is only from the years 2011 onwards. 
--********************************************************************************************************
-- FLOW full_dump --> models_data3 + bchmrk2 --> BASEA --> BASE --> T2011 --> scrd_mdls2019aug --> VAR2019aug
-- FLOW full_dump --> BENCHMARK --> bchmrk2
-- FLOW T2011 --> scrd_mdls2019aug --> VAR2019aug
-- FLOW T2011 --> Longi2019may
--********************************************************************************************************
DROP TABLE IF EXISTS Longi2019may;  -- 
CREATE TABLE Longi2019may
AS
SELECT fym,model, variables,flg as flgM,flg20 as flg20M,mnth as mnthM,raey as raeyM FROM T2011 where mnth in ('05') and flg = 1;
SELECT * FROM Longi2019may;
--********************************************************************************************************
DROP TABLE IF EXISTS Longi2019aug;  -- 
CREATE TABLE Longi2019aug
AS
SELECT fym,model, variables,flg as flgA,flg20 as flg20A,mnth as mnthA,raey as raeyA FROM T2011 where mnth in ('08') and flg = 1;
SELECT * FROM Longi2019aug order by 7;
--********************************************************************************************************
DROP TABLE IF EXISTS Longi2019jun;  -- 
CREATE TABLE Longi2019jun
AS
SELECT fym,model, variables,flg as flgJun,flg20 as flg20Jun,mnth as mnthJun,raey as raeyJun FROM T2011 where mnth in ('06') and flg = 1;
SELECT * FROM Longi2019jun order by 7;
--********************************************************************************************************
DROP TABLE IF EXISTS Longi2019jul;  -- 
CREATE TABLE Longi2019jul
AS
SELECT fym,model, variables,flg as flgJul,flg20 as flg20Jul,mnth as mnthJul,raey as raeyJul FROM T2011 where mnth in ('07') and flg = 1;
SELECT * FROM Longi2019jul order by 7;
--********************************************************************************************************
DROP TABLE IF EXISTS Longi2019sep;  -- 
CREATE TABLE Longi2019sep
AS
SELECT fym,model, variables,flg as flgSep,flg20 as flg20Sep,mnth as mnthSep,raey as raeySep FROM T2011 where mnth in ('09') and flg = 1;
SELECT * FROM Longi2019sep order by 7;
--********************************************************************************************************
-- Look into this. Important!
SELECT A.*, B.flga, B.flg20A 
from 
Longi2019may A 
JOIN
Longi2019aug B 
ON 
A.model = B.model
AND 
A.variables = B.variables
--********************************************************************************************************
--********************************************************************************************************
--********************************************************************************************************
--********************************************************************************************************
DROP TABLE IF EXISTS BestGBM;  -- 
CREATE TABLE BestGBM 
as
select * from base 
where model = 'GBM'
and
variables 
in ('corn_price+oil_price+corn_future_4+stu_ratio_new')
and 
raey > '2010'
and mnth = '08'
order by raey;

select * from  BestGBM;

 COPY BestGBM TO 'D:\Sumit\WORK\Eltsen\Data\SQL\ensemble\BestGBM.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;

 COPY BestVAR TO 'D:\Sumit\WORK\Eltsen\Data\SQL\ensemble\BestVAR.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;
--********************************************************************************************************
--********************************************************************************************************
-- FLOW full_dump --> models_data3 + bchmrk2 --> BASEA --> BASE --> T2011 --> scrd_mdls2019aug --> VAR2019aug
-- FLOW full_dump --> BENCHMARK --> bchmrk2
-- FLOW T2011 --> Longi2019may
-- Longi2019aug + VAR2019aug --> cross2019augVAR
-- VAR2019aug is a summarised table of the rank models. To get the details we join with Longi. 
--********************************************************************************************************
DROP TABLE IF EXISTS cross2019augVAR;  -- 
CREATE TABLE cross2019augVAR
AS
SELECT variables,flgA, flg20A, raeyA FROM Longi2019aug 
where variables in (select variables from VAR2019aug) ;
SELECT * FROM cross2019augVAR;
--********************************************************************************************************
DROP TABLE IF EXISTS cross2019augGBM;  -- 
CREATE TABLE cross2019augGBM
AS
SELECT variables,flgA, flg20A, raeyA FROM Longi2019aug 
where variables in (select variables from GBM2019aug) ;
SELECT * FROM cross2019augGBM;
--**********************************************************************************
DROP TABLE IF EXISTS cross2019sepVAR;  -- 
CREATE TABLE cross2019sepVAR
AS
SELECT variables,flgSep, flg20Sep, raeySep FROM Longi2019sep 
where variables in (select variables from VAR2019sep) ;
SELECT * FROM cross2019sepVAR;
--********************************************************************************************************
DROP TABLE IF EXISTS cross2019sepGBM;  -- 
CREATE TABLE cross2019sepGBM
AS
SELECT variables,flgSep, flg20Sep, raeySep FROM Longi2019sep 
where variables in (select variables from GBM2019sep) ;
SELECT * FROM cross2019sepGBM;
--**********************************************************************************
DROP TABLE IF EXISTS cross2019julVAR;  -- 
CREATE TABLE cross2019julVAR
AS
SELECT variables,flgJul, flg20Jul, raeyJul FROM Longi2019jul 
where variables in (select variables from VAR2019jul) ;
SELECT * FROM cross2019julVAR;
--********************************************************************************************************
DROP TABLE IF EXISTS cross2019julGBM;  -- 
CREATE TABLE cross2019julGBM
AS
SELECT variables,flgJul, flg20Jul, raeyJul FROM Longi2019jul 
where variables in (select variables from GBM2019jul) ;
SELECT * FROM cross2019julGBM;
--**********************************************************************************
--**********************************************************************************
--**********************************************************************************
--################################################################################################
select * from base where fym = '2018-07' and model = 'GBM' and variables in 
(
(SELECT variables FROM cross2019JulGBM where raeyJul in ('2018') )
INTERSECT (select variables from cross2019JulGBM where raeyJul in ('2017') )
INTERSECT (select variables from cross2019JulGBM where raeyJul in ('2016')  )
INTERSECT (select variables from cross2019JulGBM where raeyJul in ('2015')  )
--INTERSECT (select variables from cross2019JulGBM where raeyJul in ('2014')  )
) order by mape ;
"corn_price+corn_future_1+corn_future_6+corn_future_7+stu_ratio_old"
"corn_price+corn_future_1+corn_future_5+corn_future_7+stu_ratio_old+ethanol_production+corn_production"
"corn_price+corn_future_1+corn_future_6+corn_future_7+stu_ratio_new+stu_ratio_old+ethanol_production+corn_production"
"corn_price+corn_production"
"corn_price+corn_future_1+corn_future_6+corn_future_7+stu_ratio_old+corn_production"
--################################################################################################
DROP TABLE IF EXISTS JulGBMfinal;
CREATE TABLE JulGBMfinal
AS
select fym, actual_price,forecast, ape, mape,bcm_act_pr, bcm_ape, bcm_mape, mnth from base
where model = 'GBM' 
and variables = 'corn_price+corn_future_1+corn_future_6+corn_future_7+stu_ratio_old' --15-18
order by fym,mnth;
SELECT * FROM JulGBMfinal;
--0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
COPY JulGBMfinal TO 'D:\Sumit\WORK\Elsten\Data\June23\July2\JulGBMfinal.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;
--0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
--SELECT * FROM scrd_mdls2019aug where mnth = '07' and model = 'GBM' and variables = 'corn_price+soy_price+corn_future_3';
--select * from scrd_mdls2019aug  where model = 'GBM'  and variables = 'corn_price+soy_price+corn_future_1+stu_ratio_old+ethanol_production' and mnth = '07'
--select * from scrd_mdls2019aug  where model = 'GBM'  and variables = 'corn_price+soy_price+corn_future_1+corn_future_3+stu_ratio_old+ethanol_production' and mnth = '07'
--select * from base where model = 'GBM' and variables = 'corn_price+soy_price+corn_future_3' and fym = '2018-07';
--**********************************************************************************
--**********************************************************************************
--################################################################################################
select * from base where fym = '2018-07' and model = 'VAR' and variables in 
(
(SELECT variables FROM cross2019JulVAR where raeyJul in ('2018') )
INTERSECT (select variables from cross2019JulVAR where raeyJul in ('2017') )
INTERSECT (select variables from cross2019JulVAR where raeyJul in ('2016')  )
INTERSECT (select variables from cross2019JulVAR where raeyJul in ('2015')  )
--INTERSECT (select variables from cross2019sepVAR where raeySep in ('2014')  )
) order by mape ;
corn_future_1
oil_price+corn_future_1+corn_future_3+corn_future_4+corn_future_5+corn_future_6
oil_price+soy_price+stu_ratio_old+corn_production+corn_future_1+corn_future_3+corn_future_4+corn_future_5+corn_future_6+corn_future_7
oil_price+soy_price+stu_ratio_new+stu_ratio_old+corn_production+corn_future_1+corn_future_3+corn_future_4+corn_future_5+corn_future_6+corn_future_7
oil_price+soy_price+stu_ratio_old+corn_production+corn_future_1+corn_future_3+corn_future_4+corn_future_5+corn_future_6
--################################################################################################
DROP TABLE IF EXISTS JulVARfinal;
CREATE TABLE JulVARfinal
AS
select fym, actual_price,forecast, ape, mape,bcm_act_pr,bcm_ape, bcm_mape, mnth from base
where model = 'VAR' 
and variables = 'corn_future_1'
order by fym,mnth;
--0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
COPY JulVARfinal TO 'D:\Sumit\WORK\Elsten\Data\June23\July2\JulVARfinal.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;
--0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
--SELECT * from scrd_mdls2019aug where model = 'VAR' and variables = 'corn_future_1' and mnth = '07'
--SELECT * from base where model = 'VAR'  and variables = 'corn_future_1' and fym = '2018-07'
--select * from scrd_mdls2019aug where model = 'VAR' and variables = 'oil_price+corn_future_1+corn_future_3+corn_future_4+corn_future_5+corn_future_6' and mnth = '07'
--SELECT * from base where model = 'VAR'  and variables = 'oil_price+corn_future_1+corn_future_3+corn_future_4+corn_future_5+corn_future_6' and fym = '2018-07'
--**********************************************************************************
--**********************************************************************************
--################################################################################################
SELECT * from base where fym = '2018-08' and model = 'GBM' and variables in 
(
(SELECT variables FROM cross2019augGBM where raeya in ('2018') )
INTERSECT (SELECT variables from cross2019augGBM where raeya in ('2017') )
INTERSECT (SELECT variables from cross2019augGBM where raeya in ('2016')  )
INTERSECT (SELECT variables from cross2019augGBM where raeya in ('2015')  )
--INTERSECT (SELECT variables from cross2019augGBM where raeya in ('2014')  )
) order by mape ;
"corn_price+corn_future_5+corn_future_6+corn_future_7+stu_ratio_old"
"corn_price+corn_future_5+corn_future_6+corn_future_7+stu_ratio_new+stu_ratio_old+ethanol_production+corn_production"
"corn_price+corn_future_5+corn_future_6+corn_future_7+stu_ratio_old+corn_production"
"corn_price+corn_future_5+corn_future_6+corn_future_7+stu_ratio_new+stu_ratio_old+ethanol_production"
"corn_price+corn_future_1+corn_future_5+corn_future_7+stu_ratio_old"
--################################################################################################
DROP TABLE IF EXISTS AugGBMfinal;
CREATE TABLE AugGBMfinal
AS
SELECT fym, actual_price,forecast, ape, mape,bcm_act_pr,bcm_ape, bcm_mape, mnth from base
where model = 'GBM' 
and variables = 'corn_price+corn_future_5+corn_future_6+corn_future_7+stu_ratio_old'
order by fym,mnth;
--0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
COPY AugGBMfinal TO 'D:\Sumit\WORK\Elsten\Data\June23\July2\AugGBMfinal.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;
--0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
--select * from scrd_mdls2019aug where model = 'GBM' and variables = 'corn_price+corn_future_3' and mnth = '08'
--select * from base where model = 'GBM'  and variables = 'corn_price+corn_future_3' and fym = '2018-08'
--**********************************************************************************
--**********************************************************************************
--################################################################################################
select * from base where fym = '2018-08' and model = 'VAR' and variables in 
(
( SELECT variables FROM cross2019augVAR where raeya in ('2018') )
INTERSECT (select variables from cross2019augVAR where raeya in ('2017') )
INTERSECT (select variables from cross2019augVAR where raeya in ('2016')  )
INTERSECT (select variables from cross2019augVAR where raeya in ('2015')  )
--INTERSECT (select variables from cross2019augVAR where raeya in ('2014')  )
) order by mape ;
oil_price+soy_price+stu_ratio_new+corn_production+corn_future_1+corn_future_3+corn_future_4+corn_future_5+corn_future_6+corn_future_7
oil_price+soy_price+stu_ratio_new+corn_production+corn_future_1+corn_future_3+corn_future_4+corn_future_5+corn_future_6
oil_price+soy_price+stu_ratio_old+corn_production+corn_future_1+corn_future_3+corn_future_4+corn_future_5+corn_future_6+corn_future_7
oil_price+soy_price+stu_ratio_new+stu_ratio_old+corn_production+corn_future_1+corn_future_3+corn_future_4+corn_future_5+corn_future_6+corn_future_7
oil_price+soy_price+stu_ratio_old+corn_production+corn_future_1+corn_future_3+corn_future_4+corn_future_5+corn_future_6

--################################################################################################
DROP TABLE IF EXISTS AugVARfinal;
CREATE TABLE AugVARfinal
AS
SELECT fym, actual_price,forecast, ape, mape,bcm_act_pr,bcm_ape, bcm_mape, mnth from base
where model = 'VAR' 
and variables = 'oil_price+soy_price+stu_ratio_new+corn_production+corn_future_1+corn_future_3+corn_future_4+corn_future_5+corn_future_6+corn_future_7'
order by fym,mnth;
--0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
COPY AugVARfinal TO 'D:\Sumit\WORK\Elsten\Data\June23\July2\AugVARfinal.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;
--0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000 
--**********************************************************************************
--**********************************************************************************
--################################################################################################
select * from base where fym = '2018-09' and model = 'GBM' and variables in 
(
(SELECT variables FROM cross2019sepGBM where raeySep in ('2018') )
INTERSECT (select variables from cross2019sepGBM where raeySep in ('2017') )
INTERSECT (select variables from cross2019sepGBM where raeySep in ('2016')  )
INTERSECT (select variables from cross2019sepGBM where raeySep in ('2015')  )
--INTERSECT (select variables from cross2019sepGBM where raeySep in ('2014')  )
) order by mape ;
"corn_price+corn_future_6+corn_production"
"corn_price+corn_future_1+corn_future_5+corn_future_7+stu_ratio_old+ethanol_production+corn_production"
"corn_price+corn_future_5+corn_future_6+corn_future_7+stu_ratio_old+ethanol_production+corn_production"
"corn_price+corn_production"
"corn_price+corn_future_5+corn_future_6+corn_future_7+stu_ratio_old+ethanol_production"
--################################################################################################
DROP TABLE IF EXISTS SepGBMfinal;
CREATE TABLE SepGBMfinal
AS
SELECT fym, actual_price,forecast, ape, mape,bcm_act_pr,bcm_ape, bcm_mape, mnth from base
where model = 'GBM' 
and variables = 'corn_price+corn_future_6+corn_production'
order by fym,mnth;
--0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
COPY SepGBMfinal TO 'D:\Sumit\WORK\Elsten\Data\June23\July2\SepGBMfinal.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;
--0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
--SELECT * from scrd_mdls2019aug where model = 'GBM' and variables = 'corn_price+oil_price+corn_future_3+corn_future_4+corn_future_5+corn_future_6' and mnth = '09'
--SELECT * from base where model = 'GBM' and variables = 'corn_price+oil_price+corn_future_3+corn_future_4+corn_future_5+corn_future_6' and fym = '2018-09'
--**********************************************************************************
--**********************************************************************************
--**********************************************************************************
--################################################################################################
SELECT * from base where fym = '2018-09' and model = 'VAR' and variables in 
(
(SELECT variables FROM cross2019sepVAR where raeySep in ('2018') )
INTERSECT (SELECT variables from cross2019sepVAR where raeySep in ('2017') )
INTERSECT (SELECT variables from cross2019sepVAR where raeySep in ('2016')  )
INTERSECT (SELECT variables from cross2019sepVAR where raeySep in ('2015')  )
--INTERSECT (SELECT variables from cross2019sepVAR where raeySep in ('2014')  )
)order by mape ;
oil_price+soy_price+stu_ratio_new+corn_future_1+corn_future_3+corn_future_4+corn_future_5+corn_future_6+corn_future_7
oil_price+soy_price+stu_ratio_new+stu_ratio_old+corn_production+corn_future_1+corn_future_3+corn_future_4+corn_future_5+corn_future_6+corn_future_7
oil_price+soy_price+stu_ratio_new+corn_production+corn_future_1+corn_future_3+corn_future_4+corn_future_5+corn_future_6+corn_future_7
oil_price+soy_price+stu_ratio_old+corn_production+corn_future_1+corn_future_3+corn_future_4+corn_future_5+corn_future_6+corn_future_7
oil_price+soy_price+stu_ratio_new+stu_ratio_old+corn_future_1+corn_future_3+corn_future_4+corn_future_5+corn_future_6+corn_future_7
--################################################################################################
--**********************************************************************************
DROP TABLE IF EXISTS SepVARfinal;
CREATE TABLE SepVARfinal
AS
SELECT fym, actual_price,forecast, ape, mape,bcm_act_pr,bcm_ape, bcm_mape, mnth from base
where model = 'VAR' 
and variables = 'oil_price+soy_price+stu_ratio_new+corn_future_1+corn_future_3+corn_future_4+corn_future_5+corn_future_6+corn_future_7'
order by fym,mnth;
--0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
COPY SepVARfinal TO 'D:\Sumit\WORK\Elsten\Data\June23\July1\SepVARfinal.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;
--0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
select * from scrd_mdls2019aug 
where model = 'VAR'
 and variables = 'oil_price+stu_ratio_new+stu_ratio_old+corn_production+corn_future_3+corn_future_4+corn_future_5+corn_future_6+corn_future_7'
and mnth = '09'

select * from base 
where model = 'VAR'
 and variables = 'oil_price+stu_ratio_new+stu_ratio_old+corn_production+corn_future_3+corn_future_4+corn_future_5+corn_future_6+corn_future_7'
and fym = '2018-09'
-- 'oil_price+stu_ratio_new+stu_ratio_old+corn_production+corn_future_3+corn_future_4+corn_future_5+corn_future_6+corn_future_7'
--**********************************************************************************

select * from base 
where model = 'VAR'
and
variables 
in ('oil_price+soy_price+stu_ratio_new+stu_ratio_old+corn_production+corn_future_5+corn_future_6+corn_future_7')
and 
raey > '2010'
and mnth = '08'
order by raey;


select * from base 
where model = 'VAR'
and
variables 
in ('oil_price+soy_price+stu_ratio_new+corn_future_1+corn_future_5+corn_future_6+corn_future_7')
and 
raey > '2010'
and mnth = '08'
order by raey;


select * from base 
where model = 'VAR'
and
variables 
in ('oil_price+soy_price+stu_ratio_new+corn_production+corn_future_1+corn_future_5+corn_future_6+corn_future_7')
and 
raey > '2010'
and mnth = '08'
order by raey;




select * from base 
where model = 'VAR'
and
variables 
in ('oil_price+soy_price+stu_ratio_new+corn_production+corn_future_5+corn_future_6+corn_future_7')
and 
raey > '2010'
and mnth = '08'
order by raey;



select * from base 
where model = 'VAR'
and
variables 
in ('oil_price+soy_price+stu_ratio_new+stu_ratio_old+corn_production+corn_future_1+corn_future_5+corn_future_6+corn_future_7')
and 
raey > '2010'
and mnth = '08'
order by raey;


select * from base 
where model = 'VAR'
and
variables 
in ('oil_price+soy_price+stu_ratio_new+stu_ratio_old+corn_production+corn_future_5+corn_future_6+corn_future_7')
and 
raey > '2010'
and mnth = '08'
order by raey;


select * from base 
where model = 'VAR'
and
variables 
in ('oil_price+soy_price+stu_ratio_old+corn_production+corn_future_1+corn_future_5+corn_future_6+corn_future_7')
and 
raey > '2010'
and mnth = '08'
order by raey;


GBM


select * from base 
where model = 'GBM'
and
variables 
in ('corn_price+oil_price+corn_future_4+stu_ratio_new')
and 
raey > '2010'
and mnth = '08'
order by raey;



select * from base 
where model = 'GBM'
and
variables 
in ('corn_price+oil_price+corn_future_3+corn_future_4+corn_future_6+stu_ratio_new')
and 
raey > '2010'
and mnth = '08'
order by raey;


select * from base 
where model = 'GBM'
and
variables 
in ('corn_price+oil_price+corn_future_3+corn_future_5+corn_future_6+stu_ratio_new')
and 
raey > '2010'
and mnth = '08'
order by raey;

-------


select * from base 
where model = 'GBM'
and
variables 
in ('corn_price+oil_price+corn_future_4+corn_future_5+corn_future_6+stu_ratio_new')
and 
raey > '2010'
and mnth = '08'
order by raey;

select * from base 
where model = 'GBM'
and
variables 
in ('corn_price+oil_price+corn_future_4+corn_future_6+stu_ratio_new')
and 
raey > '2010'
and mnth = '08'
order by raey;


select * from base 
where model = 'GBM'
and
variables 
in ('corn_price+oil_price+corn_future_5+corn_future_6+corn_future_7+stu_ratio_new')
and 
raey > '2010'
and mnth = '08'
order by raey;




--WORK DONE ON June 25 . Best models for DNA time match for August 2018
select distinct variables from 
( 
select variables from base where fym in ('2015-04') and model = 'VAR' and flg20=1
and absolute_percentage_error < 0.02
--and mdl_btr > 0.98
INTERSECT
select variables from base where fym in ('2015-06') and model = 'VAR' and flg20=1
and absolute_percentage_error < 0.02
--and mdl_btr > 0.98
) X	;

select * from base where fym = '2015-06' order by absolute_percentage_error;
