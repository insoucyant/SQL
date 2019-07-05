--**************************************************************************************
-- This code finds the best models for August 2019.
--
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
DROP TABLE IF EXISTS T2011;  --- Picking data from 2011 onwards. 
CREATE TABLE T2011
AS
SELECT * FROM base where raey > '2010';
delete from base where absolute_percentage_error > 1;
SELECT DISTINCT raey from T2011 order by 1;

--********************************************************************************************************
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


DROP TABLE IF EXISTS VAR2019aug;  -- 
CREATE TABLE VAR2019aug
AS
SELECT * FROM scrd_mdls2019aug where mnth = '08' and model = 'VAR' order by 9 DESC, 8 DESC;
delete from VAR2019aug where ptge < 0.5;
delete from VAR2019aug where ptge20 < 0.3;
SELECT * FROM VAR2019aug;

DROP TABLE IF EXISTS GBM2019aug;  -- 
CREATE TABLE GBM2019aug
AS
SELECT * FROM scrd_mdls2019aug where mnth = '08' and model = 'GBM' order by 9 DESC, 8 DESC;
delete from GBM2019aug where ptge < 0.7;
delete from GBM2019aug where ptge20 < 0.7;
SELECT * FROM GBM2019aug;



DROP TABLE IF EXISTS VAR2019jul;  -- 
CREATE TABLE VAR2019jul
AS
SELECT * FROM scrd_mdls2019aug where mnth = '07' and model = 'VAR' order by 9 DESC, 8 DESC;
delete from VAR2019jul where ptge < 0.5;
delete from VAR2019jul where ptge20 < 0.3;
SELECT * FROM VAR2019jul;

DROP TABLE IF EXISTS GBM2019jul;  -- 
CREATE TABLE GBM2019jul
AS
SELECT * FROM scrd_mdls2019aug where mnth = '08' and model = 'GBM' order by 9 DESC, 8 DESC;
delete from GBM2019jul where ptge < 0.7;
delete from GBM2019jul where ptge20 < 0.7;
SELECT * FROM GBM2019jul;


DROP TABLE IF EXISTS VAR2019sep;  -- 
CREATE TABLE VAR2019sep
AS
SELECT * FROM scrd_mdls2019aug where mnth = '09' and model = 'VAR' order by 9 DESC, 8 DESC;
delete from VAR2019sep where ptge < 0.5;
delete from VAR2019sep where ptge20 < 0.3;
SELECT * FROM VAR2019sep;

DROP TABLE IF EXISTS GBM2019sep;  -- 
CREATE TABLE GBM2019sep
AS
SELECT * FROM scrd_mdls2019aug where mnth = '08' and model = 'GBM' order by 9 DESC, 8 DESC;
delete from GBM2019sep where ptge < 0.5;
delete from GBM2019sep where ptge20 < 0.6;
SELECT * FROM GBM2019sep;




DROP TABLE IF EXISTS Longi2019may;  -- 
CREATE TABLE Longi2019may
AS
SELECT fym,model, variables,flg as flgM,flg20 as flg20M,mnth as mnthM,raey as raeyM FROM T2011 where mnth in ('05') and flg = 1;

SELECT * FROM Longi2019may;


DROP TABLE IF EXISTS Longi2019aug;  -- 
CREATE TABLE Longi2019aug
AS
SELECT fym,model, variables,flg as flgA,flg20 as flg20A,mnth as mnthA,raey as raeyA FROM T2011 where mnth in ('08') and flg = 1;

SELECT * FROM Longi2019aug order by 7;


DROP TABLE IF EXISTS Longi2019jun;  -- 
CREATE TABLE Longi2019jun
AS
SELECT fym,model, variables,flg as flgJun,flg20 as flg20Jun,mnth as mnthJun,raey as raeyJun FROM T2011 where mnth in ('06') and flg = 1;

SELECT * FROM Longi2019jun order by 7;

DROP TABLE IF EXISTS Longi2019jul;  -- 
CREATE TABLE Longi2019jul
AS
SELECT fym,model, variables,flg as flgJul,flg20 as flg20Jul,mnth as mnthJul,raey as raeyJul FROM T2011 where mnth in ('07') and flg = 1;

SELECT * FROM Longi2019jul order by 7;


DROP TABLE IF EXISTS Longi2019sep;  -- 
CREATE TABLE Longi2019sep
AS
SELECT fym,model, variables,flg as flgSep,flg20 as flg20Sep,mnth as mnthSep,raey as raeySep FROM T2011 where mnth in ('09') and flg = 1;

SELECT * FROM Longi2019sep order by 7;


SELECT A.*, B.flga, B.flg20A
from 
Longi2019may A 
JOIN
Longi2019aug B 
ON 
A.fym =B.fym
AND
A.model = B.model
AND 
A.variables = B.variables



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

DROP TABLE IF EXISTS cross2019augVAR;  -- 
CREATE TABLE cross2019augVAR
AS
SELECT variables,flgA, flg20A, raeyA FROM Longi2019aug 
where variables in (select variables from VAR2019aug) ;

SELECT * FROM cross2019augVAR;


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


DROP TABLE IF EXISTS cross2019julGBM;  -- 
CREATE TABLE cross2019julGBM
AS
SELECT variables,flgJul, flg20Jul, raeyJul FROM Longi2019jul 
where variables in (select variables from GBM2019jul) ;

SELECT * FROM cross2019julGBM;
--**********************************************************************************
--**********************************************************************************


select * from base where fym = '2018-07' and model = 'GBM' and variables in 
(
(SELECT variables FROM cross2019JulGBM where raeyJul in ('2018') )
-- intersect (select variables from cross2019JulGBM where raeyJul in ('2017') )
intersect (select variables from cross2019JulGBM where raeyJul in ('2016')  )
intersect (select variables from cross2019JulGBM where raeyJul in ('2015')  )
--intersect (select variables from cross2019JulGBM where raeyJul in ('2014')  )
)
order by absolute_percentage_error
;
DROP TABLE IF EXISTS JulGBMfinal;
CREATE TABLE JulGBMfinal
AS
select fym, actual_price,forecast, absolute_percentage_error,bcm_act_pr, bcm_mape, mnth from base
where model = 'GBM' 
and variables = 'corn_price+oil_price+corn_future_5+corn_future_6+corn_future_7+stu_ratio_new'
order by fym,mnth;
--and mnth in ('06','07','08') order by mnth,fym;

 COPY JulGBMfinal TO 'D:\Sumit\WORK\Eltsen\Data\SQL\ensemble\JulGBMfinal.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;

select * from scrd_mdls2019aug 
where model = 'GBM'
 and variables = 'corn_price+oil_price+corn_future_5+corn_future_6+corn_future_7+stu_ratio_new'
and mnth = '07'

select * from base 
where model = 'GBM'
 and variables = 'corn_price+oil_price+corn_future_5+corn_future_6+corn_future_7+stu_ratio_new'
and fym = '2018-07'

select * from base where fym = '2018-07' and model = 'VAR' and variables in 
(
(SELECT variables FROM cross2019JulVAR where raeyJul in ('2018') )
intersect
(select variables from cross2019JulVAR where raeyJul in ('2017') )
intersect
(select variables from cross2019JulVAR where raeyJul in ('2016')  )
intersect
(select variables from cross2019JulVAR where raeyJul in ('2015')  )
--intersect 
--(select variables from cross2019sepVAR where raeySep in ('2014')  )
)
order by absolute_percentage_error
;
DROP TABLE IF EXISTS JulVARfinal;
CREATE TABLE JulVARfinal
AS
select fym, actual_price,forecast, absolute_percentage_error,bcm_act_pr, bcm_mape, mnth from base
where model = 'VAR' 
and variables = 'oil_price+ethanol_production+corn_future_1+corn_future_4+corn_future_5+corn_future_7'
order by fym,mnth;
--and mnth in ('06','07','08') order by mnth,fym;

 COPY JulVARfinal TO 'D:\Sumit\WORK\Eltsen\Data\SQL\ensemble\JulVARfinal.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;

select * from scrd_mdls2019aug 
where model = 'VAR'
 and variables = 'oil_price+ethanol_production+corn_future_1+corn_future_4+corn_future_5+corn_future_7'
and mnth = '07'

select * from base 
where model = 'VAR'
 and variables = 'oil_price+ethanol_production+corn_future_1+corn_future_4+corn_future_5+corn_future_7'
and fym = '2018-07'
--**********************************************************************************


SELECT * FROM cross2019augGBM 
where raeya in ('2018') 
and variables in
(select variables from cross2019augGBM where raeya in ('2017') )
and variables in 
(select variables from cross2019augGBM where raeya in ('2016')  )
and variables in 
(select variables from cross2019augGBM where raeya in ('2015')  )
and variables in 
(select variables from cross2019augGBM where raeya in ('2013')  )
;
DROP TABLE IF EXISTS AugGBMfinal;
CREATE TABLE AugGBMfinal
AS
select fym, actual_price,forecast, absolute_percentage_error,bcm_act_pr, bcm_mape, mnth from base
where model = 'GBM' 
and variables = 'corn_price+oil_price+corn_future_4+stu_ratio_new'
order by fym,mnth;
--and mnth in ('09','07','08') order by mnth,fym;

 COPY AugGBMfinal TO 'D:\Sumit\WORK\Eltsen\Data\SQL\ensemble\AugGBMfinal.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;


SELECT * FROM cross2019augVAR 
where raeya in ('2018') 
and variables in
(select variables from cross2019augVAR where raeya in ('2016') )
and variables in 
(select variables from cross2019augVAR where raeya in ('2015')  )
and variables in 
(select variables from cross2019augVAR where raeya in ('2014')  )
and variables in 
(select variables from cross2019augVAR where raeya in ('2014')  )
;
DROP TABLE IF EXISTS AugVARfinal;
CREATE TABLE AugVARfinal
AS
select fym, actual_price,forecast, absolute_percentage_error,bcm_act_pr, bcm_mape, mnth from base
where model = 'VAR' 
and variables = 'oil_price+soy_price+stu_ratio_new+stu_ratio_old+corn_future_1+corn_future_5+corn_future_6+corn_future_7'
order by fym,mnth;
--and mnth in ('09','07','08') order by mnth,fym;

 COPY AugVARfinal TO 'D:\Sumit\WORK\Eltsen\Data\SQL\ensemble\AugVARfinal.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;
--oil_price+soy_price+stu_ratio_new+stu_ratio_old+corn_future_1+corn_future_5+corn_future_6+corn_future_7 
--**********************************************************************************


select * from base where fym = '2018-09' and model = 'GBM' and variables in 
(
(SELECT variables FROM cross2019sepGBM where raeySep in ('2018') )
intersect
(select variables from cross2019sepGBM where raeySep in ('2017') )
intersect
(select variables from cross2019sepGBM where raeySep in ('2016')  )
intersect
(select variables from cross2019sepGBM where raeySep in ('2015')  )
intersect 
(select variables from cross2019sepGBM where raeySep in ('2014')  )
)
order by absolute_percentage_error
;
DROP TABLE IF EXISTS SepGBMfinal;
CREATE TABLE SepGBMfinal
AS
select fym, actual_price,forecast, absolute_percentage_error,bcm_act_pr, bcm_mape, mnth from base
where model = 'GBM' 
and variables = 'corn_price+oil_price+corn_future_3+corn_future_4+corn_future_5+corn_future_6'
order by fym,mnth;
--and mnth in ('09','10','08') order by mnth,fym;

 COPY SepGBMfinal TO 'D:\Sumit\WORK\Eltsen\Data\SQL\ensemble\SepGBMfinal.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;
-- 'corn_price+oil_price+corn_future_3+corn_future_4+corn_future_5+corn_future_6'


select * from base where fym = '2018-09' and model = 'VAR' and variables in 
(
(SELECT variables FROM cross2019sepVAR where raeySep in ('2018') )
intersect
(select variables from cross2019sepVAR where raeySep in ('2017') )
intersect
(select variables from cross2019sepVAR where raeySep in ('2016')  )
intersect
(select variables from cross2019sepVAR where raeySep in ('2015')  )
--intersect 
--(select variables from cross2019sepVAR where raeySep in ('2014')  )
)
order by absolute_percentage_error
;
DROP TABLE IF EXISTS SepVARfinal;
CREATE TABLE SepVARfinal
AS
select fym, actual_price,forecast, absolute_percentage_error,bcm_act_pr, bcm_mape, mnth from base
where model = 'VAR' 
and variables = 'oil_price+stu_ratio_new+stu_ratio_old+corn_production+corn_future_3+corn_future_4+corn_future_5+corn_future_6+corn_future_7'
order by fym,mnth;
and mnth in ('09','10','08') order by mnth,fym;

 COPY SepVARfinal TO 'D:\Sumit\WORK\Eltsen\Data\SQL\ensemble\SepVARfinal.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;
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
