--**************************************************************************************
-- This code finds the best models for August 2019.
--
--
--
--
--
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
-- FLOW 		full_dump --> BENCHMARK
--********************************************************************************************************
DROP TABLE IF EXISTS benchmark;
CREATE TABLE benchmark
AS
SELECT forecasted_year_month as FYM, actual_price, model, variables, forecast,error
 FROM full_dump WHERE variables = 'calculated_benchmark';

ALTER TABLE benchmark ADD COLUMN ape REAL;
UPDATE benchmark
SET 
ape = error/actual_price;

ALTER TABLE benchmark ADD COLUMN mnth varchar(10);
UPDATE benchmark SET 
mnth = substring(FYM,6,7);

ALTER TABLE benchmark ADD COLUMN raey varchar(10);
UPDATE benchmark SET 
raey = substring(FYM,1,4);
 
COPY benchmark TO 'D:\Sumit\WORK\Eltsen\Data\SQL\ensemble\bchmrk.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;
--*******************************
--SELECT * FROM benchmark;
DROP TABLE IF EXISTS bchmrk2;
CREATE TABLE bchmrk2 
AS
SELECT ad.mnth, ad.raey, ad.fym,ad.actual_price, ad.model, ad.variables, ad.forecast ,ad.ape,  -- This code is simply awesome. 
       AVG(ad.ape)
            OVER(ORDER BY ad.mnth, ad.raey,ad.model, ad.variables ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS mape
FROM benchmark ad  
;
SELECT * FROM bchmrk2;

COPY bchmrk2 TO 'D:\Sumit\WORK\Elsten\Data\SQL\bchmrk2.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;
--********************************************************************************************************
--***********************************		BASE		  ********************************************
-- FLOW models_data3 + benchmark --> BASE
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
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

DELETE FROM BASEA WHERE variables = 'calculated_benchmark';
ALTER TABLE BASEA ADD COLUMN mnth varchar(10);
UPDATE BASEA SET 
mnth = substring(FYM,6,7);

ALTER TABLE BASEA ADD COLUMN raey varchar(10);
UPDATE BASEA SET 
raey = substring(FYM,1,4);

ALTER TABLE BASEA ADD COLUMN ape REAL;
UPDATE BASEA
SET 
ape = error/actual_price;
--SELECT * FROM BASEA;

DROP TABLE IF EXISTS BASE;  
CREATE TABLE BASE
AS
SELECT ad.mnth, ad.raey, ad.fym,ad.actual_price, ad.model, ad.variables, ad.forecast ,ad.error,  -- This code is simply awesome. 
       ad.bcm_act_pr, ad.bcm_fcst, ad.bcm_ape,ad.bcm_mape,ad.ape ,AVG(ad.ape)
            OVER(ORDER BY ad.mnth, ad.raey,ad.model, ad.variables ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS mape
FROM BASEA ad  ;
--SELECT * FROM BASE LIMIT 20;
ALTER TABLE BASE ADD COLUMN mdl_btr_ape REAL;
UPDATE BASE
SET 
mdl_btr_ape = (bcm_ape-ape)/bcm_ape;

ALTER TABLE BASE ADD COLUMN FLG_ape INTEGER;
UPDATE BASE SET 
FLG_ape = CASE WHEN ape > bcm_ape THEN 0 ELSE 1 END;

ALTER TABLE BASE ADD COLUMN FLG20_ape INTEGER;
UPDATE BASE SET 
FLG20_ape = CASE WHEN mdl_btr_ape < 0.2 THEN 0 ELSE 1 END;


ALTER TABLE BASE ADD COLUMN mdl_btr REAL;
UPDATE BASE
SET 
mdl_btr = (bcm_mape-mape)/bcm_mape;

ALTER TABLE BASE ADD COLUMN FLG INTEGER;
UPDATE BASE SET 
FLG = CASE WHEN mape > bcm_mape THEN 0 ELSE 1 END;

ALTER TABLE BASE ADD COLUMN FLG20 INTEGER;
UPDATE BASE SET 
FLG20 = CASE WHEN mdl_btr < 0.2 THEN 0 ELSE 1 END;

--SELECT * FROM BASE;
--********************************************************************************************************
--********************************************************************************************************
DROP TABLE IF EXISTS T2011;  --- Picking data from 2011 onwards. 
CREATE TABLE T2011
AS
SELECT * FROM base where raey > '2010';
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
SELECT * FROM scrd_mdls2019aug;

SELECT * FROM scrd_mdls2019aug where mnth = '08' order by 9 DESC;


DROP TABLE IF EXISTS VAR2019aug;  -- 
CREATE TABLE VAR2019aug
AS
SELECT * FROM scrd_mdls2019aug where mnth = '08' and model = 'VAR' order by 9 DESC, 8 DESC;
SELECT * FROM VAR2019aug;
DROP TABLE IF EXISTS GBM2019aug;  -- 
CREATE TABLE GBM2019aug
AS
SELECT * FROM scrd_mdls2019aug where mnth = '08' and model = 'GBM' order by 9 DESC, 8 DESC;
delete from GBM2019aug where ptge < 0.7;
delete from GBM2019aug where ptge20 < 0.7;
SELECT * FROM GBM2019aug;



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
