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
-- FLOW models_data3 + benchmark --> BASE --> trn_bs --> bst_mdls
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************
DROP TABLE IF EXISTS bst_mdls;
CREATE TABLE bst_mdls
AS
SELECT model, variables, mnth,
SUM(flg20) as NUM1, COUNT(flg20) as NUM01
FROM trn_bs
GROUP BY model, variables, mnth
ORDER BY 1,2,3;
ALTER TABLE bst_mdls ADD COLUMN PTGE REAL;
UPDATE bst_mdls 
SET PTGE = ROUND((NUM1::numeric/NUM01),2);
SELECT * FROM bst_mdls;
--********************************************************************************************************
--********************************************************************************************************
--***********************************		bst_mdls_rnkd		  ********************************************
-- FLOW models_data3 + benchmark --> BASE --> trn_bs --> bst_mdls --> bst_mdls_rnkd
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************

DROP TABLE IF EXISTS bst_mdls_rnkd;
CREATE TABLE bst_mdls_rnkd
AS
SELECT mnth, model, variables, ptge,
RANK() OVER(
	PARTITION BY mnth
	ORDER BY ptge DESC
)
FROM bst_mdls
;
SELECT * FROM bst_mdls_rnkd; 
--********************************************************************************************************
--********************************************************************************************************
--***********************************		bst_mdl		  ********************************************
-- FLOW models_data3 + benchmark --> BASE --> trn_bs --> bst_mdls --> bst_mdls_rnkd --> bst_mdl
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************

DROP TABLE IF EXISTS bst_mdl;
CREATE TABLE bst_mdl
AS
SELECT * FROM bst_mdls_rnkd where rank < 6; 
SELECT * FROM bst_mdl; 
SELECT mnth, count(*) FROM bst_mdl GROUP BY mnth ORDER BY 1; 

--********************************************************************************************************
--********************************************************************************************************
--***********************************		tst1		  ********************************************
-- trn_bs + bst_mdl -->  tst1
-- FLOW models_data3 + benchmark --> BASE --> trn_bs --> bst_mdls --> bst_mdls_rnkd --> bst_mdl
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************
DROP TABLE IF EXISTS tst1;
CREATE TABLE tst1
AS
SELECT A.forecasted_year_month, A.actual_price, A.model, A.variables, A.forecast, A.absolute_percentage_error AS ape
,A.raey, B.rank, B.mnth
FROM 
trn_bs A
INNER JOIN
bst_mdl B 
ON 
substring(A.forecasted_year_month,6,7) = B.mnth
AND
A.model = B.model
AND
A.variables = B.variables
ORDER BY 9,1;

SELECT * FROM tst1; 
--********************************************************************************************************
--********************************************************************************************************
--***********************************		tst2		  ********************************************
-- trn_bs + bst_mdl -->  tst1 --> tst2 [average price]
-- FLOW models_data3 + benchmark --> BASE --> trn_bs --> bst_mdls --> bst_mdls_rnkd --> bst_mdl
-- FLOW models_data --> BENCHMARK
-- FLOW models_data --> models_data3
--********************************************************************************************************

DROP TABLE IF EXISTS tst2;
CREATE TABLE tst2
AS
SELECT
   mnth,
   model,
   variables,
   ape,
   AVG(ape) OVER (
      PARTITION BY mnth,model,variables
		 ORDER BY mnth,model,variables
   )avgape
FROM
tst1;
SELECT * FROM tst2; 

DROP TABLE IF EXISTS tst3;
CREATE TABLE tst3
AS
SELECT DISTINCT mnth, model, variables, avgape FROM tst2 ORDER BY 1,4;
SELECT * FROM tst3;  
--********************************************************************************************************
-- tst4 tells me the top 10 model that i am going with for each month 
--********************************************************************************************************
DROP TABLE IF EXISTS tst4;
CREATE TABLE tst4
AS
SELECT mnth, model, variables,
RANK() OVER(
	PARTITION BY mnth
	ORDER BY avgape 
) knar
FROM tst3
;

DELETE FROM tst4 where knar > 10;
SELECT * FROM tst4; 


DROP TABLE IF EXISTS tst5;
CREATE TABLE tst5
AS
SELECT A.*, B.mnth as mnth2 , B.knar
FROM
tst1 A
JOIN
tst4 B
ON
A.mnth=B.mnth
AND
A.model = B.model
AND
A.variables = B.variables;
SELECT * FROM tst5;


DROP TABLE IF EXISTS tst6;
CREATE TABLE tst6
AS
SELECT forecasted_year_month, actual_price, model, variables, forecast,knar from tst5;
SELECT * FROM tst6;

-- the AVERAGE ACTUAL PRICE CALULATION
DROP TABLE IF EXISTS tst7;
CREATE TABLE tst7
AS
SELECT
   forecasted_year_month,
   model,
   variables,
   forecast,
   knar,
   AVG(actual_price) OVER (
      PARTITION BY forecasted_year_month
   )avgactprice
FROM
tst6
ORDER BY 1,5;
--SELECT * FROM tst7; 

ALTER TABLE tst7
ADD COLUMN rankC VARCHAR(20);
UPDATE tst7
SET rankC = (
		CASE 
			WHEN knar = 1 THEN 'X1'
			WHEN knar = 2 THEN 'X2'
			WHEN knar = 3 THEN 'X3'
			WHEN knar = 4 THEN 'X4'
			WHEN knar = 5 THEN 'X5'
			WHEN knar = 6 THEN 'X6'
			WHEN knar = 7 THEN 'X7'
			WHEN knar = 8 THEN 'X8'
			WHEN knar = 9 THEN 'X9'
			WHEN knar = 10 THEN 'X10'
		END
		);
SELECT * FROM tst7; 


DROP TABLE IF EXISTS tst8;
CREATE TABLE tst8
AS
SELECT forecasted_year_month AS FYM, forecast AS fcst, avgactprice, rankC FROM tst7 order By 1,4;
SELECT * FROM tst8; 




DROP TABLE IF EXISTS pvt_tst;  -- pivoted test
CREATE TABLE pvt_tst
AS
SELECT * 
FROM crosstab( 'select fym::text, rankc, fcst::float8 from tst8 order by 1,2') 
     AS pvt_tst(fym text, X1 float8, X2 float8,X3 float8,X4 float8
	 ,X5 float8,X6 float8,X7 float8,X8 float8,X9 float8,X10 float8);
	 
SELECT * FROM pvt_tst;
 
 

DROP TABLE IF EXISTS avgactprice;  -- pivoted test
CREATE TABLE avgactprice
AS
SELECT distinct forecasted_year_month, avgactprice FROM tst7 order by 1;
SELECT * FROM avgactprice;
 --********************************************************************************************************
--********************************************************************************************************
DROP TABLE IF EXISTS pvt_tst2;  -- pivoted test
CREATE TABLE pvt_tst2
AS
SELECT A.*, B.avgactprice
FROM 
pvt_tst A
LEFT JOIN
avgactprice B 
ON
A.fym = B.forecasted_year_month;

SELECT * FROM pvt_tst2;


--********************************************************************************************************
--********************************************************************************************************
--********************************************************************************************************
--********************************************************************************************************
--********************************************************************************************************
--********************************************************************************************************
DROP TABLE IF EXISTS chck_yr1_2011;  
CREATE TABLE chck_yr1_2011
AS
SELECT forecasted_year_month as fym, model, variables, forecast, actual_price 
FROM tst_bs where substring(forecasted_year_month,1,4) = '2011' ;

SELECT * FROM chck_yr1_2011 order by 1;





DROP TABLE IF EXISTS chck_yr2_2011;  
CREATE TABLE chck_yr2_2011
AS
SELECT A.*, B.knar
FROM 
chck_yr1_2011 A 
JOIN 
tst4 B 
ON
substring(A.fym,6,7) = B.mnth
AND
A.model = B.model
AND
A.variables = B.variables
ORDER BY 1,5;
SELECT * FROM chck_yr2_2011 order by 1;

ALTER TABLE chck_yr2_2011
ADD COLUMN rankC VARCHAR(20);
UPDATE chck_yr2_2011
SET rankC = (
		CASE 
			WHEN knar = 1 THEN 'X1'
			WHEN knar = 2 THEN 'X2'
			WHEN knar = 3 THEN 'X3'
			WHEN knar = 4 THEN 'X4'
			WHEN knar = 5 THEN 'X5'
			WHEN knar = 6 THEN 'X6'
			WHEN knar = 7 THEN 'X7'
			WHEN knar = 8 THEN 'X8'
			WHEN knar = 9 THEN 'X9'
			WHEN knar = 10 THEN 'X10'
		END
		);
SELECT * FROM chck_yr2_2011 order by 1,6;



-- the AVERAGE ACTUAL PRICE CALULATION again
DROP TABLE IF EXISTS aap_2011;
CREATE TABLE aap_2011
AS
select fym, avg(actual_price) from chck_yr2_2011
group by fym
order by 1;
--SELECT * FROM aap_2011; 


-- CROSS TAB ONCE MORE 

SELECT fym, forecast, rankc from chck_yr2_2011 order by 1,3;


DROP TABLE IF EXISTS pvt_2011;  -- pivoted test
CREATE TABLE pvt_2011
AS
SELECT * 
FROM crosstab( 'select fym::text, rankc, forecast::float8 from chck_yr2_2011 order by 1,2') 
     AS pvt_tst(fym text, X1 float8, X2 float8,X3 float8,X4 float8
	 ,X5 float8,X6 float8,X7 float8,X8 float8,X9 float8,X10 float8);
	 
SELECT * FROM pvt_2011;



DROP TABLE IF EXISTS TEST_2011;  
CREATE TABLE TEST_2011
AS
SELECT A.*, B. avg as aap
from 
pvt_2011 A
JOIN 
aap_2011 B 
ON 
A.fym = B.fym ;

	 
SELECT * FROM TEST_2011;