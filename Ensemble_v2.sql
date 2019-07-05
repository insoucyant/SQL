-- First create the model_data table using R 
--SELECT * from models_data Top 20;

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
--********************************************************************************************************
--***********************************		md_201X		  ********************************************
-- FLOW 		models_data --> models_data3 --> md_201X
--********************************************************************************************************
DROP TABLE IF EXISTS md_201X;
CREATE TABLE md_201X
AS
SELECT * from models_data3 where SUBSTRING(forecasted_year_month, 1,3) in ('201');

-- SELECT * FROM md_201X;
-- SELECT DISTINCT SUBSTRING(forecasted_year_month, 1,4) from md_201X ORDER BY 1;

-- IMPORTANT TEST CASES
-- SELECT DISTINCT actual_price from models_data3 where SUBSTRING(forecasted_year_month, 1,7) in ('2010-01') order by 1;
--SELECT * from models_data3 where SUBSTRING(forecasted_year_month, 1,7) in ('2010-01') order by 1,2,3;
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
--***********************************		md_201X2		  ********************************************
-- FLOW md_201X + benchmark --> md_201X2
-- FLOW models_data --> BENCHMARK
-- FLOW 		models_data --> models_data3 --> md_201X
--********************************************************************************************************

DROP TABLE IF EXISTS md_201X2;
CREATE TABLE md_201X2
AS
SELECT A.*,B.actual_price as bcm_act_pr, B.forecast as bcm_fcst, B.mape as bcm_mape FROM 
md_201X A
LEFT JOIN
benchmark B
ON
A.forecasted_year_month = B.forecasted_year_month
ORDER BY 1,3,4;
DELETE FROM md_201X2 WHERE variables = 'calculated_benchmark';
ALTER TABLE md_201X2 ADD COLUMN mdl_btr REAL;
UPDATE md_201X2
SET 
mdl_btr = (bcm_mape-absolute_percentage_error)/bcm_mape;

ALTER TABLE md_201X2 ADD COLUMN FLG INTEGER;
UPDATE md_201X2 SET 
FLG = CASE WHEN absolute_percentage_error > bcm_mape THEN 0 ELSE 1 END;
ALTER TABLE md_201X2 ADD COLUMN FLG20 INTEGER;

UPDATE md_201X2 SET 
FLG20 = CASE WHEN MDL_BTR < 0.2 THEN 0 ELSE 1 END;
-- SELECT * FROM md_201X2 LIMIT 20;
--********************************************************************************************************
--***********************************		md_201X3		  ********************************************
-- FLOW 	md_201X + benchmark --> md_201X2 --> md_201X3
-- FLOW 	models_data --> BENCHMARK
-- FLOW 	models_data --> models_data3 --> md_201X
--********************************************************************************************************


	DROP TABLE IF EXISTS md_201X3;
	CREATE TABLE md_201X3
	AS
	SELECT
		forecasted_year_month
		,actual_price
		,model
		,variables
		,forecast
		--,absolute_percentage_error
		,RANK () OVER (
			PARTITION BY forecasted_year_month
			ORDER BY absolute_percentage_error
		) 
	FROM 
		md_201X2;
	DELETE FROM md_201x3 where rank > 10;
	ALTER TABLE md_201X3 ALTER COLUMN forecasted_year_month TYPE DATE using to_date(forecasted_year_month, 'YYYY-MM');
	ALTER TABLE md_201X3 ADD COLUMN MTF TIMESTAMP;
	UPDATE  md_201X3 
	SET MTF = (forecasted_year_month + interval '1 month') ;   -- The previous month column addition
	ALTER TABLE md_201X3 ALTER COLUMN MTF TYPE VARCHAR(20);
	UPDATE md_201X3 SET mtf = substring(mtf,1,7) ;
	ALTER TABLE md_201X3 ALTER COLUMN forecasted_year_month TYPE VARCHAR(20);
	UPDATE md_201X3 SET forecasted_year_month = substring(forecasted_year_month,1,7) ;
	-- SELECT * FROM md_201X3 ORDER BY 1,2,6;
	
	
--********************************************************************************************************
--***********************************TRAINING DATA tr_md_201x3 		  ********************************************
-- FLOW 	md_201X + benchmark --> md_201X2 --> md_201X3 -->   tr_md_201X3
-- FLOW 	models_data --> BENCHMARK
-- FLOW 	models_data --> models_data3 --> md_201X
--********************************************************************************************************	
--SELECT * FROM md_201X3 order by 1,6;
DROP TABLE IF EXISTS tr_md_201X3;
CREATE TABLE tr_md_201X3
AS 
SELECT 
	forecasted_year_month
	,actual_price
	,forecast
	,rank
FROM 
md_201X3;
--SELECT * FROM tr_md_201X3 order by 1,3;
	
ALTER TABLE tr_md_201X3
ADD COLUMN rankC VARCHAR(20);
UPDATE tr_md_201X3
SET rankC = (
		CASE 
			WHEN rank = 1 THEN 'ONE'
			WHEN rank = 2 THEN 'TWO'
			WHEN rank = 3 THEN 'THREE'
			WHEN rank = 4 THEN 'FOUR'
			WHEN rank = 5 THEN 'FIVE'
			WHEN rank = 6 THEN 'SIX'
			WHEN rank = 7 THEN 'SEVEN'
			WHEN rank = 8 THEN 'EIGHT'
			WHEN rank = 9 THEN 'NINE'
			WHEN rank = 10 THEN 'TEN'
		END
		);

ALTER TABLE tr_md_201X3
DROP COLUMN rank;
SELECT * FROM tr_md_201X3 order by 1,3;

SELECT distinct forecasted_year_month, actual_price FROM tr_md_201X3 order by 1,2;
CREATE extension tablefunc; -- To enable crosstab


--SELECT pg_typeof("forecasted_year_month"), pg_typeof(forecast) from tr_md_201X3 limit 1;
--********************************************************************************************************
--***********************************		TRAINING DATA 	tr_md_201X3_2	  ********************************************
-- AVERAGE OF ACTUAL PRICE IS CALCULATED HERE
--	SOURCE TABLE tr_md_201X3
--********************************************************************************************************
-- FLOW 	md_201X + benchmark --> md_201X2 --> [Ranked + < 11] md_201X3 --> [1 becomes ONE]   tr_md_201X3 --> [avg actual price] tr_md_201X3_2
-- FLOW 	models_data --> BENCHMARK
-- FLOW 	models_data --> models_data3 --> [>1999] md_201X
--********************************************************************************************************	

DROP TABLE IF EXISTS tr_md_201X3_2;
CREATE TABLE tr_md_201X3_2
AS
SELECT
		forecasted_year_month
		,actual_price
		,forecast
		, rankc
		,AVG (actual_price) OVER (
			PARTITION BY forecasted_year_month
		) avg_act_pr
	FROM 
		tr_md_201X3;
ALTER TABLE tr_md_201X3_2 DROP COLUMN actual_price;		
 --SELECT * FROM tr_md_201X3_2;
--		********************************************************************************************************
--***********************************		TRAINING DATA 	tr_md_201X3_3  ********************************************
-- PIVOTING IS DONE HERE 
--SOURCE TABLE tr_md_201X3
--********************************************************************************************************
-- FLOW 	md_201X + benchmark --> md_201X2 --> md_201X3 -->   tr_md_201X3 --> tr_md_201X3_3
-- FLOW 	md_201X + benchmark --> md_201X2 --> md_201X3 -->   tr_md_201X3 --> tr_md_201X3_2
-- FLOW 	models_data --> BENCHMARK
-- FLOW 	models_data --> models_data3 --> md_201X
--		********************************************************************************************************	
DROP TABLE IF EXISTS tr_md_201X3_3;
CREATE TABLE tr_md_201X3_3
AS
SELECT * 
FROM crosstab( 'select forecasted_year_month,rankc, forecast from tr_md_201X3 order by 1,3') 
     AS tr_md_201X3_3(forecasted_year_month VARCHAR(20), ONE float8, TWO float8,THREE float8,FOUR float8
	 ,FIVE float8,SIX float8,SEVEN float8,EIGHT float8,NINE float8,TEN float8);
	 
 SELECT * FROM tr_md_201X3_3;
 
 --		********************************************************************************************************
--***********************************		TRAINING DATA 	AVG ACT PR  ********************************************
--SOURCE TABLE tr_md_201X3_2
--********************************************************************************************************
-- FLOW 	md_201X + benchmark --> md_201X2 --> md_201X3 -->   tr_md_201X3 --> tr_md_201X3_3
-- FLOW 	md_201X + benchmark --> md_201X2 --> md_201X3 -->   tr_md_201X3 --> tr_md_201X3_2 --> tr_md_201X3_avg_act_pr
-- FLOW 	models_data --> BENCHMARK
-- FLOW 	models_data --> models_data3 --> md_201X
--		********************************************************************************************************
 DROP TABLE IF EXISTS tr_md_201X3_avg_act_pr;
CREATE TABLE tr_md_201X3_avg_act_pr
AS
SELECT 
distinct forecasted_year_month, avg_act_pr FROM tr_md_201X3_2 order by 1;
 SELECT * FROM tr_md_201X3_avg_act_pr;
  --		********************************************************************************************************
--***********************************		TRAINING DATA 	FINAL  ********************************************
-- SOURCE TABLE tr_md_201X3_3 + tr_md_201X3_avg_act_pr
--********************************************************************************************************
-- FLOW 	tr_md_201X3_3 + tr_md_201X3_avg_act_pr --> training
-- FLOW 	md_201X + benchmark --> md_201X2 --> md_201X3 -->   tr_md_201X3 --> tr_md_201X3_3
-- FLOW 	md_201X + benchmark --> md_201X2 --> md_201X3 -->   tr_md_201X3 --> tr_md_201X3_2 --> tr_md_201X3_avg_act_pr
-- FLOW 	models_data --> BENCHMARK
-- FLOW 	models_data --> models_data3 --> md_201X
--		********************************************************************************************************
  DROP table if exists training;
 CREATE TABLE training 
 AS
 SELECT A.*, B.avg_act_pr
 FROM 
 tr_md_201X3_3 A
 LEFT JOIN 
 tr_md_201X3_avg_act_pr B
 ON 
 A.forecasted_year_month = B.forecasted_year_month;
 SELECT * from training;
 
 DROP TABLE IF EXISTS TRAIN1015;
CREATE TABLE TRAIN1015
AS
SELECT *
FROM TRAINING
WHERE substring(forecasted_year_month,1,4) IN ('2010','2011','2012','2013','2014','2015');
SELECT * FROM TRAIN1015;

DROP TABLE IF EXISTS TRAIN1016;
CREATE TABLE TRAIN1016
AS
SELECT *
FROM TRAINING
WHERE substring(forecasted_year_month,1,4) IN ('2010','2011','2012','2013','2014','2015','2016');

SELECT * FROM TRAIN1016;

DROP TABLE IF EXISTS TRAIN1017;
CREATE TABLE TRAIN1017
AS
SELECT *
FROM TRAINING
WHERE substring(forecasted_year_month,1,4) IN ('2010','2011','2012','2013','2014','2015','2016','2017');
SELECT * FROM TRAIN1017;
 
--********************************************************************************************************
--***********************************		md_201X4_test		  ********************************************
--********************************************************************************************************
-- FLOW 	md_201X3 + md_201X --> md_201X4_test
-- FLOW 	md_201X + benchmark --> md_201X2 --> md_201X3
-- FLOW 	models_data --> models_data3 --> md_201X
--********************************************************************************************************
	
DROP TABLE IF EXISTS md_201X4_test;
CREATE TABLE md_201X4_test
AS
SELECT A.mtf, A.model, A.variables, A.rank, B.forecast
from 
md_201X3  A
LEFT JOIN 
md_201X B
on A.mtf = B.forecasted_year_month
AND
A.model = B.model
AND
A.variables = B.variables;
--SELECT * from md_201X4_test ORDER BY 1,4;
--********************************************************************************************************
--***********************************		avg_act_prc	  ********************************************
-- tr_md_201X3_2 -->  avg_act_prc
--********************************************************************************************************
DROP TABLE IF EXISTS avg_act_prc;
CREATE TABLE avg_act_prc
AS
SELECT  DISTINCT forecasted_year_month, avg_act_pr FROM tr_md_201X3_2 ORDER BY 1;
SELECT * FroM avg_act_prc;

--********************************************************************************************************
--***********************************		md_201X4_test2		  ********************************************
-- md_201X4_test --> md_201X4_test2
--********************************************************************************************************
DROP TABLE IF EXISTS md_201X4_test2;
CREATE TABLE md_201X4_test2
AS
SELECT * from md_201X4_test ORDER BY 1,4; --where SUBSTRING(mtf,1,4) in ('2016','2017','2018','2019') ORDER BY 1,4; 
--SELECT * from md_201X4_test2;
--
ALTER TABLE md_201X4_test2
ADD COLUMN rankC VARCHAR(20);
--****************************
UPDATE md_201X4_test2
SET rankC = (
		CASE 
			WHEN rank = 1 THEN 'ONE'
			WHEN rank = 2 THEN 'TWO'
			WHEN rank = 3 THEN 'THREE'
			WHEN rank = 4 THEN 'FOUR'
			WHEN rank = 5 THEN 'FIVE'
			WHEN rank = 6 THEN 'SIX'
			WHEN rank = 7 THEN 'SEVEN'
			WHEN rank = 8 THEN 'EIGHT'
			WHEN rank = 9 THEN 'NINE'
			WHEN rank = 10 THEN 'TEN'
		END
		);
--****************************
ALTER TABLE md_201X4_test2
DROP COLUMN rank;
SELECT * FROM md_201X4_test2 order by 1,3;
--		********************************************************************************************************
--***********************************		TEST DATA 	md_201X4_test3  ********************************************
-- PIVOTING IS DONE HERE
-- md_201X4_test --> md_201X4_test2 --> md_201X4_test3
--		********************************************************************************************************	
DROP TABLE IF EXISTS md_201X4_test3;
CREATE TABLE md_201X4_test3
AS
SELECT * 
FROM crosstab( 'select mtf,rankc, forecast from md_201X4_test2 order by 1,3') 
     AS md_201X4_test3(mtf VARCHAR(20), ONE float8, TWO float8,THREE float8,FOUR float8
	 ,FIVE float8,SIX float8,SEVEN float8,EIGHT float8,NINE float8,TEN float8);
	 
 SELECT * FROM md_201X4_test3;

--********************************************************************************************************
--***********************************		TEST TABLE		  ********************************************
-- AVerage actual price is added here 
-- md_201X4_test3 + avg_act_prc --> test
-- md_201X4_test --> md_201X4_test2 --> md_201X4_test3
--********************************************************************************************************
DROP TABLE IF EXISTS test;
CREATE TABLE test
AS
SELECT A.*, B.avg_act_pr
FROM
md_201X4_test3 A
LEFT JOIN 
avg_act_prc B
ON 
A.mtf = B.forecasted_year_month;
SELECT * from test;

DROP TABLE IF EXISTS BTRAIN_15;
CREATE TABLE BTRAIN_15
AS
SELECT * from test 
WHERE 
SUBSTRING(MTF,1,4) IN ('2010','2011','2012','2013','2014','2015');
SELECT * FROM BTRAIN_15;

DROP TABLE IF EXISTS BTRAIN_16;
CREATE TABLE BTRAIN_16
AS
SELECT * from test 
WHERE 
SUBSTRING(MTF,1,4) IN ('2010','2011','2012','2013','2014','2015','2016');
SELECT * FROM BTRAIN_16;

DROP TABLE IF EXISTS BTRAIN_17;
CREATE TABLE BTRAIN_17
AS
SELECT * from test 
WHERE 
SUBSTRING(MTF,1,4) IN ('2010','2011','2012','2013','2014','2015','2016','2017');
SELECT * FROM BTRAIN_17;


DROP TABLE IF EXISTS TEST16;
CREATE TABLE TEST16
AS
SELECT * from test 
WHERE 
SUBSTRING(MTF,1,4) IN ('2016');
SELECT * FROM TEST16;

DROP TABLE IF EXISTS TEST17;
CREATE TABLE TEST17
AS
SELECT * from test 
WHERE 
SUBSTRING(MTF,1,4) IN ('2017');
SELECT * FROM TEST17;


DROP TABLE IF EXISTS TEST18;
CREATE TABLE TEST18
AS
SELECT * from test 
WHERE 
SUBSTRING(MTF,1,4) IN ('2018');
SELECT * FROM TEST18;



--TEST CASES
--SELECT * FROM md_201X where SUBSTRING(forecasted_year_month, 1,7) in ('2018-02') order by 1,7;
-- SELECT * FROM  tr_md_201X3 where SUBSTRING(forecasted_year_month, 1,7) in ('2018-02') order by 1,3;
-- SELECT * FROM tr_md_201X3_2 where SUBSTRING(forecasted_year_month, 1,7) in ('2018-02')order by 1,3;
-- SELECT * FROM tr_md_201X3_3 where SUBSTRING(forecasted_year_month, 1,4) in ('2018');



-- SELECT * FROM tr_md_201X3_3 where SUBSTRING(forecasted_year_month, 1,7) in ('2018-02');

-- SELECT * FROM tr_md_201X3_2 where SUBSTRING(forecasted_year_month, 1,4) in ('2018') ;

--  SELECT * from training where SUBSTRING(forecasted_year_month, 1,7) in ('2018-02');

-- SELECT * FROM md_201X3 where SUBSTRING(forecasted_year_month, 1,7) in ('2018-02')  ORDER BY 1,2,6
-- SELECT * FROM md_201X3 where SUBSTRING(forecasted_year_month, 1,7) in ('2018-02') 



