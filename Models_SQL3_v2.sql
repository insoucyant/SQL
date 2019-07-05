--************************************************************************************--
--SELECT version();
--************************************************************************************--
------------------------------------------------------
--**************************************************************************************
--******************************        DNA FILE      *******************************
--**************************************************************************************
DROP TABLE IF EXISTS file1;
DROP TABLE IF EXISTS file2;
DROP TABLE IF EXISTS file3;
DROP TABLE IF EXISTS file4;
DROP TABLE IF EXISTS file5;
-------------
CREATE TABLE file1(	
VarianceTimeline int,
YEARMONTH VARCHAR(20)
,MatchingTimeFrame VARCHAR(20)
,SD_Corn_MeanReversed real
,NCOMM_OI_Percent real
,SD_Crude_MeanReversed real
);
CREATE TABLE file2
AS
SELECT * FROM file1;
CREATE TABLE file3
AS
SELECT * FROM file1;
CREATE TABLE file4
AS
SELECT * FROM file1;
CREATE TABLE file5
AS
SELECT * FROM file1;
----------------
COPY file1(
VarianceTimeline,YEARMONTH,MatchingTimeFrame, SD_Corn_MeanReversed, NCOMM_OI_Percent, SD_Crude_MeanReversed
)
FROM 'D:\Sumit\WORK\Eltsen\Data\SQL\MatchedTimeframes_Corn.csv' DELIMITER ',' CSV HEADER; ---------------------------------------------
COPY file2(
VarianceTimeline,YEARMONTH,MatchingTimeFrame, SD_Corn_MeanReversed, NCOMM_OI_Percent, SD_Crude_MeanReversed
)
FROM 'D:\Sumit\WORK\Eltsen\Data\SQL\MatchedTimeframes_Corn_Crude_FiftyPercent.csv' DELIMITER ',' CSV HEADER; ---------------------------------------------
COPY file3(
VarianceTimeline,YEARMONTH,MatchingTimeFrame, SD_Corn_MeanReversed, NCOMM_OI_Percent, SD_Crude_MeanReversed
)
FROM 'D:\Sumit\WORK\Eltsen\Data\SQL\MatchedTimeframes_Corn_Crude_TenPercent.csv' DELIMITER ',' CSV HEADER; ---------------------------------------------
COPY file4(
VarianceTimeline,YEARMONTH,MatchingTimeFrame, SD_Corn_MeanReversed, NCOMM_OI_Percent, SD_Crude_MeanReversed
)
FROM 'D:\Sumit\WORK\Eltsen\Data\SQL\MatchedTimeframes_Corn_OI.csv' DELIMITER ',' CSV HEADER; ---------------------------------------------
COPY file5(
VarianceTimeline,YEARMONTH,MatchingTimeFrame, SD_Corn_MeanReversed, NCOMM_OI_Percent, SD_Crude_MeanReversed
)
FROM 'D:\Sumit\WORK\Eltsen\Data\SQL\MatchedTimeframes_Corn_OI_Crude_FiftyPercent.csv' DELIMITER ',' CSV HEADER; ---------------------------------------------
----------------
DROP TABLE IF EXISTS MatchedTimeframes;
-------------
CREATE TABLE MatchedTimeframes
AS
SELECT yearmonth, matchingtimeframe, variancetimeline FROM file1;
--SELECT * from MatchedTimeframes LIMIT 10;
--**************************************************************************************
--**************************************************************************************
--******************************        RDS FILE      *******************************
--**************************************************************************************
--********************************************************************************************************
--***********************************		models_data		  ********************************************
--********************************************************************************************************

-- First create the model_data table using R 
--SELECT * from models_data;
ALTER TABLE models_data ADD COLUMN absolute_percentage_error REAL;
UPDATE models_data
SET 
absolute_percentage_error = models_data."MAPE";
--********************************************************************************************************
--***********************************		models_data2		******************************************
--********************************************************************************************************
DROP TABLE IF EXISTS models_data2;
--***********************************
CREATE TABLE models_data2
AS
SELECT * from models_data where forecasted_year_month IN ( 
SELECT distinct yearmonth from MatchedTimeframes UNION
SELECT distinct matchingtimeframe from MatchedTimeframes);
--***********************************
--SELECT * from models_data2;
--SELECT "MAPE" from models_data2
--********************************************************************************************************
--***********************************		models_data3		******************************************
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
models_data2;
--SELECT * from models_data3;
--********************************************************************************************************
--***********************************		Top_20   		  ********************************************
--********************************************************************************************************
DROP TABLE IF EXISTS Top_20;
CREATE TABLE Top_20 (
	forecasted_year_month VARCHAR(20)
	, actual_price real
	, model VARCHAR(25)
	, variables VARCHAR(255)
	, absolute_percentage_error real
	, rank INT
);


-- DATA INSERTION
INSERT INTO Top_20(forecasted_year_month, actual_price, model, variables, absolute_percentage_error, rank)
SELECT rank_filter.forecasted_year_month, rank_filter.actual_price,rank_filter.model,rank_filter.variables, rank_filter.absolute_percentage_error, rank_filter.rank FROM (
        SELECT models_data3.*, 
        rank() OVER (
            PARTITION BY forecasted_year_month
            ORDER BY absolute_percentage_error 
        )
        FROM models_data3
        --WHERE models_data3.
    ) rank_filter WHERE RANK < 21;
--SELECT * from Top_20;
	--*****************************************************************************************
--   			JOINING THE TWO - MatchedTimeframes & Top_20
--*****************************************************************************************
DROP TABLE IF EXISTS Top100;
CREATE TABLE Top100
AS
SELECT
  MatchedTimeframes.yearmonth
 ,MatchedTimeframes.MatchingTimeFrame
 ,MatchedTimeframes.VarianceTimeline
 ,Top_20.model
 ,Top_20.variables
-- ,Top_20.absolute_percentage_error
FROM
 MatchedTimeframes 
LEFT JOIN Top_20 ON MatchedTimeframes.yearmonth = Top_20.forecasted_year_month;

--SELECT * FROM Top100 LIMIT 20;
--********************************************************************************************************
--***********************************		top100_2		  ********************************************
--********************************************************************************************************
DROP TABLE IF EXISTS Top100_2;
CREATE TABLE Top100_2
AS
SELECT * FROM Top100 
UNION
Select distinct yearmonth, yearmonth as matchingtimeframe, variancetimeline, model, variables from Top100 
order by 1,2,3;


ALTER TABLE Top100_2 ALTER COLUMN matchingtimeframe TYPE DATE using to_date(matchingtimeframe, 'YYYY-MM');
--SELECT * FROM Top100_2 ORDER BY 1,2,3;
--********************************************************************************************************
--***********************************		top100_3		  ********************************************
--********************************************************************************************************
DROP TABLE IF EXISTS top100_3;
CREATE TABLE top100_3
AS
SELECT top100_2.*, generate_series(1,Top100_2.variancetimeline) AS SUB_MN 
FROM 
top100_2 
ORDER BY 1,2,3,4,5,6;
--*******************************
--SELECT * FROM Top100_3 ORDER BY 1,2,3,4,5,6;
--*******************************
ALTER TABLE Top100_3 ADD COLUMN MTF TIMESTAMP;
UPDATE  Top100_3 
SET sub_mn = (sub_mn-1) ;
UPDATE  Top100_3 
SET MTF = (matchingtimeframe - interval '1 month' * sub_mn ) ;
--*******************************
ALTER TABLE Top100_3 ALTER COLUMN MTF TYPE VARCHAR(20);
UPDATE Top100_3 SET mtf = substring(mtf,1,7) ;
--SELECT * FROM Top100_3 ORDER BY 1,2,3,5,6;
--********************************************************************************************************
--***********************************		BENCHMARK		  ********************************************
--********************************************************************************************************
DROP TABLE IF EXISTS benchmark;
CREATE TABLE benchmark
AS
SELECT forecasted_year_month, actual_price, model, variables, forecast,error, absolute_percentage_error AS MAPE
 FROM models_data WHERE variables = 'calculated_benchmark';
--*******************************
--SELECT * FROM benchmark;
--********************************************************************************************************
--											Top100_bcm
--********************************************************************************************************
DROP TABLE IF EXISTS Top100_bcm;
CREATE TABLE Top100_bcm
AS
SELECT a.*, b.forecasted_year_month, b.actual_price, b.forecast as benchmark, b.error as bchmrk_error
, b.mape as bchmrk_mape, (b.error/b.actual_price) as bchmrk_ape
FROM 
Top100_3 as a
LEFT JOIN 
benchmark AS b 
ON
a.mtf = b.forecasted_year_month;

--SELECT * FROM Top100_bcm order by 1,2,3,4,5,6;

--********************************************************************************************************
--													mdl_cmprsn
--********************************************************************************************************
DROP TABLE IF EXISTS mdl_cmprsn;
CREATE TABLE mdl_cmprsn
AS
SELECT a.*, b.actual_price AS mdl_actual_price, b.forecast as mdl_frcst_prc, b.error as mdl_error
, b.absolute_percentage_error as mdl_ape
FROM 
Top100_bcm AS a
LEFT JOIN
models_data AS b
ON
a.mtf = b.forecasted_year_month
AND a.model = b.model
AND a.variables = b.variables;

ALTER TABLE mdl_cmprsn ADD COLUMN mdl_btr REAL;
UPDATE mdl_cmprsn
SET 
mdl_btr = (bchmrk_mape-mdl_ape)/bchmrk_ape;


ALTER TABLE mdl_cmprsn ADD COLUMN mdl_cmp_flg INT;
UPDATE mdl_cmprsn SET 
mdl_cmp_flg = CASE WHEN mdl_btr < 0 THEN 0 ELSE 1 END;

--SELECT * FROM mdl_cmprsn ORDER BY 1,2,3,4,5,6;

COPY mdl_cmprsn TO 'D:\Sumit\WORK\Eltsen\Data\SQL\mdl_cmprsn_Corn.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;
--********************************************************************************************************
--													mdl_cmprsn2
--********************************************************************************************************
DROP TABLE IF EXISTS mdl_cmprsn2;
CREATE TABLE mdl_cmprsn2
AS
SELECT yearmonth, model, variables,
SUM(mdl_cmp_flg) as NUM1, COUNT(mdl_cmp_flg) as NUM01
FROM mdl_cmprsn
GROUP BY yearmonth, model, variables
ORDER BY 1,2,3;
ALTER TABLE mdl_cmprsn2 ADD COLUMN PTGE REAL;
UPDATE mdl_cmprsn2 
SET PTGE = ROUND((NUM1::numeric/NUM01),2);
-- SELECT * FROM mdl_cmprsn2 order by 1,2,3;


COPY mdl_cmprsn2 TO 'D:\Sumit\WORK\Eltsen\Data\SQL\mdl_cmprsn2_Corn.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;

