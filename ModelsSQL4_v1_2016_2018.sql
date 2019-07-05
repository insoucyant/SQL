--**************************************************************************************
--******************************        RDS FILE      *******************************
--**************************************************************************************
--********************************************************************************************************
--***********************************		models_data		  ********************************************
--********************************************************************************************************

-- First create the model_data table using R 
--SELECT * from models_data Top 20;--SELECT * from models_data;
ALTER TABLE models_data ADD COLUMN absolute_percentage_error REAL;
UPDATE models_data
SET 
absolute_percentage_error = models_data."MAPE";

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
models_data;

DROP TABLE IF EXISTS md_201X;
CREATE TABLE md_201X
AS
SELECT * from models_data3 where
SUBSTRING(forecasted_year_month, 1,4)::int > 2015 
AND 
SUBSTRING(forecasted_year_month, 1,4)::int < 2019
ORDER BY 1
;

-- SELECT * FROM md_201X;
-- SELECT DISTINCT SUBSTRING(forecasted_year_month, 1,4) from md_201X ORDER BY 1;



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
-- SELECT * FROM md_201X2;

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

ALTER TABLE md_201X2 ADD COLUMN QTR INTEGER;
ALTER TABLE md_201X2 ADD COLUMN HY INTEGER;
ALTER TABLE md_201X2 ADD COLUMN FY INTEGER;
ALTER TABLE md_201X2 ADD COLUMN yr char(5);
ALTER TABLE md_201X2 ADD COLUMN mnth char(5);

UPDATE MD_201X2 SET
yr = SUBSTRING(forecasted_year_month, 1,4);
UPDATE MD_201X2 SET
mnth = SUBSTRING(forecasted_year_month, 6,2);

UPDATE MD_201X2 SET
FY = SUBSTRING(forecasted_year_month, 4,1)::INT ;
SELECT DISTINCT forecasted_year_month, FY FROM md_201X2 ORDER BY 1;



UPDATE MD_201X2 SET
qtr =
CASE
	WHEN  SUBSTRING(forecasted_year_month, 6,2) IN ('01','02','03') THEN 1 
	WHEN  SUBSTRING(forecasted_year_month, 6,2) IN ('04','05','06') THEN 2
	WHEN  SUBSTRING(forecasted_year_month, 6,2) IN ('07','08','09') THEN 3
	WHEN  SUBSTRING(forecasted_year_month, 6,2) IN ('10','11','12') THEN 4 
END	
	;
--SELECT DISTINCT forecasted_year_month, QTR FROM md_201X2 ORDER BY 1;



UPDATE MD_201X2 SET
hy =
CASE
	WHEN  SUBSTRING(forecasted_year_month, 6,2) IN ('01','02','03') THEN 1 
	WHEN  SUBSTRING(forecasted_year_month, 6,2) IN ('04','05','06') THEN 1
	WHEN  SUBSTRING(forecasted_year_month, 6,2) IN ('07','08','09') THEN 2
	WHEN  SUBSTRING(forecasted_year_month, 6,2) IN ('10','11','12') THEN 2 
END	
	;
--SELECT DISTINCT forecasted_year_month, hy FROM md_201X2 ORDER BY 1;

-- SELECT * FROM md_201X2 WHERE FLG > 0;
-- SELECT * FROM md_201X2 WHERE FLG20 > 0;


--SELECT forecasted_year_month, model, variables, mnth, qtr, hy, fy, yr FROM md_201X2; 

DROP TABLE IF EXISTS md_201x3;
CREATE TABLE md_201x3
AS
SELECT forecasted_year_month, model, variables,FLG,FLG20, mnth, qtr, hy, fy, yr FROM md_201X2 ORDER BY 1,2,3,4; 


COPY md_201X2 TO 'D:\Sumit\WORK\Eltsen\Data\SQL\md_201X2_2016_2018.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;
--**********************************************  MONTH
DROP TABLE IF EXISTS md_btr_mnth_201x;
CREATE TABLE md_btr_mnth_201x
AS
SELECT model, variables, mnth,
SUM(flg) as NUM1, COUNT(flg) as NUM01
FROM md_201X2
GROUP BY model, variables, mnth
ORDER BY 1,2,3;
ALTER TABLE md_btr_mnth_201x ADD COLUMN PTGE REAL;
UPDATE md_btr_mnth_201x 
SET PTGE = ROUND((NUM1::numeric/NUM01),2);


DROP TABLE IF EXISTS md_btr_mnth_201x_2;
CREATE TABLE md_btr_mnth_201x_2
AS
SELECT mnth, model, variables, ptge,
RANK() OVER(
	PARTITION BY mnth
	ORDER BY ptge DESC
)
FROM md_btr_mnth_201x
;
--SELECT * FROM md_btr_mnth_201x_2; 

DROP TABLE IF EXISTS BEATS_BCHMRK_RANK1_MNTH_WISE;
CREATE TABLE BEATS_BCHMRK_RANK1_MNTH_WISE
AS
SELECT * FROM md_btr_mnth_201x_2 where rank = 1; 

COPY BEATS_BCHMRK_RANK1_MNTH_WISE TO 'D:\Sumit\WORK\Eltsen\Data\SQL\BEATS_BCHMRK_RANK1_MNTH_WISE_1618.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;
--************* MONTH 20
DROP TABLE IF EXISTS md_btr20_mnth_201x;
CREATE TABLE md_btr20_mnth_201x
AS
SELECT model, variables, mnth,
SUM(flg20) as NUM1, COUNT(flg20) as NUM01
FROM md_201X2
GROUP BY model, variables, mnth
ORDER BY 1,2,3;
ALTER TABLE md_btr20_mnth_201x ADD COLUMN PTGE REAL;
UPDATE md_btr20_mnth_201x 
SET PTGE = ROUND((NUM1::numeric/NUM01),2);
-- SELECT * FROM md_btr20_mnth_201x;


DROP TABLE IF EXISTS md_btr20_mnth_201x_2;
CREATE TABLE md_btr20_mnth_201x_2
AS
SELECT mnth, model, variables, ptge,
RANK() OVER(
	PARTITION BY mnth
	ORDER BY ptge DESC
)
FROM md_btr20_mnth_201x
;
--SELECT * FROM md_btr20_mnth_201x_2; 

DROP TABLE IF EXISTS BEATS_BCHMRK20_RANK1_MNTH_WISE;
CREATE TABLE BEATS_BCHMRK20_RANK1_MNTH_WISE
AS
SELECT * FROM md_btr20_mnth_201x_2 where rank < 3; 
--SELECT * FROM BEATS_BCHMRK20_RANK1_MNTH_WISE;

COPY BEATS_BCHMRK20_RANK1_MNTH_WISE TO 'D:\Sumit\WORK\Eltsen\Data\SQL\BEATS_BCHMRK20_RANK1_MNTH_WISE_1618.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;
--**********************************************  QUARTER

DROP TABLE IF EXISTS md_btr_QRTR_201x;
CREATE TABLE md_btr_QRTR_201x
AS
SELECT model, variables, QTR,
SUM(flg) as NUM1, COUNT(flg) as NUM01
FROM md_201X2
GROUP BY model, variables, QTR
ORDER BY 1,2,3;
ALTER TABLE md_btr_QRTR_201x ADD COLUMN PTGE REAL;
UPDATE md_btr_QRTR_201x 
SET PTGE = ROUND((NUM1::numeric/NUM01),2);
--SELECT * FROM md_btr_QRTR_201x


DROP TABLE IF EXISTS md_btr_QRTR_201x_2;
CREATE TABLE md_btr_QRTR_201x_2
AS
SELECT QTR, model, variables, ptge,
RANK() OVER(
	PARTITION BY QTR
	ORDER BY ptge DESC
)
FROM md_btr_QRTR_201x
;
--SELECT * FROM md_btr_QRTR_201x_2; 

DROP TABLE IF EXISTS BEATS_BCHMRK_RANK1_QRTR_WISE;
CREATE TABLE BEATS_BCHMRK_RANK1_QRTR_WISE
AS
SELECT * FROM md_btr_QRTR_201x_2 where rank < 9; 
--SELECT * FROM BEATS_BCHMRK_RANK1_QRTR_WISE; 

COPY BEATS_BCHMRK_RANK1_QRTR_WISE TO 'D:\Sumit\WORK\Eltsen\Data\SQL\BEATS_BCHMRK_RANK1_QRTR_WISE_1618.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;
--************************** QUARTER 20

DROP TABLE IF EXISTS md_btr_QRTR20_201x;
CREATE TABLE md_btr_QRTR20_201x
AS
SELECT model, variables, QTR,
SUM(flg20) as NUM1, COUNT(flg20) as NUM01
FROM md_201X2
GROUP BY model, variables, QTR
ORDER BY 1,2,3;
ALTER TABLE md_btr_QRTR20_201x ADD COLUMN PTGE REAL;
UPDATE md_btr_QRTR20_201x 
SET PTGE = ROUND((NUM1::numeric/NUM01),2);
--SELECT * FROM md_btr_QRTR20_201x;


DROP TABLE IF EXISTS md_btr_QRTR20_201x_2;
CREATE TABLE md_btr_QRTR20_201x_2
AS
SELECT QTR, model, variables, ptge,
RANK() OVER(
	PARTITION BY QTR
	ORDER BY ptge DESC
)
FROM md_btr_QRTR20_201x
;
--SELECT * FROM md_btr_QRTR20_201x_2; 

DROP TABLE IF EXISTS BEATS_BCHMRK20_RANK1_QRTR_WISE;
CREATE TABLE BEATS_BCHMRK20_RANK1_QRTR_WISE
AS
SELECT * FROM md_btr_QRTR20_201x_2 where rank < 6; 
--SELECT * FROM BEATS_BCHMRK20_RANK1_QRTR_WISE; 

COPY BEATS_BCHMRK20_RANK1_QRTR_WISE TO 'D:\Sumit\WORK\Eltsen\Data\SQL\BEATS_BCHMRK20_RANK1_QRTR_WISE_1618.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;

--**********************************************  HALF YEARLY

DROP TABLE IF EXISTS md_btr_hy_201x;
CREATE TABLE md_btr_hy_201x
AS
SELECT model, variables, HY,
SUM(flg) as NUM1, COUNT(flg) as NUM01
FROM md_201X2
GROUP BY model, variables, HY
ORDER BY 1,2,3;
ALTER TABLE md_btr_hy_201x ADD COLUMN PTGE REAL;
UPDATE md_btr_hy_201x 
SET PTGE = ROUND((NUM1::numeric/NUM01),2);
--SELECT * FROM md_btr_hy_201x;


DROP TABLE IF EXISTS md_btr_HY_201x_2;
CREATE TABLE md_btr_HY_201x_2
AS
SELECT HY, model, variables, ptge,
RANK() OVER(
	PARTITION BY HY
	ORDER BY ptge DESC
)
FROM md_btr_hy_201x
;
--SELECT * FROM md_btr_HY_201x_2; 

DROP TABLE IF EXISTS BEATS_BCHMRK_RANK1_HY_WISE;
CREATE TABLE BEATS_BCHMRK_RANK1_HY_WISE
AS
SELECT * FROM md_btr_HY_201x_2 where rank < 10; 
--SELECT * FROM BEATS_BCHMRK_RANK1_HY_WISE; 

COPY BEATS_BCHMRK_RANK1_HY_WISE TO 'D:\Sumit\WORK\Eltsen\Data\SQL\BEATS_BCHMRK_RANK1_HY_WISE_1618.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;
--************************** HALF YEARLY 20

DROP TABLE IF EXISTS md_btr_HY20_201x;
CREATE TABLE md_btr_HY20_201x
AS
SELECT model, variables, HY,
SUM(flg20) as NUM1, COUNT(flg20) as NUM01
FROM md_201X2
GROUP BY model, variables, HY
ORDER BY 1,2,3;
ALTER TABLE md_btr_HY20_201x ADD COLUMN PTGE REAL;
UPDATE md_btr_HY20_201x 
SET PTGE = ROUND((NUM1::numeric/NUM01),2);
--SELECT * FROM md_btr_HY20_201x;


DROP TABLE IF EXISTS md_btr_HY20_201x_2;
CREATE TABLE md_btr_HY20_201x_2
AS
SELECT HY, model, variables, ptge,
RANK() OVER(
	PARTITION BY HY
	ORDER BY ptge DESC
)
FROM md_btr_HY20_201x
;
--SELECT * FROM md_btr_HY20_201x_2; 

DROP TABLE IF EXISTS BEATS_BCHMRK20_RANK1_HY_WISE;
CREATE TABLE BEATS_BCHMRK20_RANK1_HY_WISE
AS
SELECT * FROM md_btr_HY20_201x_2 where rank < 4; 
--SELECT * FROM BEATS_BCHMRK20_RANK1_HY_WISE; 

COPY BEATS_BCHMRK20_RANK1_HY_WISE TO 'D:\Sumit\WORK\Eltsen\Data\SQL\BEATS_BCHMRK20_RANK1_HY_WISE_1618.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;
--**********************************************   YEARLY

DROP TABLE IF EXISTS md_btr_y_201x;
CREATE TABLE md_btr_y_201x
AS
SELECT model, variables, YR,
SUM(flg) as NUM1, COUNT(flg) as NUM01
FROM md_201X2
GROUP BY model, variables, YR
ORDER BY 1,2,3;
ALTER TABLE md_btr_y_201x ADD COLUMN PTGE REAL;
UPDATE md_btr_y_201x 
SET PTGE = ROUND((NUM1::numeric/NUM01),2);
--SELECT * FROM md_btr_y_201x;


DROP TABLE IF EXISTS md_btr_Y_201x_2;
CREATE TABLE md_btr_Y_201x_2
AS
SELECT YR, model, variables, ptge,
RANK() OVER(
	PARTITION BY YR
	ORDER BY ptge DESC
)
FROM md_btr_y_201x
;
--SELECT * FROM md_btr_Y_201x_2; 

DROP TABLE IF EXISTS BEATS_BCHMRK_RANK1_Y_WISE;
CREATE TABLE BEATS_BCHMRK_RANK1_Y_WISE
AS
SELECT * FROM md_btr_Y_201x_2 where rank = 1; 
--SELECT * FROM BEATS_BCHMRK_RANK1_Y_WISE; 

COPY BEATS_BCHMRK_RANK1_Y_WISE TO 'D:\Sumit\WORK\Eltsen\Data\SQL\BEATS_BCHMRK_RANK1_Y_WISE_1618.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;

--**************************  YEARLY 20

DROP TABLE IF EXISTS md_btr_Y20_201x;
CREATE TABLE md_btr_Y20_201x
AS
SELECT model, variables, YR,
SUM(flg20) as NUM1, COUNT(flg20) as NUM01
FROM md_201X2
GROUP BY model, variables, YR
ORDER BY 1,2,3;
ALTER TABLE md_btr_Y20_201x ADD COLUMN PTGE REAL;
UPDATE md_btr_Y20_201x 
SET PTGE = ROUND((NUM1::numeric/NUM01),2);
--SELECT * FROM md_btr_Y20_201x;


DROP TABLE IF EXISTS md_btr_Y20_201x_2;
CREATE TABLE md_btr_Y20_201x_2
AS
SELECT YR, model, variables, ptge,
RANK() OVER(
	PARTITION BY YR
	ORDER BY ptge DESC
)
FROM md_btr_Y20_201x
;
--SELECT * FROM md_btr_Y20_201x_2; 

DROP TABLE IF EXISTS BEATS_BCHMRK20_RANK1_Y_WISE;
CREATE TABLE BEATS_BCHMRK20_RANK1_Y_WISE
AS
SELECT * FROM md_btr_Y20_201x_2 where rank < 4; 
--SELECT * FROM BEATS_BCHMRK20_RANK1_Y_WISE; 

COPY BEATS_BCHMRK20_RANK1_Y_WISE TO 'D:\Sumit\WORK\Eltsen\Data\SQL\BEATS_BCHMRK20_RANK1_Y_WISE_1618.csv' ---------------------------------------
DELIMITER ',' CSV HEADER;

