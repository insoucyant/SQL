--TRy
--CREATE TABLE try ( Date date, Open_Corn real )
--COPY try(Date, Open_Corn) FROM 'D:\Sumit\WORK\Eltsen\Data\Corn_pgsql.csv' DELIMITER ',' CSV HEADER;
--SELECT CAST ('01-OCT-2015' AS DATE);
--SELECT CAST ('10.2' AS DOUBLE PRECISION);
--************************************************************************************--
-- CREATE TABLE Models_7Levels ( Sno serial, Month VARCHAR(20), Forecast_date DATE, Actual_Price real, Model VARCHAR(255),Forecast_Price real,Error real,Absolute_Percentage_Error real)
--COPY Models_7Levels(Sno, Month, Forecast_date, Actual_Price, Model, Forecast_Price, Error, Absolute_Percentage_Error) 
--FROM 'D:\Sumit\WORK\Eltsen\Data\Models_7LevelsPGS.csv' DELIMITER ',' CSV HEADER;
--SELECT * FROM Models_7levels;
--**************************************************************************************
--CREATE TABLE UsefulMonthlyData ( Sno	serial,corn_price	real,YEARMONTH VARCHAR(20),crude_price real	,soy_price real	,new_STU_ratio real	,old_STU_ratio real	,placeholder real	,corn_production real	,contract1 real	,contract3 real	,contract4 real	,contract5 real,contract6 real	,contract7 real)
--COPY UsefulMonthlyData(
--Sno,	corn_price,	YEARMONTH,	crude_price,	soy_price,	new_STU_ratio,	old_STU_ratio,	placeholder,	corn_production,	contract1,	contract3,	contract4,	contract5,	contract6,	contract7)
--FROM 'D:\Sumit\WORK\Eltsen\Data\UsefulMonthlyData.csv' DELIMITER ',' CSV HEADER;
--SELECT * FROM UsefulMonthlyData;
--SELECT * FROM Models_7levels where month = '1990-01' order by absolute_percentage_error;
-----------------------------------------------------
--SELECT Models_7Levels.*, 
--        rank() OVER (
--            PARTITION BY month
--            ORDER BY absolute_percentage_error 
--        )
--        FROM Models_7Levels
--        WHERE Models_7Levels.month = '1990-01'
------------------------------------------------------
--SELECT * from Top_5;
	
--**************************************************************************************
--CREATE TABLE MatchedTimeframes_24MonthsVariance(	Sno serial	,Gr integer	,YEARMONTH real	,matched_timeframe_1	real	,matched_timeframe_2	real	,matched_timeframe_3	real	,matched_timeframe_4	real	,matched_timeframe_5    real)
--COPY MatchedTimeframes_24MonthsVariance(Sno, Gr, YEARMONTH, matched_timeframe_1, matched_timeframe_2, matched_timeframe_3, matched_timeframe_4, matched_timeframe_5)
--FROM 'D:\Sumit\WORK\Eltsen\MatchedTimeframes_24MonthsVariance.csv' DELIMITER ',' CSV HEADER;
--SELECT * from MatchedTimeframes_24MonthsVariance;

