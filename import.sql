--TRy
--CREATE TABLE try ( Date date, Open_Corn real )
--COPY try(Date, Open_Corn) FROM 'D:\Sumit\WORK\Eltsen\Data\Corn_pgsql.csv' DELIMITER ',' CSV HEADER;
--SELECT CAST ('01-OCT-2015' AS DATE);
--SELECT CAST ('10.2' AS DOUBLE PRECISION);
--************************************************************************************--
-- CREATE TABLE Models_7Levels ( Sno serial, Month VARCHAR(20), Forecast_date DATE, Actual_Price real, Model VARCHAR(255),Forecast_Price real,Error real,Absolute_Percentage_Error real)
--COPY Models_7Levels(Sno, Month, Forecast_date, Actual_Price, Model, Forecast_Price, Error, Absolute_Percentage_Error) 
--FROM 'D:\Sumit\WORK\Eltsen\Data\Models_7LevelsPGS.csv' DELIMITER ',' CSV HEADER;
SELECT * FROM Models_7levels;