CREATE TABLE tbl_EmployeePivotTest
(
    EmpName VARCHAR(255)
    ,EmpDeptName VARCHAR(255)
    ,EmpAvgWorkingHours INTEGER
);
 
INSERT INTO tbl_EmployeePivotTest VALUES
('Anvesh','Computer-IT',226)
,('Anvesh','Computer-IT',100)
,('Anvesh','Account',142)
,('Anvesh','Marketing',110)
,('Anvesh','Finance',236)
,('Anvesh','Account',120)
,('Jeeny','Computer-IT',120)
,('Jeeny','Finance',852)
,('Jeeny','Account',326)
,('Jeeny','Marketing',50)
,('Jeeny','Finance',140);

SELECT * FROM tbl_EmployeePivotTest


SELECT 
     EmpName
    ,SUM(Computer_IT) AS Total_IT
    ,SUM(Account) AS Total_Account
    ,SUM(Marketing) AS Total_Marketing
    ,SUM(Finance) AS Total_Finance
FROM 
(
    SELECT
	EmpName
	,CASE WHEN EmpDeptName = 'Computer-IT' 
		THEN EmpAvgWorkingHours END AS Computer_IT
	,CASE WHEN EmpDeptName = 'Account' 
		THEN EmpAvgWorkingHours END AS Account
	,CASE WHEN EmpDeptName = 'Marketing' 
		THEN EmpAvgWorkingHours END AS Marketing
	,CASE WHEN EmpDeptName = 'Finance' 
		THEN EmpAvgWorkingHours END AS Finance    
    FROM tbl_EmployeePivotTest
) AS T
GROUP BY EmpName;