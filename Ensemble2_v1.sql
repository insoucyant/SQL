drop table if exists r16;
create table r16 
as
select mtf, avg_act_pr, aprdct_all, bprdct_all, aprdct_123, bprdct_123 from test16;
select * from r16;

drop table if exists r17;
create table r17 
as
select mtf, avg_act_pr, aprdct_all, bprdct_all, aprdct_123, bprdct_123 from test17;
select * from r17;


drop table if exists r18;
create table r18 
as
select mtf, avg_act_pr, aprdct_all, bprdct_all, aprdct_123, bprdct_123 from test18;
select * from r18;

drop table if exists r;
create table r 
as
select mtf, avg_act_pr, aprdct_all, bprdct_all, aprdct_123, bprdct_123 from test16
UNION
select mtf, avg_act_pr, aprdct_all, bprdct_all, aprdct_123, bprdct_123 from test17
UNION
select mtf, avg_act_pr, aprdct_all, bprdct_all, aprdct_123, bprdct_123 from test18
;
-- select * from r order by 1;



drop table if exists r18;
create table r18
as
select A.*, B.forecast as calbnchmrk
from 
r A
LEFT JOIN 
benchmark B
ON
A.mtf = B.forecasted_year_month
order by 1;

ALTER TABLE r18 ADD COLUMN cal_bchm REAL;
UPDATE r18 SET 
cal_bchm = abs(avg_act_pr-calbnchmrk)/avg_act_pr;

ALTER TABLE r18 ADD COLUMN a_all REAL;
UPDATE r18 SET 
a_all = abs(avg_act_pr-aprdct_all)/avg_act_pr;

ALTER TABLE r18 ADD COLUMN b_all REAL;
UPDATE r18 SET 
b_all = abs(avg_act_pr-bprdct_all)/avg_act_pr;

ALTER TABLE r18 ADD COLUMN a_123 REAL;
UPDATE r18 SET 
a_123 = abs(avg_act_pr-aprdct_123)/avg_act_pr;

ALTER TABLE r18 ADD COLUMN b_123 REAL;
UPDATE r18 SET 
b_123 = abs(avg_act_pr-bprdct_123)/avg_act_pr;




ALTER TABLE r18 ADD COLUMN FLGa_all INTEGER;
UPDATE r18 SET 
FLGa_all = CASE WHEN a_all > cal_bchm THEN 0 ELSE 1 END;

ALTER TABLE r18 ADD COLUMN FLGb_all INTEGER;
UPDATE r18 SET 
FLGb_all = CASE WHEN b_all > cal_bchm THEN 0 ELSE 1 END;

ALTER TABLE r18 ADD COLUMN FLGa_123 INTEGER;
UPDATE r18 SET 
FLGa_123 = CASE WHEN a_123 > cal_bchm THEN 0 ELSE 1 END;

ALTER TABLE r18 ADD COLUMN FLGb_123 INTEGER;
UPDATE r18 SET 
FLGb_123 = CASE WHEN b_123 > cal_bchm THEN 0 ELSE 1 END;

select * from r18;

select sum(FLGa_all),sum(FLGb_all),sum(FLGa_123), sum(FLGb_123)  from r18;