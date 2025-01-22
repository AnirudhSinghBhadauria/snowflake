use database food;
use schema core;
use warehouse compute_wh;
use role accountadmin;

-- Setting the retention time for global table
alter table global set data_retention_time_in_days = 1;

-- updated the spice_level to Extream,
update global
set spice_level = 'Extream'
where spice_level = 'High';

-- using time travel to detect changes in the table
select count(*) 
from global
before (statement => '01b9ca5f-0000-d52b-0008-89ae000540e6') -- this is the id of the query that updated the records
where spice_level = 'High';

select *
from global
limit 10;

-- Example 2, query id for this update statement - '01b9db73-0000-d518-0008-89ae0005b9d6';
update global
set EXERCISE_LEVEL = 'Medium'
where EXERCISE_LEVEL = 'Moderate';

-- Now fetching results before this update statement,
select *
from global
before (statement => '01b9db73-0000-d518-0008-89ae0005b9d6');

-- creating a new table with data retention, time travel,
create or replace sequence core_employee_sequence
start 0 increment 1;

create table core_employee (
    id number primary key default core_employee_sequence.nextval,
    name varchar not null,
    department varchar not null,
    salary number not null,
    location varchar not null
)
data_retention_time_in_days = 1;

-- To see retention time on a particular table
show tables like 'core_%';


-- VARIATIONS OF TIME TRAVEL

-- This will fetch how records looked at a particular query
select *
from core_employee
at (statement => '01b9dbb7-0000-d5e8-0008-89ae0006a336');

-- This will fetch how records looked 2 minutes ago!
select *
from core_employee
at (offset => -60 * 2);

-- This will fetch how records looked at particular timestamp,
select *
from core_employee
at (timestamp => '2025-01-21T04:33:00Z'::timestamptz);

-- INSTEAD OF 'AT' WE CAN ALSO USE 'BEFORE'
-- we can undrop a table / database / schema if TT is not set to 0


-- To see how much space a table is occupying,
select * 
from snowflake.account_usage.table_storage_metrics
where table_name = 'CORE_EMPLOYEE';


select round(active_bytes / 1000000, 2) as "Storage occupied in MBs"
from snowflake.account_usage.table_storage_metrics
where table_name = 'CORE_EMPLOYEE'
order by active_bytes desc
limit 1;


--how to calculate the time travel cost vs table storage cost
--lets assume that we are paying 40$ per TB per month
    
select table_name, 
    sum(ACTIVE_BYTES)/(1024*1024*1024*1024)  as "Active(Tb)", 
    sum(TIME_TRAVEL_BYTES)/(1024*1024*1024*1024) as "TT(Tb)", 
    sum(FAILSAFE_BYTES)/(1024*1024*1024*1024) as  "FF(Tb)",
    ((sum(ACTIVE_BYTES)+sum(TIME_TRAVEL_BYTES)+sum(FAILSAFE_BYTES))/(1024*1024*1024*1024))*(40/30) as "Cost Per Day ($)"
from 
"SNOWFLAKE"."ACCOUNT_USAGE"."TABLE_STORAGE_METRICS" 
where table_name ='STOCK_PRICE_DATA'
group by table_name;

-- for all my schemas
select TABLE_SCHEMA, 
    sum(ACTIVE_BYTES)/(1024*1024*1024*1024)  as "Active(Tb)", 
    sum(TIME_TRAVEL_BYTES)/(1024*1024*1024*1024) as "TT(Tb)", 
    sum(FAILSAFE_BYTES)/(1024*1024*1024*1024) as  "FF(Tb)",
    ((sum(ACTIVE_BYTES)+sum(TIME_TRAVEL_BYTES)+sum(FAILSAFE_BYTES))/(1024*1024*1024*1024))*(40/30) as "Cost Per Day ($)"
from 
"SNOWFLAKE"."ACCOUNT_USAGE"."TABLE_STORAGE_METRICS" 
group by TABLE_SCHEMA;