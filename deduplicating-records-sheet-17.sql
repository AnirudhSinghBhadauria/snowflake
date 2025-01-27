use warehouse compute_wh;
use role accountadmin;
use database food;
use schema core;

-- check if a table has duplicate records
WITH duplicate_check AS (
    SELECT 
        sale_date, 
        product_category, 
        region, 
        sales_rep, 
        sale_amount,
        COUNT(*) AS occurrence_count
    FROM sales_duplicates
    GROUP BY 
        sale_date, 
        product_category, 
        region, 
        sales_rep, 
        sale_amount
    HAVING COUNT(*) > 1
)
SELECT 
    COUNT(*) AS duplicate_record_count
FROM duplicate_check;


-- deduplicate records in existing table
CREATE OR REPLACE TABLE sales_duplicates AS
SELECT * 
FROM (
    SELECT *,
    ROW_NUMBER() OVER (
        PARTITION BY sale_date, product_category, region, sales_rep, sale_amount
        ORDER BY sale_date
    ) AS rownumber
    FROM sales_duplicates
)
WHERE rownumber = 1;
alter table sales_duplicates drop column rownumber;

select * from sales_duplicates;


-- deduplicate records while loading csv into snowflake
select *
from sales_data;

-- loading data (with duplicate records) from stage to table,

CREATE OR REPLACE TABLE user_profiles (
    id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    gender CHAR(1),
    about_me VARCHAR(255)
);

create or replace file format user_data_ff
type = 'csv'
compression = 'none'
field_delimiter = ','
record_delimiter = '\n'
skip_header = 1
field_optionally_enclosed_by = '\042'
escape = '\134';

create or replace stage user_data_stage
file_format = 'user_data_ff';

-- this copy command will not allow any duplicates in the tables.
copy into user_profiles
from (
    select distinct *
    from @user_data_stage/history/user_data.csv
) force = true;

select t.$1 
from @user_data_stage/history/user_data.csv 
(file_format => 'user_data_ff') t;

list @user_data_stage/history;

select *
from user_profiles
limit 10;

-- performing the same thing with external and external table
create or replace file format user_profile_database_ff
type = 'csv'
compression = 'none'
field_delimiter = ','
record_delimiter = '\n'
skip_header = 1
field_optionally_enclosed_by = '\042'
escape = '\134';

create or replace stage user_profile_database_stage
url = 's3://core-management/stage/user-data/'
credentials = (
    aws_key_id = '' 
    aws_secret_key = ''
)
file_format = 'user_profile_database_ff';

list @user_profile_database_stage/;

create external table  user_profile_database_collection (
    id int as (value:c1::int),
    first_name VARCHAR(50) as (value:c2::VARCHAR(50)),
    last_name VARCHAR(50) as (value:c3::VARCHAR(50)),
    email VARCHAR(100) as (value:c4::VARCHAR(100)),
    gender CHAR(1) as (value:c5::char(1)),
    about_me VARCHAR(255) as (value:c6::VARCHAR(255))
)
with location = @user_profile_database_stage
auto_refresh = false
file_format = (format_name = user_profile_database_ff);

-- NO COPY INTO REQUIRED FOR EXTERNAL TABLES

select count(*)
from user_profile_database_collection;

select count(*)
from user_profile_database_collection;

desc table user_profile_database_collection;









