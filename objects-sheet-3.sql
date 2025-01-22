use database food;
use schema core;
use warehouse compute_wh;
use role accountadmin;



-- SEQUENCE

show sequences;

create or replace sequence test_sequence 
start 1 increment 2
comment = 'this increment can be -ve also';

-- this will always generate a unique number
select test_sequence.nextval;

create table test_sequence_table(
    employee_id int primary key default test_sequence.nextval,
    name varchar(255)
);

insert into test_sequence_table(name) values ('anirudh');

select *
from test_sequence_table;



-- FILE FORMATS
show file formats;
create or replace file format my_format type = 'csv' field_delimiter = ',';

CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = 'CSV'
COMPRESSION = 'GZIP'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1;



-- STAGE
create stage test_stage;
list @test_stage;

-- Using this we can directly query data from stage only! 
select
    t.$1 as "Invoice/Item Number",
    t.$2 as "Date",
    t.$3 as "Store Number",
    t.$4 as "Store Name",
    t.$5 as "Address",
    t.$6 as "City",
    t.$7 as "Zip Code",
    t.$8 as "Store Location",
    t.$9 as "County Number",
    t.$10 as "County",
    t.$11 as "Category",
    t.$12 as "Category Name",
    t.$13 as "Vendor Number",
    t.$14 as "Vendor Name",
    t.$15 as "Item Number",
    t.$16 as "Item Description",
    t.$17 as "Pack",
    t.$18 as "Bottle Volume (ml)",
    t.$19 as "State Bottle Cost",
    t.$20 as "State Bottle Retail",
    t.$21 as "Bottles Sold",
    t.$22 as "Sale (Dollars)",
    t.$23 as "Volume Sold (Liters)",
    t.$24 as "Volume Sold (Gallons)"
from @test_stage (file_format => 'my_format') t;



-- PIPE

-- Creating table for pipe

-- CREATE TABLE sales_data (
--     invoice_item_number STRING,
--     date STRING,
--     store_number INTEGER,
--     store_name STRING,
--     address STRING,
--     city STRING,
--     zip_code STRING,
--     store_location STRING,
--     county_number FLOAT,
--     county STRING,
--     category FLOAT,
--     category_name STRING,
--     vendor_number STRING,
--     vendor_name STRING,
--     item_number INTEGER,
--     item_description STRING,
--     pack FLOAT,
--     bottle_volume_ml FLOAT,
--     state_bottle_cost STRING,
--     state_bottle_retail STRING,
--     bottles_sold INTEGER,
--     sale_dollars STRING,
--     volume_sold_liters FLOAT,
--     volume_sold_gallons FLOAT
-- );

-- Creating file format for my gzip file in stage 'test_stage'
CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = 'CSV'
COMPRESSION = 'GZIP'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1;

-- Creating pipe for loading data into chunks continuously
CREATE OR REPLACE PIPE test_pipe
AS
COPY INTO sales_data
FROM @test_stage
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format');

-- Manually copying data from stage to table
COPY INTO sales_data
FROM @test_stage/Iowa_Liquor_Sales.csv.gz
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format');

-- to check if pipe is running or not!
select SYSTEM$PIPE_STATUS('test_pipe');

select count(*)
from sales_data;

list @test_stage;



-- STREAMS

show streams;
select get_ddl("stream", "test_stream");

create or replace stream test_stream 
on table global;

-- No records in this stream table
select *
from test_stream;

-- making some changes to be captured by stream
update global
set age = 70
where person_id = 2;

-- Changes captured
select *
from test_stream;



-- TASK

show tasks;

-- Thats how we create tasks to scedule actions.
create or replace task test_task
warehouse = compute_wh
schedule = '5 minute'
as 
select current_date;

-- Making streams and task work together!
create or replace task test_insert_task
warehouse = compute_wh
schedule = '5 minute'
when 
    SYSTEM$STREAM_HAS_DATA('test_stream')
as
    insert into global select * from global where person_id = 2;


drop task if exists test_task;