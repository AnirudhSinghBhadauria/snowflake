use warehouse compute_wh;
use role accountadmin;
use database food;
use schema core;


-- Creating an external stage with S3
create or replace stage game_sales
url = 's3://pipe-auto-ingest-test/history/'
credentials = (
    aws_key_id = '' 
    aws_secret_key = ''
);
  
desc stage game_sales;

-- lists all the file in the given s3 location!
list @game_sales;

-- creating a file format for reading the csv
CREATE OR REPLACE FILE FORMAT game_sales_ff
TYPE = 'CSV'
COMPRESSION = 'NONE'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
RECORD_DELIMITER = '\n'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('\\N', '')
TRIM_SPACE = TRUE
EMPTY_FIELD_AS_NULL = TRUE;

-- Querying data in the stage without creating the table
select 
$1 as Rank, $2
from @game_sales (file_format => 'game_sales_ff');

-- create external table for sales data,
CREATE OR REPLACE EXTERNAL TABLE game_sales (
    Rank INTEGER AS (CASE WHEN value:c1 = 'N/A' THEN NULL ELSE TO_NUMBER(value:c1) END),
    Name VARCHAR(200) AS (value:c2::VARCHAR(200)),
    Platform VARCHAR(50) AS (value:c3::VARCHAR(50)),
    Year INTEGER AS (CASE WHEN value:c4 = 'N/A' THEN NULL ELSE TO_NUMBER(value:c4) END),
    Genre VARCHAR(50) AS (value:c5::VARCHAR(50)),
    Publisher VARCHAR(50) AS (value:c6::VARCHAR(50)),
    NA_Sales FLOAT AS (CASE WHEN value:c7 = 'N/A' THEN NULL ELSE TO_DOUBLE(value:c7) END),
    EU_Sales FLOAT AS (CASE WHEN value:c8 = 'N/A' THEN NULL ELSE TO_DOUBLE(value:c8) END),
    JP_Sales FLOAT AS (CASE WHEN value:c9 = 'N/A' THEN NULL ELSE TO_DOUBLE(value:c9) END),
    Other_Sales FLOAT AS (CASE WHEN value:c10 = 'N/A' THEN NULL ELSE TO_DOUBLE(value:c10) END),
    Global_Sales FLOAT AS (CASE WHEN value:c11 = 'N/A' THEN NULL ELSE TO_DOUBLE(value:c11) END)
)
WITH LOCATION = @game_sales
AUTO_REFRESH = FALSE
FILE_FORMAT = (FORMAT_NAME = game_sales_ff);

-- Fetching out data from the external stage
select *
from game_sales;

-- selecting file name of the file in s3
select metadata$filename 
from game_sales;

-- selecting all the rows coming from a particular file
select *
from game_sales
where metadata$filename = '<name-of-file>';

-- list the features of the stage
desc external table game_sales type = 'column';
desc external table game_sales type = 'stage';





-- EXTERNAL TABLE MAPPING FOR AUTO INGEST

-- create folder in the csv in which delta fiels will be added.
-- Here this delta folder is the folder where files will get added, s3://pipe-auto-ingest-test/delta/

-- Creating the external stage to ingest data from
create or replace stage game_sales_delta
url = 's3://pipe-auto-ingest-test/delta/'
credentials = (
    aws_key_id = '' 
    aws_secret_key = ''
);

list @game_sales_delta;

-- create table for all the data to be saved
CREATE OR REPLACE EXTERNAL TABLE game_sales_base (
    Rank INTEGER AS (CASE WHEN value:c1 = 'N/A' THEN NULL ELSE TO_NUMBER(value:c1) END),
    Name VARCHAR(200) AS (value:c2::VARCHAR(200)),
    Platform VARCHAR(50) AS (value:c3::VARCHAR(50)),
    Year INTEGER AS (CASE WHEN value:c4 = 'N/A' THEN NULL ELSE TO_NUMBER(value:c4) END),
    Genre VARCHAR(50) AS (value:c5::VARCHAR(50)),
    Publisher VARCHAR(50) AS (value:c6::VARCHAR(50)),
    NA_Sales FLOAT AS (CASE WHEN value:c7 = 'N/A' THEN NULL ELSE TO_DOUBLE(value:c7) END),
    EU_Sales FLOAT AS (CASE WHEN value:c8 = 'N/A' THEN NULL ELSE TO_DOUBLE(value:c8) END),
    JP_Sales FLOAT AS (CASE WHEN value:c9 = 'N/A' THEN NULL ELSE TO_DOUBLE(value:c9) END),
    Other_Sales FLOAT AS (CASE WHEN value:c10 = 'N/A' THEN NULL ELSE TO_DOUBLE(value:c10) END),
    Global_Sales FLOAT AS (CASE WHEN value:c11 = 'N/A' THEN NULL ELSE TO_DOUBLE(value:c11) END)
)
WITH LOCATION = @game_sales_delta
AUTO_REFRESH = TRUE
FILE_FORMAT = (FORMAT_NAME = game_sales_ff);

show external tables; // copy arn for this game_sale_base table and give it in the s3 notifications.

select count(*)
from game_sales_base;

select *
from game_sales_base;

-- give us the informations about the files associated with a particular external table
select * 
from table(
    information_schema.EXTERNAL_TABLE_FILES(
        table_name=>'game_sales_base'
    )
);

-- check the queue and the pending files
select system$external_table_pipe_status('game_sales_base');

list @game_sales_delta;


-- working with parquet files, 
-- https://data-engineering-simplified.medium.com/work-with-external-table-in-snowflake-b86714f5a6df