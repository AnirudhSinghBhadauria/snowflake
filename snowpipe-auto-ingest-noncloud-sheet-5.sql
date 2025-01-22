use warehouse compute_wh;
use role accountadmin;
use database food;
use schema core;


-- 1 creating the base table that will contain all the data,
CREATE OR REPLACE TABLE comments (
    author STRING,              
    updated_at TIMESTAMP,      
    like_count INTEGER,         
    text STRING,                
    negative FLOAT,             
    neutral FLOAT,              
    positive FLOAT,             
    polarity STRING             
);

-- 2 Crating a file format to accosicate with the internal stage
create or replace file format comment_file_format
type = 'parquet';

-- 3 Created the stage,
create stage comments_stage
file_format = 'comment_file_format';

-- location of historical data of the base table
list @comments_stage/rajshamani/history; 

-- location of delta files to be added in the base table
list @comments_stage/rajshamani/delta; 

-- 4 Loaded initial data into the base table 'comments'
copy into comments from (
    SELECT 
    $1:author::STRING,
    $1:updated_at::TIMESTAMP,
    $1:like_count::INTEGER,
    $1:text::STRING,
    $1:negative::FLOAT,
    $1:neutral::FLOAT,
    $1:positive::FLOAT,
    $1:polarity::STRING
    FROM @comments_stage/rajshamani/history/file-1.parquet
) 
file_format = (type = 'parquet', compression = 'snappy');

select *
from comments
limit 10;

-- 5 Creating the pipe to ingest data
CREATE OR REPLACE PIPE comments_pipe
AS 
COPY INTO comments FROM (
    SELECT 
    $1:author::STRING,
    $1:updated_at::TIMESTAMP,
    $1:like_count::INTEGER,
    $1:text::STRING,
    $1:negative::FLOAT,
    $1:neutral::FLOAT,
    $1:positive::FLOAT,
    $1:polarity::STRING
    FROM @comments_stage/rajshamani/delta/
)
FILE_FORMAT = (FORMAT_NAME = 'comment_file_format');

-- after creating the pipe we have to activate it using
alter pipe comments_pipe refresh;
select SYSTEM$PIPE_STATUS('comments_pipe');

-- 6 load the delta file in the delta location of the same stage
-- 7 Now send notification to pipe from calling api,

-- 8 data from delta file will be added in the table,


-- pause the pipe
alter pipe comments_pipe set pipe_execution_paused = false;


-- after resuming the pipe only the files that match these pattern will get processed!
-- alter pipe my_pipe_10 refresh prefix='/customer_10*' modified_after='2021-11-01T13:56:46-07:00';


-- GETTING MORE INFORMATION ABOUT PIPE

select *
from "SNOWFLAKE"."ACCOUNT_USAGE"."WAREHOUSE_LOAD_HISTORY";

select * 
from "SNOWFLAKE"."ACCOUNT_USAGE"."COPY_HISTORY"
where pipe_name = 'comments_pipe';

select *
from "SNOWFLAKE"."ACCOUNT_USAGE"."METERING_HISTORY" 
where service_type = 'PIPE';
