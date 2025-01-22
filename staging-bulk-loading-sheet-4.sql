use warehouse compute_wh;
use role accountadmin;
use database food;
use schema core;

-- accessing stages @~ for user stage, @% for table stage, @<stage-name> for named stages

-- listing user stage
list @~;

-- listing table stages
list @%global;

-- named stages - internal and external stages
show stages;

-- list all the files in the stage
list @test_stage;

-- remove files stage
-- remove @~<file_name>;

-- putting file from local machine into a user stage
-- put file://<filepath-in-local-machine> @~<foldername> auto_compress=false;

-- listing files in stage with a pattern (just like the `like` statement)
list @~/stocks pattern = '.*.csv';


-- STEPS TO LOAD DATA FROM STAGE TO TABLE

-- step 1 - create the table you want the data in,
-- step 2 - crate appropriate fileformat
-- step 3 - use copy into commnad to load data into the table

drop table stock_price_data;
CREATE OR REPLACE TABLE stock_price_data (
    date varchar(255) NOT NULL,
    ticker STRING NOT NULL,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume INTEGER,
    dividends FLOAT,
    stock_splits FLOAT
);

create or replace file format stock_data_format
type = 'csv'
skip_header = 1;

copy into stock_price_data
from @~/stocks/all_stock_data.csv
file_format = (
    format_name = 'stock_data_format',
    error_on_column_count_mismatch = false
);

desc table stock_price_data;

-- Query data directly form stage only without loading data into table
select
t.$1
from @~/stocks/all_stock_data.csv (file_format => 'stock_data_format') t;

-- parquet file format only has $1 column.
-- the copy into command is same for user/ table / named stages

-- creating a stage with a file format associated,
create or replace stage stock_stage
file_format = 'stock_data_format';

-- if we have craeted a stage with file format, we dont need to give ff again in copy into commnad.
show stages;

-- one more variation of copyinto command

-- shows top 10 rows from the file,
copy into <table-name>
from @test_parquet/home/
files = (<filename.csv>)
validation_mode = 'RETURN_10_ROWS';



-- PROCESSING PARQUET FILE
create or replace file format comments_file_format
type = 'parquet';

create or replace stage test_parquet
file_format = 'comments_file_format';

select 
$1:author::string as "Author name"
from @test_parquet/commnets.parquet;

CREATE OR REPLACE TABLE yt_comments (
    author STRING,              
    updated_at TIMESTAMP,      
    like_count INTEGER,         
    text STRING,                
    negative FLOAT,             
    neutral FLOAT,              
    positive FLOAT,             
    polarity STRING             
);

copy into yt_comments from (
    SELECT 
    $1:author::STRING,
    $1:updated_at::TIMESTAMP,
    $1:like_count::INTEGER,
    $1:text::STRING,
    $1:negative::FLOAT,
    $1:neutral::FLOAT,
    $1:positive::FLOAT,
    $1:polarity::STRING
    FROM @test_parquet/commnets.parquet
)
file_format = (type = 'parquet', compression = 'none');

select *
from yt_comments
limit 10;


-- Copying into table from stage with file name patterns

-- copy into ti from @%t1/region/state/city/2021/06/01/11/
-- files=('mydata1.csv', 'mydata2.csv');

-- copy into ti from @%t1/region/state/city/2021/2016/06/01/11/ 
-- pattern='.*mydata[^[0-9]{1,3}$$].csv';

-- copy into people_data from @%people_data/data1/
-- pattern='.*person_data[^0-91,3}$$].csv';





-- MONITOR ACCOUNT USAGE

-- copy history table
select * 
from "SNOWFLAKE"."ACCOUNT_USAGE"."COPY_HISTORY";

-- load history table
select *
from "SNOWFLAKE"."ACCOUNT_USAGE"."LOAD_HISTORY";

-- load storage usage of named stages
select *
from "SNOWFLAKE"."ACCOUNT_USAGE"."STAGES";