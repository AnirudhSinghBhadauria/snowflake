use warehouse compute_wh;
use role accountadmin;
use database food;
use schema core;


create or replace stage game_sales_delta
url = 's3://pipe-auto-ingest-test/delta/'
credentials = (
    aws_key_id = '' 
    aws_secret_key = ''
)
;

create or replace file format game_sales_ff
type = 'csv'
compression = 'none'
field_delimiter = ','
record_delimiter = '\n'
skip_header = 1
field_optionally_enclosed_by = '"'
trim_space = true
skip_blank_lines = true
;

create or replace external table game_sales (
    rank number as (value:c1::number),
    name varchar as (value:c2::varchar),
    platform varchar as (value:c3::varchar),
    year number as (value:c4::number),
    genre varchar as (value:c5::varchar),
    publisher varchar as (value:c6::varchar),
    na_sales number as (value:c7::number),
    eu_sales number as (value:c8::number),
    jp_sales number as (value:c9::number),
    other_sales number as (value:c10::number),
    global_sales number as (value:c11::number)
)
with location = @game_sales_delta
auto_refresh = true
file_format = (format_name = game_sales_ff)
;

-- copy arn paste in sqs queue in s3 notifications
show external tables like 'game_sales';

select system$external_table_pipe_status('game_sales');

select *
from game_sales;

list @game_sales_delta;

-- check copy history
select *
from table(
    information_schema.copy_history(
        TABLE_NAME=>'game_sales', START_TIME => '2025-01-28 09:00:00'::TIMESTAMP
    )
)
;