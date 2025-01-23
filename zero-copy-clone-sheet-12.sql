use warehouse compute_wh;
use role accountadmin;
use database food;
use schema core;

/*
    WE CAN CLONE -

    databases, schemas, table, views, sterams,
    stage, ff
*/

-- Creating a zero copy clone of this global table,
create or replace table global_clone 
clone global;

-- similary we can create clone of any snowflake object

-- To check if a table is clone or not
select *
from information_schema.tables
where table_name like 'GLOBAL_%';

select *
from snowflake.account_usage.table_storage_metrics
where table_name like 'GLOBAL_%';


create or replace pipe test_pipe_clone clone TEST_PIPE;


create or replace view global_test_view
as select * from global;

create or replace view global_test_view_clone clone global_test_view;