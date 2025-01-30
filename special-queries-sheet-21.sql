show parameters;
alter session set timezone = 'Asia/Kolkata';

select current_timestamp();

-- To check if a table is clone or not
select *
from information_schema.tables
where table_name like 'GLOBAL_%';

select *
from snowflake.account_usage.table_storage_metrics
where table_name like 'GLOBAL_%';

-- check copy history
select *
from table(
    information_schema.copy_history(
        TABLE_NAME=>'game_sales', START_TIME => '2025-01-28 09:00:00'::TIMESTAMP
    )
);