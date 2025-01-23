use warehouse compute_wh;
use role accountadmin;
use database food;
use schema core;

-- creating task
create or replace task give_timestamp
warehouse = compute_wh
schedule = '1 minute'
as select current_timestamp();

drop task give_timestamp;

-- using cron jobs
create or replace task test_cron_task
warehouse = compute_wh
schedule = 'USING CRON */1 * * * * UTC'
as
    insert into core_employee (name, department, salary, location) 
    values
    ('anirudh', 'Marketing', 70000, 'Mumbai');

-- Starts the task,
ALTER TASK test_cron_task resume;
alter task test_cron_task suspend;

-- other operations on tasks
ALTER TASK task SUSPEND;
ALTER TASK task UNSET SCHEDULE;
ALTER TASK task RESUME;

-- forcefully executing a task,
execute task test_cron_task;

select count(*) as "Total rows"
from core_employee;

-- get taks history for a particular task,
select *
from table(information_schema.task_history())
where name = 'TEST_CRON_TASK'
order by query_start_time desc;

select * 
from table(information_schema.SERVERLESS_TASK_HISTORY())
where name = 'TEST_CRON_TASK'
order by query_start_time desc;


-- using a autoscalable serverless setup for tasks
create or replace task test_serverless_task
user_task_managed_initial_warehouse_size = 'XSMALL'
schedule = 'USING CRON */1 * * * * UTC'
as
    insert into core_employee (name, department, salary, location) 
    values
    ('anirudh', 'Marketing', 70000, 'Mumbai');

alter task test_serverless_task resume;
alter task test_serverless_task suspend;

desc task test_serverless_task;


-- CREATING A WORKFLOW USING TASK TREE



create or replace task parent_task
user_task_managed_initial_warehouse_size = 'XSMALL'
schedule = 'USING CRON */1 * * * * UTC'
as
    insert into core_employee (name, department, salary, location) 
    values
    ('parent', 'Marketing', 560000, 'Mumbai');

create or replace task first_child_task
user_task_managed_initial_warehouse_size = 'XSMALL'
after parent_task
as
    insert into core_employee (name, department, salary, location) 
    values
    ('first_child', 'Marketing', 550000, 'Mumbai');

create or replace task second_child_task
user_task_managed_initial_warehouse_size = 'XSMALL'
after parent_task
as
    insert into core_employee (name, department, salary, location) 
    values
    ('second_child', 'Marketing', 540000, 'Mumbai');

create or replace task third_child_task
user_task_managed_initial_warehouse_size = 'XSMALL'
after parent_task
as
    insert into core_employee (name, department, salary, location) 
    values
    ('third_child', 'Marketing', 530000, 'Mumbai');

-- now first resume the child tasks and the the parent task,
alter task parent_task resume;
alter task first_child_task resume;
alter task second_child_task resume;
alter task third_child_task resume;

alter task parent_task suspend;
alter task first_child_task suspend;
alter task second_child_task suspend;
alter task third_child_task suspend;

select *
from table(information_schema.task_history())
where name = 'SECOND_CHILD_TASK'
order by query_start_time desc;

select count(*) as "Total rows"
from core_employee;

select *
from core_employee;