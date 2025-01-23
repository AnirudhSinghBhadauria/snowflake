use warehouse compute_wh;
use role accountadmin;
use database food;
use schema core;

-- By default, Triggered Tasks run at most every 30 seconds
-- We cannot create a triggered task on an external table,

select * 
from global;

create or replace stream global_spice_stream
on table global;

update global
set salt_intake = 'Moderate'
where salt_intake = 'medium';

-- To create a trigger like we do in sql we will combine streams an tasks!
CREATE TASK global_spice_trigger  
  WAREHOUSE = compute_wh
  WHEN system$stream_has_data('global_spice_stream') 
  AS
    insert into core_employee (name, department, salary, location) 
    values
    ('anirudh', 'Marketing', 70000, 'Mumbai');

-- Now whenever i do any DML operaton on global table a row will be added in core_employee table

desc task global_spice_trigger;
ALTER TASK global_spice_trigger RESUME;
alter task global_spice_trigger suspend;

select count(*)
from core_employee;



-- same trigger with insert only feature,

CREATE OR REPLACE STREAM global_spice_stream 
ON TABLE your_source_table
APPEND_ONLY = TRUE;

CREATE OR REPLACE TASK global_spice_trigger
  WAREHOUSE = compute_wh
  WHEN system$stream_has_data('global_spice_stream')
AS
  INSERT INTO core_employee (name, department, salary, location)
  SELECT name, department, salary, location
  FROM global_spice_stream
  WHERE METADATA$ACTION = 'INSERT';

ALTER TASK global_spice_trigger RESUME;