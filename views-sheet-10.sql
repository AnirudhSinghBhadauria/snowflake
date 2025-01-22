use warehouse compute_wh;
use role accountadmin;
use database food;
use schema core;

-- secure view
create or replace secure view global_view as 
select *
from global 
limit 10;

select *
from global_view;

-- describe view
desc view global_view;

-- force view
create or replace force view test_force_view as 
select * 
from global_force_underlying;

-- list views
show views; -- more information about each view
show terse views; -- less information (columns) about each view 

-- show all the views in existing account
show views in account;

-- show views in particular db or schema
show views in database food;

-- materialized views (can only be created in enterprise edition)
create or replace secure materialized view test_mat_view as
select * 
from global
where age > 70;

-- creating view on stream object
create or replace stream global_stream
on table global;

update global 
set age = 70
where age = 68;

create view global_stream_view as
select *
from global_stream;

select *
from global_stream;