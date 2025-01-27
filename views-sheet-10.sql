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


-- creating materialized views,
create materialized view global_mat_view
as select person_id, spice_level, health_score, health_impact
from global;

-- cannot perform,

/*

1. order by
2. limit
3. cannot perform any kind of joins
4. cannot query another materialized view in this
5. cannot perform dml
6. cannot truncate it

*/

-- CREATING A SECURE VIEW
create secure view global_secure_view
as select person_id, spice_level, health_score, health_impact
from global;


select *
from global_secure_view;

select get_ddl('table', 'global_secure_view');
show views like '%secure_view';

-- only a secure view can be shared using data sharing feature of snowflake