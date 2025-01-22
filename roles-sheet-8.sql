use warehouse compute_wh;
use role accountadmin;
use database food;
use schema core;

select current_role();

-- creating roles
create or replace role project_lead;
create or replace role data_engineer;
create or replace role data_anayst;


-- creating user
create user anirudh
password = 'anirudhone'
must_change_password = false;

create user shiven
password = 'shivenone'
must_change_password = false;

show users;

-- granting role to user
grant role data_engineer to user anirudh;
grant role data_anayst to user shiven;


alter user anirudh set default_role = data_engineer;


-- granting privilages of one role to another,

-- now securityadmin can do everything that data_enginner can do,
grant role data_enginner to role securityadmin; 
-- revoke role accountadmin from role data_engineer;

-- YOU GRANT PRPRIVILEGES TO ROLES, 

-- granting different privilages to different roles,
grant usage on database food to role data_engineer;
grant usage on warehouse compute_wh to role data_engineer;

grant usage on database food to role data_anayst;
grant usage on warehouse compute_wh to role data_anayst;

grant all privileges on schema core to role data_engineer;
grant all privileges on schema public to role data_engineer; 

grant all privileges on schema public to role data_anayst;
grant all privileges on schema core to role data_anayst;

grant select on all tables in schema core to role data_engineer;
grant select on all tables in schema public to role data_engineer;

grant select on all tables in schema public to role data_anayst;

grant all privileges 
on all procedures in schema food.core 
to role data_engineer;

grant usage
on procedure test_final_procedure() 
to role data_anayst;

/*
    Now open the login page of snowflake
    and login as user 'anirudh' with the password!

    It will only have the privilages of role 'data_engineer'
*/


-- describing user and grants
describe user anirudh;
show grants to user anirudh;


/*
    REAL LIFE USE CASE FOR ROLES
    
    Role Hiraricy - 
    https://lh3.googleusercontent.com/d/1fxeOF9CyeaDbp7yuI8zmpD8G6lR8TARu
*/

create or replace role DE_PM;
create or replace role DE_DEV;
create or replace role DE_ANALYST;
create or replace role DE_QA;

grant role DE_PM to role securityadmin;
grant role DE_DEV to role DE_PM;
grant role DE_ANALYST to role DE_PM;
grant role DE_QA to role DE_ANALYST;