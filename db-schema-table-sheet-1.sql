create database test;
create schema core;

select current_database(), current_schema(), current_role();

-- when we need to list anything!
show databases;
show schemas;

-- we can add comments with query like this!
create schema main
comment = 'this is how you comment in snowflake';

-- Creating table
drop table if exists employee;
create table employee(
    emp_id int autoincrement unique,
    name varchar(255),
    department varchar(255),
    salary decimal(8, 2),
    location varchar(255)
)
comment = 'this is how you create a table';

-- creting transient & temprorary table
create transitent table trans_test (name varchar(255))
create temprorary table temp_test (name varchar(255));

-- describe a table
desc table employee;

-- get the ddl defination of any object
select get_ddl('table', 'employee');

-- inserting values into table
insert into employee
(name, department, salary, location) 
values 
('anirudh', 'Data & Analytics', 20000, 'Hyderabad'),
('shiven', 'Cyber security', 30000, 'Delhi');

-- full name of any table - mydb.myschema.mytable.

-- change the session level timezone and see the result
alter session set timezone = 'America/Los_Angeles';
alter session set timezone = 'Japan';
alter session set timestamp_output_format = 'YYYY-MM-DD HH24:MI:SS.FF';

-- STAGE and how to load data into a stage using commnads only

-- Creating a stage
create stage test_stage;

-- list all files in the stage
list @test_stage;

 -- lets view the data first
create or replace file format my_format type = 'csv' field_delimiter = ',';
select t.$1, t.$2, t.$3,t.$4, t.$5, t.$6 from @my_stg (file_format => 'my_format') t; 

-- now we can use copy command to load data

drop table if exists my_stg_table;
create table my_stg_table (
  num number,
  num10_1 number(10,1),
  decimal_20_2 decimal(20,2),
  numeric numeric(30,3),
  int int,
  integer integer
);

-- lets check data
select * from my_stg_table;

-- now load data via copy command
copy into my_stg_table 
from @my_stg;

-- lets check data
select * from my_stg_table;