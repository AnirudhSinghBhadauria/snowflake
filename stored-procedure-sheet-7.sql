use warehouse compute_wh;
use role accountadmin;
use database food;
use schema core;

-- basic structure of stored procedure in snowflake,
create or replace procedure test_procedure(age number)
returns number
language sql
as 
// this dollar sign notation in not req. in snowsight,
    $$
        begin
            return age;
        end;
    $$
;

call test_procedure(50);

-- procedure with some optonal keywords
create or replace procedure test_optional_procedure(age number)
returns number
language sql
strict
execute as owner
as 
    begin
        return age;
    end
;

call test_optional_procedure(90);

show procedures;

-- we must give the input type while describing the procedure.
desc procedure test_procedure(number);

-- SOME IMP PARAMETERS
create or replace procedure test_parameters(age number)
returns number
language sql
returns NULL on NULL input // strict, called on null input
comment = 'this is the procedure'
execute as owner
as
    begin 
        return age;
    end
;

call test_parameters(NULL);

-- volatile and immutable parameters,
create procedure test_volatile_immutable(name string)
returns string
language sql
immutable // volatile
comment = 'this is the thing!'
as 
    begin
        return name;
    end
;

call test_volatile_immutable('anirudh');

-- create procedure test_final_procedure()
create or replace procedure test_final_procedure(likecount number)
returns table(
    author varchar,
    like_count number,
    polarity varchar
)
language sql
called on null input
immutable
comment = 'fetches the top 10 most liked comments'
as 
    declare
    result_set resultset default (
        select author, like_count, polarity
        from yt_comments
        where like_count > :likecount
        order by like_count desc
        limit 10
    );
    begin
        return table (result_set);
    end
;

call test_final_procedure(500);


-- EXCEPTION HANDELING IN STORED PROCEDURES
create or replace procedure test_exception(ageuserinput number)
returns table (
    person_id number,
    age number,
    diet_type varchar,
    primary_cuisine varchar
)
language sql
called on null input
execute as owner
as
declare 
    result_set resultset default (
        select person_id, age, diet_type, primary_cuisine
        from global
        where age > :ageuserinput
    );
    age_exception exception(
        -20001, 
        'Age must be greater than 0'
    );
begin
    if (:ageuserinput < 0) then
        raise age_exception;
    end if;
        
    return table(result_set);
end;

call test_exception(-1);