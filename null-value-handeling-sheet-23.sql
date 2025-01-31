use warehouse compute_wh;
use role accountadmin;
use database food;
use schema core;

/*
We have 3 functions to deal with null values

1. nvl
2. nvl2
3. coalesce
*/


/*
Using nvl function - takes 2 Args - gives us the first non-null value

    nvl(100, 200) - 100
    nvl(null, 400) - 400
    nvl(null, null) - null
*/

select *
from global
where health_score is null;

update global 
set health_score = null
where health_score < 5;

-- puts the average of health_score wherever health_score is null! (in this select statemnt only)
select *, nvl(
    health_score, 
    (select round(avg(health_score), 0) from global)
) as "Health Score"
from global;

-- nulls still there
select *
from global
limit 10;

-- making the changes permanent
update global
set health_score = nvl(
    health_score, 
    (select round(avg(health_score), 0) from global)
);

-- nulls filled with average health_score!
select *
from global
limit 10;

-- similarly,

/*
Using nvl2 function - takes 3 args - gives us the value next to the first non-null value

    nvl2(100, 200, 300) - 200
    nvl2(null, 400, 500) - 500
    nvl2(null, null, 4) - 4
    nvl2(40, null, 4) - null
    nvl2(null, null, null) - null
*/

-- similarly,

/*
Using coalesce function - takes n Args - gives us the first non-null value

    coalesce(100, 200, 500, 800, 300) - 100
    coalesce(null, 400 .....) - 400
    coalesce(null, null, ... , null) - null
*/