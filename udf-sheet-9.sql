use warehouse compute_wh;
use role accountadmin;
use database food;
use schema core;

-- creating a function

-- fucntion returning a scalar
create or replace function calculate_profit(
    retail number, 
    purchase number, 
    sold_quantity number
)
returns number
as 
$$
    ((retail - purchase) * sold_quantity)
$$
;

select calculate_profit(100, 50, 60);

-- returning a table from udf
create or replace function test_table_udf()
returns table (
    age number
)
as 
$$
    select age
    from global
    limit 10
$$;

select * 
from table(test_table_udf());

-- show functions
show functions;
show functions in database food;
show functions in schema food.core;
show functions like 'test_%';

-- describe functions
desc function test_table_udf();


-- creating a secure udf
create or replace secure function secure_function(
    retail number, 
    purchase number, 
    sold_quantity number
)
returns number
not null
as 
$$
    ((retail - purchase) * sold_quantity)
$$
;

select secure_function(100, 50, 60) as profit;