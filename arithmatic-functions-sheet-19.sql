use warehouse compute_wh;
use role accountadmin;
use database food;
use schema core;

select 90 / 35;
select abs(-1.07895);
select round(1.6789, 2);
select sqrt(2);
select square(45);
select pow(2, 5);
select floor(1.78968);
select ceil(2.76678);
select factorial(3);

select 
    product_category, 
    round(avg(sale_amount), 2) as "Average Sales Amount"
from sales_data
group by product_category;

select corr(age, health_score) as "Correlation"
from global
;

select 
    product_category as "Category", 
    max(sale_amount) as "Max sales amount"
    from sales_data 
    group by "Category"
    order by "Max sales amount" desc
;

select *
    from sales_data
    where sale_amount between 1000 and 2000
;

-------------------- CONDITIONALS 
select 
    person_id as "Person ID",
    health_score as "Health Score", 
        case 
            when health_score > 90 then 'Good'
            when health_score between 60 and 90 then 'Moderate'
            else 'Bad'
        end as "Health Status"
from global
;

/*
GREATEST FUNCTION - similarly theres a LEAST FUNCTION too,

+-------+-------+-------+----------+
| COL_1 | COL_2 | COL_3 | GREATEST |
|-------+-------+-------+----------|
|     1 |     2 |     3 |        3 |
|     2 |     4 |    -1 |        4 |
|     3 |     6 |  NULL |     NULL |
+-------+-------+-------+----------+
*/
select greatest(12, 45, 56, 9);


-- single line coiditional expression
create secure view health_status as
    select 
        person_id, 
        iff(health_score > 60, 'Good', 'Bad') as "Health Status"
    from global
;

-- try_cast (cast thing returns null if cannot cast, otherwise return null)
select try_cast('2025-01-01' as DATE);
select try_cast('2025-13-01' as DATE);

-- there are try versions of most of arithmatic / conversion functions available in snowflake

select 
    try_to_date('2025-01-01') as "Current Date";