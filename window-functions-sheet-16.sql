use warehouse compute_wh;
use role accountadmin;
use database food;
use schema core;
-- Window functions - functions that only apply to subset of rows, or a single partition.
select
    *
from
    global
limit
    10;
-- row number,
select
    *,
    row_number() over(
        order by
            person_id
    ) as "Row number"
from
    global;
-- rank (ranks records with skips)
select
    rank() over(
        partition by age
        order by
            health_score desc
    ) as "Rank by HEalth Score",
    *
from
    global;
-- dense rank (ranks records without skip)
select
    dense_rank() over(
        partition by age
        order by
            health_score desc
    ) as "Rank by Health Score",
    *
from
    global;
-- ntile (divides records into n number of groups)
select
    ntile(4) over(
        partition by age
        order by
            health_score
    ) as "group of 4",
    *
from
    global;
-- lead / lag
select
    *,
    lag(health_score, 1) over(
        order by
            age desc
    ) as "healthScore_of_pervious_person"
from
    global;
select
    *,
    lead(health_score, 1) over(
        order by
            age desc
    ) as "healthScore_of_pervious_person"
from
    global;
-- first vlaue / last value
select
    *,
    first_value(health_score) over(
        partition by age
        order by
            health_score desc
    ) as "Highest Health score for this age"
from
    global;
select
    *,
    last_value(health_score) over(
        partition by age
        order by
            health_score asc
    ) as "Highest Health score for this age"
from
    global;
-- Working with SALES DATA,
select
    *
from
    sales_data;
-- Rank sales reps within each region
select
    *,
    row_number() over(
        partition by region
        order by
            sale_amount desc
    ) as "Rank for sales in this category"
from
    sales_data;
-- Rank sale amounts with potential ties
select
    *,
    dense_rank() over(
        order by
            sale_amount desc
    ) as "Rank for sales"
from
    sales_data;
-- Previous day's sales comparison
select
    *,
    lag(sale_amount, 1) over(
        order by
            sale_date
    ) as "Previous Day Sales",
    (
        round(
            (sale_amount - "Previous Day Sales") / (sale_amount),
            2
        ) * 100
    ) as "Growth from previous day (%)"
from
    sales_data;

-- Highest sale amount for each category
select *,
first_value(sale_amount) over(
    partition by product_category order by sale_amount desc
) as "Highest sale in this category"
from sales_data;

-- Dividing sales reps into 4 performance groups
select *,
ntile(4) over(order by sale_amount desc) "Groups wrt sales"
from sales_data;

-- Cumulative Sum of Sales by Category
select *,
sum(sale_amount) over(
    partition by product_category order by sale_date
) as "Cummulative sum of sales"
from sales_data;

-- timestamps
select current_date(), current_timestamp(), current_time();






















    