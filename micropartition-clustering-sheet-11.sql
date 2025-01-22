use warehouse compute_wh;
use role accountadmin;
use database food;
use schema core;


-- creating a table with cluser key
create or replace table test_cluster(
    id number primary key autoincrement,
    name varchar,
    salary number,
    department varchar
)
cluster by (department);

-- without cluster took 27s for 34.6M rows with x-small
select *
from stock_price_data;

-- altering an existing table to create clusters
alter table stock_price_data
cluster by (ticker);

desc table stock_price_data;
show tables like 'stock%';

-- without cluster took 28s for 34.6M rows with x-small
select *
from stock_price_data;

-- Drop clustering key in stock_price_data;
alter table stock_price_data 
drop clustering key;


-- Enable and disable automatic reclustering for a table

-- if you want to disable automatic reclustering
alter table 
stock_price_data suspend recluster;

-- if you want to enable automatic reclustering
alter table 
stock_price_data resume recluster;

-- finding out clustering depth for any clustered table
select system$clustering_depth('stock_price_data') as "Clustering depth";

/* 
    Listing out the cluster information of a table.
    (gives us all the information about our clustering - best function!)
*/
select system$clustering_information(
    'stock_price_data', 
    '(ticker)'
);