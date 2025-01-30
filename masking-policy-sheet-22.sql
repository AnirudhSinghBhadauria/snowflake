use warehouse compute_wh;
use role accountadmin;
use database food;
use schema core;

create or replace masking policy health_score_masking
as (col number) returns number ->
    case 
        when current_role() = 'accountadmin' then col
        else 0
    end;

alter table global
modify column health_score
set masking policy health_score_masking;

-- this will not work if masking policy is applied to some col,
drop masking policy health_score_masking;

alter table global
modify column health_score
unset masking policy;
