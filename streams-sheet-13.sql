use warehouse compute_wh;
use role accountadmin;
use database food;
use schema core;

select *
from yt_comments
limit 10;

drop stream yt_comments_stream;

create stream yt_comments_stream 
on table yt_comments;

update yt_comments
set polarity = 'positive'
where polarity = 'POS';

select *
from yt_comments_stream;

select *
from yt_comments
at (offset => -60 * 2);


select 
    round(avg(positive), 2) as "Average Positivity Score",
    round(avg(neutral), 2) as "Average Neutral Score",
    round(avg(negative), 2) as "Average Negativity Score"
from yt_comments;

select count(*) from yt_comments;

select *
from yt_comments_stream;


-- append only stream - will only insert capture data
create or replace stream global_append_only_stream 
on table global
append_only = True;


select * 
from global_append_only_stream;

-- streams call be set for all kinds of tables,

create external table done(
    name varchar
);