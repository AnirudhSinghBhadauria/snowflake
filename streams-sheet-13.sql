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

-- checks if stream has data
select system$stream_has_data('name of stream');

-- MERGE statement in streams

-- in this way we can update a target table using streams and changes made to the source table...

merge into target_table using source_table_stream
on target_table.name = source_table_stream.name
when matched
    and METADATA$ACTION = 'INSERT'
    and METADATA$ISUPDATE = 'TRUE'
then UPDATE
set target_table.weight = source_table_stream.weight
--        ... other columns to set
;   

-- Same action for delete statement
merge into target table using source_table_stream
on target_table.name = source_table_stream.name
when matched
    and METADATA$ACTION = 'DELETE'
    and METADATA$ISUPDATE = 'FALSE'
then DELETE;


-- perfoming merge on all the DML operations;

merge into target table using source_table_stream
on target_table.name = source_table_stream.name

WHEN MATCHED
    AND source_table_stream.METADATA$ACTION = 'DELETE' 
    AND source_table_stream.METADATASI SUPDATE = 'FALSE' 
THEN DELETE

WHEN MATCHED
AND source_table_stream.METADATA$ACTION = 'INSERT'
AND source_table_stream.METADATA$ISUPDATE = 'TRUE' I THEN UPDATE
SET 
    target.CITY = source_table_stream.CITY, 
    target.state = source_table_stream.state,
    target.country= source_table_stream.country

WHEN NOT MATCHED
AND source_table_stream.METADATASACTION = 'INSERT'
THEN INSERT
(NAME, WEIGHT, CITY, STATE, COUNTRY)
VALUES (
        source_table_stream.NAME, 
        source_table_stream.WEIGHT, 
        source_table_stream.CITY, 
        source_table_stream.STATE, 
        source_table_stream.COUNTRY
);


