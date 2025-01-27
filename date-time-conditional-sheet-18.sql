use warehouse compute_wh;
use role accountadmin;
use database food;
use schema core;

-- CONSTRUCTION

select 
to_date('2025-01-01'), 
to_time('10:10:00'), 
to_timestamp('2025-01-01 10:10:00');

select 
    '2025-01-01 10:00:00'::TIMESTAMP; // Same for date & time,

-- Creating date from PARTS
select date_from_parts(2025, 01, 01);

/*
This can also work like this (with a range)

100th day (from January 1, 2010)
24 months (from January 1, 2010)
*/

SELECT 
DATE_FROM_PARTS(2010, 1, 100), 
DATE_FROM_PARTS(2010, 1 + 24, 1);

-- similarly theres time from parts,
select time_from_parts(10, 56, 56);

/*
Components outside normal ranges:

100th minute (from midnight)
12345 seconds (from noon)
*/

select 
time_from_parts(0, 100, 0), 
time_from_parts(12, 0, 12345);

-- Timestamp from parts
-- TIMESTAMP_FROM_PARTS( <year>, <month>, <day>, <hour>, <minute>, <second> )

select timestamp_from_parts(2025, 01, 01, 12, 40, 00);

-- EXTREACTION

-- Extracting year, day, month, minute, seconds, hour out of a timestamp,
select to_timestamp(
    '2024-04-08T23:39:20.123-07:00') AS "Timestamp",
    DATE_PART(year, "Timestamp") AS "Extracted value"; 

-- selecting day name from a date,
SELECT dayname(TO_DATE('2024-04-01')) AS DAY;

-- using EXTRACT
SELECT EXTRACT(dayofweek FROM TO_TIMESTAMP('2024-04-10T23:39:20.123-07:00'));
SELECT EXTRACT(dayofmonth FROM TO_TIMESTAMP('2024-04-10T23:39:20.123-07:00'));
SELECT EXTRACT(hour FROM TO_TIMESTAMP('2024-04-10T23:39:20.123-07:00'));

-- Extracting hour, minute, second from timestamp
SELECT '2013-05-08T23:39:20.123-07:00'::TIMESTAMP AS TSTAMP,
HOUR(tstamp) AS "HOUR",
MINUTE(tstamp) AS "MINUTE",
SECOND(tstamp) AS "SECOND";

-- Return the LAST DAY OF THE MONTH for the specified date (from a timestamp):
SELECT TO_DATE('2015-05-08T23:39:20.123-07:00') AS "DATE",
LAST_DAY("DATE") AS "LAST DAY OF MONTH";

-- Return the LAST DAY OF THE YEAR for the specified date (from a timestamp):
SELECT TO_DATE('2015-05-08T23:39:20.123-07:00') AS "DATE",
       LAST_DAY("DATE", 'year') AS "LAST DAY OF YEAR";

-- Select monthname from the timestamp, 
SELECT MONTHNAME(TO_TIMESTAMP('2015-04-03 10:00')) AS MONTH;

-- Returns the date of the first specified day of week that occurs after the input date.
SELECT CURRENT_DATE() AS "Today's Date",
NEXT_DAY("Today's Date", 'Friday') AS "Next Friday";

SELECT CURRENT_DATE() AS "Today's Date",
previous_day("Today's Date", 'Friday') AS "Last Friday";

/*
YEAR( <date_or_timestamp_expr> )

YEAROFWEEK( <date_or_timestamp_expr> )
YEAROFWEEKISO( <date_or_timestamp_expr> )

DAY( <date_or_timestamp_expr> )

DAYOFMONTH( <date_or_timestamp_expr> )
DAYOFWEEK( <date_or_timestamp_expr> )
DAYOFWEEKISO( <date_or_timestamp_expr> )
DAYOFYEAR( <date_or_timestamp_expr> )

WEEK( <date_or_timestamp_expr> )

WEEKOFYEAR( <date_or_timestamp_expr> )
WEEKISO( <date_or_timestamp_expr> )

MONTH( <date_or_timestamp_expr> )

QUARTER( <date_or_timestamp_expr> )
*/

select year(to_timestamp('2025-01-01 10:00:00')) as Year;
SELECT 
       '2013-05-08T23:39:20.123-07:00'::TIMESTAMP AS tstamp,
       YEAR(tstamp) AS "YEAR", 
       QUARTER(tstamp) AS "QUARTER OF YEAR",
       MONTH(tstamp) AS "MONTH", 
       DAY(tstamp) AS "DAY",
       DAYOFMONTH(tstamp) AS "DAY OF MONTH",
       DAYOFYEAR(tstamp) AS "DAY OF YEAR",
       WEEK(tstamp) AS "WEEK",
       WEEKISO(tstamp) AS "WEEK ISO",
       WEEKOFYEAR(tstamp) AS "WEEK OF YEAR",
       YEAROFWEEK(tstamp) AS "YEAR OF WEEK",
       YEAROFWEEKISO(tstamp) AS "YEAR OF WEEK ISO"
;

-- ADDITION & SUBSTRACTION,

-- Add months (added 2 months here)
select add_months(to_date('2025-01-01'), 2);

-- Add years to a date:
SELECT TO_DATE('2022-05-08') AS original_date,
       DATEADD(year, 2, TO_DATE('2022-05-08')) AS date_plus_two_years;

-- Finding difference of months between two dates,
select DATEDIFF(month, '2021-01-01'::DATE, current_date());
select MONTHS_BETWEEN('2025-01-01'::DATE, add_months(current_date(), 10));

-- finding the time difference between two dates
SELECT TIMEDIFF(YEAR, '2017-01-01', '2019-01-01') AS Years;
SELECT TIMEDIFF(MONTH, '2017-01-01', '2019-01-01') AS Months;

-- SIMILARLY timestamp add and difference,


-- CONVERSION

/*
TRY_TO_DATE (similarly TRY_TO_TIME, TRY_TO_TIMESTAMP)

A special version of the TO_DATE function that performs the same operation (i.e. converts an input expression to a date), but with error-handling support (i.e. if the conversion cannot be performed, it returns a NULL value instead of raising an error).
*/

select try_to_date('2025-400-01'); // returns null (instead of error)
select try_to_date('2025-01-01');


-- 007E04127E8C67800074A4E79E4C59043B4D1F71FC0F7A4CAC83CA32A6A32228A94719
select encrypt('anirudh', 'poiuqewjlkfsd');
select decrypt_raw(
    '007E04127E8C67800074A4E79E4C59043B4D1F71FC0F7A4CAC83CA32A6A32228A94719',           
    'poiuqewjlkfsd'
);