/* 

To enable table schema evolution, do the following:

If you are creating a new table, set the ENABLE_SCHEMA_EVOLUTION parameter to TRUE when you use the CREATE TABLE command.

For an existing table, modify the table using the ALTER TABLE command and set the ENABLE_SCHEMA_EVOLUTION parameter to TRUE.

Loading data from files evolves the table columns when all of the following are true:

The Snowflake table has the ENABLE_SCHEMA_EVOLUTION parameter set to TRUE.

The COPY INTO <table> statement uses the MATCH_BY_COLUMN_NAME option.

The role used to load the data has the EVOLVE SCHEMA or OWNERSHIP privilege on the table.

Additionally, for schema evolution with CSV, when used with MATCH_BY_COLUMN_NAME and PARSE_HEADER, ERROR_ON_COLUMN_COUNT_MISMATCH must be set to false.

*/

/*
     Loading data from csv using copy into require , 

               MATCH_BY_COLUMN_NAME = TRUE
               PARSE_HEADER = TRUE
               ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
     
     in the copyinto command!
*/   

