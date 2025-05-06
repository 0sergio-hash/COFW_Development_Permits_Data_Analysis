--------------------------------------------------------------------------------------------------------------------------------------------------
-- City of Fort Worth public data analysis: Development pemits dataset
-- By Sergio Ramos 
-- linkedin.com/in/sergio-ramos-analyst/
-- Data from: https://data.fortworthtexas.gov/Development-Infrastructure/Development-Permits/quz7-xnsy/about_data 
--------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------
-- Creating table to house COFW Development Permits data 
----------------------------------------------------------------------------

-- DROP TABLE IF EXISTS cofw_development_permits
CREATE TABLE cofw_development_permits (
permit_number     text,                     -- Tried using as primary key, duplicates exist
permit_type       text,
permit_sub_type   text,
permit_category   text,
special_text      text,
work_description  text,
legal_description text,
owner_name        text,
filing_date       date,                     -- Source format: MM/DD/YYYY
status            text,
status_date       timestamp with time zone, -- Adding timezone to this field after import
permit_location   text,                     -- Location data type, lat, long, and address. Parse later
job_value         numeric,
use_type          text,
specific_use      text,
units             text,
square_feet       text
	
);

-- Importing data from CSV
COPY cofw_development_permits
FROM 'C:\...\Development_Permits_20240717.csv'
WITH (FORMAT CSV, HEADER);

SELECT COUNT(*) AS total_records
FROM cofw_development_permits;

----------------------------------------------------------------------------
-- Cleaning data 
----------------------------------------------------------------------------

-- Create backup table 
CREATE TABLE cofw_development_permits_backup
AS SELECT * FROM cofw_development_permits;

-- Check backup
SELECT (SELECT COUNT(*) FROM cofw_development_permits)        AS original -- 1452365
,      (SELECT COUNT(*) FROM cofw_development_permits_backup) AS backup;  -- 1452365

-- Check structure
SELECT permit_number
,      COUNT(*) AS total_records;  -- MAX = 169, MIN = 1

FROM cofw_development_permits
GROUP BY permit_number
ORDER BY total_records DESC
LIMIT 20;

-- Checking 169 record permit 
SELECT * 
FROM cofw_development_permits
WHERE permit_number = 'UFC13-0068';

-- Status is the same for all
SELECT status
,      COUNT(*) 
FROM cofw_development_permits
WHERE permit_number = 'UFC13-0068'
GROUP BY status;

-- Filing date is the same for all
SELECT filing_date
,      COUNT(*) 
FROM cofw_development_permits
WHERE permit_number = 'UFC13-0068'
GROUP BY filing_date;

-- Several have the same legal description
SELECT legal_description
,      COUNT(*) 
FROM cofw_development_permits
WHERE permit_number = 'UFC13-0068'
GROUP BY legal_description;

-- These appear to be 26 duplicate records
SELECT *
FROM cofw_development_permits
WHERE 1 = 1
AND permit_number = 'UFC13-0068'
AND legal_description = 'WEISENBERGER ADDITION BLK   2   LOT   3 & 4';

-- They all have the same hash_val 
SELECT MD5(CAST(cofw_development_permits.* AS TEXT)) AS hash_val
,      COUNT(*)                                      AS total_records
FROM cofw_development_permits
WHERE 1 = 1
AND permit_number = 'UFC13-0068'
AND legal_description = 'WEISENBERGER ADDITION BLK   2   LOT   3 & 4'
GROUP BY hash_val;

-- Checking how long since the last update from the 26 records
SELECT AGE(CAST('2014-02-12 00:00:00-06' AS TIMESTAMP WITH TIME ZONE));
-- 10 years 5 mons 10 days

-- This is likely not an error with appending instead of overwriting the dataset
-- Initilly thought the monthly refreshes were appending but there are too many duplicates
-- for that to be the case here.

-- Using a hash function I see a ton of exaxct duplicates
SELECT MD5(CAST(cofw_development_permits.* AS TEXT)) AS hash_val
,      COUNT(*)                                      AS total_records
FROM cofw_development_permits
GROUP BY hash_val
ORDER BY total_records DESC;

-- Couting total duplicate records in the whole table
WITH duplicates AS

(

SELECT hash_subquery.*
,      ROW_NUMBER() OVER (PARTITION BY hash_value ORDER BY permit_number) AS row_num

FROM 

(

SELECT permits.*
,      MD5(CAST(permits.* AS TEXT)) AS hash_value
FROM cofw_development_permits AS permits
  
) AS hash_subquery
	
)

SELECT COUNT(*) AS total_duplicates  -- 17241
FROM duplicates 
WHERE row_num > 1;

-- 1.19% of records in this table are exact duplicates
SELECT CAST( ROUND( ( CAST(17241 AS NUMERIC) / CAST(1452365 AS NUMERIC) ) * 100, 2 ) AS TEXT)||'%' AS percent_duplicates;

-- Subtract duplicate cont from total = 1435124
SELECT 1452365 - 17241

-- Retruns right # of rows = 1435124
SELECT DISTINCT *
FROM cofw_development_permits;

-- Creating a new table with unique values
CREATE TABLE cofw_development_permits_distinct (LIKE cofw_development_permits);

-- Inserting distinct values to remove duplicates
INSERT INTO cofw_development_permits_distinct
SELECT DISTINCT *
FROM cofw_development_permits;

-- Checking results
SELECT COUNT(*) AS total_rows           -- 1435124
FROM cofw_development_permits_distinct;

-- Dropping source table with duplicates
-- DROP TABLE cofw_development_permits;

-- Renaming de-duplicated table
-- ALTER TABLE cofw_development_permits_distinct
-- RENAME TO cofw_development_permits;

-- Checking results
SELECT COUNT(*) AS total_rows  -- 1435124
FROM cofw_development_permits;

-- Check individual columns for data quality

-- permit_number

-- All permit lengths are 10, 11, or 12 characters 
-- with 10 being the most common appearing in 1432562 rows
SELECT CHAR_LENGTH(permit_number) AS str_len
,      COUNT(*) AS total_records
FROM cofw_development_permits
GROUP BY str_len
ORDER BY str_len DESC;

/*

After calling the data source owner, the leading letters represent the type:
The first two numbers the year, then the numbers after the hyphen are for the sequence #

There is also a difference in legal description between duplciate rows which could 
specify a different lot within the same parcel or vice versa or many pacels to many lots

There can also be one row per owner per lot when a lot has multiple owners

Fort Worth City = lots, Tarrant appraisal district = parcels

I was also given a reference doc with nomenclature info.

*/

SELECT permit_type
,      permit_sub_type
,      (REGEXP_MATCH(permit_number, '([A-Za-z]+).*-'))[1] AS left_chars
,      COUNT(*) AS total_records
FROM cofw_development_permits
GROUP BY permit_type, permit_sub_type, left_chars
ORDER BY permit_type, permit_sub_type;

-- These should all start with 'HCLC' -  all others match reference doc 
SELECT *
FROM cofw_development_permits
WHERE 1 = 1
AND permit_type = 'Design Review'
AND permit_sub_type = 'HCLC'  
AND (REGEXP_MATCH(permit_number, '([A-Za-z]+).*-'))[1] IN ('EST','TMP');

-- Many copies per permit_number
SELECT permit_number
,      COUNT(*) AS total_records
FROM cofw_development_permits
GROUP BY permit_number
ORDER BY total_records DESC
LIMIT 10;

-- permit_type -- Clean, unique values
SELECT DISTINCT permit_type
FROM cofw_development_permits
ORDER BY permit_type;

SELECT *
FROM cofw_development_permits
WHERE 1 = 1
AND permit_sub_type ILIKE 'NA'  --  7283
AND permit_category ILIKE 'NA'; -- Adding this condition, still 7283

-- These values are never NULL - meaning 'NA' is a stand-in for NULL here 
SELECT *
FROM cofw_development_permits
WHERE 1 = 1
AND permit_sub_type IS NULL OR permit_category IS NULL;

-- Updating both columns to replace 'NA' with NULL
START TRANSACTION;

-- 7283 records changed
UPDATE cofw_development_permits
SET permit_sub_type = NULL
WHERE permit_sub_type ILIKE 'NA'

RETURNING *;

-- Commit changes after confirming changes look as intended.
-- COMMIT;

-- Rollback if changes do not look right
-- ROLLBACK;

SELECT COUNT(*) AS total_records  -- 1352178
FROM cofw_development_permits
WHERE 1 = 1
AND permit_category ILIKE 'NA';

SELECT DISTINCT permit_category  -- "NA"
FROM cofw_development_permits
WHERE 1 = 1
AND permit_category ILIKE 'NA';


START TRANSACTION;

-- 1352178 records changed
UPDATE cofw_development_permits
SET permit_category = NULL
WHERE permit_category ILIKE 'NA';

-- Commit changes after confirming changes look as intended.
-- COMMIT;

-- Rollback if changes do not look right
-- ROLLBACK;

-- Final check on these updates confirms they are all good now
SELECT DISTINCT permit_sub_type
,               permit_category
FROM cofw_development_permits;

-- Checking permit_type -- all good
SELECT DISTINCT permit_type
FROM cofw_development_permits;

-- Checking permit_sub_type -- all good
SELECT DISTINCT permit_sub_type
FROM cofw_development_permits;

-- Checking permit_categoty -- all good
SELECT DISTINCT permit_category
FROM cofw_development_permits;

-- Checking special_text
SELECT COUNT(*) AS total_records  -- 1228913 most records are NULL
FROM cofw_development_permits
WHERE special_text IS NULL;


-- A lot of names reappear with different capitalization or spelling
-- The are also about 107 records for "DUPLICATE"
SELECT special_text  
,      COUNT(*) AS total_records
FROM cofw_development_permits
WHERE special_text IS NOT NULL
GROUP BY special_text
ORDER BY total_records DESC;

-- Create a backup column
-- ALTER TABLE cofw_development_permits ADD COLUMN special_text_copy TEXT;

-- Copy over contents from original column
UPDATE cofw_development_permits
SET special_text_copy = special_text;

-- Clean up column 

-- Start by standardizing case, and removing leading and trailing white space
UPDATE cofw_development_permits
SET special_text_copy = TRIM(UPPER(special_text_copy));

-- Check results so far
-- You still have cases like "METRO CODE", "METRO CODE", and "METRO CODE ANALYSIS"
-- being seperated. Not sure if analysis is any different, but the other two are likely the same
SELECT special_text_copy  
,      COUNT(*) AS total_records
FROM cofw_development_permits
WHERE special_text_copy IS NOT NULL
GROUP BY special_text_copy
ORDER BY total_records DESC;

-- Standardize spaces, replacing any instance of one or more spaces, with a single space
UPDATE cofw_development_permits
SET special_text_copy = REGEXP_REPLACE(special_text_copy, '\s+', ' ', 'g');

-- Comparing differences in names with spaces in the middle (1190 rows)
WITH special_txt_spaces AS

(
-- Text without spaces removed
SELECT special_text_copy
,      COUNT(*) AS total_records 
FROM cofw_development_permits
WHERE special_text_copy IS NOT NULL
GROUP BY special_text_copy
)

,

special_txt_no_spaces AS

(
-- Text with spaces removed
SELECT REPLACE(special_text_copy, ' ', '') AS special_text_copy_trimmed
,      COUNT(*) AS total_records
FROM cofw_development_permits
WHERE special_text_copy IS NOT NULL
GROUP BY special_text_copy_trimmed
)

SELECT special_txt_spaces.special_text_copy            AS original_txt 
,      special_txt_no_spaces.special_text_copy_trimmed AS trimmed_txt
,      special_txt_spaces.total_records                AS original_total
,      special_txt_no_spaces.total_records             AS new_total
FROM special_txt_spaces
JOIN special_txt_no_spaces 
    ON REPLACE(special_txt_spaces.special_text_copy, ' ', '') = special_txt_no_spaces.special_text_copy_trimmed
WHERE special_txt_spaces.total_records <> special_txt_no_spaces.total_records   
ORDER BY original_total DESC;

-- Turn above query into a temp table

CREATE TEMP TABLE special_txt_mapping_temp AS

(

WITH special_txt_spaces AS

(
-- Text without spaces removed
SELECT special_text_copy
,      COUNT(*) AS total_records 
FROM cofw_development_permits
WHERE special_text_copy IS NOT NULL
GROUP BY special_text_copy
)

,

special_txt_no_spaces AS

(
-- Text with spaces removed
SELECT REPLACE(special_text_copy, ' ', '') AS special_text_copy_trimmed
,      COUNT(*) AS total_records
FROM cofw_development_permits
WHERE special_text_copy IS NOT NULL
GROUP BY special_text_copy_trimmed
)

SELECT special_txt_spaces.special_text_copy            AS original_txt 
,      special_txt_no_spaces.special_text_copy_trimmed AS trimmed_txt
,      special_txt_spaces.total_records                AS original_total
,      special_txt_no_spaces.total_records             AS new_total
,      ROW_NUMBER() OVER 
       (
	   PARTITION BY special_txt_no_spaces.special_text_copy_trimmed 
	   ORDER BY special_txt_spaces.total_records DESC
	   ) AS most_used

FROM special_txt_spaces
JOIN special_txt_no_spaces 
    ON REPLACE(special_txt_spaces.special_text_copy, ' ', '') = special_txt_no_spaces.special_text_copy_trimmed
WHERE special_txt_spaces.total_records <> special_txt_no_spaces.total_records   

);

SELECT * 
FROM special_txt_mapping_temp
WHERE 1 = 1
ORDER BY original_total DESC;

-- Added a row number to temp table 
-- partitioned by trimmed_txt, ordered by original total
SELECT * 
FROM special_txt_mapping_temp
WHERE 1 = 1
AND most_used = 1  -- pairs the normalized text (trimmed text) with its most commonly used spacing (original text X top original total)
ORDER BY original_total DESC;

-- Dropping all but the top use from mapping table
DELETE FROM special_txt_mapping_temp
WHERE most_used <> 1;

SELECT *
FROM special_txt_mapping_temp
ORDER BY original_total DESC;

-- Preview changes 
SELECT perms.special_text_copy                   AS original_val
,      REPLACE(perms.special_text_copy, ' ', '') AS perms_join_val
,      map_tmp.trimmed_txt                       AS map_tmp_join_val 
,      map_tmp.original_txt                      AS map_tmp_replacement_val

FROM cofw_development_permits AS perms
JOIN special_txt_mapping_temp AS map_tmp
    ON REPLACE(perms.special_text_copy, ' ', '') = map_tmp.trimmed_txt; 

START TRANSACTION;

UPDATE cofw_development_permits
SET special_text_copy = original_txt

FROM special_txt_mapping_temp
WHERE REPLACE(special_text_copy, ' ', '') = trimmed_txt;

-- Check output
SELECT special_text_copy  
,      COUNT(*) AS total_records
FROM cofw_development_permits
WHERE special_text_copy IS NOT NULL
GROUP BY special_text_copy
ORDER BY total_records DESC;

-- Commit changes after confirming changes look as intended.
-- COMMIT;

-- Rollback if changes do not look right
-- ROLLBACK;

-- Drop temp table
DROP TABLE IF EXISTS special_txt_mapping_temp;

-- Now, any differences in spacing from otherwise identical names have been normalized.
-- There are still names which are very similar however, wich I want to look into.

-- Exporting to Excel to take a closer look at areas to fix
COPY (

SELECT special_text_copy  
,      COUNT(*) AS total_records
FROM cofw_development_permits
WHERE special_text_copy IS NOT NULL
GROUP BY special_text_copy
ORDER BY total_records DESC

)

TO 'C:\Users\...\special_txt_cofw.csv'
WITH (FORMAT CSV, HEADER);

/*

A lot of rows just have random numbers in special text

A handful contain some variation of "*** X TEAM ***" which provides expidited plan reviews
Link: https://www.fortworthtexas.gov/departments/development-services/XTeam

A lot of these hold addresses but there is no uniform format. 
Some include blv, st, etc at the end and some don't.

Others add info at the end like the permit reason and/or the building name/use etc

Decided against doing more cleaning on this field.
Because anything can be typed in, there are far too many mispellings to clean up

*/

-- Compare pre/post cleanup number of distinct text
WITH comp_txt AS
(

SELECT (SELECT COUNT(DISTINCT special_text) FROM cofw_development_permits)      AS total_original
,      (SELECT COUNT(DISTINCT special_text_copy) FROM cofw_development_permits) AS total_cleaned 

)

SELECT comp_txt.*
,      (total_original - total_cleaned) AS names_deduplicated  -- 3990
,      ROUND(((total_original::NUMERIC - total_cleaned::NUMERIC) / total_original::NUMERIC) * 100,2)||'%' AS pct_recution  -- "5.84%"
FROM comp_txt;

-- Dropping backup column
ALTER TABLE cofw_development_permits DROP COLUMN special_text;

-- Renaming new column
ALTER TABLE cofw_development_permits RENAME COLUMN special_text_copy TO special_text;

-- Verify changes
SELECT special_text
,      COUNT(*) AS total_records
FROM cofw_development_permits
WHERE special_text IS NOT NULL
GROUP BY special_text
ORDER BY total_records DESC;

-- Checking work_description
SELECT COUNT(*) AS total_records  -- 572031 most records are not NULL
FROM cofw_development_permits
WHERE work_description IS NULL;

SELECT MIN(LENGTH(work_description)) AS min_len  -- 1
,      MAX(LENGTH(work_description)) AS max_len  -- 3659 
,      AVG(LENGTH(work_description)) AS avg_len  -- 45.1033654542442124
FROM cofw_development_permits
WHERE work_description IS NOT NULL;

SELECT permit_number
,      permit_type
,      permit_sub_type
,      permit_category
,      special_text    
,      work_description  -- Random single letters and characters
,      legal_description -- Does not appear to be missing text 

FROM cofw_development_permits
WHERE 1 = 1
AND LENGTH(work_description); = 1  -- min length

SELECT permit_number
,      permit_type
,      permit_sub_type
,      permit_category
,      special_text    
,      work_description  -- Denial letter text
,      legal_description 

FROM cofw_development_permits
WHERE 1 = 1
AND LENGTH(work_description); = 3659  -- max length

SELECT permit_number
,      permit_type
,      permit_sub_type
,      permit_category
,      special_text    
,      work_description  -- decription of work, its location and other misc. notes
,      legal_description 

FROM cofw_development_permits
WHERE 1 = 1
AND LENGTH(work_description) BETWEEN 40 AND 50   -- average length
LIMIT 50;

-- Checking legal_description
SELECT COUNT(*) AS total_records  -- 75215 most records are not NULL
FROM cofw_development_permits
WHERE legal_description IS NULL;

SELECT MIN(LENGTH(legal_description)) AS min_len  -- 3
,      MAX(LENGTH(legal_description)) AS max_len  -- 1203
,      AVG(LENGTH(legal_description)) AS avg_len  -- 41.1838468603413905
FROM cofw_development_permits
WHERE legal_description IS NOT NULL;

SELECT permit_number
,      permit_type
,      legal_description  -- "#26"
FROM cofw_development_permits

WHERE 1 = 1
AND LENGTH(legal_description); = 3 -- min length

SELECT permit_number
,      permit_type
,      legal_description  --  Very verbose desciption of lots making up a tract of land
FROM cofw_development_permits

WHERE 1 = 1
AND LENGTH(legal_description); = 1203 -- max length

SELECT permit_number
,      permit_type
,      legal_description  --  most commomnly used as [Project name][Block/blk][Lot(s)]
FROM cofw_development_permits;

WHERE 1 = 1
AND LENGTH(legal_description) BETWEEN 10 AND 60  -- Closer to average
ORDER BY RANDOM()
LIMIT 50;

-- Checking owner_name
SELECT COUNT(*) AS total_records  -- 105171 most records are not NULL
FROM cofw_development_permits
WHERE owner_name IS NULL;

SELECT COUNT(DISTINCT owner_name) AS distinct_owners  -- 222335
FROM cofw_development_permits
WHERE owner_name IS NOT NULL;

-- Looking for null values
SELECT owner_name
,      COUNT(permit_number) AS total_permits
FROM cofw_development_permits
WHERE owner_name ILIKE 'na'
GROUP BY owner_name
ORDER BY total_permits DESC;

-- "NA"	100814
-- "na"	9
-- "Na"	1

-- Updating owner column to replace 'NA' with NULL
START TRANSACTION;

UPDATE cofw_development_permits
SET owner_name = NULL
WHERE owner_name ILIKE 'NA'

RETURNING *;

-- Commit changes after confirming changes look as intended.
-- COMMIT;

-- Rollback if changes do not look right
-- ROLLBACK;

-- Ran earlier query and confirmed these were gone

SELECT MAX(LENGTH(owner_name))           AS max_len
,      MIN(LENGTH(owner_name))           AS min_len
,      ROUND(AVG(LENGTH(owner_name)), 2) AS avg_len
FROM cofw_development_permits
WHERE owner_name IS NOT NULL;

-- Checking for more na values
SELECT owner_name
,      COUNT(permit_number) AS total_permits
FROM cofw_development_permits
WHERE LENGTH(owner_name) BETWEEN 1 AND 5
GROUP BY owner_name
ORDER BY owner_name; 

-- A lot of random chars + full addresses in this field
-- "N/A"	3829
-- "N/S"	2
-- "NA/"	2
-- "NA//"	525
-- "NA///"	45

-- Using a different search to find any names which contain 2 letter characters starting with n
SELECT owner_name
,      COUNT(permit_number) AS total_permits
FROM cofw_development_permits
WHERE LENGTH(REGEXP_REPLACE(owner_name, '[^a-zA-Z]', '', 'g')) = 2
AND owner_name ILIKE 'n%'
GROUP BY owner_name
ORDER BY owner_name;

SELECT (3829 + 2 + 2 + 525 + 45 +1) AS total;  -- 4404 records

-- Updating owner column to replace these values with NULLs
START TRANSACTION;

UPDATE cofw_development_permits
SET owner_name = NULL
WHERE LENGTH(REGEXP_REPLACE(owner_name, '[^a-zA-Z]', '', 'g') ) = 2
AND owner_name ILIKE 'n%'

RETURNING *;

-- Commit changes after confirming changes look as intended.
-- COMMIT;

-- Rollback if changes do not look right
-- ROLLBACK;

-- Permits by owner
SELECT owner_name
,      COUNT(DISTINCT permit_number) AS total_permits
FROM cofw_development_permits
GROUP BY owner_name
ORDER BY total_permits DESC;

-- In top 50 or so owner names by count of permits, there's some
-- duplication I want to clean up

START TRANSACTION;

UPDATE cofw_development_permits
SET owner_name = 'DR HORTON - TEXAS LTD'  -- Second most common
WHERE owner_name IN ('D R HORTON-TEXAS LTD', 'D R HORTON - TEXAS LTD',  
                     'DR HORTON-TEXAS LTD', 'DR HORTON TEXAS LTD');

UPDATE cofw_development_permits
SET owner_name = 'DR HORTON'  -- Second most common. Assuming this is a seperate entity from "TX LTD"
WHERE owner_name IN ('D R HORTON', 'D R Horton');

UPDATE cofw_development_permits
SET owner_name = 'CITY OF FORT WORTH'  -- Second most common
WHERE owner_name = 'FORT WORTH, CITY OF';  -- Would rather not have commas in here

UPDATE cofw_development_permits
SET owner_name = 'LENNAR HOMES OF TEXAS LAND & CONSTRUCTION  LTD'  -- Most common. I assume its a seperate entity from "LENNAR HOMES OF TEXAS"
WHERE owner_name IN ('LENNAR HOMES OF TEXAS LAND & C', 'LENNAR HOMES OF TEXAS LAND AND',  
                     'LENNAR HMS OF TEXAS LAND & CON');

-- Commit changes after confirming changes look as intended.
-- COMMIT;

-- Rollback if changes do not look right
-- ROLLBACK;

-- Standardize based on trimming non text and removing spaces. Normalize to most common speling

SELECT COUNT(DISTINCT owner_name) AS names_start  -- 222316
FROM cofw_development_permits

-- Create a backup column
-- ALTER TABLE cofw_development_permits ADD COLUMN owner_name_copy TEXT;

-- Copy over contents from original column
-- UPDATE cofw_development_permits
-- SET owner_name_copy = owner_name;

-- Found a more streamlined way to do cleanup I did for earlier column

CREATE TEMP TABLE owner_mapping_temp AS

(

WITH cleaned_names AS (

SELECT REGEXP_REPLACE(TRIM(owner_name), '\s+', ' ', 'g') AS cleaned_name
,      COUNT(*)                                          AS total_occ
FROM cofw_development_permits
GROUP BY cleaned_name

)

, norm_names AS (

SELECT REGEXP_REPLACE(TRIM(UPPER(owner_name)), '[^a-zA-Z]', '', 'g') AS normalized_name
FROM cofw_development_permits

)

, most_common AS (

SELECT cleaned_names.cleaned_name
,      norm_names.normalized_name

,      ROW_NUMBER() OVER 
       (
	   PARTITION BY norm_names.normalized_name
	   ORDER BY cleaned_names.total_occ DESC
	   ) AS most_used
  
FROM cleaned_names
JOIN norm_names 
    ON REGEXP_REPLACE(TRIM(UPPER(cleaned_names.cleaned_name)), '[^a-zA-Z]', '', 'g') = norm_names.normalized_name  

)

SELECT *
FROM most_common
WHERE most_used = 1;

)

-- Checking results
SELECT *
FROM owner_mapping_temp
LIMIT 50;

-- Preview changes 
SELECT perms.owner_name                                              AS original_val
,      REGEXP_REPLACE(TRIM(UPPER(owner_name)), '[^a-zA-Z]', '', 'g') AS perms_join_val
,      map_tmp.normalized_name                                       AS map_tmp_join_val 
,      map_tmp.cleaned_name                                          AS map_tmp_replacement_val

FROM cofw_development_permits AS perms
JOIN owner_mapping_temp       AS map_tmp
    ON REGEXP_REPLACE(TRIM(UPPER(owner_name)), '[^a-zA-Z]', '', 'g') = map_tmp.normalized_name
ORDER BY RANDOM()	
LIMIT 50; 

START TRANSACTION;

UPDATE cofw_development_permits
SET owner_name = cleaned_name

FROM owner_mapping_temp
WHERE REGEXP_REPLACE(TRIM(UPPER(owner_name)), '[^a-zA-Z]', '', 'g') = normalized_name;

-- Check output
SELECT owner_name  
,      COUNT(*) AS total_records
FROM cofw_development_permits
WHERE owner_name IS NOT NULL
GROUP BY owner_name
ORDER BY total_records DESC;

-- Commit changes after confirming changes look as intended.
-- COMMIT;

-- Rollback if changes do not look right
-- ROLLBACK;

SELECT COUNT(DISTINCT owner_name) AS names_end  -- 210253
FROM cofw_development_permits;


SELECT (222316 - 210253) AS total_deduped;  -- 12063 names de-duplicated

-- Drop temp table
DROP TABLE IF EXISTS owner_mapping_temp;

-- Dropping backup column
ALTER TABLE cofw_development_permits DROP COLUMN owner_name_copy;  -- Used copy as a backup in this operation and did cleanup directly in original

-- Repeating the na search for columns I cleaned earlier
-- Substituting col names here to save space
SELECT legal_description 
,      COUNT(permit_number) AS total_permits
FROM cofw_development_permits
WHERE LENGTH(REGEXP_REPLACE(legal_description, '[^a-zA-Z]', '', 'g') ) = 2
AND legal_description  ILIKE 'n%'
GROUP BY legal_description 
ORDER BY legal_description; 

-- special_text
-- "N/A"	18
-- "NA"	1
-- "NA/"	1

SELECT COUNT(permit_number) AS total_permits
FROM cofw_development_permits
WHERE special_text IN ('N/A', 'NA', 'NA/');  -- 20

-- Updating special text
START TRANSACTION;

UPDATE cofw_development_permits
SET special_text = NULL
WHERE special_text IN ('N/A', 'NA', 'NA/')

RETURNING *;

-- Commit changes after confirming changes look as intended.
-- COMMIT;

-- Rollback if changes do not look right
-- ROLLBACK;

-- work_description
-- "N/A" 3
-- "na"	1
-- "nn" 1
-- "NN" 1

SELECT COUNT(permit_number) AS total_permits
FROM cofw_development_permits
WHERE LOWER(work_description) IN ('n/a', 'na', 'nn');  -- 6

-- Updating work description
START TRANSACTION;

UPDATE cofw_development_permits
SET work_description = NULL
WHERE LOWER(work_description) IN ('n/a', 'na', 'nn') 

RETURNING *;

-- Commit changes after confirming changes look as intended.
-- COMMIT;

-- Rollback if changes do not look right
-- ROLLBACK;

-- Checking data quality of filing_date 
SELECT COUNT(*) AS total_nulls  -- 0
FROM cofw_development_permits
WHERE filing_date IS NULL;

SELECT MAX(LENGTH(filing_date::TEXT))           AS max_len  -- 10
,      MIN(LENGTH(filing_date::TEXT))           AS min_len  -- 10
,      ROUND(AVG(LENGTH(filing_date::TEXT)), 2) AS avg_len  -- 10
FROM cofw_development_permits;

SELECT COUNT(*) AS total_incomplete_dts  -- 0
FROM cofw_development_permits
WHERE LENGTH(filing_date::TEXT) <> 10; 

-- Checking data quality of status

-- NULL and other valid statuses
SELECT DISTINCT status
FROM cofw_development_permits;

SELECT COUNT(*) AS total_records  -- 15
FROM cofw_development_permits
WHERE status IS NULL;

-- Checking data quality of status_date
SELECT MAX(LENGTH(status_date::TEXT))           AS max_len  -- 22
,      MIN(LENGTH(status_date::TEXT))           AS min_len  -- 22
,      ROUND(AVG(LENGTH(status_date::TEXT)), 2) AS avg_len  -- 22
FROM cofw_development_permits;

SELECT COUNT(*) AS total_incomplete_dts  -- 0
FROM cofw_development_permits
WHERE LENGTH(status_date::TEXT) <> 22; 

-- Timestamps don't have uniform timezone
SELECT permit_number
,      filing_date
,      status
,      status_date
FROM cofw_development_permits
LIMIT 20;

SELECT DISTINCT EXTRACT(TIMEZONE FROM status_date) -- -06 and -05 timezones. Probably DST
FROM cofw_development_permits;

-- Distinctive timestamps, not all defaulted to 00:00:00
SELECT DISTINCT DATE_PART('hour', status_date) AS hr
FROM cofw_development_permits
ORDER BY hr;

-- Checking data quality of permit_location  
SELECT MAX(LENGTH(permit_location))           AS max_len  -- 40
,      MIN(LENGTH(permit_location))           AS min_len  -- 34
,      ROUND(AVG(LENGTH(permit_location)), 2) AS avg_len  -- 39.05
FROM cofw_development_permits;

SELECT COUNT(DISTINCT permit_number) AS unique_permits  -- 1383654
FROM cofw_development_permits
WHERE permit_location IS NULL;

-- Parsing location to create lat and long columns
ALTER TABLE cofw_development_permits ADD COLUMN location_lat NUMERIC;
ALTER TABLE cofw_development_permits ADD COLUMN location_long NUMERIC;

SELECT permit_location
,      CAST(TRIM(SPLIT_PART(REGEXP_REPLACE(permit_location, '[()]', '', 'g'), ',', 1)) AS NUMERIC) AS latitude
,      CAST(TRIM(SPLIT_PART(REGEXP_REPLACE(permit_location, '[()]', '', 'g'), ',', 2)) AS NUMERIC) AS longitude
FROM cofw_development_permits
WHERE permit_location IS NOT NULL
LIMIT 5;

START TRANSACTION;

UPDATE cofw_development_permits
SET

location_lat  = CAST(TRIM(SPLIT_PART(REGEXP_REPLACE(permit_location, '[()]', '', 'g'), ',', 1)) AS NUMERIC),
location_long = CAST(TRIM(SPLIT_PART(REGEXP_REPLACE(permit_location, '[()]', '', 'g'), ',', 2)) AS NUMERIC);

-- Double checking
SELECT permit_location
,      location_lat
,      location_long
FROM cofw_development_permits
WHERE permit_location IS NOT NULL
LIMIT 5;

-- Commit changes after confirming changes look as intended.
-- COMMIT;

-- Rollback if changes do not look right
-- ROLLBACK;

-- These values make me assume the WGS 84 coordinate system.
-- Could not find other docs to confirm
SELECT MAX(location_lat)  AS max_lat
,      MIN(location_lat)  AS min_lat

,      MAX(location_long) AS max_long
,      MIN(location_long) AS min_long
FROM cofw_development_permits;

-- Enabling POSTGIS
-- CREATE EXTENSION postgis;

-- Looking at 25 most recent permit locations. Scattered across city not concentrated.
SELECT permit_number
,      filing_date
,      permit_location
,      ST_SETSRID(ST_MAKEPOINT(location_long, location_lat), 4326)::GEOGRAPHY AS permit_point

FROM cofw_development_permits
ORDER BY filing_date DESC
LIMIT 25;

-- Adding a goegraphy column
-- ALTER TABLE cofw_development_permits ADD COLUMN permit_geog_point GEOGRAPHY(POINT, 4326);

-- UPDATE cofw_development_permits
-- SET permit_geog_point = ST_SETSRID(ST_MAKEPOINT(location_long, location_lat), 4326)::GEOGRAPHY;

-- Checking output
SELECT permit_number
,      filing_date
,      permit_location
,      permit_geog_point

FROM cofw_development_permits
ORDER BY filing_date DESC
LIMIT 25;

-- Adding index
CREATE INDEX cofw_dev_permits_pts_idx 
ON cofw_development_permits 
USING GIST (permit_geog_point);

-- Checking data quality of job_value
SELECT MAX(job_value)           AS max_jv  -- 122222222222.00
,      MIN(job_value)           AS min_jv  -- 0.00
,      ROUND(AVG(job_value), 2) AS avg_jv  -- 924298.77
FROM cofw_development_permits;

SELECT SUM(CASE WHEN job_value IS NULL THEN 1 ELSE 0 END)           AS jv_nulls  -- 1081234
,      SUM(CASE WHEN job_value = 0 THEN 1 ELSE 0 END)               AS jv_mins   -- 7578
,      SUM(CASE WHEN job_value = 122222222222.00 THEN 1 ELSE 0 END) AS jv_maxes  -- 1 
FROM cofw_development_permits;

-- Can't see a pattern as to why job value would be 0
SELECT *
FROM cofw_development_permits
WHERE job_value = 0
ORDER BY RANDOM()
LIMIT 25; 

-- 2012 commercial building church addition. Void as of "1900-01-01 00:00:00-06"
-- Did someone sit on their keyboard???
SELECT *
FROM cofw_development_permits
WHERE job_value = 122222222222.00;

-- Checking data quality of use_type
SELECT COUNT(DISTINCT use_type) AS distinct_use_types  -- 58
FROM cofw_development_permits;

SELECT use_type
,      COUNT(permit_number) AS total_rows  -- most common is null = 647457
FROM cofw_development_permits
GROUP BY use_type
ORDER BY total_rows DESC;

-- Why do we need "General", "Other", AND "Miscellaneous" ??? 
-- How'd we get so many nulls with these three overly broad categories ???

-- Checking data quality of specific_use
SELECT COUNT(DISTINCT specific_use) AS distinct_specific_use  -- 485
FROM cofw_development_permits;

SELECT specific_use
,      COUNT(permit_number) AS total_rows  -- most common is null = 1371405
FROM cofw_development_permits
GROUP BY specific_use
ORDER BY total_rows DESC;

-- Clean up column

-- Set "NA" to NULL = 2153 records
UPDATE cofw_development_permits
SET specific_use = NULL 
WHERE LOWER(specific_use) IN ('n/a', 'na', 'na/');

-- Create a backup column
-- ALTER TABLE cofw_development_permits ADD COLUMN specific_use_copy TEXT;

-- Copy over contents from original column
UPDATE cofw_development_permits
SET specific_use_copy = specific_use; 

-- Start by standardizing case, and removing leading and trailing white space
UPDATE cofw_development_permits
SET specific_use = TRIM(UPPER(specific_use));

SELECT specific_use
,      COUNT(permit_number) AS total_rows
FROM cofw_development_permits
GROUP BY specific_use
ORDER BY specific_use;

-- Checking in order of use. Complete non standardization and full of mispellings:
-- "COLLEGE"
-- "COLLEGE / UNIVERSITY"
-- "COLLEGE OR UNIVERSITY"
-- "COLLEGE/UNIVERSITY"

SELECT specific_use
,      COUNT(permit_number) AS total_rows  -- most common is null = 1371405
FROM cofw_development_permits
GROUP BY specific_use
ORDER BY total_rows DESC;

-- Standardizing white space around special characters 

--  Check output first
SELECT specific_use
,      REGEXP_REPLACE(specific_use, '\s*([^\w\s])\s*', ' \1 ', 'g') AS cleaned
FROM cofw_development_permits
WHERE specific_use ~ '\s*([^\w\s])\s*'
LIMIT 25;

-- This will add exactly one space on either side of special chars, and 
-- remove any extra spaces around special chars with more than one.
UPDATE cofw_development_permits
SET specific_use = REGEXP_REPLACE(specific_use, '\s*([^\w\s])\s*', ' \1 ', 'g')
WHERE specific_use ~ '\s*([^\w\s])\s*';

SELECT COUNT(DISTINCT specific_use) AS distinct_specific_use  -- 429. Down 56 from 485
FROM cofw_development_permits;

-- Any further cleanup would require manually going through mispellings and 
-- duplicate categories. Stopping here with cleanup of this col.

-- Dropping backup column
ALTER TABLE cofw_development_permits
DROP COLUMN specific_use_copy;

-- Checking data quality of units
SELECT MAX(LENGTH(units))           AS max_units  -- 9
,      MIN(LENGTH(units))           AS min_units  -- 1
,      ROUND(AVG(LENGTH(units)), 2) AS avg_units  -- 1.01
FROM cofw_development_permits;

-- Check for non-number characters in the column
SELECT COUNT(*) AS total_rows  -- 0
FROM cofw_development_permits
WHERE units !~ '^[0-9.]+$';

-- Select some sample columns
SELECT permit_number
,      units
FROM cofw_development_permits
WHERE units IS NOT NULL
ORDER BY RANDOM()
LIMIT 50;

-- Column looks clean. Need to change data type from text to numeric.
-- Should have done that on import.

START TRANSACTION;

-- ALTER TABLE cofw_development_permits
-- ALTER COLUMN units TYPE NUMERIC USING units::NUMERIC;

-- Select some sample columns to confirm changes look okay
SELECT permit_number
,      units
FROM cofw_development_permits
WHERE units IS NOT NULL
ORDER BY RANDOM()
LIMIT 50;

-- Commit changes after confirming changes look as intended.
-- COMMIT;

-- Rollback if changes do not look right
-- ROLLBACK;

-- Checking data quality of square_feet
SELECT MAX(LENGTH(square_feet))           AS max_square_feet  -- 18
,      MIN(LENGTH(square_feet))           AS min_square_feet  -- 1
,      ROUND(AVG(LENGTH(square_feet)), 2) AS avg_square_feet  -- 3.03
FROM cofw_development_permits;

-- Check for non-number characters in the column
SELECT COUNT(*) AS total_rows  -- 0
FROM cofw_development_permits
WHERE square_feet !~ '^[0-9.]+$';

-- Check for presence of NULLs
SELECT COUNT(*) AS total_nulls  -- 1134414
FROM cofw_development_permits
WHERE square_feet IS NULL;

-- Select some sample columns
SELECT permit_number
,      square_feet
FROM cofw_development_permits
WHERE square_feet IS NOT NULL
ORDER BY RANDOM()
LIMIT 50;

-- Column looks clean. Need to change data type from text to numeric.
-- Should have done that import for this one too.

START TRANSACTION;

ALTER TABLE cofw_development_permits
ALTER COLUMN square_feet TYPE NUMERIC USING square_feet::NUMERIC;

-- Select some sample columns to confirm changes look okay
SELECT permit_number
,      square_feet
FROM cofw_development_permits
WHERE units IS NOT NULL
ORDER BY RANDOM()
LIMIT 50;

-- Commit changes after confirming changes look as intended.
-- COMMIT;

-- Rollback if changes do not look right
-- ROLLBACK;