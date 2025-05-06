--------------------------------------------------------------------------------------------------------------------------------------------------
-- City of Fort Worth public data analysis: Development pemits dataset
-- By Sergio Ramos 
-- linkedin.com/in/sergio-ramos-analyst/
-- Data from: https://data.fortworthtexas.gov/Development-Infrastructure/Development-Permits/quz7-xnsy/about_data 
--------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------
-- Analyzing cleaned data 
----------------------------------------------------------------------------

SELECT *
FROM cofw_development_permits
LIMIT 20;

SELECT DISTINCT status
FROM cofw_development_permits;

SELECT DISTINCT permit_type
FROM cofw_development_permits;

-- How many distinct permits have been filed over time 
WITH monthly_permits AS (

SELECT TO_CHAR(filing_date, 'YYYY-MM') AS year_mnth
,      COUNT(DISTINCT permit_number)   AS total_permits
FROM cofw_development_permits
GROUP BY year_mnth

)

SELECT year_mnth
,      total_permits

,      total_permits - LAG(total_permits, 1) OVER (ORDER BY year_mnth) AS month_over_month_diff
,      ROUND(AVG(total_permits) OVER (ORDER BY year_mnth ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS running_3_month_avg

FROM monthly_permits
ORDER BY year_mnth;

-- Permits by status
SELECT status
,      COUNT(DISTINCT permit_number) AS total_permits
FROM cofw_development_permits
GROUP BY status
ORDER BY total_permits DESC;

-- Permits by owner
SELECT owner_name
,      COUNT(DISTINCT permit_number) AS total_permits
FROM cofw_development_permits
WHERE owner_name IS NOT NULL
GROUP BY owner_name
ORDER BY total_permits DESC;

-- Some DR HORTON entity or another show up 4x in the top 50
-- Lennar homes shows up 2x

-- Permits by owner and year

WITH yearly_ranking AS (

SELECT TO_CHAR(filing_date, 'YYYY') AS filing_yr
,      owner_name
,      COUNT(DISTINCT permit_number) AS total_permits

,      ROW_NUMBER() OVER (
                           PARTITION BY TO_CHAR(filing_date, 'YYYY')
						   ORDER BY COUNT(DISTINCT permit_number) DESC) AS yr_rank

FROM cofw_development_permits
WHERE owner_name IS NOT NULL
GROUP BY filing_yr, owner_name

)

SELECT *
FROM yearly_ranking 
WHERE yr_rank < 6
ORDER BY filing_yr, yr_rank;

-- Permits by type
SELECT TO_CHAR(filing_date, 'YYYY') AS filing_yr
,      permit_type
,      permit_sub_type
,      COUNT(DISTINCT permit_number) AS total_permits
FROM cofw_development_permits
GROUP BY filing_yr, permit_type, permit_sub_type
ORDER BY filing_yr;

-- Permits by value
SELECT TO_CHAR(filing_date, 'YYYY') AS filing_yr
,      SUM(job_value)               AS total_job_value
FROM cofw_development_permits
GROUP BY filing_yr
ORDER BY filing_yr;

/*

If several permits are required at different steps of a development
it might not be accurate to conceptualize each permit as a job

And I am unsure job_value would be an accurate measure of real total
job value over a period of time since it may be duplicated across permits 
for the same project.

What I need to do, then, is better understand permits as they relate to a common project
and figure out how to identify them in the data.

Need to confirm that in terms of data, it is correct to think of a permit as 
a child record of a project (conceptually at least). 

I am pretty sure projects are not captured anywhere else, though that would make things easier.

EDIT: When comparing the sum of job values year over year against figures from the City present in the Economic Development Strategic Plan 
and the 2023 comprehensive plan, I am able to get pretty close. So, I believe its safe to assume this is the same methodology used by the COFW. 

*/

SELECT permit_location
,      COUNT(DISTINCT permit_number) AS total_permits
FROM cofw_development_permits
GROUP BY permit_location
ORDER BY total_permits DESC
LIMIT 5;

SELECT permit_number
,      filing_date
,      permit_type
,      permit_sub_type
,      permit_category
,      work_description
,      permit_location
,      status
,      status_date
,      job_value

FROM cofw_development_permits

WHERE 1 = 1 
AND permit_location = '(32.96477567329806, -97.41853617297778)'

ORDER BY filing_date

-- Found an example of several permits along the lifecycle of a single project
SELECT *
FROM cofw_development_permits
WHERE owner_name = 'ARMSTRONG WESTERN CENTER BEACH'
ORDER BY filing_date;

/*

In the above example, it seems like there was a single entity created
for the purposes of this building. 

This is further confirmed by the identical legal_description across permits.

This is however, likely not a reliable way to group a distinct project,
as legal entities are handled differently between a one off development and a 
home building company or other business I imagine.

The first permit filed seemded to be the "Commercial Building Permit".
This record was the most complete, and was the only record with a job value,
use type, specific use, and sqft recorded.

"Finaled" seems to be the final status of permits, with all but the expired permit and 
the Urban Forestry permit having a finaled status date preceding the Commercial building permit
leading me to assume those needed to be obtained prior to finalizing CB

The Urban Forestry permit is left as 'issued' with a status date of 1900-01-01
which makes me think its not finalized, or that workflow does not capture a 
status date, and someone inserted this placeholder in the ETL process.

EDIT: "Finaled" also appears in cited figures from the City within the Economic Development Strategic Plan 
and the 2023 comprehensive plan.

*/

-- Above guess is incorrect. Issued has many values associated with status_date. Must be default val.
SELECT status
,      TO_CHAR(status_date, 'YYYY-MM') AS year_mnth
,      COUNT(DISTINCT permit_number)   AS dist_perms
FROM cofw_development_permits
WHERE permit_type = 'Urban Forestry'
GROUP BY status, year_mnth
ORDER BY dist_perms DESC;


-- All type of permits have an associated value
SELECT permit_type
,      COUNT(DISTINCT permit_number) AS dist_perms
FROM cofw_development_permits
WHERE job_value IS NOT NULL
GROUP BY permit_type
ORDER BY permit_type;

/*

Any NEW comercial development (ground up apts or commercial buildings) will be reviewed by all departments, individual trades people will need
to pull thier own permits in order to perform the work.

If DR Horton or another developer builds a community, each house is permitted by itself.
job_value = cost to build. 

Statuses:

1. In Review & Plan Review (Initial review)
2. Approved
	Cust svc makes sure all fees are paid
3. Issued
	Start building
4. Finaled
	Certificate of Occupancy

In Review & Plan Review means depts still reviewing it
All depts need to review before client replies.

Awaiting client reply means every dept reviewed and you address all concerns
from various depts, not just one because replying reopens the workflow for ALL depts

Close and Closed by rule are done by development support under special circumstances.
Unsure if its the same as finaled or if its a form of cancellation or something.

All records are related to a proper address in the internal facing system, making reconciling
permits with their "parent" project much more straightforward.

Might need to ask if its possible to expose this data on the public
facing dataset.

-- Giselle Gonzales (Senior plans examiner at the City of Fort Worth)

*/

SELECT permit_type  -- Urban Forestry 
,      COUNT(DISTINCT permit_number) AS total_permits  -- 34 total 
FROM cofw_development_permits
WHERE status = 'Complete'
GROUP BY permit_type;
LIMIT 20;

SELECT permit_type  -- Design Review 
,      COUNT(DISTINCT permit_number) AS total_permits  -- 1 total
FROM cofw_development_permits
WHERE status = 'Continued'
GROUP BY permit_type;