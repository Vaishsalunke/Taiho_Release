/*
CCDM countryplannedrecruitment mapping
Notes: Standard mapping to CCDM countryplannedrecruitment table
*/

WITH included_studies AS (
							SELECT distinct studyid FROM study ),
	
max_date as ( 
 select country_code, month, year, max(actual_date) as actual_date from (	
select distinct actual_date::date as actual_date,extract(month from actual_date::date) as month,
extract(year from actual_date::date) as year, country_code
From  tas3681_101_ctms.country_milestones
where actual_date <> ''
)a group by 1, 2, 3
order by 1,3,2
),
							
rec_cnt as (
select a.*, md.actual_date  from (
select count(*) as rec_cnt, country_code,extract (month from actual_date::date) as month,
                extract (year from actual_date::date) as year         
from  tas3681_101_ctms.country_milestones ms
where actual_date <> ''
group by 2,3,4
order by 2,4,3
)a, max_date md
where a.month = md.month
and a.year = md.year
and a.country_code = md.country_code
),


countryplannedrecruitment_data AS (
select studyid ,
studyname,
sitecountry,
sitecountrycode,
category,
frequency,
rc.actual_date as enddate,
type,
rc.rec_cnt as recruitmentcount
from (
SELECT DISTINCT
										 'TAS3681_101_DOSE_ESC'::text AS studyid,
										 'TAS3681_101_DOSE_ESC'::text AS studyname,
										 country_name::text AS sitecountry,
										 country_code::text AS sitecountrycode,
										 'Enrollment'::text AS category,
										 'Monthly'::text AS frequency,
										null::date as enddate,
										 'Planned'::text AS type,
										 null::numeric AS recruitmentcount,
										 extract (month from ms.actual_date::date) as month,
                                         extract (year from ms.actual_date::date) as year
									     FROM tas3681_101_ctms.country_milestones ms
									     where ms.actual_date <> ''
									     ) ms									     
									     , rec_cnt rc
									     where ms.sitecountrycode = rc.country_code
									     and ms.month = rc.month
									     and ms.year = rc.year									     
									     )
									     

SELECT
        /*KEY cpr.studyid::text AS comprehendid, KEY*/
        cpr.studyid::text AS studyid,
        cpr.studyname::text AS studyname,
        cpr.sitecountry::text AS sitecountry,
        cpr.sitecountrycode::text AS sitecountrycode,
        cpr.category::text AS category,
        cpr.frequency::text AS frequency,
        cpr.enddate::date AS enddate,
        cpr.type::text AS type,
        cpr.recruitmentcount::NUMERIC AS recruitmentcount
        /*KEY ,(cpr.studyid || '~' || cpr.sitecountrycode || '~' || cpr.category || '~' || cpr.frequency || '~' || cpr.enddate || '~' || cpr.type)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM countryplannedrecruitment_data cpr
JOIN included_studies st ON (st.studyid = cpr.studyid); 

