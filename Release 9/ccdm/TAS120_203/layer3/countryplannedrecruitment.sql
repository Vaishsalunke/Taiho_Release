/*
CCDM countryplannedrecruitment mapping
Notes: Standard mapping to CCDM countryplannedrecruitment table
*/

WITH included_studies AS (
						SELECT distinct studyid FROM study ),
	
	max_date as ( 
			select country, month, year, max(month_period) as month_period from (	
			select distinct month_period::date as month_period,extract(month from month_period::date) as month,
			extract(year from month_period::date) as year, country
			From tas120_203_ctms.enrollment
			where month_period <> ''
			)a group by 1, 2, 3
			order by 1,3,2
	),
							
	rec_cnt as (
			select a.*, md.month_period  from (
			select count(*) as rec_cnt, country,extract (month from month_period::date) as month,
			                extract (year from month_period::date) as year         
			from tas120_203_ctms.enrollment e
			where month_period <> ''
			group by 2,3,4
			order by 2,4,3
			)a, max_date md
	where a.month = md.month
	and a.year = md.year
and a.country = md.country
),


countryplannedrecruitment_data AS (
			select studyid ,
			studyname,
			sitecountry,
			sitecountrycode,
			category,
			frequency,
			rc.month_period as enddate,
			type,
			rc.rec_cnt as recruitmentcount
			from (
					SELECT DISTINCT
										 'TAS120_203'::text AS studyid,
										 'TAS120_203'::text AS studyname,
										 src.country_name::text AS sitecountry,
										 e.country::text AS sitecountrycode,
										 'Enrollment'::text AS category,
										 'Monthly'::text AS frequency,
										 null::date as enddate,
										 'Planned'::text AS type,
										 null::numeric AS recruitmentcount,
										 extract (month from e.month_period::date) as month,
                                         extract (year from e.month_period::date) as year
									     FROM tas120_203_ctms.enrollment e
									     left join internal_config.site_region_config  src
									     on e.country=src.alpha_3_code
									     where e.month_period <> ''
									     ) e									     
									     , rec_cnt rc
									     where e.sitecountrycode = rc.country
									     and e.month = rc.month
									     and e.year = rc.year									     
									     )
									     

SELECT
        /*KEY (cpr.studyid || '~' || cpr.sitecountrycode)::text AS comprehendid, KEY*/
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



