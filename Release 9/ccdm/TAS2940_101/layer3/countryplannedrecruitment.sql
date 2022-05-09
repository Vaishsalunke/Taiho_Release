/*
CCDM countryplannedrecruitment mapping
Notes: Standard mapping to CCDM countryplannedrecruitment table
*/
WITH included_studies AS (
                SELECT distinct studyid FROM study ),

     max_date as ( select country_code, 
					 month, 
					 year,
					 max(actual_date) as actual_date
			  from (
						select country as country_code,
							   extract(month from actual_date::date) as month,
					           extract(year from actual_date::date) as year,
					           actual_date::date as actual_date
			 			from tas2940_101_ctms.milestone_status_country
			 			where actual_date <>''
			 		)e	group by 1,2,3
			 ),
			 
			 
rec_count as (
				select r.* , md.actual_date
				from (

						select count(*) as count, 
							   extract(month from actual_date::date) as month,
					           extract(year from actual_date::date) as year,
					           country as country_code
			 			from tas2940_101_ctms.milestone_status_country
			 			where actual_date <>''
			 			group by 2,3,4
					  )r , max_date md
				where r.month=md.month
				and   r.year= md.year
				and   r.country_code=md.country_code



			  ),
			  
			  
countryplannedrecruitment_data AS (

select distinct 
		studyid ::text AS studyid,
	   studyname::text AS studyname,
	   sitecountry::text AS sitecountry,
	   sitecountrycode::text AS sitecountrycode,
	   category::text AS category,
	   frequency::text AS frequency,
	   rc.actual_date::date as enddate,
	   type::text AS type,
	   rc.count::NUMERIC as recruitmentcount
from 							(	   


                					SELECT  'TAS2940_101'::text AS studyid,
                        					'TAS2940_101'::text AS studyname,
                        					case
											When trim(ms.country) = 'USA' then 'United States of America'
											When trim(ms.country) = 'GBR' then 'United Kingdom'
											When trim(ms.country) = 'ESP' then 'Spain'
											When trim(ms.country) = 'PRT' then 'Portugal'
											When trim(ms.country) = 'FRA' then 'France'
											When trim(ms.country) = 'ITA' then 'Italy'
											When trim(ms.country) = 'CAN' then 'Canada'
											else 'United States of America'
											end::text AS sitecountry,
                        					country::text AS sitecountrycode,
                        					'Enrollment'::text AS category,
                        					'Monthly'::text AS frequency,
                        					null::date AS enddate,
                        					'Planned'::text AS type,
                       						 null::NUMERIC AS recruitmentcount,
                       						 extract(month from actual_date::date) as month,
					           				 extract(year from actual_date::date) as year
                 					 from tas2940_101_ctms.milestone_status_country ms
                 					 where actual_date<>''
                 				  ) msc	 , rec_count rc
where msc.month=rc.month
and   msc.year=rc.year
and msc.sitecountrycode=rc.country_code

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

