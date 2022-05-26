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

		     				select country_code,
	   				   			   actual_date,
	   				   			   extract(month from actual_date::date) as month,
	   				   			   extract(year from actual_date::date) as year
	   				
	   		   			   from tas117_201_ctms.country_milestones
	   		    		   where actual_date<>''

						)r group by 1,2,3
					),
		
		
rec_count as (
				   select a.*,md.actual_date as actual_date
				   from (
				   			select count(*) as count,
				   				   extract(month from actual_date::date) as month,
	   				   			   extract(year from actual_date::date) as year,
	   				   			   country_code as country_code
	   				
	   		    		    from tas117_201_ctms.country_milestones
	   		    		    where actual_date<>''
	   		    		    group by 2,3,4
	   		    		   )a,  max_date md
	   		    	where a.country_code=md.country_code
	   		    	and   a.month=md.month
	   		    	and	  a.year=md.year
				   
			 ),

countryplannedrecruitment_data AS (

select distinct
	   studyid::text AS studyid,
	   studyname::text AS studyname,
	   sitecountry::text AS sitecountry,
	   sitecountrycode::text AS sitecountrycode,
	   category::text AS category,
	   frequency::text AS frequency,
	   rc.actual_date::date AS enddate,
	   type::text AS type,
	   rc.count::NUMERIC AS recruitmentcount
	   
from                          (	   
                					SELECT  'TAS117_201'::text AS studyid,
                        					'TAS117_201'::text AS studyname,
                        					country_name::text AS sitecountry,
                        					country_code::text AS sitecountrycode,
                        					'Enrollment'::text AS category,
                        					'Monthly'::text AS frequency,
                        					null::date AS enddate,
                        					'Planned'::text AS type,
                        					null::NUMERIC AS recruitmentcount,
                        					extract(month from actual_date::date) as month,
	   				   			   			extract(year from actual_date::date) as year
                					from   tas117_201_ctms.country_milestones     
                        			where actual_date<>''
                               ) ccm, rec_count rc
                               
where ccm.sitecountrycode=rc.country_code
and   ccm.month=rc.month
and   ccm.year=rc.year
                               
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

