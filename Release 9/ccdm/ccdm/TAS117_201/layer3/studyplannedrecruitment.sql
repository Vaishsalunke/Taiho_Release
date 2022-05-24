/*
CCDM studyplannedrecruitment mapping
Notes: Standard mapping to CCDM studyplannedrecruitment table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),

     subject_count as (
					select count(*) as subject_count,
						   extract(month from completed_date::date) as month,
						   extract(year from completed_date::date) as year
					from tas117_201_ctms.subjects
					where subject_status in ('Screening', 'Completed')
					and nullif(completed_date,'') is not null 
					group by 2,3
                  ),
                  
max_date_subject as (	select month, 
							   year,
							   max(completed_date) as completed_date
						from
						(
						select extract(month from completed_date::date) as month,
						   	   extract(year from completed_date::date) as year,
						   	   completed_date as completed_date
						from tas117_201_ctms.subjects
						where nullif(completed_date,'') is not null  
						 )e group by 1,2
					  ),                  


site_count as (
 				select count(*) as site_count, 
 					   extract (month from planned_visit_date::date) as month,
                	   extract (year from planned_visit_date::date) as year
 				from   tas117_201_ctms.site_visits
 				where visit_status in ('Completed','Projected','Scheduled')
 				and nullif(planned_visit_date,'') is not null
 				group by 2,3
 			   )	,
 			   
 	
 	
 	
max_date_site as ( 
					select month, 
						   year, 
						   max(planned_visit_date) as planned_visit_date
					from (
					select extract (month from planned_visit_date::date) as month,
                	   	   extract (year from planned_visit_date::date) as year,
                	   	   planned_visit_date as planned_visit_date
                	from   tas117_201_ctms.site_visits
 					where nullif(planned_visit_date,'') is not null
 						  )w group by 1,2


				  ),

screen_count as (
						  select count(*) as screen_count,
						  		 extract (month from screening_date::date) as month,
						  		 extract (year from screening_date::date) as year
						  from tas117_201_ctms.subject_visits
						  where visit_reference='SCR' 
						  and nullif(screening_date,'') is not null
						  group by 2,3
						 ),
						
max_date_screen as 		(
							select month, 
								   year,
								   max(screening_date) as screening_date
							from 
							(
							select extract (month from screening_date::date) as month,
						  		   extract (year from screening_date::date) as year,
						  		   screening_date as screening_date
						  	from tas117_201_ctms.subject_visits
						  	where nullif(screening_date,'') is not null
							)e
							group by 1,2


							 )	,					 

studyplannedrecruitment_data AS (
				 SELECT  'TAS117_201'::text AS studyid,
                        'Enrollment'::text AS category,
                        'Monthly'::text AS frequency,
                        mds.completed_date::date AS enddate,
                        'Planned'::text AS type,
                        sc.subject_count::NUMERIC AS recruitmentcount
				 from max_date_subject mds , subject_count sc
				 where mds.month=sc.month 
				 and mds.year=sc.year
				 


union all 

				SELECT  'TAS117_201'::text AS studyid,
                        'Site Activation'::text AS category,
                        'Monthly'::text AS frequency,
                        mds.planned_visit_date::date AS enddate,
                        'Planned'::text AS type,
                        site_count::NUMERIC AS recruitmentcount
				from site_count sc , max_date_site mds
				where mds.month=sc.month 
				and mds.year=sc.year


union all 

				SELECT  'TAS117_201'::text AS studyid,
                        'Screening'::text AS category,
                        'Monthly'::text AS frequency,
                        mds.screening_date::date AS enddate,
                        'Planned'::text AS type,
                        sc.screen_count::NUMERIC AS recruitmentcount
				from screen_count sc , max_date_screen mds
				where mds.month=sc.month 
				and mds.year=sc.year

									)

SELECT
        /*KEY spr.studyid::text AS comprehendid, KEY*/
        spr.studyid::text AS studyid,
        spr.category::text AS category,
        spr.frequency::text AS frequency,
        spr.enddate::date AS enddate,
        spr.type::text AS type,
        spr.recruitmentcount::NUMERIC AS recruitmentcount
        /*KEY ,(spr.studyid || '~' || spr.category || '~' || spr.frequency || '~' || spr.enddate || '~' || spr.type)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM studyplannedrecruitment_data spr
JOIN included_studies st ON (st.studyid = spr.studyid);


