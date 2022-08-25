/*
CCDM studyplannedrecruitment mapping
Notes: Standard mapping to CCDM studyplannedrecruitment table
*/


WITH included_studies AS (
                SELECT studyid FROM study ),
                
 Subject_count AS (
               select count(*) as sub_cnt, extract (month from completed_date::date) as month,
                extract (year from completed_date::date) as year FROM tas3681_101_ctms.subjects 
                where subject_status in ('Completed','Screening')
                and nullif(completed_date,'') is not null
                group by 2,3),
                
 max_date_sub as (
 select month, year, max(completed_date) as completed_date from (	
select distinct completed_date::date as completed_date,extract(month from completed_date::date) as month,
extract(year from completed_date::date) as year
from tas3681_101_ctms.subjects
 where nullif(completed_date,'') is not null
)a group by 1, 2
 
 ),
                
 site_count AS (
                select count(*) as site_cnt, extract (month from planned_visit_date::date) as month,
                extract (year from planned_visit_date::date) as year
                from   tas3681_101_ctms.site_visits
				where visit_status in ('Completed','Projected','Scheduled')
				group by 2,3
                ),
                
 max_date_site as ( 
 select month, year, max(planned_visit_date) as planned_visit_date from (	
select distinct planned_visit_date::date as planned_visit_date,extract(month from planned_visit_date::date) as month,
extract(year from planned_visit_date::date) as year
from tas3681_101_ctms.site_visits
)a group by 1, 2
               
 ),
 
                
screen_count AS (
                 select count(*) as screen_cnt, extract (month from convert_to_date(screening_date)) as month,
                extract (year from convert_to_date(screening_date)) as year FROM tas3681_101_ctms.subject_visits
				where visit_reference = 'SCR'
				and screening_date <> ''
				group by 2,3),
				
max_date_scr as (
 select month, year, max(screening_date) as screening_date from (	
select distinct convert_to_date(screening_date) as screening_date,extract(month from convert_to_date(screening_date)) as month,
extract(year from convert_to_date(screening_date)) as year
from tas3681_101_ctms.subject_visits
where screening_date <>''
)a group by 1, 2
 
 ),


     studyplannedrecruitment_data AS (
                SELECT  'TAS3681_101_DOSE_EXP'::text AS studyid,
                        'Enrollment'::text AS category,
                        'Monthly'::text AS frequency,
                       msub.completed_date::date AS enddate,
                        'Planned'::text AS type,
                        sc.sub_cnt ::int AS recruitmentcount
               From  max_date_sub msub,  Subject_count sc
                where sc.month = msub.month
            and sc.year = msub.year
              
               union all
               
                   SELECT  'TAS3681_101_DOSE_EXP'::text AS studyid,
                        'Site Activation'::text AS category,
                        'Monthly'::text AS frequency,
                        msite.planned_visit_date::date AS enddate,
                        'Planned'::text AS type,
                        sc.site_cnt ::int AS recruitmentcount
            From site_count sc, max_date_site msite
            where sc.month = msite.month
            and sc.year = msite.year
			 union all
                   SELECT  'TAS3681_101_DOSE_EXP'::text AS studyid,
                        'Screening'::text AS category,
                        'Monthly'::text AS frequency,
                        mscr.screening_date::date AS enddate,
                        'Planned'::text AS type,
                        sc.screen_cnt ::int AS recruitmentcount
            from max_date_scr mscr , screen_count sc
             where sc.month = mscr.month
            and sc.year = mscr.year
                )

SELECT
        /*KEY spr.studyid::text AS comprehendid, KEY*/
        spr.studyid::text AS studyid,
        spr.category::text AS category,
        spr.frequency::text AS frequency,
        spr.enddate::date AS enddate,
        spr.type::text AS type,
        spr.recruitmentcount::int AS recruitmentcount
        /*KEY ,(spr.studyid || '~' || spr.category || '~' || spr.frequency || '~' || spr.enddate || '~' || spr.type)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM studyplannedrecruitment_data spr
JOIN included_studies st ON (st.studyid = spr.studyid); 


