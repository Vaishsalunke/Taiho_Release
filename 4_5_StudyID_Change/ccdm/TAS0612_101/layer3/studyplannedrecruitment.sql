   /*
CCDM studyplannedrecruitment mapping
Notes: Standard mapping to CCDM studyplannedrecruitment table
*/


WITH included_studies AS (
                SELECT studyid FROM study ),
                
 Subject_count AS (
               select count(*) as sub_cnt, extract (month from COALESCE("MinCreated" ,"RecordDate")::date) as month,
                extract (year from COALESCE("MinCreated" ,"RecordDate")::date) as year from tas0612_101."IE" 
                where "IEYN" = 'Yes'
                group by 2,3),
                
 max_date_sub as (
 select month, year, max(completed_date) as completed_date from (	
select distinct COALESCE("MinCreated" ,"RecordDate") as completed_date,extract(month from COALESCE("MinCreated" ,"RecordDate")::date) as month,
extract(year from COALESCE("MinCreated" ,"RecordDate")::date) as year
from tas0612_101."IE"
)a group by 1, 2
 
 ),
                
 site_count AS (
                select count(*) as site_cnt, extract (month from "siv"::date) as month,
                extract (year from "siv"::date) as year
                from   tas0612_101_ctms.site_startup_metrics
				where trim(site_status_icon) = 'Ongoing' 
			 and "siv" not in('NULL','','N/A') 
			 and "siv" is not null
				group by 2,3
                ),
                
 max_date_site as ( 
 select month, year, max(planned_visit_date) as planned_visit_date from (	
select distinct "siv"::date as planned_visit_date,extract(month from "siv"::date) as month,
extract(year from "siv"::date) as year
from   tas0612_101_ctms.site_startup_metrics
where  "siv" not in('NULL','','N/A') 
			 and "siv" is not null
)a group by 1, 2	
               
 ),
 
                
screen_count AS (
                 select count(*) as screen_cnt, extract (month from "DMICDAT"::date) as month,
                extract (year from "DMICDAT"::date) as year 
               From tas0612_101."DM"
           		 where "Folder" = 'SCRN'
				group by 2,3),
				
max_date_scr as (
 select month, year, max(screening_date) as screening_date from (	
select distinct "DMICDAT"::date as screening_date,extract(month from "DMICDAT"::date) as month,
extract(year from "DMICDAT"::date) as year
From tas0612_101."DM"
)a group by 1, 2	
 
 ),


     studyplannedrecruitment_data AS (
				SELECT  'TAS0612-101'::text AS studyid,
                        'Enrollment'::text AS category,
                        'Monthly'::text AS frequency,
                       msub.completed_date::date AS enddate,
                        'Planned'::text AS type,
                        sc.sub_cnt::int AS recruitmentcount
                From  max_date_sub msub,  Subject_count sc
                where sc.month = msub.month
				and sc.year = msub.year 
	 
            union all
                SELECT  'TAS0612-101'::text AS studyid,
                        'Site Activation'::text AS category,
                        'Monthly'::text AS frequency,                        
                         msite.planned_visit_date::date AS enddate,
                        'Planned'::text AS type,
                        sc.site_cnt ::int AS recruitmentcount
				From  max_date_site msite,  site_count sc
                where sc.month = msite.month
				and sc.year = msite.year 
				
			   
            union all
			SELECT  'TAS0612-101'::text AS studyid,
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
        spr.recruitmentcount::numeric AS recruitmentcount
        /*KEY ,(spr.studyid || '~' || spr.category || '~' || spr.frequency || '~' || spr.enddate || '~' || spr.type)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM studyplannedrecruitment_data spr
JOIN included_studies st ON (st.studyid = spr.studyid); 
 
 