/*
CCDM studyplannedrecruitment mapping
Notes: Standard mapping to CCDM studyplannedrecruitment table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),

subject_count as (
					select count(*) as subject_count,
						   extract(month from IEDAT::date) as month,
						   extract(year from IEDAT::date) as year
						   
					from (						   
							select coalesce("MinCreated","RecordDate") as IEDAT
							from   tas2940_101."IE"
							where "IEYN"='Yes'
						  )w group by 2,3

				  ),
				 
max_date_subject as (
						select month,
							   year,
							   max(IEDAT) as IEDAT
						from (
								select extract(month from IEDAT::date) as month,
						   			   extract(year from IEDAT::date) as year,
						   			   IEDAT::date as IEDAT
						   		from   (
						   					select coalesce("MinCreated","RecordDate") as IEDAT
											from   tas2940_101."IE"
						   				)	w   
						
							  )	e group by 1,2 

					  )		,
					  
					  
site_count as (
				select count(*) as site_count,
					   extract (month from "siv"::date) as month,
					   extract (year from "siv"::date) as year
   			    from tas2940_101_ctms.site_startup_metrics
   			    where trim(site_status_icon)='Ongoing'
				group by 2,3
				

				)	,
				
max_date_site as (
					select month,
						   year,
						   max("siv") as "siv"
					from (
							select extract (month from "siv"::date) as month,
					   			   extract (year from "siv"::date) as year,
					   			   "siv"::date
					   		from tas2940_101_ctms.site_startup_metrics	   
							)e group by 1,2	   

				  ),
				  
				  
screen_count as (
					select count(*) as screen_count,
						   extract (month from DMDAT::date) as month,
					   	   extract (year from DMDAT::date) as year
					from (
					   	   select coalesce("MinCreated","RecordDate") as DMDAT
					   	   from tas2940_101."DM" where "Folder"='SCRN'   
					   	 )e
					group by 2,3   	 

					
					
					)	,
					
max_date_screen as (
					select month,
						   year,
						   max(DMDAT) as DMDAT
					from (	   
							select extract (month from DMDAT::date) as month,
					   	   		   extract (year from DMDAT::date) as year,
					   	  		   DMDAT::date
							from (   	   
									select coalesce("MinCreated","RecordDate")::date as DMDAT
									from tas2940_101."DM" 

						  			)e	
						  			
						  	) r group by 1,2
						  	
						  )	,
					
studyplannedrecruitment_data AS (
                SELECT  'TAS2940_101'::text AS studyid,
                        'Enrollment'::text AS category,
                        'Monthly'::text AS frequency,
                        mds.IEDAT::date AS enddate,
                        'Planned'::text AS type,
                        sc.subject_count::NUMERIC AS recruitmentcount
				 from max_date_subject mds , subject_count sc
				 where mds.month=sc.month 
				 and mds.year=sc.year
				 


union all 

				SELECT  'TAS2940_101'::text AS studyid,
                        'Site Activation'::text AS category,
                        'Monthly'::text AS frequency,
                        mds."siv"::date AS enddate,
                        'Planned'::text AS type,
                        sc.site_count::NUMERIC AS recruitmentcount
				from site_count sc , max_date_site mds
				where mds.month=sc.month 
				and mds.year=sc.year


union all 

				SELECT  'TAS2940_101'::text AS studyid,
                        'Screening'::text AS category,
                        'Monthly'::text AS frequency,
                        mds.DMDAT::date AS enddate,
                        'Planned'::text AS type,
                        sc.screen_count::NUMERIC AS recruitmentcount
				from screen_count sc , max_date_screen mds
				where mds.month=sc.month 
				and mds.year=sc.year )
				
				
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

