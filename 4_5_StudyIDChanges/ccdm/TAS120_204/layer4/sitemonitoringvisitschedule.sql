/*
CCDM SiteMonitoringVisitSchedule mapping
Notes: Standard mapping to CCDM SiteMonitoringVisitSchedule table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site ),

     sitemonitoringvisitschedule_data AS (
                select studyid,
					   siteid,
					   visitname||'~'||row_number()over(partition by visitname,siteid ORDER by startdate,smvvtype ASC) as visitname,
					   smvvtype,
					   plannedvisitdate
				from  (	   
						SELECT  'TAS-120-204'::text AS studyid,
								concat('TAS120_204_',split_part(site_number,'_',2))::text AS siteid,
								visit_name::text AS visitname,
								visit_type::text AS smvvtype,
								planned_visit_date::date AS plannedvisitdate,
								visit_start_date as startdate
                        
						from tas120_204_ctms.site_visits  
					  )c   				)

SELECT 
        /*KEY (smvs.studyid || '~' || smvs.siteid)::text AS comprehendid, KEY*/
        smvs.studyid::text AS studyid,
        smvs.siteid::text AS siteid,
        smvs.visitname::text AS visitname,
        smvs.smvvtype::text AS smvvtype,
        smvs.plannedvisitdate::date AS plannedvisitdate
        /*KEY , (smvs.studyid || '~' || smvs.siteid || '~' || smvs.visitname)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM sitemonitoringvisitschedule_data smvs
JOIN included_sites si ON (smvs.studyid = si.studyid AND smvs.siteid = si.siteid);

