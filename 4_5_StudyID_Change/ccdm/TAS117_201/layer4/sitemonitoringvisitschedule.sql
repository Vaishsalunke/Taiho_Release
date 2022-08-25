/*
CCDM SiteMonitoringVisitSchedule mapping
Notes: Standard mapping to CCDM SiteMonitoringVisitSchedule table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site ),

     sitemonitoringvisitschedule_data AS (
               SELECT distinct 'TAS117-201'::text AS studyid,
                        concat('TAS117_201_',split_part("site_number",'_',2))::text AS siteid,
                        "visit_name"::text AS visitname,
                        "planned_visit_date"::date AS plannedvisitdate,
						"visit_type"::text as smvvtype
			from TAS117_201_ctms.site_visits
			where nullif("planned_visit_date",'') is not null)

SELECT 
        /*KEY (smvs.studyid || '~' || smvs.siteid)::text AS comprehendid, KEY*/
        smvs.studyid::text AS studyid,
        smvs.siteid::text AS siteid,
        smvs.visitname::text AS visitname,
        smvs.plannedvisitdate::date AS plannedvisitdate,
		smvs.smvvtype::text as smvvtype
        /*KEY  , (smvs.studyid || '~' || smvs.siteid || '~' || smvs.visitname)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM sitemonitoringvisitschedule_data smvs
JOIN included_sites si ON (smvs.studyid = si.studyid AND smvs.siteid = si.siteid);


