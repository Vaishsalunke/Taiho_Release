/*
CCDM SiteMonitoringVisitSchedule mapping
Notes: Standard mapping to CCDM SiteMonitoringVisitSchedule table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site ),

     sitemonitoringvisitschedule_data AS (
                SELECT  null::text AS studyid,
                        null::text AS siteid,
                        null::text AS visitname,
                        null::text AS smvvtype,
                        null::date AS plannedvisitdate )

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
JOIN included_sites si ON (smvs.studyid = si.studyid AND smvs.siteid = si.siteid)
WHERE 1=2; 
