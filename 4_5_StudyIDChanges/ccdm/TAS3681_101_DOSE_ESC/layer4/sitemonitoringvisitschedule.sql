/*
 CCDM SiteMonitoringVisitSchedule mapping
 Notes: Standard mapping to CCDM SiteMonitoringVisitSchedule table
*/
WITH included_sites AS (
              SELECT DISTINCT studyid, siteid FROM site ),

   sitemonitoringvisitschedule_data AS (  
            SELECT   --protocol_id::text AS studyid,
                      'TAS3681_101_DOSE_ESC'::text AS studyid,
                      site_number::text AS siteid,
                      visit_name||'~' || row_number() OVER(partition by "visit_type","visit_name","site_number" ORDER BY "visit_start_date" ASC)::text AS visitname,
					  coalesce(nullif(planned_visit_date,''), nullif(visit_completed_date, ''))::date AS plannedvisitdate,
visit_type::text AS smvvtype
              from tas3681_101_ctms.site_visits
              )

SELECT
      /*KEY (smvs.studyid || '~' || smvs.siteid)::text AS comprehendid, KEY*/
      smvs.studyid::text AS studyid,
      smvs.siteid::text AS siteid,
      smvs.visitname::text AS visitname,
      smvs.plannedvisitdate::date AS plannedvisitdate,
      smvs.smvvtype::text AS smvvtype
      /*KEY, (smvs.studyid || '~' || smvs.siteid || '~' || smvs.visitname)::text  AS objectuniquekey KEY*/
      /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM sitemonitoringvisitschedule_data smvs
JOIN included_sites si ON (smvs.studyid = si.studyid AND smvs.siteid = si.siteid); 

