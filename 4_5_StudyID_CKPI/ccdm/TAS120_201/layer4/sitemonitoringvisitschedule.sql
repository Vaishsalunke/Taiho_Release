/*
CCDM SiteMonitoringVisitSchedule mapping
Notes: Standard mapping to CCDM SiteMonitoringVisitSchedule table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site ),

     sitemonitoringvisitschedule_data AS (
               SELECT  'TAS-120-201'::text AS studyid,
                        concat('TAS120_201_',site_id)::text AS siteid,
						 "site_status"::text  as visitname,
						 --case when "siv_plannned_date"='NULL' then Null else "siv_plannned_date" end ::date AS plannedvisitdate,
						 "siv_plannned_date" ::date AS plannedvisitdate,
						 "siv_actual_or_planned"::text as smvvtype
				 from tas120_201_ctms.site_closeout
				 where "siv_plannned_date" <> 'NULL' and  "site_status" not in ('Site Ready to Enroll','IRB/IEC Approval'))

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



