/*
CCDM SitePlannedEnrollment mapping
Notes: Standard mapping to CCDM SitePlannedEnrollment table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site ),

     siteplannedenrollment_data AS (
                SELECT  null::text AS studyid,
                        null::text AS siteid,
                        null::text AS frequency,
                        null::date AS enddate,
                        null::text AS enrollmenttype,
                        null::NUMERIC AS enrollmentcount )

SELECT 
        /*KEY (spe.studyid || '~' || spe.siteid)::text AS comprehendid, KEY*/
        spe.studyid::text AS studyid,
        spe.siteid::text AS siteid,
        spe.frequency::text AS frequency,
        spe.enddate::date AS enddate,
        spe.enrollmenttype::text AS enrollmenttype,
        spe.enrollmentcount::NUMERIC AS enrollmentcount
        /*KEY , (spe.studyid || '~' || spe.siteid || '~' || spe.enrollmentType || '~' || spe.frequency || '~' || spe.endDate)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM siteplannedenrollment_data spe
JOIN included_sites si ON (spe.studyid = si.studyid AND spe.siteid = si.siteid)
WHERE 1=2; 
