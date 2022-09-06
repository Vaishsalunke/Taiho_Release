/*
CCDM Source System Refresh Time Table mapping
Notes: Standard mapping to CCDM Source System Refresh Time table
*/

WITH included_studies AS (
        SELECT studyid from Study),
    ss_refresh_time_data AS (
        SELECT  null::text AS comprehendid,
                null::text AS studyid,
                null::text AS studyname,
                null::text AS source_system_name,
                null::timestamp AS source_system_refresh_time
                )

SELECT 
    /*KEY (ssrt.studyid ) ::text AS comprehendid, KEY*/ 
    ssrt.studyid::text AS studyid,
    ssrt.studyname::text AS studyname,
    ssrt.source_system_name::text AS source_system_name,
    ssrt.source_system_refresh_time::timestamp AS source_system_refresh_time
    /*KEY , (ssrt.studyid || '~' || ssrt.source_system_name )::text  AS objectuniquekey KEY*/
    /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM ss_refresh_time_data ssrt JOIN included_studies ON
(ssrt.studyid = included_studies.studyid);

