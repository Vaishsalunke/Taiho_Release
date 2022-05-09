/*
CCDM SitePlannedStatistic mapping
Notes: Standard mapping to CCDM SitePlannedStatistic table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site),

     siteplannedstatistic_data AS (
                SELECT  null::text AS studyid,
                        null::text AS siteid,
                        null::text AS statcat,
                        null::text AS statsubcat,
                        null::NUMERIC AS statval,
                        null::text AS statunit )

SELECT 
        /*KEY (sps.studyid || '~' || sps.siteid)::text AS comprehendid, KEY*/
        sps.studyid::text AS studyid,
        sps.siteid::text AS siteid,
        sps.statcat::text AS statcat,
        sps.statsubcat::text AS statsubcat,
        sps.statval::NUMERIC AS statval,
        sps.statunit::text AS statunit
        /*KEY , (sps.studyid || '~' || sps.siteid || '~' || sps.statCat)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM siteplannedstatistic_data sps
JOIN included_sites si ON (sps.studyid = si.studyid AND sps.siteid = si.siteid)
WHERE 1=2; 
