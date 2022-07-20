/*
CCDM Cohortplannedrecruitment mapping
Notes: Standard mapping to CCDM Cohortplannedrecruitment table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site ),

     cohortplannedrecruitment_data AS (
                SELECT  null::text AS studyid,
                        null::text AS siteid,
                        null::text AS countrycode,
                        null::text AS cohortid,
                        null::text AS milestonelevel,
                        null::text AS frequency,
                        null::text AS category,
                        null::text AS metricid,
                        null::text AS metricname,
                        null::text AS metrictype,
                        null::text AS metricdate,
                        null::numeric AS recruitmentcount)

SELECT 
        /*KEY (cpr.studyid || '~' || cpr.cohortid || '~' || cpr.siteid)::text AS comprehendid, KEY*/
        cpr.studyid::text AS studyid,
        cpr.siteid::text AS siteid,
        cpr.countrycode::text AS countrycode,
        cpr.cohortid::text AS cohortid,
        cpr.milestonelevel::text AS milestonelevel,
        cpr.frequency::text AS frequency,
        cpr.category::text AS category,
        cpr.metricid::text AS metricid,
        cpr.metricname::text AS metricname,
        cpr.metrictype::text AS metrictype,
        cpr.metricdate::text AS metricdate,
        cpr.recruitmentcount::numeric AS recruitmentcount
        /*KEY , (cpr.studyid || '~' || cpr.cohortid || '~' || cpr.siteid || '~' || cpr.countrycode || '~' || cpr.milestonelevel || '~' || cpr.frequency || '~'|| cpr.category || '~' || cpr.metrictype || '~' || cpr.metricdate)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM cohortplannedrecruitment_data cpr
JOIN included_sites s ON (s.studyid = cpr.studyid AND s.siteid = cpr.siteid)
WHERE 1=2; 

