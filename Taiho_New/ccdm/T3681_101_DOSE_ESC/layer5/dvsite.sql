/*
CCDM DV mapping
Notes: Standard mapping to CCDM DV SITE table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site),

     dv_data AS (
                SELECT  null::text AS studyid,
                        null::text AS siteid,
                        null::integer AS dvseq,
                        null::text AS dvcat,
                        null::text AS dvterm,
                        null::date AS dvstdtc,
                        null::date AS dvendtc,
                        null::text AS dvscat,
                        null::text AS dvid )

SELECT
        /*KEY (dv.studyid || '~' || dv.siteid)::text AS comprehendid, KEY*/
        dv.studyid::text AS studyid,
        dv.siteid::text AS siteid,
        dv.dvseq::integer AS dvseq,
        dv.dvcat::text AS dvcat,
        dv.dvterm::text AS dvterm,
        dv.dvstdtc::date AS dvstdtc,
        dv.dvendtc::date AS dvendtc,
        dv.dvscat::text AS dvscat,
        dv.dvid::text AS dvid
        /*KEY , (dv.studyid || '~' || dv.siteid || '~' || '~' || dv.dvseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM dv_data dv
JOIN included_sites s ON (dv.studyid = s.studyid AND dv.siteid = s.siteid)
WHERE 1=2;
