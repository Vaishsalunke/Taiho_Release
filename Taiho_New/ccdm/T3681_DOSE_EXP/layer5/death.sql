/*
CCDM Death mapping
Notes: Standard mapping to CCDM Death table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     death_data AS (
                SELECT  null::text AS studyid,
                        null::text AS siteid,
                        null::text AS usubjid,
                        null::text AS ddseq,
                        null::text AS ddtestcd,
                        null::text AS ddtest,
                        null::text AS ddorres,
                        null::text AS ddstresc,
                        null::text AS dddtc,
                        null::text AS dddy)

SELECT 
        /*KEY (d.studyid || '~' || d.siteid || '~' || d.usubjid)::text AS comprehendid, KEY*/
        d.studyid::text AS studyid,
        d.siteid::text AS siteid,
        d.usubjid::text AS usubjid,
        d.ddseq::text AS ddseq,
        d.ddtestcd::text AS ddtestcd,
        d.ddtest::text AS ddtest,
        d.ddorres::text AS ddorres,
        d.ddstresc::text AS ddstresc,
        d.dddtc::text AS dddtc,
        d.dddy::text AS dddy
        /*KEY , (d.studyid || '~' || d.siteid || '~' || d.usubjid)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM death_data d
JOIN included_subjects s ON (s.studyid = d.studyid AND s.siteid = d.siteid AND s.usubjid = d.usubjid)
WHERE 1=2; 

