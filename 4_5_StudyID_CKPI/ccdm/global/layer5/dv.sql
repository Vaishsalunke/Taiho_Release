/*
CCDM DV mapping
Notes: Standard mapping to CCDM DV table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     dv_data AS (
                SELECT  null::text AS studyid,
                        null::text AS siteid,
                        null::text AS usubjid,
                        null::text AS visit,
                        null::text AS formid,
                        null::integer AS dvseq,
                        null::text AS dvcat,
                        null::text AS dvterm,
                        null::date AS dvstdtc,
                        null::date AS dvendtc,
                        null::text AS dvscat,
                        null::text AS dvid,
                        null::text AS dvcls)

SELECT 
        /*KEY (dv.studyid || '~' || dv.siteid || '~' || dv.usubjid)::text AS comprehendid, KEY*/
        dv.studyid::text AS studyid,
        dv.siteid::text AS siteid,
        dv.usubjid::text AS usubjid,
        dv.visit::text AS visit,
        dv.formid::text AS formid,
        dv.dvseq::integer AS dvseq,
        dv.dvcat::text AS dvcat,
        dv.dvterm::text AS dvterm,
        dv.dvstdtc::date AS dvstdtc,
        dv.dvendtc::date AS dvendtc,
        dv.dvscat::text AS dvscat,
        dv.dvid::text AS dvid,
        dv.dvcls::text AS dvcls
        /*KEY , (dv.studyid || '~' || dv.siteid || '~' || dv.usubjid || '~' || dv.dvseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM dv_data dv
JOIN included_subjects s ON (dv.studyid = s.studyid AND dv.siteid = s.siteid AND dv.usubjid = s.usubjid)
WHERE 1=2;  
