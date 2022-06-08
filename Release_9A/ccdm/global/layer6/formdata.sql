/*
CCDM FormData mapping
Notes: Standard mapping to CCDM FormData table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     formdata_data AS (
                SELECT  null::text AS studyid,
                        null::text AS siteid,
                        null::text AS usubjid,
                        null::text AS formid,
                        null::integer AS formseq,
                        null::text AS visit,
                        1::integer AS visitseq, /* defaulted to 1 - deprecated */
                        null::date AS dataentrydate,
                        null::date AS datacollecteddate,
                        null::date AS sdvdate )

SELECT 
        /*KEY (fd.studyid || '~' || fd.siteid || '~' || fd.usubjid)::text AS comprehendid, KEY*/
        fd.studyid::text AS studyid,
        fd.siteid::text AS siteid,
        fd.usubjid::text AS usubjid,
        fd.formid::text AS formid,
        fd.formseq::integer AS formseq,
        fd.visit::text AS visit,
        fd.visitseq::integer AS visitseq,
        fd.dataentrydate::date AS dataentrydate,
        fd.datacollecteddate::date AS datacollecteddate,
        fd.sdvdate::date AS sdvdate
        /*KEY , (fd.studyid || '~' || fd.siteid || '~' || fd.usubjid || '~' || fd.formSeq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM formdata_data fd
JOIN included_subjects s ON (fd.studyid = s.studyid AND fd.siteid = s.siteid AND fd.usubjid = s.usubjid)
WHERE 1=2;
