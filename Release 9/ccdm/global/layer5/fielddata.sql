/*
CCDM FieldData mapping
Notes: Mapping to CCDM FieldData table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     fielddata_data AS (
                SELECT  null::text AS studyid,
                        null::text AS siteid,
                        null::text AS usubjid,
                        null::text AS formid,
                        null::integer AS formseq,
                        null::text AS fieldid,
                        null::integer AS fieldseq,
                        null::text AS visit,
                        1::integer AS visitseq, /* defaulted to 1 - deprecated */
                        null::integer AS log_num,
                        null::text AS datavalue,
                        null::date AS dataentrydate,
                        null::date AS datacollecteddate,
                        null::date AS sdvdate, 
                        null::text AS sourcerecordprimarykey, 
                        null::timestamp AS sourcerecorddate,
                        null::boolean AS isdeleted  /* Internal Field - leave as null */ )

SELECT 
        /*KEY (fd.studyid || '~' || fd.siteid || '~' || fd.usubjid)::text AS comprehendid, KEY*/
        fd.studyid::text AS studyid,
        fd.siteid::text AS siteid,
        fd.usubjid::text AS usubjid,
        fd.formid::text AS formid,
        fd.formseq::integer AS formseq,
        fd.fieldid::text AS fieldid,
        fd.fieldseq::integer AS fieldseq,
        fd.visit::text AS visit,
        fd.visitseq::integer AS visitseq, 
        fd.log_num::integer AS log_num,
        fd.datavalue::text AS datavalue,
        fd.dataentrydate::date AS dataentrydate,
        fd.datacollecteddate::date AS datacollecteddate,
        fd.sdvdate::date AS sdvdate,
        fd.sourcerecordprimarykey::text AS sourcerecordprimarykey,
        fd.sourcerecorddate::timestamp AS sourcerecorddate,
        fd.isdeleted::boolean AS isdeleted 
        /*KEY , (fd.studyid || '~' || fd.siteid || '~' || fd.usubjid || '~' || '~' || fd.visit || '~' || fd.formid || '~' || fd.formseq || '~' || fd.fieldid || '~' || fd.fieldseq || '~' || fd.log_num)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM fielddata_data fd
JOIN included_subjects s ON (fd.studyid = s.studyid AND fd.siteid = s.siteid AND fd.usubjid = s.usubjid)
WHERE 1=2;
