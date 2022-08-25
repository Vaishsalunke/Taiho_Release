/*
CCDM Query mapping
Notes: Standard mapping to CCDM Query table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     query_data AS (
                SELECT  null::text AS studyId,
                        null::text AS studyname,
                        null::text AS siteId,
                        null::text AS sitename,
                        null::text AS sitecountry,
                        null::text AS sitecountrycode,
                        null::text AS siteregion,
                        null::text AS usubjId,
                        null::text AS queryId,
                        null::text AS formId,
                        null::text AS fieldId,
                        null::text AS querytext,
                        null::text AS querytype,
                        null::text AS querystatus,
                        null::date AS queryopeneddate,
                        null::date AS queryresponsedate,
                        null::date AS querycloseddate,
                        null::text AS visit,
                        null::int AS formseq,
                        null::int AS log_num,
                        null::text AS querycreator )

SELECT 
        /*KEY (q.studyid || '~' || q.siteid || '~' || q.usubjid)::text AS comprehendid, KEY*/
        q.studyId::text AS studyId,
        q.studyname::text AS studyname,
        q.siteId::text AS siteId,
        q.sitename::text AS sitename,
        q.sitecountry::text AS sitecountry,
        q.sitecountrycode::text AS sitecountrycode,
        q.siteregion::text AS siteregion,
        q.usubjId::text AS usubjId,
        q.queryId::text AS queryId,
        q.formId::text AS formId,
        q.fieldId::text AS fieldId,
        q.querytext::text AS querytext,
        q.querytype::text AS querytype,
        q.querystatus::text AS querystatus,
        q.queryopeneddate::date AS queryopeneddate,
        q.queryresponsedate::date AS queryresponsedate,
        q.querycloseddate::date AS querycloseddate,
        q.visit::text AS visit,
        q.formseq::int AS formseq,
        q.log_num::int AS log_num,
        q.querycreator::text AS querycreator
        /*KEY , (q.studyid || '~' || q.siteid || '~' || q.usubjid || '~' || q.queryid)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM query_data q
JOIN included_subjects s ON (q.studyid = s.studyid AND q.siteid = s.siteid AND q.usubjid = s.usubjid)
WHERE 1=2;
