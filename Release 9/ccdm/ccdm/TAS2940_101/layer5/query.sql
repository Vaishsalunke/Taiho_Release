/*
CCDM Query mapping
Notes: Standard mapping to CCDM Query table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),
                
     included_site AS (
			SELECT DISTINCT studyid, siteid, sitename, sitecountry, sitecountrycode, siteregion from site),

     query_data AS (
                SELECT   substring(trim(study),1,position ('-' in study)-2)::text AS studyid,
                        'TAS2940_101'::text AS studyname,
                        concat(substring(trim(study),1,position ('-' in study)-2),'_',split_part(sitename,'_',1))::text AS siteId,
                        null::text AS sitename,
                        null::text AS sitecountry,
                        null::text AS sitecountrycode,
                        null::text AS siteregion,
                        "subjectname" ::text AS usubjId,
                        id_ ::text AS queryId,----in USDM it is ID
                        form ::text AS formId,
                        field ::text AS fieldId,
                        querytext ::text AS querytext,
                        markinggroupname ::text AS querytype,
                        "name" ::text AS querystatus,
                        nullif (qryopendate,'') ::date AS queryopeneddate,
                        nullif (qryresponsedate,'') ::date AS queryresponsedate,
                        nullif (qrycloseddate,'') ::date AS querycloseddate,
                        folder::text AS visit,
                        1::int AS formseq,
                        log ::int AS log_num,
                        null::text AS querycreator
                        from TAS2940_101.stream_query_detail sqd )
                        
      		

SELECT 
        /*KEY (q.studyid || '~' || q.siteid || '~' || q.usubjid)::text AS comprehendid, KEY*/
        q.studyid::text AS studyid,
        q.studyname::text AS studyname,
        q.siteid::text AS siteid,
        si.sitename::text AS sitename,
        si.sitecountry::text AS sitecountry,
        si.sitecountrycode::text AS sitecountrycode,
        si.siteregion::text AS siteregion,
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
        /*KEY, (q.studyid || '~' || q.siteid || '~' || q.usubjid || '~' || q.queryid)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM query_data q
JOIN included_subjects s ON (q.studyid = s.studyid AND 
q.siteid = s.siteid AND 
q.usubjid = s.usubjid)
JOIN included_site si ON (q.studyid = si.studyid AND q.siteid = si.siteid);



