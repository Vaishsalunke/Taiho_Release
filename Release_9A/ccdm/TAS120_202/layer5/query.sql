/*
CCDM Query mapping
Notes: Standard mapping to CCDM Query table
*/

WITH included_site AS (
SELECT DISTINCT studyid, siteid, sitename, sitecountry, sitecountrycode, siteregion FROM site),
included_subjects AS (
SELECT DISTINCT studyid, siteid, usubjid FROM subject),
querydata as(SELECT 'TAS-120-202'::text AS studyId,
concat('TAS120_202_',left("sitename"::text, strpos("sitename", '_') - 1))::text AS siteId,
"subjectname"::text AS usubjId,
"id_"::text AS queryId,
"form"::text AS formId,
"folder"::text AS visit,
1::int AS formseq,
"log"::int AS log_num,
"field"::text AS fieldId,
"querytext"::text AS querytext,
"markinggroupname"::text AS querytype,
"name"::text AS querystatus,
convert_to_date("qryopendate")::date AS queryopeneddate,
convert_to_date("qryresponsedate")::date AS queryresponsedate,
convert_to_date("qrycloseddate")::date AS querycloseddate,
"qryopenby":: text as querycreator
FROM TAS120_202."stream_query_detail"
WHERE lower(left("markinggroupname", 9)) = 'site from'
OR lower(right(trim("markinggroupname"), 7)) = 'to site')

SELECT
/*KEY (q.studyid || '~' || q.siteid || '~' || q.usubjid)::text AS comprehendid, KEY*/
q.studyId::text AS studyId,
null::text AS studyname,
q.siteId::text AS siteId,
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
/*KEY , (q.studyid || '~' || q.siteid || '~' || q.usubjid || '~' || q.queryid)::text AS objectuniquekey KEY*/
/*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
FROM querydata q
JOIN included_subjects s ON (q.studyid = s.studyid AND q.siteid = s.siteid AND q.usubjid = s.usubjid)
JOIN included_site si ON (q.studyid = si.studyid AND q.siteid = si.siteid);

