--DROP TABLE IF EXISTS "medical_review"."cqs_all_query";
--CREATE TABLE "medical_review"."cqs_all_query" AS
with included_site as (
select
    distinct studyid,
    siteid,
    sitename,
    sitecountry,
    siteregion
from
    cqs.site),
included_subjects as (
select
    distinct studyid,
    siteid,
    usubjid
from
    cqs.subject),
querydata as( (
select
    'TAS0612_101'::text as studyId,
    concat('TAS0612_101_', left("sitename"::text, strpos("sitename", '_') - 1))::text as siteId,
    "subjectname"::text as usubjId,
    "recordid"::text as queryId,
    "form"::text as formId,
    "folder"::text as visit,
    1::int as formseq,
    "log"::int as log_num,
    "field"::text as fieldId,
    "querytext"::text as querytext,
    "markinggroupname"::text as querytype,
    "name"::text as querystatus,
    "qryopendate" as queryopeneddate,
    "qryresponsedate" as queryresponsedate,
    "qrycloseddate" as querycloseddate,
    "qryopenby" as querycreator
from
    tas0612_101."stream_query_detail"
where
    lower(left("markinggroupname", 9)) = 'site from'
    or lower(right(trim("markinggroupname"), 7)) = 'to site')
union all (
select
'TAS120_201'::text as studyId,
concat('TAS120_201_', left("sitename"::text, strpos("sitename", '_') - 1))::text as siteId,
"subjectname"::text as usubjId,
"recordid"::text as queryId,
"form"::text as formId,
"folder"::text as visit,
1::int as formseq,
"log"::int as log_num,
"field"::text as fieldId,
"querytext"::text as querytext,
"markinggroupname"::text as querytype,
"name"::text as querystatus,
"qryopendate" as queryopeneddate,
"qryresponsedate" as queryresponsedate,
"qrycloseddate" as querycloseddate,
"qryopenby" as querycreator
from
TAS120_201."stream_query_detail"
where
lower(left("markinggroupname", 9)) = 'site from'
    or lower(right(trim("markinggroupname"), 7)) = 'to site')
union all (
select
'TAS120_202'::text as studyId,
concat('TAS120_202_', left("sitename"::text, strpos("sitename", '_') - 1))::text as siteId,
"subjectname"::text as usubjId,
"recordid"::text as queryId,
"form"::text as formId,
"folder"::text as visit,
1::int as formseq,
"log"::int as log_num,
"field"::text as fieldId,
"querytext"::text as querytext,
"markinggroupname"::text as querytype,
"name"::text as querystatus,
"qryopendate" as queryopeneddate,
"qryresponsedate" as queryresponsedate,
"qrycloseddate" as querycloseddate,
"qryopenby" as querycreator
from
TAS120_202."stream_query_detail"
where
lower(left("markinggroupname", 9)) = 'site from'
or lower(right(trim("markinggroupname"), 7)) = 'to site')
union all (
select
'TAS3681_101'::text as studyId,
concat('TAS3681101_', left("sitename"::text, strpos("sitename", '-') - 1))::text as siteId,
"subjectname"::text as usubjId,
"recordid"::text as queryId,
"form"::text as formId,
"folder"::text as visit,
1::int as formseq,
"log"::int as log_num,
"field"::text as fieldId,
"querytext"::text as querytext,
"markinggroupname"::text as querytype,
"name"::text as querystatus,
"qryopendate" as queryopeneddate,
"qryresponsedate" as queryresponsedate,
"qrycloseddate" as querycloseddate,
"qryopenby" as querycreator
from
tas3681_101."stream_query_detail"
where
lower(left("markinggroupname", 9)) = 'site from'
or lower(right(trim("markinggroupname"), 7)) = 'to site')),
sub1 as (
select
    q.studyId::text as studyId,
    s.siteId::text as siteId,
    q.usubjId::text as usubjId,
    q.queryId::text as queryId,
    q.formId::text as formId,
    q.fieldId::text as fieldId,
    q.querytext::text as querytext,
    q.querytype::text as querytype,
    q.querystatus::text as querystatus,
    q.queryopeneddate as queryopeneddate,
    q.queryresponsedate as queryresponsedate,
    q.querycloseddate as querycloseddate,
    q.visit::text as visit,
    q.formseq::int as formseq,
    q.log_num::int as log_num,
    q.querycreator:: text as querycreator,
    si.sitename::text as sitename,
    si.sitecountry::text as sitecountry,
    si.siteregion::text as siteregion
from
    querydata q
join included_subjects s on
    (q.studyid = s.studyid
        and q.siteid = s.siteid
        and q.usubjid = s.usubjid)
join included_site si on
    (q.studyid = si.studyid
        and q.siteid = si.siteid)),
sub2 as (
select
    *,
    row_number () over(partition by studyId,
    siteId,
    usubjId,
    queryId,
    fieldId
order by
    queryopeneddate) ::integer seq
from
    sub1)
select
    studyId,
    siteId,
    usubjId,
    queryId,
    formId,
    fieldId,
    querytext,
    querytype,
    querystatus,
    querycreator,
    convert_to_date(queryopeneddate)as queryopeneddate,
    convert_to_date(queryresponsedate) as queryresponsedate,
    convert_to_date(querycloseddate) as querycloseddate,
    visit,
    formseq,
    log_num,
    sitename,
    sitecountry,
    siteregion,
    (studyid||'~'||siteid||'~'||usubjid||'~'||queryId||'~'||fieldId||'~'||seq) as objectuniquekey
from
    sub2
    
--ALTER SCHEMA "medical_review" OWNER TO "taiho-stage-app";
--ALTER TABLE "medical_review"."cqs_all_query" OWNER TO "taiho-stage-app-clinical-master-write";

