/*
CCDM SV mapping
Notes: Standard mapping to CCDM SV table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),
sv_data AS (
Select
sv.studyid,
sv.siteid,
sv.usubjid,
sv.visitnum,
sv.visit,
sv.visitseq,
sv.svstdtc,
sv.svendtc


from(
SELECT  "project"::text AS studyid,
"SiteNumber"::text AS siteid,
"Subject"::text AS usubjid,
"FolderSeq"::numeric AS visitnum,
"InstanceName"::text AS visit,
null::int AS visitseq, /* defaulted to 1 - deprecated */
min("VISITDAT")::date AS svstdtc,
                        max("VISITDAT")::date AS svendtc
from tas120_202."VISIT"
group by 1,2,3,4,5,6

union all

SELECT  "project"::text AS studyid,
"SiteNumber"::text AS siteid,
"Subject"::text AS usubjid,
"FolderSeq"::numeric AS visitnum,
"InstanceName"::text AS visit,
null::int AS visitseq, /* defaulted to 1 - deprecated */
min("VISITDAT")::date AS svstdtc,
                        max("VISITDAT")::date AS svendtc
from tas120_202."VISIT1"
group by 1,2,3,4,5,6
union all

SELECT  "project"::text AS studyid,
"SiteNumber"::text AS siteid,
"Subject"::text AS usubjid,
"FolderSeq"::numeric AS visitnum,
"InstanceName"::text AS visit,
null::int AS visitseq, /* defaulted to 1 - deprecated */
min("VISITDAT")::date AS svstdtc,
                        max("VISITDAT")::date AS svendtc
from tas120_202."VISIT2"
group by 1,2,3,4,5,6

union all

SELECT  "project"::text AS studyid,
"SiteNumber"::text AS siteid,
"Subject"::text AS usubjid,
"FolderSeq"::numeric AS visitnum,
"InstanceName"::text AS visit,
null::int AS visitseq, /* defaulted to 1 - deprecated */
min("VISITDAT")::date AS svstdtc,
                        max("VISITDAT")::date AS svendtc
from tas120_202."VISIT3"
group by 1,2,3,4,5,6

union all

SELECT  "project"::text AS studyid,
"SiteNumber"::text AS siteid,
"Subject"::text AS usubjid,
"FolderSeq"::numeric AS visitnum,
"InstanceName"::text AS visit,
null::int AS visitseq, /* defaulted to 1 - deprecated */
min("VISITDAT")::date AS svstdtc,
                        max("VISITDAT")::date AS svendtc
from tas120_202."VISIT4"
group by 1,2,3,4,5,6) sv
)
,formdata_visits AS (SELECT DISTINCT fd.studyid,
                                    fd.siteid,
                                    fd.usubjid,
                                    99::numeric AS visitnum, -- will be updated by cleanup script
                                    fd.visit,
                                    coalesce(datacollecteddate,dataentrydate)::date AS svstdtc,
                                    coalesce(datacollecteddate,dataentrydate)::date AS svendtc
                            FROM formdata fd
                            LEFT JOIN sv_data sd ON (fd.studyid = sd.studyid and fd.siteid = sd.siteid and fd.usubjid = sd.usubjid and trim(fd.visit) = trim(sd.visit))
                            WHERE sd.studyid IS NULL AND fd.studyid='TAS120_202'
                        ),

all_visits AS (
                        SELECT studyid,
                                siteid,
                                usubjid,
                                visitnum,
                                trim(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(visit,'<W[0-9]DA[0-9]/>\sExpansion',''),'<WK[0-9]DA[0-9]/>\sExpansion',''),'<WK[0-9]DA[0-9][0-9]/>\sExpansion',''), '<W[0-9]DA[0-9][0-9]/>\sExpansion',''), '<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),' Escalation ',' '),'Escalation',''))
                                ::text as visit,
                                svstdtc,
                                svendtc
                        FROM sv_data
                        UNION ALL
                        SELECT studyid,
                                siteid,
                                usubjid,
                                visitnum,
                                trim(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(visit,'<W[0-9]DA[0-9]/>\sExpansion',''),'<WK[0-9]DA[0-9]/>\sExpansion',''),'<WK[0-9]DA[0-9][0-9]/>\sExpansion',''), '<W[0-9]DA[0-9][0-9]/>\sExpansion',''), '<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),' Escalation ',' '),'Escalation',''))
                                ::text as visit,
                                min(svstdtc) as svstdtc,
                                max(svendtc) as svendtc
                        FROM formdata_visits
                        group by studyid, siteid, usubjid, visitnum,visit
                        ),

     included_sites AS (
                  SELECT DISTINCT studyid,studyname,siteid,sitecountry,sitecountrycode,sitename,siteregion from site)

SELECT
        /*KEY(sv.studyid || '~' || sv.siteid || '~' || sv.usubjid)::text AS comprehendid,KEY*/
        sv.studyid::text AS studyid,
		si.studyname::text AS studyname,
        sv.siteid::text AS siteid,
		si.sitename::text AS sitename,
        si.siteregion::text AS siteregion,
        si.sitecountry::text AS sitecountry,
        si.sitecountrycode::text AS sitecountrycode,
        sv.usubjid::text AS usubjid,
        sv.visitnum::numeric AS visitnum,
        sv.visit::text AS visit,
        1::int AS visitseq,
        sv.svstdtc::date AS svstdtc,
        sv.svendtc::date AS svendtc
        /*KEY, (sv.studyid || '~' || sv.siteid || '~' || sv.usubjid || '~' || sv.visit)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM all_visits sv
JOIN included_subjects s ON (sv.studyid = s.studyid AND sv.siteid = s.siteid AND sv.usubjid = s.usubjid)
LEFT JOIN included_sites si ON (sv.studyid = si.studyid AND sv.siteid = si.siteid);

