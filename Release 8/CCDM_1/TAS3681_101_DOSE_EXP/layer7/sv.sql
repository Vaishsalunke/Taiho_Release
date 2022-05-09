/*
CCDM SV mapping
Notes: Standard mapping to CCDM SV table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

        sv_data AS (
SELECT B.studyid, B.siteid, B.usubjid,max(B.visitnum)   as visitnum     ,trim(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(B.visit,'<WK[0-9]D[0-9]/>\sExpansion','')
								,'<WK[0-9]D[0-9][0-9]/>\sExpansion','')
								,'<WK[0-9]DA[0-9]/>\sExpansion','')
								,'<WK[0-9]DA[0-9][0-9]/>\sExpansion','')
								,'<W[0-9]DA[0-9]/>\sExpansion','')
								,'<W[0-9]DA[0-9][0-9]/>\sExpansion','')
								,'Expansion','')
								,'\s\([0-9][0-9]\)','')
								,'\s\([0-9]\)','')
								,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
								,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
				    )	  AS      visit, B.visitseq, min(B.svstdtc) as      svstdtc, max(B.svendtc) as svendtc FROM        (
SELECT studyid, siteid, usubjid, (case when cnt>1 OR visit like '%Escalation Cycle%' or visit like '%Cycle%' then (visitnum::int||'.'|| rnk+1)::numeric else visitnum end)::numeric AS visitnum, visit, visitseq, svstdtc, svendtc FROM
(  
                                                                SELECT  'TAS3681_101_DOSE_EXP'::text AS studyid,
                                                                        "SiteNumber"::text AS siteid,
                                                                        "Subject"::text AS usubjid,
                                                                        "FolderSeq"::numeric AS visitnum,
                                                                        REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("InstanceName",'<WK[0-9]DA[0-9]/>\sExpansion',''),'<WK[0-9]DA[0-9][0-9]/>\sExpansion',''),'<W[0-9]DA[0-9]/>\sExpansion',''),'<W[0-9]DA[0-9][0-9]/>\sExpansion',''),'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),' Escalation ',' '),'\s\([0-9]\)',''),' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''):: text as visit,
                                                                       null::int AS visitseq, /* defaulted to 1 - deprecated */
                                                                        "PEDAT"::date AS svstdtc,
                                                                        "PEDAT"::date AS svendtc,
                                                                        ROW_NUMBER() OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName" ORDER BY "RecordId" ASC) AS rnk
                                                                 , COUNT(*) OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName" ) AS cnt
                                                                from tas3681_101."PE"
                                            ) A

                                                       
                                                union all
                                               
                                                SELECT studyid, siteid, usubjid
                                                                , (case when cnt>1 OR visit like '%Escalation Cycle%' or visit like '%Cycle%' then (visitnum::int||'.'|| rnk+1)::numeric else visitnum end)::numeric AS visitnum
                                                                , visit
                                                                , visitseq,
svstdtc,
svstdtc
                                                 FROM
                                                    (
                                                    SELECT  'TAS3681_101_DOSE_EXP'::text AS studyid,
                                                                "SiteNumber"::text AS siteid,
                                                                "Subject"::text AS usubjid,
                                                                "FolderSeq"::numeric AS visitnum,
                                                                REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("InstanceName",'<WK[0-9]DA[0-9]/>\sExpansion',''),'<WK[0-9]DA[0-9][0-9]/>\sExpansion',''),'<W[0-9]DA[0-9]/>\sExpansion',''),'<W[0-9]DA[0-9][0-9]/>\sExpansion',''),'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),' Escalation ',' '),'\s\([0-9]\)',''),' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''):: text as visit,
                                                               null::int AS visitseq, /* defaulted to 1 - deprecated */
                                                                "CYCLEDAT"::date AS svstdtc,
                                                                "CYCLEDAT"::date AS svendtc
                                                                 , ROW_NUMBER() OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName" ORDER BY "RecordId" ASC) AS rnk
                                                                 , COUNT(*) OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName" ) AS cnt
                                                        FROM tas3681_101."CYCLE"
)A

                                                        union all
                                                    SELECT studyid, siteid, usubjid
                                                                , (case when cnt>1 OR visit like '%Escalation Cycle%' or visit like '%Cycle%' then (visitnum::int||'.'|| rnk+1)::numeric else visitnum end)::numeric AS visitnum
                                                                , visit
                                                                , visitseq,
svstdtc,
svstdtc
FROM
                                                    (
SELECT  'TAS3681_101_DOSE_EXP'::text AS studyid,
                                                                "SiteNumber"::text AS siteid,
                                                                "Subject"::text AS usubjid,
                                                                "FolderSeq"::numeric AS visitnum,
                                                                REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("InstanceName",'<WK[0-9]DA[0-9]/>\sExpansion',''),'<WK[0-9]DA[0-9][0-9]/>\sExpansion',''),'<W[0-9]DA[0-9]/>\sExpansion',''),'<W[0-9]DA[0-9][0-9]/>\sExpansion',''),'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),' Escalation ',' '),'\s\([0-9]\)',''),' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''):: text as visit,
                                                               null::int AS visitseq, /* defaulted to 1 - deprecated */
                                                               coalesce("LBDAT","MinCreated")::date AS svstdtc,
                                                                coalesce("LBDAT","MinCreated")::date AS svendtc
                                                                 , ROW_NUMBER() OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName" ORDER BY "RecordId" ASC) AS rnk
                                                                 , COUNT(*) OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName" ) AS cnt
                                                        FROM tas3681_101."PSA"
)A  



UNION   ALL

SELECT studyid, siteid, usubjid,
(case when cnt>1 OR visit like '%Escalation Cycle%' or visit like '%Cycle%' then (visitnum::int||'.'|| rnk+1)::numeric else visitnum end)::numeric AS visitnum,
visit, visitseq, svstdtc, svendtc FROM
                                                                                                                       
(SELECT  'TAS3681_101_DOSE_EXP'::text AS studyid,
"SiteNumber"::text AS siteid,
 "Subject"::text AS usubjid,
"FolderSeq"::numeric AS visitnum,
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("InstanceName",'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),' Escalation ',' '),'\s\([0-9]\)',''),' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''):: text as visit,
null::int AS visitseq, /* defaulted to 1 - deprecated */
"CTCDT"::date AS svstdtc,
"CTCDT"::date AS svendtc,
ROW_NUMBER() OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName" ORDER BY "RecordId" ASC) AS rnk,
COUNT(*) OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName" ) AS cnt
from tas3681_101."CTC"
) B

UNION   ALL

SELECT studyid, siteid, usubjid,
(case when cnt>1 OR visit like '%Escalation Cycle%' or visit like '%Cycle%' then (visitnum::int||'.'|| rnk+1)::numeric else visitnum end)::numeric AS visitnum,
visit, visitseq, svstdtc, svendtc FROM
                                                                                                                       
(SELECT  'TAS3681_101_DOSE_EXP'::text AS studyid,
"SiteNumber"::text AS siteid,
 "Subject"::text AS usubjid,
"FolderSeq"::numeric AS visitnum,
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("InstanceName",'<WK[0-9]DA[0-9]/>\sExpansion',''),'<WK[0-9]DA[0-9][0-9]/>\sExpansion',''),'<W[0-9]DA[0-9]/>\sExpansion',''),'<W[0-9]DA[0-9][0-9]/>\sExpansion',''),'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),' Escalation ',' '),'\s\([0-9]\)',''),' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''):: text as visit,
null::int AS visitseq, /* defaulted to 1 - deprecated */
"TLDAT"::date AS svstdtc,
"TLDAT"::date AS svendtc,
ROW_NUMBER() OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName" ORDER BY "RecordId" ASC) AS rnk,
COUNT(*) OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName" ) AS cnt
from tas3681_101."TL"
) B


UNION   ALL

SELECT studyid, siteid, usubjid,
(case when cnt>1 OR visit like '%Escalation Cycle%' or visit like '%Cycle%' then (visitnum::int||'.'|| rnk+1)::numeric else visitnum end)::numeric AS visitnum,
visit, visitseq, svstdtc, svendtc FROM
                                                                                                                       
(SELECT  'TAS3681_101_DOSE_EXP'::text AS studyid,
"SiteNumber"::text AS siteid,
 "Subject"::text AS usubjid,
"FolderSeq"::numeric AS visitnum,
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("InstanceName",'<WK[0-9]DA[0-9]/>\sExpansion',''),'<WK[0-9]DA[0-9][0-9]/>\sExpansion',''),'<W[0-9]DA[0-9]/>\sExpansion',''),'<W[0-9]DA[0-9][0-9]/>\sExpansion',''),'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),' Escalation ',' '),'\s\([0-9]\)',''),' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''):: text as visit,
null::int AS visitseq, /* defaulted to 1 - deprecated */
"NTLDAT"::date AS svstdtc,
"NTLDAT"::date AS svendtc,
ROW_NUMBER() OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName" ORDER BY "RecordId" ASC) AS rnk,
COUNT(*) OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName" ) AS cnt
from tas3681_101."NTL"
) B

UNION   ALL

SELECT studyid, siteid, usubjid,
(case when cnt>1 OR visit like '%Escalation Cycle%' or visit like '%Cycle%' then (visitnum::int||'.'|| rnk+1)::numeric else visitnum end)::numeric AS visitnum,
visit, visitseq, svstdtc, svendtc FROM
                                                                                                                       
(SELECT  'TAS3681_101_DOSE_EXP'::text AS studyid,
"SiteNumber"::text AS siteid,
 "Subject"::text AS usubjid,
"FolderSeq"::numeric AS visitnum,
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("InstanceName",'<WK[0-9]DA[0-9]/>\sExpansion',''),'<WK[0-9]DA[0-9][0-9]/>\sExpansion',''),'<W[0-9]DA[0-9]/>\sExpansion',''),'<W[0-9]DA[0-9][0-9]/>\sExpansion',''),'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),' Escalation ',' '),'\s\([0-9]\)',''),' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''):: text as visit,
null::int AS visitseq, /* defaulted to 1 - deprecated */
"ORDAT"::date AS svstdtc,
"ORDAT"::date AS svendtc,
ROW_NUMBER() OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName" ORDER BY "RecordId" ASC) AS rnk,
COUNT(*) OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName" ) AS cnt
from tas3681_101."OR"
) B

UNION   ALL

SELECT studyid, siteid, usubjid,
(case when cnt>1 OR visit like '%Escalation Cycle%' or visit like '%Cycle%' then (visitnum::int||'.'|| rnk+1)::numeric else visitnum end)::numeric AS visitnum,
visit, visitseq, svstdtc, svendtc FROM
                                                                                                                       
(SELECT  'TAS3681_101_DOSE_EXP'::text AS studyid,
"SiteNumber"::text AS siteid,
 "Subject"::text AS usubjid,
"FolderSeq"::numeric AS visitnum,
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("InstanceName",'<WK[0-9]DA[0-9]/>\sExpansion',''),'<WK[0-9]DA[0-9][0-9]/>\sExpansion',''),'<W[0-9]DA[0-9]/>\sExpansion',''),'<W[0-9]DA[0-9][0-9]/>\sExpansion',''),'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),' Escalation ',' '),'\s\([0-9]\)',''),' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''):: text as visit,
null::int AS visitseq, /* defaulted to 1 - deprecated */
"NLIMGDAT"::date AS svstdtc,
"NLIMGDAT"::date AS svendtc,
ROW_NUMBER() OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName" ORDER BY "RecordId" ASC) AS rnk,
COUNT(*) OVER (PARTITION BY "project", "SiteNumber", "Subject", "InstanceName" ) AS cnt
from tas3681_101."NL"
) B

)B
group   by       B.studyid, B.siteid, B.usubjid, B.visit,B.visitseq
                                     
                        ),

        formdata_visits AS (SELECT DISTINCT fd.studyid,
                                    fd.siteid,
                                    fd.usubjid,
                                    99::numeric AS visitnum, -- will be updated by cleanup script
                                    trim(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(fd.visit,'<WK[0-9]D[0-9]/>\sExpansion','')
								,'<WK[0-9]D[0-9][0-9]/>\sExpansion','')
								,'<WK[0-9]DA[0-9]/>\sExpansion','')
								,'<WK[0-9]DA[0-9][0-9]/>\sExpansion','')
								,'<W[0-9]DA[0-9]/>\sExpansion','')
								,'<W[0-9]DA[0-9][0-9]/>\sExpansion','')
								,'Expansion','')
								,'\s\([0-9][0-9]\)','')
								,'\s\([0-9]\)','')
								,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
								,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
				    )	as visit,
									coalesce(datacollecteddate,dataentrydate)::date AS svstdtc,
                                    coalesce(datacollecteddate,dataentrydate)::date AS svendtc
                            FROM formdata fd
                            LEFT JOIN sv_data sd ON (fd.studyid = sd.studyid and fd.siteid = sd.siteid and fd.usubjid = sd.usubjid and fd.visit = sd.visit)
                            WHERE sd.studyid IS NULL
                            ),

 all_visits AS (
                        SELECT  v.studyid,
                                v.siteid,
                                v.usubjid,
                                v.visitnum,
                                v.visit,
                                min(v.svstdtc) as svstdtc,
                                max(v.svendtc) as svendtc
                        from   (SELECT studyid,
                                       siteid,
                                       usubjid,
                                       visitnum,
                                       visit,
                                       svstdtc,
                                       svendtc
                           FROM   sv_data

                                UNION all
                               

                                SELECT studyid,
                          siteid,
                          usubjid,
                          visitnum,
                          visit,
                          svstdtc,
                          svendtc
                           FROM formdata_visits
                            ) v
                      group by v.studyid, v.siteid, v.usubjid, v.visitnum, v.visit
                        )
SELECT
        /*KEY (sv.studyid || '~' || sv.siteid || '~' || sv.usubjid)::text AS comprehendid, KEY*/
        sv.studyid::text AS studyid,
		null::text AS studyname,
        sv.siteid::text AS siteid,
		null::text AS sitename,
        null::text AS siteregion,
        null::text AS sitecountry,
       null::text AS sitecountrycode,
        sv.usubjid::text AS usubjid,
        sv.visitnum::numeric AS visitnum,
        sv.visit::text AS visit,
        1::int AS visitseq,
        sv.svstdtc::date AS svstdtc,
        sv.svendtc::date AS svendtc
       /*KEY  ,(sv.studyid || '~' || sv.siteid || '~' || sv.usubjid || '~' ||  sv.visit )::text as objectuniquekey  KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM all_visits sv
JOIN included_subjects s ON (sv.studyid = s.studyid AND sv.siteid = s.siteid AND sv.usubjid = s.usubjid)
;


