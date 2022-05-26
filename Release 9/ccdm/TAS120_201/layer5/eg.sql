/*
CCDM EG mapping
Notes: Standard mapping to CCDM EG table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

          eg_data AS (
     SELECT    eg.studyid,
                    eg.siteid,
                    eg.usubjid,
                    --(row_number() over (partition by eg.studyid, eg.siteid, eg.usubjid order by eg.egseq, eg.egdtc))::int AS egseq,
					case when egtest = 'RR Interval' then concat(egseq,12)
					when egtest = 'Derived QTcF Interval' then concat(egseq,23)
					when egtest = 'HR' then concat(egseq,34)
					when egtest = 'QT Interval' then concat(egseq,45)
					when egtest in ('QTcF','QTcB') then concat(egseq,56) end::int as egseq,
                    eg.egtestcd,
                    eg.egtest,
                    eg.egcat,
                    eg.egscat,
                    eg.egpos,
                    eg.egorres,
                    eg.egorresu,
                    eg.egstresn,
                    eg.egstresu,
                    eg.egstat,
                    eg.egloc,
                    eg.egblfl,
                    eg.visit,
                    eg.egdtc,
                    eg.egtm 
                    from (
                SELECT
"project"::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
"instanceId"::int AS egseq,
'ECGRR'	::text	AS	egtestcd	,
'RR Interval'	::text	AS	egtest	,
'ECG'	::text	AS	egcat	,
'ECG'	::text	AS	egscat	,
'NA'::text	AS	egpos	,
"ECGRR"::text	AS	egorres	,
"ECGRR_Units"::text	AS	egorresu	,
"ECGRR"::numeric	AS	egstresn	,
"ECGRR_Units"::text	AS	egstresu	,
'NA'::text	AS	egstat	,
'NA'::text	AS	egloc	,
'NA'::text	AS	egblfl	,
trim(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			("InstanceName",'\s\([0-9][0-9]\)','')
						   ,'\s\([0-9]\)','')
						   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
						   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: text as visit,
"ECGDAT"::timestamp without time zone	AS	egdtc,	
NULL::time without time zone	AS	egtm
from tas120_201."ECG"

union all

SELECT
"project"::text AS studyid,
"SiteNumber"::text AS siteid,
"Subject"::text AS usubjid,
"instanceId"::int AS egseq,
'ECGQTCF'::text AS egtestcd,
'Derived QTcF Interval'::text AS egtest,
'ECG'::text AS egcat,
'ECG'::text AS egscat,
'NA'::text AS egpos,
"ECGQTCF"::text AS egorres,
"ECGQTCF_Units"::text AS egorresu,
"ECGQTCF"::numeric AS egstresn,
"ECGQTCF_Units"::text AS egstresu,
'NA'::text AS egstat,
'NA'::text AS egloc,
'NA'::text AS egblfl,
trim(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			("InstanceName",'\s\([0-9][0-9]\)','')
						   ,'\s\([0-9]\)','')
						   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
						   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: text as visit,
"ECGDAT"::timestamp without time zone AS egdtc,
NULL::time without time zone AS egtm
from tas120_201."ECG"

UNION ALL

SELECT 
"project"::text AS studyid,
"SiteNumber"::text AS siteid,
"Subject"::text AS usubjid,
"instanceId"::int AS egseq,
'ECGHR'::text AS egtestcd,
'HR'::text AS egtest,
'ECG'::text AS egcat,
'ECG'::text AS egscat,
'NA'::text AS egpos,
"ECGHR"::text AS egorres,
"ECGHR_Units"::text AS egorresu,
"ECGHR"::numeric AS egstresn,
"ECGHR_Units"::text AS egstresu,
'NA'::text AS egstat,
'NA'::text AS egloc,
'NA'::text AS egblfl,
trim(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			("InstanceName",'\s\([0-9][0-9]\)','')
						   ,'\s\([0-9]\)','')
						   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
						   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: text as visit,
"ECGDAT"::timestamp without time zone AS egdtc,
NULL::time without time zone AS egtm
from tas120_201."ECG"

UNION ALL

SELECT
"project"::text AS studyid,
"SiteNumber"::text AS siteid,
"Subject"::text AS usubjid,
"instanceId"::int AS egseq,
'ECGQT'::text AS egtestcd,
'QT Interval'::text AS egtest,
'ECG'::text AS egcat,
'ECG'::text AS egscat,
'NA'::text AS egpos,
"ECGQT"::text AS egorres,
"ECGQT_Units"::text AS egorresu,
"ECGQT"::numeric AS egstresn,
"ECGQT_Units"::text AS egstresu,
'NA'::text AS egstat,
'NA'::text AS egloc,
'NA'::text AS egblfl,
trim(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			("InstanceName",'\s\([0-9][0-9]\)','')
						   ,'\s\([0-9]\)','')
						   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
						   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''))::text AS visit,
"ECGDAT"::timestamp without time zone AS egdtc,
NULL::time without time zone AS egtm
from tas120_201."ECG"


UNION ALL

SELECT
"project"::text AS studyid,
"SiteNumber"::text AS siteid,
"Subject"::text AS usubjid,
"instanceId"::int AS egseq,
'ECGQTCF'::text AS egtestcd,
"QTCTYPE"::text AS egtest,
'ECG'::text AS egcat,
'ECG'::text AS egscat,
'NA'::text AS egpos,
"ECGQTCF"::text AS egorres,
'msec'::text AS egorresu,
"ECGQTCF"::numeric AS egstresn,
'msec'::text AS egstresu,
'NA'::text AS egstat,
'NA'::text AS egloc,
'NA'::text AS egblfl,
trim(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			("InstanceName",'\s\([0-9][0-9]\)','')
						   ,'\s\([0-9]\)','')
						   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
						   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: text as visit,
"ECGDAT"::timestamp without time zone AS egdtc,
NULL::time without time zone AS egtm
from tas120_201."ECG"
where "QTCTYPE" <> '') EG )

SELECT
        /*KEY (eg.studyid::text || '~' || eg.siteid::text || '~' || eg.usubjid::text) AS comprehendid, KEY*/
        eg.studyid::text AS studyid,
        eg.siteid::text AS siteid,
        eg.usubjid::text AS usubjid,
        eg.egseq::int AS egseq,
        eg.egtestcd::text AS egtestcd,
        eg.egtest::text AS egtest,
        eg.egcat::text AS egcat,
        eg.egscat::text AS egscat,
        eg.egpos::text AS egpos,
        eg.egorres::text AS egorres,
        eg.egorresu::text AS egorresu,
        eg.egstresn::numeric AS egstresn,
        eg.egstresu::text AS egstresu,
        eg.egstat::text AS egstat,
        eg.egloc::text AS egloc,
        eg.egblfl::text AS egblfl,
        eg.visit::text AS visit,
        eg.egdtc::timestamp without time zone AS egdtc,
        eg.egtm::time without time zone AS egtm
        /*KEY, (eg.studyid || '~' || eg.siteid || '~' || eg.usubjid || '~' || eg.egseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM eg_data eg
JOIN included_subjects s ON (eg.studyid = s.studyid AND eg.siteid = s.siteid AND eg.usubjid = s.usubjid);





