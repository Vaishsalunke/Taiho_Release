/*
CCDM VS mapping
Notes: Standard mapping to CCDM VS table
*/


WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),

vs_data AS (
                SELECT
studyid	,
siteid	,
usubjid	,
case when vstest='Diastolic Blood Pressure' then concat(vsseq,1)
                          when vstest='Systolic Blood Pressure'  then concat(vsseq,2)
                          when vstest='Pressure Rate'  then concat(vsseq,3)
                          when vstest='Respiratory Rate'  then concat(vsseq,4)
                          when vstest='Temperature'  then concat(vsseq,5)
                          when vstest='Weight'  then concat(vsseq,6)
                          when vstest='Height'  then concat(vsseq,7)
                     end ::int as vsseq,
--(row_number() over (partition by vs.studyid, vs.siteid, vs.usubjid order by vs.vsdtc, vs.vstm))::int as vsseq	,
vstestcd	,
vstest	,
vscat	,
vsscat,
vspos	,
vsorres	,
vsorresu	,
vsstresn	,
vsstresu	,
vsstat	,
vsloc	,
vsblfl	,
visit	,
vsdtc	,
vstm

from

 (  SELECT
"project"::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
"instanceId"::integer	AS	vsseq	,
'VSDBP'::text	AS	vstestcd	,
'Diastolic Blood Pressure'::text	AS	vstest	,
'Vital Signs'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
null::text	AS	vspos	,
"VSDBP"::text	AS	vsorres	,
"VSDBP_Units"::text	AS	vsorresu	,
"VSDBP"::numeric	AS	vsstresn	,
"VSDBP_Units"::text	AS	vsstresu	,
null::text	AS	vsstat	,
null::text	AS	vsloc	,
null::text	AS	vsblfl	,
trim(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			("InstanceName",'\s\([0-9][0-9]\)','')
						   ,'\s\([0-9]\)','')
						   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
						   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: text as visit,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
null::time without time zone	AS	vstm
FROM tas120_201."VS"

UNION ALL

SELECT
"project"::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
"instanceId"::integer	AS	vsseq	,
'1_VSSBP'::text	AS	vstestcd	,
'Systolic Blood Pressure'::text	AS	vstest	,
'Vital Signs'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
null::text	AS	vspos	,
"VSSBP"::text	AS	vsorres	,
"VSSBP_Units"::text	AS	vsorresu	,
"VSSBP"::numeric	AS	vsstresn	,
"VSSBP_Units"::text	AS	vsstresu	,
null	::text	AS	vsstat	,
null	::text	AS	vsloc	,
null	::text	AS	vsblfl	,
trim(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			("InstanceName",'\s\([0-9][0-9]\)','')
						   ,'\s\([0-9]\)','')
						   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
						   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: text as visit,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
null	::time without time zone	AS	vstm
FROM tas120_201."VS"


UNION ALL

SELECT
"project"::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
"instanceId"::integer	AS	vsseq	,
'VSPR'::text	AS	vstestcd	,
'Pressure Rate'::text	AS	vstest	,
'Vital Signs'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
null::text	AS	vspos	,
"VSPR"::text	AS	vsorres	,
"VSPR_Units"::text	AS	vsorresu	,
"VSPR"::numeric	AS	vsstresn	,
"VSPR_Units"::text	AS	vsstresu	,
null::text	AS	vsstat	,
null::text	AS	vsloc	,
null::text	AS	vsblfl	,
trim(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			("InstanceName",'\s\([0-9][0-9]\)','')
						   ,'\s\([0-9]\)','')
						   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
						   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: text as visit,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
null::time without time zone	AS	vstm
FROM tas120_201."VS"

UNION ALL

SELECT	
"project"::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
"instanceId"::integer	AS	vsseq	,
'VSRESP'::text	AS	vstestcd	,
'Respiratory Rate'::text	AS	vstest	,
'Vital Signs'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
null::text	AS	vspos	,
"VSRESP"::text	AS	vsorres	,
"VSRESP_Units"::text	AS	vsorresu	,
"VSRESP"::numeric	AS	vsstresn	,
"VSRESP_Units"::text	AS	vsstresu	,
null::text	AS	vsstat	,
null::text	AS	vsloc	,
null::text	AS	vsblfl	,
trim(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			("InstanceName",'\s\([0-9][0-9]\)','')
						   ,'\s\([0-9]\)','')
						   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
						   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: text as visit,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
null::time without time zone	AS	vstm
FROM tas120_201."VS"

UNION ALL

SELECT
"project"::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
"instanceId"::integer	AS	vsseq	,
'VSTEMP'::text	AS	vstestcd	,
'Temperature'::text	AS	vstest	,
'Vital Signs'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
null::text	AS	vspos	,
"VSTEMP"::text	AS	vsorres	,
"VSTEMP_Units"::text	AS	vsorresu	,
"VSTEMP"::numeric	AS	vsstresn	,
"VSTEMP_Units"::text	AS	vsstresu	,
null::text	AS	vsstat	,
null::text	AS	vsloc	,
null::text	AS	vsblfl	,
trim(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			("InstanceName",'\s\([0-9][0-9]\)','')
						   ,'\s\([0-9]\)','')
						   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
						   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: text as visit,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
null::time without time zone	AS	vstm
FROM tas120_201."VS"

UNION ALL

SELECT
"project"::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
"instanceId"	::integer	AS	vsseq	,
'VSWT'::text	AS	vstestcd	,
'Weight'::text	AS	vstest	,
'Vital Signs'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
null::text	AS	vspos	,
"VSWT"::text	AS	vsorres	,
"VSWT_Units"::text	AS	vsorresu	,
"VSWT"::numeric	AS	vsstresn	,
"VSWT_Units"::text	AS	vsstresu	,
NULL::text	AS	vsstat	,
NULL::text	AS	vsloc	,
NULL::text	AS	vsblfl	,
trim(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			("InstanceName",'\s\([0-9][0-9]\)','')
						   ,'\s\([0-9]\)','')
						   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
						   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: text as visit,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
NULL::time without time zone	AS	vstm
FROM tas120_201."VS"

UNION ALL

SELECT
"project"::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
"instanceId"::integer	AS	vsseq	,
'VSHT'::text	AS	vstestcd	,
'Height'::text	AS	vstest	,
'Vital Signs'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
null::text	AS	vspos	,
"VSHT"::text	AS	vsorres	,
"VSHT_Units"::text	AS	vsorresu	,
"VSHT"::numeric	AS	vsstresn	,
"VSHT_Units"::text	AS	vsstresu	,
NULL::text	AS	vsstat	,
NULL::text	AS	vsloc	,
NULL::text	AS	vsblfl	,
trim(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			("InstanceName",'\s\([0-9][0-9]\)','')
						   ,'\s\([0-9]\)','')
						   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
						   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: text as visit,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
NULL::time without time zone	AS	vstm
FROM tas120_201."VS"

UNION ALL

select 
"project"::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
"instanceId"::integer	AS	vsseq	,
'VSDBP'::text	AS	vstestcd	,
'Diastolic Blood Pressure'::text	AS	vstest	,
'Vital Signs'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
null::text	AS	vspos	,
"VSDBP"::text	AS	vsorres	,
"VSDBP_Units"::text	AS	vsorresu	,
"VSDBP"::numeric	AS	vsstresn	,
"VSDBP_Units"::text	AS	vsstresu	,
NULL::text	AS	vsstat	,
NULL::text	AS	vsloc	,
NULL::text	AS	vsblfl	,
trim(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			("InstanceName",'\s\([0-9][0-9]\)','')
						   ,'\s\([0-9]\)','')
						   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
						   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: text as visit,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
NULL::time without time zone	AS	vstm
FROM tas120_201."VS2"
						
UNION ALL						

SELECT						
"project"::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
"instanceId"::integer	AS	vsseq	,
'1_VSSBP'::text	AS	vstestcd	,
'Systolic Blood Pressure'::text	AS	vstest	,
'Vital Signs'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
null::text	AS	vspos	,
"VSSBP"::text	AS	vsorres	,
"VSSBP_Units"::text	AS	vsorresu	,
"VSSBP"::numeric	AS	vsstresn	,
"VSSBP_Units"::text	AS	vsstresu	,
NULL::text	AS	vsstat	,
NULL::text	AS	vsloc	,
NULL::text	AS	vsblfl	,
trim(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			("InstanceName",'\s\([0-9][0-9]\)','')
						   ,'\s\([0-9]\)','')
						   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
						   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: text as visit,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
NULL::time without time zone	AS	vstm
FROM tas120_201."VS2"
						
UNION ALL						
	
SELECT	
"project"::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
"instanceId"::integer	AS	vsseq	,
'VSPR'::text	AS	vstestcd	,
'Pressure Rate'::text	AS	vstest	,
'Vital Signs'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
null::text	AS	vspos	,
"VSPR"::text	AS	vsorres	,
"VSPR_Units"::text	AS	vsorresu	,
"VSPR"::numeric	AS	vsstresn	,
"VSPR_Units"::text	AS	vsstresu	,
NULL::text	AS	vsstat	,
NULL::text	AS	vsloc	,
NULL::text	AS	vsblfl	,
trim(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			("InstanceName",'\s\([0-9][0-9]\)','')
						   ,'\s\([0-9]\)','')
						   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
						   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: text as visit,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
NULL::time without time zone	AS	vstm
FROM tas120_201."VS2"
						
UNION ALL	
SELECT					
						
"project"::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
"instanceId"::integer	AS	vsseq	,
'VSRESP'::text	AS	vstestcd	,
'Respiratory Rate'::text	AS	vstest	,
'Vital Signs'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
null::text	AS	vspos	,
"VSRESP"::text	AS	vsorres	,
"VSRESP_Units"::text	AS	vsorresu	,
"VSRESP"::numeric	AS	vsstresn	,
"VSRESP_Units"::text	AS	vsstresu	,
NULL::text	AS	vsstat	,
NULL::text	AS	vsloc	,
NULL::text	AS	vsblfl	,
trim(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			("InstanceName",'\s\([0-9][0-9]\)','')
						   ,'\s\([0-9]\)','')
						   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
						   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: text as visit,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
NULL::time without time zone	AS	vstm
FROM tas120_201."VS2"
						
UNION ALL		

SELECT						
"project"::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
"instanceId"::integer	AS	vsseq	,
'VSTEMP'::text	AS	vstestcd	,
'Temperature'::text	AS	vstest	,
'Vital Signs'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
null::text	AS	vspos	,
"VSTEMP"::text	AS	vsorres	,
"VSTEMP_Units"::text	AS	vsorresu	,
"VSTEMP"::numeric	AS	vsstresn	,
"VSTEMP_Units"::text	AS	vsstresu	,
NULL::text	AS	vsstat	,
NULL::text	AS	vsloc	,
NULL::text	AS	vsblfl	,
trim(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			("InstanceName",'\s\([0-9][0-9]\)','')
						   ,'\s\([0-9]\)','')
						   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
						   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: text as visit,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
NULL::time without time zone	AS	vstm
FROM tas120_201."VS2"
						
UNION ALL	
					
SELECT						
"project"::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
"instanceId"::integer	AS	vsseq	,
'VSWT'::text	AS	vstestcd	,
'Weight'::text	AS	vstest	,
'Vital Signs'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
null::text	AS	vspos	,
"VSWT"::text	AS	vsorres	,
"VSWT_Units"::text	AS	vsorresu	,
"VSWT"::numeric	AS	vsstresn	,
"VSWT_Units"::text	AS	vsstresu	,
NULL::text	AS	vsstat	,
NULL::text	AS	vsloc	,
NULL::text	AS	vsblfl	,
trim(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			("InstanceName",'\s\([0-9][0-9]\)','')
						   ,'\s\([0-9]\)','')
						   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
						   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: text as visit,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
NULL::time without time zone	AS	vstm
FROM tas120_201."VS2"
						
UNION ALL						

SELECT						
"project"::text	AS	studyid	,
"SiteNumber"::text	AS	siteid	,
"Subject"::text	AS	usubjid	,
"instanceId"::integer	AS	vsseq	,
'VSHT'::text	AS	vstestcd	,
'Height'::text	AS	vstest	,
'Vital Signs'::text	AS	vscat	,
'Vital Signs'::text	AS	vsscat	,
null::text	AS	vspos	,
NULL::text	AS	vsorres	,
"VSHT_Units"::text	AS	vsorresu	,
NULL::numeric	AS	vsstresn	,
"VSHT_Units"::text	AS	vsstresu	,
NULL::text	AS	vsstat	,
NULL::text	AS	vsloc	,
NULL::text	AS	vsblfl	,
trim(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			("InstanceName",'\s\([0-9][0-9]\)','')
						   ,'\s\([0-9]\)','')
						   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
						   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: text as visit,
"VSDAT"::timestamp without time zone	AS	vsdtc	,
NULL::time without time zone	AS	vstm
FROM tas120_201."VS2" ) vs )

SELECT
        /*KEY (vs.studyid || '~' || vs.siteid || '~' || vs.usubjid)::text AS comprehendid, KEY*/
        vs.studyid::text AS studyid,
        vs.siteid::text AS siteid,
        vs.usubjid::text AS usubjid,
        vs.vsseq::int AS vsseq, 
        vs.vstestcd::text AS vstestcd,
        vs.vstest::text AS vstest,
        vs.vscat::text AS vscat,
        vs.vsscat::text AS vsscat,
        vs.vspos::text AS vspos,
        vs.vsorres::text AS vsorres,
        vs.vsorresu::text AS vsorresu,
        vs.vsstresn::numeric AS vsstresn,
        vs.vsstresu::text AS vsstresu,
        vs.vsstat::text AS vsstat,
        vs.vsloc::text AS vsloc,
        vs.vsblfl::text AS vsblfl,
        vs.visit::text AS visit,
        vs.vsdtc::timestamp without time zone AS vsdtc,
        vs.vstm::time without time zone AS vstm
        /*KEY, (vs.studyid || '~' || vs.siteid || '~' || vs.usubjid || '~' || vs.vsseq)::text  AS objectuniquekey KEY*/ 
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM vs_data vs
JOIN included_subjects s ON (vs.studyid = s.studyid AND vs.siteid = s.siteid AND vs.usubjid = s.usubjid);




