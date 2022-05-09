/*
CCDM VS mapping
Notes: Standard mapping to CCDM VS table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     vs_data AS (
                SELECT  project ::text AS studyid,
                    concat(project,substring("SiteNumber",position('_' in "SiteNumber")))::text AS siteid,
                    "Subject" ::text AS usubjid,
                    vsseq::int AS vsseq, /*(row_number() over (partition by [studyid],[siteid],[usubjid] order [vsdtc,vstm]))::int as vsseq,*/
                    vstestcd::text AS vstestcd,
                    vstest::text AS vstest,
                    'Vital Signs'::text AS vscat,
                    'Vital Signs'::text AS vsscat,
                    null::text AS vspos,
                    vsorres::text AS vsorres,
                    vsorresu::text AS vsorresu,
                    vsstresn::numeric AS vsstresn,
                    vsstresu::text AS vsstresu,
                    null::text AS vsstat,
                    null::text AS vsloc,
                    null::text AS vsblfl,
                    trim(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						("InstanceName",'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')) ::text AS visit,
                    "VSDAT" ::timestamp without time zone AS vsdtc,
                    "VSTM" ::time without time zone AS vstm
                    from tas117_201."VS" v 
                    cross join lateral(
                    values
                    (concat("instanceId",1),'Diastolic Blood Pressure'	, 'VSDBP' ,  "VSDBP"	,"VSDBP_Units"	  ,"VSDBP"	,"VSDBP_Units"),
					(concat("instanceId",2),'Systolic Blood Pressure'	, ' VSSBP' , "VSSBP"	,"VSSBP_Units"	  ,"VSSBP"	,"VSSBP_Units"),
					(concat("instanceId",3),'Pressure Rate'				, 'VSPR'  ,  "VSPR"	    ,"VSPR_Units"	  ,"VSPR"	,"VSPR_Units"),
					(concat("instanceId",4),'Respiratory Rate'			, 'VSRESP', "VSRESP"	,"VSRESP_Units"	  ,"VSRESP"	,"VSRESP_Units"),
					(concat("instanceId",5),'Temperature'				, 'VSTEMP',  "VSTEMP"	,"VSTEMP_Units"	  ,"VSTEMP"	,"VSTEMP_Units"),
					(concat("instanceId",6),'Weight'					, 'VSWT'  ,  "VSWT"		,"VSWT_Units"	  ,"VSWT"	,"VSWT_Units"),
					(concat("instanceId",7),'Height'					, 'VSHT'  ,  "VSHT"		,"VSHT_Units"	  ,"VSHT"	,"VSHT_Units"),
					(concat("instanceId",8), 'BMI'						, 'VSBMI' ,  "VSBMI"    , ''			  , "VSBMI" , '')
                    
                    )as t (vsseq,vstest,vstestcd,vsorres,vsorresu,vsstresn,vsstresu) )

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
        /*KEY , (vs.studyid || '~' || vs.siteid || '~' || vs.usubjid || '~' || vs.vsseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM vs_data vs
JOIN included_subjects s ON (vs.studyid = s.studyid AND vs.siteid = s.siteid AND vs.usubjid = s.usubjid);






