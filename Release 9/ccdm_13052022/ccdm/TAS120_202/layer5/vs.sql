/*
CCDM VS mapping
Notes: Standard mapping to CCDM VS table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     vs_data AS (
                SELECT  v."studyid"::text AS studyid,
                    v."siteid"::text AS siteid,
                    v."usubjid"::text AS usubjid,
					--(row_number() over (partition by v."siteid", v."usubjid" order by v."vsdtc"))::int as vsseq,
					v."vsseq"::int as vsseq,
                    v."vstestcd"::text AS vstestcd,
                    v."vstest"::text AS vstest,
                    v.vscat::text AS vscat,
                    v.vsscat::text AS vsscat,
                    v.vspos::text AS vspos,
                    v."vsorres"::text AS vsorres,
                    v."vsorresu"::text AS vsorresu,
                    v."vsstresn"::numeric AS vsstresn,
                    v."vsstresu"::text AS vsstresu,
                    v."vsstat"::text AS vsstat,
                    v."vsloc"::text AS vsloc,
                    v."vsblfl"::text AS vsblfl,
                    v."visit"::text AS visit,
                    v."vsdtc"::timestamp without time zone AS vsdtc,
                    v."vstm"::time without time zone AS vstm
					FROM (
							SELECT  vs."project"::text AS studyid,
								vs."SiteNumber"::text AS siteid,
								vs."Subject"::text AS usubjid,
								vsseq::int as vsseq,
								vstestcd::text AS vstestcd,
								vstest::text AS vstest,
								vscat::text AS vscat,
								vsscat::text AS vsscat,
								null::text AS vspos,
								vsorres::text AS vsorres,
								vsorresu::text AS vsorresu,
								vsstresn::numeric AS vsstresn,
								vsstresu::text AS vsstresu,
								null::text AS vsstat,
								null::text AS vsloc,
								null::text AS vsblfl,
								--vs."FolderName"::text AS visit,
								trim(REGEXP_REPLACE
									(REGEXP_REPLACE
									(REGEXP_REPLACE
									(REGEXP_REPLACE
									(vs."InstanceName",'\s\([0-9][0-9]\)','')
									,'\s\([0-9]\)','')
									,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: text as visit,
								vs."VSDAT"::timestamp without time zone AS vsdtc,
								null::time without time zone AS vstm
							FROM "tas120_202"."VS" vs
							cross join lateral(
							values
					(concat("instanceId",12),'Diastolic Blood Pressure'	, 'VSDBP' , 'Vital Signs','Vital Signs', "VSDBP"	,"VSDBP_Units"	  ,"VSDBP"	,"VSDBP_Units"),
					(concat("instanceId",34),'Systolic Blood Pressure'	, '1_VSSBP' , 'Vital Signs','Vital Signs', "VSSBP"	,"VSSBP_Units"	  ,"VSSBP"	,"VSSBP_Units"),
					(concat("instanceId",56),'Pressure Rate'			, 'VSPR'  , 'Vital Signs','Vital Signs', "VSPR"	    ,"VSPR_Units"	  ,"VSPR"	,"VSPR_Units"),
					(concat("instanceId",78),'Respiratory Rate'			, 'VSRESP', 'Vital Signs','Vital Signs', "VSRESP"	,"VSRESP_Units"	  ,"VSRESP"	,"VSRESP_Units"),
					(concat("instanceId",90),'Temperature'				, 'VSTEMP', 'Vital Signs','Vital Signs', "VSTEMP"	,"VSTEMP_Units"	  ,"VSTEMP"	,"VSTEMP_Units"),
					(concat("instanceId",13),'Weight'					, 'VSWT'  , 'Vital Signs','Vital Signs', "VSWT"		,"VSWT_Units"	  ,"VSWT"	,"VSWT_Units"),
					(concat("instanceId",46),'Height'					, 'VSHT'  , 'Vital Signs','Vital Signs', "VSHT"		,"VSHT_Units"	  ,"VSHT"	,"VSHT_Units")
					
							)as t
								(vsseq,vstest,vstestcd,vscat,vsscat,vsorres,vsorresu,vsstresn,vsstresu)
								) v
)

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

