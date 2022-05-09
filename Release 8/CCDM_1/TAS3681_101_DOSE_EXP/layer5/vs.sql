/*
CCDM VS mapping
Notes: Standard mapping to CCDM VS table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     vs_data AS (
                select 
                     studyid,
                     siteid, 
                     usubjid,
					 case when vstest='Diastolic Blood Pressure' then concat(vsseq,1)
                          when vstest='Systolic Blood Pressure'  then concat(vsseq,2)
                          when vstest='Heart Rate'  then concat(vsseq,3)
                          when vstest='Respiratory Rate'  then concat(vsseq,4)
                          when vstest='Temperature'  then concat(vsseq,5)
                          when vstest='Weight'  then concat(vsseq,6)
                          when vstest='Height'  then concat(vsseq,7)
                     end ::int as vsseq,
                     --(row_number() over (partition by studyid, siteid, usubjid order by vsdtc, vstm))::int as vsseq,
                     vstestcd,
                     vstest,
                     vscat,
                     vsscat,
                     vspos,
                     vsorres,
                     vsorresu,
                     vsstresn,
                     vsstresu,
                     vsstat,
                     vsloc,
                     vsblfl,
                     visit,
                     vsdtc,
                     vstm
                     from
(SELECT  'TAS3681_101_DOSE_EXP'::text AS studyid,
                    vs."SiteNumber"::text AS siteid, 
				 	vs."Subject"::text    AS usubjid,
						"instanceId"::int AS vsseq,
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
                    --REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("InstanceName",'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),'Escalation',''):: text as visit,
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
					("InstanceName",'<WK[0-9]D[0-9]/>\sExpansion','')
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
				    )	:: text as visit,
                    vs."VSDAT"::timestamp without time zone AS vsdtc,
                    null::time without time zone AS vstm
                FROM tas3681_101."VS" vs
                cross join lateral(
				values
					('Diastolic Blood Pressure'	, 'VSDBP' , 'Vital Signs','Vital Signs', "VSDBP"	,"VSDBP_Units"	  ,"VSDBP"	,"VSDBP_Units"),
					('Systolic Blood Pressure'	, '1_VSSBP' , 'Vital Signs','Vital Signs', "VSSBP"	,"VSSBP_Units"	  ,"VSSBP"	,"VSSBP_Units"),
					('Heart Rate'			    , 'VSHR'  , 'Vital Signs','Vital Signs', "VSHR"	    ,"VSHR_Units"	  ,"VSHR"	,"VSHR_Units"),
					('Respiratory Rate'			, 'VSRESP', 'Vital Signs','Vital Signs', "VSRESP"	,"VSRESP_Units"	  ,"VSRESP"	,"VSRESP_Units"),
					('Temperature'				, 'VSTEMP', 'Vital Signs','Vital Signs', "VSTEMP"	,"VSTEMP_Units"	  ,"VSTEMP"	,"VSTEMP_Units"),
					('Weight'					, 'VSWT'  , 'Vital Signs','Vital Signs', "VSWT"		,"VSWT_Units"	  ,"VSWT"	,"VSWT_Units")
					
				)as t
					(vstest,vstestcd,vscat,vsscat,vsorres,vsorresu,vsstresn,vsstresu)
								
				union 
				
				-- TAS3681-101  VSB
                SELECT  'TAS3681_101_DOSE_EXP'::text AS studyid,
                    vsb."SiteNumber"::text AS siteid, 
				 	vsb."Subject"::text    AS usubjid,
						"instanceId"::int AS vsseq,
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
                    --vsb."FolderName"::text AS visit,
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
					("InstanceName",'<WK[0-9]D[0-9]/>\sExpansion','')
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
				    )::text AS visit,
                    vsb."VSDAT"::timestamp without time zone AS vsdtc,
                    null::time without time zone AS vstm
                FROM tas3681_101."VSB" vsb
                cross join lateral(
				values
					('Diastolic Blood Pressure'	, 'VSDBP' , 'Vital Signs','Vital Signs', "VSDBP"	,"VSDBP_Units"	  ,"VSDBP"	,"VSDBP_Units"),
					('Systolic Blood Pressure'	, '1_VSSBP' , 'Vital Signs','Vital Signs', "VSSBP"	,"VSSBP_Units"	  ,"VSSBP"	,"VSSBP_Units"),
					('Heart Rate'			    , 'VSHR'  , 'Vital Signs','Vital Signs', "VSHR"	    ,"VSHR_Units"	  ,"VSHR"	,"VSHR_Units"),
					('Respiratory Rate'			, 'VSRESP', 'Vital Signs','Vital Signs', "VSRESP"	,"VSRESP_Units"	  ,"VSRESP"	,"VSRESP_Units"),
					('Temperature'				, 'VSTEMP', 'Vital Signs','Vital Signs', "VSTEMP"	,"VSTEMP_Units"	  ,"VSTEMP"	,"VSTEMP_Units"),
					('Weight'					, 'VSWT'  , 'Vital Signs','Vital Signs', "VSWT"		,"VSWT_Units"	  ,"VSWT"	,"VSWT_Units"),
					('Height'					, 'VSHT'  , 'Vital Signs','Vital Signs', "VSHT"		,"VSHT_Units"	  ,"VSHT"	,"VSHT_Units")
					
				)as t
					(vstest,vstestcd,vscat,vsscat,vsorres,vsorresu,vsstresn,vsstresu)) eg
	
     ),
     all_data as (
                SELECT
                    vs.studyid::text AS studyid,
                    vs.siteid::text AS siteid,
                    vs.usubjid::text AS usubjid,
                    vs.vsseq::int as vsseq,
                    vs.vstestcd::text AS vstestcd,
                    vs.vstest::text AS vstest,
                    vs.vscat::text AS vscat,
                    vs.vsscat::text AS vsscat,
                    vs.vspos::text AS vspos,
                    vs.vsorres::text AS vsorres,
                    vs.vsorresu::text AS vsorresu,
					vs.vsstresn::numeric AS vsstresn,
					vs.vsstresu ::text AS vsstresu,
					vs.vsstat::text AS vsstat,
                    vs.vsloc::text AS vsloc,
                    vs.vsblfl::text AS vsblfl,
                    vs.visit::text AS visit,
                    vs.vsdtc::text::timestamp without time zone AS vsdtc,
                    vs.vstm::text::time without time zone AS vstm
                FROM vs_data vs
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
        /*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
FROM all_data vs
JOIN included_subjects s ON (vs.studyid = s.studyid AND vs.siteid = s.siteid AND vs.usubjid = s.usubjid)
;
