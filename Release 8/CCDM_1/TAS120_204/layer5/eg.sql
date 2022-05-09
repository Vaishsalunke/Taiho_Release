/*
CCDM EG mapping
Notes: Standard mapping to CCDM EG table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     eg_data AS (
                SELECT  distinct project ::text AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber"))::text AS siteid,
                        "Subject" ::text AS usubjid,
                        concat(concat("instanceId","RecordPosition"),1) ::int AS egseq,
                        'ECG'::text AS egtestcd,
                        'ECG'::text AS egtest,
                        'ECG'::text AS egcat,
                        null::text AS egscat,
                        null::text AS egpos,
                        null::text AS egorres,
                        null ::text AS egorresu,
                        null ::numeric AS egstresn,
                        null ::text AS egstresu,
                        null::text AS egstat,
                        null::text AS egloc,
                        null::text AS egblfl,
                        trim(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						("InstanceName",'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')) ::text AS visit,
                        "ECGDAT" ::timestamp without time zone AS egdtc,
                        null ::time without time zone AS egtm
                        from tas120_204."ECG" e 
                        
	union all 
				SELECT distinct project ::text AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber")) ::text AS siteid,
                        "Subject" ::text AS usubjid,
                        concat(concat("instanceId","RecordPosition"),2) ::int AS egseq,
                        'ECG Part 1'::text AS egtestcd,
                        'ECG Part 1'::text AS egtest,
                        'ECG'::text AS egcat,
                        "ECGTMPT"::text AS egscat,
                        null::text AS egpos,
                        null ::text AS egorres,
                        null::text AS egorresu,
                        null ::numeric AS egstresn,
                        null ::text AS egstresu,
                        "ECGND"::text AS egstat,
                        null::text AS egloc,
                        null::text AS egblfl,
                        trim(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						("InstanceName",'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')) ::text AS visit,
                        "ECGDAT" ::timestamp without time zone AS egdtc,
                        "ECGTM" ::time without time zone AS egtm
                        from tas120_204."ECG1" e  )

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





