/*
CCDM EG mapping
Notes: Standard mapping to CCDM EG table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     eg_data AS (
                SELECT  project ::text AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber"))::text AS siteid,
                        "Subject" ::text AS usubjid,
                        concat("instanceId",1) ::int AS egseq,
                        'ECG'::text AS egtestcd,
                        'ECG'::text AS egtest,
                        'ECG'::text AS egcat,
                        'Derived QTcF Interval'::text AS egscat,
                        null::text AS egpos,
                        "ECGQTC" ::text AS egorres,
                        "ECGQTC_Units" ::text AS egorresu,
                        "ECGQTC" ::numeric AS egstresn,
                        "ECGQTC_Units" ::text AS egstresu,
                        null::text AS egstat,
                        null::text AS egloc,
                        null::text AS egblfl,
                        REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("FolderName" ,'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),'Escalation',''):: text as visit,
                        "ECGDAT" ::timestamp without time zone AS egdtc,
                        "ECGTM" ::time without time zone AS egtm
                        from tas120_203."ECG" e 
                        
	union all 
				SELECT  project ::text AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber")) ::text AS siteid,
                        "Subject" ::text AS usubjid,
                        concat("instanceId",2) ::int AS egseq,
                        'ECG'::text AS egtestcd,
                        'ECG'::text AS egtest,
                        'ECG'::text AS egcat,
                        'HR'::text AS egscat,
                        null::text AS egpos,
                        "ECGHR" ::text AS egorres,
                        "ECGHR_Units" ::text AS egorresu,
                        "ECGHR" ::numeric AS egstresn,
                        "ECGHR_Units" ::text AS egstresu,
                        null::text AS egstat,
                        null::text AS egloc,
                        null::text AS egblfl,
                        REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("FolderName" ,'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),'Escalation',''):: text as visit,
                        "ECGDAT" ::timestamp without time zone AS egdtc,
                        "ECGTM" ::time without time zone AS egtm
                        from tas120_203."ECG" e 

                        
	union all 
				SELECT  project ::text AS studyid,
                        project||substring("SiteNumber",position ('_' in "SiteNumber")) ::text AS siteid,
                        "Subject" ::text AS usubjid,
                        concat("instanceId",3) ::int AS egseq,
                        'ECG'::text AS egtestcd,
                        'ECG'::text AS egtest,
                        'ECG'::text AS egcat,
                        'QT Interval'::text AS egscat,
                        null::text AS egpos,
                        "ECGQT" ::text AS egorres,
                        "ECGQT_Units" ::text AS egorresu,
                        "ECGQT" ::numeric AS egstresn,
                        "ECGQT_Units" ::text AS egstresu,
                        null::text AS egstat,
                        null::text AS egloc,
                        null::text AS egblfl,
                        REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("FolderName" ,'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),'Escalation',''):: text as visit,
                        "ECGDAT" ::timestamp without time zone AS egdtc,
                        "ECGTM" ::time without time zone AS egtm
                        from tas120_203."ECG" e )

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
        /*KEY , (eg.studyid || '~' || eg.siteid || '~' || eg.usubjid || '~' || eg.egseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM eg_data eg
JOIN included_subjects s ON (eg.studyid = s.studyid AND eg.siteid = s.siteid AND eg.usubjid = s.usubjid);

