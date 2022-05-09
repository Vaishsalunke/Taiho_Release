/*
CCDM EG mapping
Notes: Standard mapping to CCDM EG table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     eg_data AS (
                SELECT  project ::text AS studyid,
                        concat(project,substring("SiteNumber",position('_' in "SiteNumber")))::text AS siteid,
                        "Subject" ::text AS usubjid,
                        egseq::int AS egseq,
                        egtestcd::text AS egtestcd,
                        egtest::text AS egtest,
                        egcat::text AS egcat,
                        egscat::text AS egscat,
                        null::text AS egpos,
                        egorres::text AS egorres,
                        egorresu::text AS egorresu,
                        egstresn::numeric AS egstresn,
                        egstresu::text AS egstresu,
                        null::text AS egstat,
                        null::text AS egloc,
                        null::text AS egblfl,
                        trim(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(e."InstanceName",'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')) ::text AS visit,
                        "ECGDAT" ::timestamp without time zone AS egdtc,
                        null::time without time zone AS egtm
                        from tas117_201."ECG" e 
                        cross join lateral(
                        values
                        (concat("RecordId",1),'ECGRR','RR Interval','ECG','RR Interval',"PRINTERV","PRINTERV_Units","PRINTERV","PRINTERV_Units"),
                        (concat("RecordId",2),'ECGQTCF','QTCF Interval','ECG','QTCF Interval',"QTCFINTER" ,"QTCFINTER_Units","QTCFINTER" ,"QTCFINTER_Units"),
                        (concat("RecordId",3),'ECGHR','Heart Rate','ECG','Heart Rate',"HRTRATE" ,"HRTRATE_Units","HRTRATE" ,"HRTRATE_Units"),
                        (concat("RecordId",4),'ECGQT','QT Interval','ECG','QT Interval',"QTINTERV","QTINTERV_Units","QTINTERV","QTINTERV_Units"),
                        (concat("RecordId",5),'ECGPR','PR Interval','ECG','PR Interval',"PRINTERV","PRINTERV_Units","PRINTERV","PRINTERV_Units")
                        
                        ) as eg (egseq,egtestcd,egtest,egcat,egscat,egorres,egorresu,egstresn,egstresu)

	union all

				SELECT  project ::text AS studyid,
                        concat(project,substring("SiteNumber",position('_' in "SiteNumber")))::text AS siteid,
                        "Subject" ::text AS usubjid,
                        egseq::int AS egseq,
                        egtestcd::text AS egtestcd,
                        egtest::text AS egtest,
                        egcat::text AS egcat,
                        egscat::text AS egscat,
                        null::text AS egpos,
                        egorres::text AS egorres,
                        egorresu::text AS egorresu,
                        egstresn::numeric AS egstresn,
                        egstresu::text AS egstresu,
                        null::text AS egstat,
                        null::text AS egloc,
                        null::text AS egblfl,
                        trim(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(e."InstanceName",'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')) ::text AS visit,
                        "ECGCDAT" ::timestamp without time zone AS egdtc,
                        null::time without time zone AS egtm
                        from tas117_201."ECGCD" e 
                        cross join lateral(
                        values
                        (concat("RecordId",1),'ECGRR','RR Interval','ECG - Triplicate Assessments',"ECGCTP","ECGRRI" ,'msec',"ECGRRI" ,'msec'),
                        (concat("RecordId",2),'ECGQTCF','QTCF Interval','ECG - Triplicate Assessments',"ECGCTP","ECGQTCF",'msec',"ECGQTCF",'msec'),
                        (concat("RecordId",3),'ECGHR','Heart Rate','ECG - Triplicate Assessments',"ECGCTP","ECGHRAT" ,'bpm',"ECGHRAT" ,'bpm'),
                        (concat("RecordId",4),'ECGQT','QT Interval','ECG - Triplicate Assessments',"ECGCTP","ECGQTI" ,'msec',"ECGQTI" ,'msec'),
                        (concat("RecordId",5),'ECGPR','PR Interval','ECG - Triplicate Assessments',"ECGCTP","ECGPRI",'msec',"ECGPRI",'msec')
                        
                        ) as eg (egseq,egtestcd,egtest,egcat,egscat,egorres,egorresu,egstresn,egstresu)  )

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








