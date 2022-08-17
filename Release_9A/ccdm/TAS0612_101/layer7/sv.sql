/*
CCDM SV mapping
Notes: Standard mapping to CCDM SV table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     sv_data AS (
                SELECT  "project"::text AS studyid,
                        --"SiteNumber"::text AS siteid,
						concat('TAS0612_101_',split_part("SiteNumber",'_',2))::text AS siteid,
                        "Subject"::text AS usubjid, 
                        "FolderSeq"::numeric AS visitnum,
                        trim(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						("InstanceName",'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''))::text AS visit,
                        1::int AS visitseq, /* defaulted to 1 - deprecated */
                        min("VISITDAT")::date AS svstdtc,
                        max("VISITDAT")::date AS svendtc
                        FROM tas0612_101."VISIT" 
						group by 1,2,3,4,5,6),
	
	formdata_visits AS (SELECT DISTINCT fd.studyid,
                                    fd.siteid,
                                    --concat('TAS0612_101_',split_part(fd.siteid,'_',2))::text AS siteid,
                                    fd.usubjid,
                                    99::numeric AS visitnum, -- will be updated by cleanup script
                                     trim(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(fd.visit,'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')) ::text AS visit,
                                    coalesce(datacollecteddate,dataentrydate)::date AS svstdtc,
                                    coalesce(datacollecteddate,dataentrydate)::date AS svendtc
                            FROM formdata fd
                            LEFT JOIN sv_data sd ON (fd.studyid = sd.studyid and fd.siteid = sd.siteid and fd.usubjid = sd.usubjid and fd.visit = sd.visit)
                            WHERE sd.studyid IS NULL AND fd.studyid='TAS0612-101'
                        ),
						
	all_visits AS (
	SELECT replace (v.studyid,'TAS0612_101','TAS0612-101') as studyid,
                                v.siteid,
                                v.usubjid,
                                v.visitnum,
                                v.visit,
                                min(v.svstdtc) as svstdtc,
                                max(v.svendtc) as svendtc
                        from   (
                        SELECT studyid,
                                siteid,
                                usubjid,
                                visitnum,
                                visit,
                                svstdtc,
                                svendtc
                        FROM sv_data
                        UNION ALL
                        SELECT studyid,
                                siteid,
                                usubjid,
                                visitnum,
                                visit,
                                svstdtc ,
                                svendtc
                        FROM formdata_visits
						) v
                        group by studyid, siteid, usubjid, visitnum,visit
                        )
						

SELECT 
        /*KEY (sv.studyid || '~' || sv.siteid || '~' || sv.usubjid)::text AS comprehendid, KEY*/
        sv.studyid::text AS studyid,
        sv.siteid::text AS siteid,
        sv.usubjid::text AS usubjid, 
        sv.visitnum::numeric AS visitnum,
        sv.visit::text AS visit,
        1::int  AS visitseq,
        sv.svstdtc::date AS svstdtc,
        sv.svendtc::date AS svendtc
        /*KEY , (sv.studyid || '~' || sv.siteid || '~' || sv.usubjid || '~' || sv.visit)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM all_visits sv
JOIN included_subjects s ON (sv.studyid = s.studyid AND sv.siteid = s.siteid AND sv.usubjid = s.usubjid);






