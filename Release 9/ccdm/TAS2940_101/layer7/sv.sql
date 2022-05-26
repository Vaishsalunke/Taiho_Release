/*
CCDM SV mapping
Notes: Standard mapping to CCDM SV table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid from  subject ),

     sv_data AS (
     		Select
sv.studyid,
sv.studyname,
sv.siteid,
sv.usubjid,
sv.visitnum,
sv.visit,
sv.visitseq,
sv.svstdtc,
sv.svendtc from(
                SELECT  "project"::text AS studyid,
                        'TAS2940_101'::text AS studyname,
                        'TAS2940_101_' || split_part("SiteNumber",'_',2)::text AS siteid,
                        "Subject"::text AS usubjid, 
                        "FolderSeq"::numeric AS visitnum,
                        --"InstanceName"::text AS visit,
                        trim(REGEXP_REPLACE
									 (REGEXP_REPLACE
									 (REGEXP_REPLACE
									 (REGEXP_REPLACE
									 ("InstanceName",'\s\([0-9][0-9]\)','')
												 ,'\s\([0-9]\)','')
												 ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
												 ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									 ) as visit,
                        1::int AS visitseq, /* defaulted to 1 - deprecated */
                        min("VISITDAT")::date AS svstdtc,
                        max("VISITDAT")::date AS svendtc from TAS2940_101."VISIT"
                        group by 1,2,3,4,5,6)sv),
                        
                        fd_visit AS (SELECT DISTINCT fd.studyid,
                                    fd.siteid,
                                    fd.usubjid,
                                    99::numeric AS visitnum,
                                    --fd.visit,
                                    trim(REGEXP_REPLACE
									 (REGEXP_REPLACE
									 (REGEXP_REPLACE
									 (REGEXP_REPLACE
									 (fd.visit,'\s\([0-9][0-9]\)','')
												 ,'\s\([0-9]\)','')
												 ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
												 ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									 ) as visit,
                                    coalesce(datacollecteddate,dataentrydate)::date AS svstdtc,
                                    coalesce(datacollecteddate,dataentrydate)::date AS svendtc
                            FROM formdata fd
                            LEFT JOIN sv_data sd ON (fd.studyid = sd.studyid and fd.siteid = sd.siteid and fd.usubjid = sd.usubjid and trim(fd.visit) = trim(sd.visit))
                            WHERE sd.studyid IS NULL AND fd.studyid='TAS2940_101'),
                                 
                        all_visits AS (
                        select studyid,
                        	   studyname,
                               siteid,
                               usubjid,
                               visitnum,
                               visit, 
                               case when rnk = '2' then 1.5 else visitseq end as visitseq,
                               svstdtc,
                               svendtc from(
                        			select	studyid,
                        	   				studyname,
                                			siteid,
                                			usubjid,
                                			visitnum,
                                			visit, 
                                			visitseq,
                                			svstdtc,
                                			svendtc,
                        					rank () over (partition by studyid,siteid,usubjid,visit order by visitnum) as rnk
                        from(SELECT studyid,
                        	   studyname,
                                siteid,
                                usubjid,
                                visitnum,
                                  visit, 
                                 visitseq,
                                svstdtc,
                                svendtc
                        FROM sv_data
                        UNION ALL
                        SELECT studyid,
                        		'TAS120_204' as studyname,
                                siteid,
                                usubjid,
                                visitnum,
                                 visit, 
                                1 as visitseq,
                                min(svstdtc) as svstdtc,
                                max(svendtc) as svendtc
                        FROM fd_visit
                        group by 1,2,3,4,5,6) a)b 
                        --where rnk =1
                        ),

     included_sites AS (
                  SELECT DISTINCT studyid,studyname,siteid,sitecountry,sitecountrycode,sitename,siteregion FROM site)

SELECT distinct
        /*KEY (sv.studyid || '~' || sv.siteid || '~' || sv.usubjid)::text AS comprehendid, KEY*/
        sv.studyid::text AS studyid,
        sv.studyname::text AS studyname,
        sv.siteid::text AS siteid,
        si.sitename::text AS sitename,
        si.siteregion::text AS siteregion,
        si.sitecountry::text AS sitecountry,
        si.sitecountrycode::text AS sitecountrycode,
        sv.usubjid::text AS usubjid, 
        sv.visitnum::numeric AS visitnum,
        sv.visit::text AS visit,
        sv.visitseq::numeric AS visitseq,
        sv.svstdtc::date AS svstdtc,
        sv.svendtc::date AS svendtc
         /*KEY , (sv.studyid || '~' || sv.siteid || '~' || sv.usubjid || '~' || sv.visit)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM all_visits sv
JOIN included_subjects s ON (sv.studyid = s.studyid AND sv.siteid = s.siteid AND sv.usubjid = s.usubjid)
LEFT JOIN included_sites si ON (sv.studyid = si.studyid AND sv.siteid = si.siteid);

