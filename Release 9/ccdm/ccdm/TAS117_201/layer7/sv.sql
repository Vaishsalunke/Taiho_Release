/*
CCDM SV mapping
Notes: Standard mapping to CCDM SV table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     sv_data AS (
                --screening
SELECT  project ::text AS studyid,
                        concat(project,substring("SiteNumber",position('_' in "SiteNumber"))) ::text AS siteid,
                        "Subject" ::text AS usubjid, 
                        "FolderSeq" ::numeric AS visitnum,
                         trim(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(v."InstanceName",'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''))
                        ::text AS visit,
                        1::int AS visitseq, /* defaulted to 1 - deprecated */
                        "VISITDAT" ::date AS svstdtc,
                        "VISITDAT" ::date AS svendtc
                        from tas117_201."VISIT" v 
union all
---C1D1

SELECT  project ::text AS studyid,
                        concat(project,substring("SiteNumber",position('_' in "SiteNumber"))) ::text AS siteid,
                        "Subject" ::text AS usubjid, 
                        "FolderSeq" ::numeric AS visitnum,
                        trim(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(d."InstanceName",'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''))
                        ::text AS visit,
                        1::int AS visitseq, /* defaulted to 1 - deprecated */
                        "VISITDAT" ::date AS svstdtc,
                        "VISITDAT" ::date AS svendtc
                        from tas117_201."DOVCD" d
union all
---C1D8

SELECT  project ::text AS studyid,
                        concat(project,substring("SiteNumber",position('_' in "SiteNumber"))) ::text AS siteid,
                        "Subject" ::text AS usubjid, 
                        "FolderSeq" ::numeric AS visitnum,
                       trim(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(d."InstanceName",'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''))
                        ::text AS visit,
                        1::int AS visitseq, /* defaulted to 1 - deprecated */
                        "VISITDAT" ::date AS svstdtc,
                        "VISITDAT" ::date AS svendtc
                        from tas117_201."DOVCD8" d
union all
---C1D15

SELECT  project ::text AS studyid,
                        concat(project,substring("SiteNumber",position('_' in "SiteNumber"))) ::text AS siteid,
                        "Subject" ::text AS usubjid, 
                        "FolderSeq" ::numeric AS visitnum,
                        trim(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(d."InstanceName",'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''))
                        ::text AS visit,
                        1::int AS visitseq, /* defaulted to 1 - deprecated */
                        "VISITDAT" ::date AS svstdtc,
                        "VISITDAT" ::date AS svendtc
                        from tas117_201."DOVCD15" d 
union all
---Cycle 2 Onwards

SELECT  project ::text AS studyid,
                        concat(project,substring("SiteNumber",position('_' in "SiteNumber"))) ::text AS siteid,
                        "Subject" ::text AS usubjid, 
                        "FolderSeq" ::numeric AS visitnum,
                       trim(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(d."InstanceName",'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''))
                        ::text AS visit,
                        1::int AS visitseq, /* defaulted to 1 - deprecated */
                        max("VISITDAT") ::date AS svstdtc,
                        max("VISITDAT") ::date AS svendtc
                        from tas117_201."DOVC2" d group by 1,2,3,4,5,6
union all
---Safety Follow Up

SELECT  project ::text AS studyid,
                        concat(project,substring("SiteNumber",position('_' in "SiteNumber"))) ::text AS siteid,
                        "Subject" ::text AS usubjid, 
                        "FolderSeq" ::numeric AS visitnum,
                        trim(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(d."InstanceName",'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''))
                        ::text AS visit,
                        1::int AS visitseq, /* defaulted to 1 - deprecated */
                        "VISIT"::date AS svstdtc,
                        "VISIT" ::date AS svendtc
                        from tas117_201."DOVSFU" d 
                ),
				fd_visit AS (
                        SELECT DISTINCT fd.studyid,
                                    fd.siteid,
                                    fd.usubjid,
                                    99::numeric AS visitnum,
     trim(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(fd.visit,'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''))::text AS visit,
                                    coalesce(datacollecteddate,dataentrydate)::date AS svstdtc,
                                    coalesce(datacollecteddate,dataentrydate)::date AS svendtc
                            FROM formdata fd
                            LEFT JOIN sv_data sd ON (fd.studyid = sd.studyid and fd.siteid = sd.siteid and fd.usubjid = sd.usubjid and trim(fd.visit) = trim(sd.visit))
                            WHERE sd.studyid IS NULL AND fd.studyid='TAS117_201'),
                                 
                        all_visits AS (
                        SELECT studyid,
                        	   --studyname,
                                siteid,
                                usubjid,
                                visitnum,
                                 visit, 
                                 visitseq,
                                svstdtc,
                                svendtc 
                        FROM sv_data
                        UNION ALL
                        SELECT studyid,--'TAS117_201' as studyname,
                                siteid,
                                usubjid,
                                visitnum,
                                visit,
                               1 as visitseq,
                                min(svstdtc) as svstdtc,
                                max(svendtc) as svendtc
                        FROM fd_visit
                        group by 1,2,3,4,5,6
                        ),

     included_sites AS (
                  SELECT DISTINCT studyid,studyname,siteid,sitecountry,sitecountrycode,sitename,siteregion FROM site)

SELECT 
        /*KEY (sv.studyid || '~' || sv.siteid || '~' || sv.usubjid)::text AS comprehendid, KEY*/
        sv.studyid::text AS studyid,
        si.studyname::text AS studyname,
        sv.siteid::text AS siteid,
        si.sitename::text AS sitename,
        si.siteregion::text AS siteregion,
        si.sitecountry::text AS sitecountry,
        si.sitecountrycode::text AS sitecountrycode,
        sv.usubjid::text AS usubjid, 
        sv.visitnum::numeric AS visitnum,
        sv.visit::text AS visit,
        sv.visitseq::int AS visitseq,
        sv.svstdtc::date AS svstdtc,
        sv.svendtc::date AS svendtc
        /*KEY , (sv.studyid || '~' || sv.siteid || '~' || sv.usubjid || '~' || sv.visit)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM all_visits sv
JOIN included_subjects s ON (sv.studyid = s.studyid AND sv.siteid = s.siteid AND sv.usubjid = s.usubjid)
LEFT JOIN included_sites si ON (sv.studyid = si.studyid AND sv.siteid = si.siteid);









