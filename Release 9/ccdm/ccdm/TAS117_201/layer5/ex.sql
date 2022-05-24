/*
CCDM EX mapping
Notes: Standard mapping to CCDM EX table
*/


WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     ex_data AS (
                SELECT  project ::text AS studyid,
                        null::text AS studyname,
                        concat(project,substring("SiteNumber",position('_' in "SiteNumber")))::text AS siteid,
                        null::text AS sitename,
                        null::text AS sitecountry,
                        "Subject" ::text AS usubjid,
                        concat("instanceId","PageRepeatNumber","RecordPosition")::int AS exseq, /*(row_number() over (partition by [studyid],[siteid],[usubjid] order [exstdtc,exsttm]))::int AS exseq,*/
                        trim(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						("InstanceName",'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')) ::text AS visit,
                        'TAS117'::text AS extrt,
                        'Advanced or Metastatic Solid Tumors With Germline PTEN Inactivating Mutations'::text AS excat,
                        null::text AS exscat,
                        convert_to_numeric("DAACDS")::numeric AS exdose,
                        null::text AS exdostxt,
                        'mg'::text AS exdosu,
                        null::text AS exdosfrm,
                        null::text AS exdosfrq,
                        null::numeric AS exdostot,
                        "DADAT" ::date AS exstdtc,
                        null::time AS exsttm,
                        null::int AS exstdy,
                        "DAENDAT" ::date AS exendtc,
                        null::time AS exendtm,
                        null::int AS exendy,
                        null::text AS exdur,
                        null::text AS drugrsp,
                        null::text AS drugrspcd
                        from tas117_201."DA" d  ),
	
     site_data as (select distinct studyid,siteid,sitename,sitecountry,sitecountrycode,siteregion from site)	

SELECT
        /*KEY (ex.studyid || '~' || ex.siteid || '~' || ex.usubjid)::text AS comprehendid, KEY*/
        ex.studyid::text AS studyid,
        ex.studyname::text AS studyname,
        ex.siteid::text AS siteid,
        sd.sitename::text AS sitename,
        sd.sitecountry::text AS sitecountry,
        ex.usubjid::text AS usubjid,
        ex.exseq::int AS exseq, 
        ex.visit::text AS visit,
        ex.extrt::text AS extrt,
        ex.excat::text AS excat,
        ex.exscat::text AS exscat,
        ex.exdose::numeric AS exdose,
        ex.exdostxt::text AS exdostxt,
        ex.exdosu::text AS exdosu,
        ex.exdosfrm::text AS exdosfrm,
        ex.exdosfrq::text AS exdosfrq,
        ex.exdostot::numeric AS exdostot,
        ex.exstdtc::date AS exstdtc,
        ex.exsttm::time AS exsttm,
        ex.exstdy::int AS exstdy,
        ex.exendtc::date AS exendtc,
        ex.exendtm::time AS exendtm,
        ex.exendy::int AS exendy,
        ex.exdur::text AS exdur,
        ex.drugrsp::text AS drugrsp,
        ex.drugrspcd::text AS drugrspcd
        /*KEY , (ex.studyid || '~' || ex.siteid || '~' || ex.usubjid || '~' || ex.exseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM ex_data ex
JOIN included_subjects s ON (ex.studyid = s.studyid AND ex.siteid = s.siteid AND ex.usubjid = s.usubjid)
join site_data sd on (ex.studyid = sd.studyid AND ex.siteid = sd.siteid);












