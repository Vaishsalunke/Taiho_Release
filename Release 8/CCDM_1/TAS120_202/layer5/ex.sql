/*
CCDM EX mapping
Notes: Standard mapping to CCDM EX table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),
	
	included_site AS (
	SELECT DISTINCT studyid, siteid, sitename, sitecountry, sitecountrycode, siteregion FROM site),	

     ex_data AS (
                SELECT  "project"::text AS studyid,
                        "SiteNumber"::text AS siteid,
                        "Subject"::text AS usubjid,
                       -- (row_number() over (partition by "project","SiteNumber","Subject" order by  "EXOSTDAT","EXOENDAT"))::int AS exseq,
					   /*(row_number() over (partition by [studyid],[siteid],[usubjid] order [exstdtc,exsttm]))::int AS exseq,*/
					   concat("instanceId","RecordPosition")::int as exseq,
                        "FolderName"::text AS visit,
                        'TAS120_202'::text AS extrt,
                        'Solid Tumors, Gastric or GEJ Cancers and Myeloid or Lymphoid Neoplasms'::text AS excat,
                        null::text AS exscat,
                        "EXOSDOSE"::numeric AS exdose,
                        null::text AS exdostxt,
                        "EXOSDOSE_Units"::text AS exdosu,
                        null::text AS exdosfrm,
                        null::text AS exdosfrq,
                        null::numeric AS exdostot,
                        "EXOSTDAT"::date AS exstdtc,
                        null::time without time zone AS exsttm,
                        null::int AS exstdy,
                        "EXOENDAT"::date AS exendtc,
                        null::time without time zone AS exendtm,
                        null::int AS exendy,
                        null::text AS exdur 
				from "tas120_202"."EXO" )

SELECT
        /*KEY (ex.studyid || '~' || ex.siteid || '~' || ex.usubjid)::text AS comprehendid, KEY*/
        ex.studyid::text AS studyid,
        null::text AS studyname,
        ex.siteid::text AS siteid,
        si.sitename::text AS sitename,
        si.sitecountry::text AS sitecountry,
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
        null::text AS drugrsp,
        null::text AS drugrspcd
        /*KEY , (ex.studyid || '~' || ex.siteid || '~' || ex.usubjid || '~' || ex.exseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM ex_data ex
JOIN included_subjects s ON (ex.studyid = s.studyid AND ex.siteid = s.siteid AND ex.usubjid = s.usubjid)
JOIN included_site si ON (ex.studyid = si.studyid AND ex.siteid = si.siteid);

