/*
CCDM EX mapping
Notes: Standard mapping to CCDM EX table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     ex_data AS (
                select *,
                row_number() over (partition by e.studyid, e.siteid, e.usubjid ORDER BY e.exstdtc)::int AS exseq
                from(     
                SELECT distinct project ::text AS studyid,
                        'TAS2940_101'::text AS studyname,
                        project||substring("SiteNumber",position ('_' in "SiteNumber"))::text AS siteid,
                        null::text AS sitename,
                        null::text AS sitecountry,
                        "Subject" ::text AS usubjid,
                        --concat("RecordId","PageRepeatNumber","RecordPosition")::int as exseq,                        
                        trim(REGEXP_REPLACE
							(REGEXP_REPLACE
							(REGEXP_REPLACE
							(REGEXP_REPLACE
							("InstanceName",'\s\([0-9][0-9]\)','')
										,'\s\([0-9]\)','')
										,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
										,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
							) ::text AS visit,
                        concat("EXOPFREQ",'-TAS2940-Dose Adjusted') ::text AS extrt,
                        'Locally Advanced or Metastatic Solid Tumor Cancer'::text AS excat,
                        null::text AS exscat,
                        "EXOSDOSE"::numeric AS exdose,
                        null::text AS exdostxt,
                        "EXOSDOSE_Units"::text AS exdosu,
                        null::text AS exdosfrm,
                        null::text AS exdosfrq,
                        null::numeric AS exdostot,
						"EXOCYCSDT"::date AS exstdtc,
                        null::time AS exsttm,
                        null::int AS exstdy,
						"EXOCYCEDT"::date AS exendtc,
                        null::time AS exendtm,
                        null::int AS exendy,
                        null::text AS exdur,
                        null::text AS drugrsp,
                        null::text AS drugrspcd,
						"EXOADJRE"::text AS exacn,
						"EXOMIDRE"::text AS exreason
                from TAS2940_101."EXO" exo
                where "EXOCYCSDT" is not null 
                
                UNION ALL
                
                SELECT distinct project ::text AS studyid,
                        'TAS2940_101'::text AS studyname,
                        project||substring("SiteNumber",position ('_' in "SiteNumber"))::text AS siteid,
                        null::text AS sitename,
                        null::text AS sitecountry,
                        "Subject" ::text AS usubjid,
                        --concat("RecordId","PageRepeatNumber","RecordPosition")::int as exseq,
                        trim(REGEXP_REPLACE
							(REGEXP_REPLACE
							(REGEXP_REPLACE
							(REGEXP_REPLACE
							("InstanceName",'\s\([0-9][0-9]\)','')
										,'\s\([0-9]\)','')
										,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
										,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
							) ::text AS visit,
                        Concat("EXOPFREQ",'-TAS2940-Planned Dose')::text AS extrt,
                        'Locally Advanced or Metastatic Solid Tumor Cancer'::text AS excat,
                        null::text AS exscat,
                        "EXOPDOSE"::numeric AS exdose,
                        null::text AS exdostxt,
                        "EXOPDOSE_Units"::text AS exdosu,
                        null::text AS exdosfrm,
                        null::text AS exdosfrq,
                        null::numeric AS exdostot,
                        "EXOCYCSDT"::date AS exstdtc,
                        null::time AS exsttm,
                        null::int AS exstdy,
                        "EXOCYCEDT"::date AS exendtc,
                        null::time AS exendtm,
                        null::int AS exendy,
                        null::text AS exdur,
                        null::text AS drugrsp,
                        null::text AS drugrspcd,
						"EXOADJRE"::text AS exacn,
						"EXOMIDRE"::text AS exreason
                from TAS2940_101."EXO" exo
                where "EXOCYCSDT" is not null 
				)e
				),
						
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
        ex.drugrspcd::text AS drugrspcd,
		ex.exacn::text AS exacn,
		ex.exreason::text AS exreason
        /*KEY, (ex.studyid || '~' || ex.siteid || '~' || ex.usubjid || '~' || ex.exseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM ex_data ex
JOIN included_subjects s ON (ex.studyid = s.studyid AND ex.siteid = s.siteid AND ex.usubjid = s.usubjid)
join site_data sd on (ex.studyid = sd.studyid AND ex.siteid = sd.siteid)
;




