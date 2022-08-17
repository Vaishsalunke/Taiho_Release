/*
CCDM EX mapping
Notes: Standard mapping to CCDM EX table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     ex_data AS (
     			SELECT
     REPLACE(studyid,'TAS120_204','TAS-120-204') AS studyid,
     siteid,
     usubjid,
     row_number() over (partition by ex."studyid", ex."siteid", ex."usubjid" ORDER BY ex."exstdtc")::int AS exseq,
	 --exseq,
     visit,
     extrt,
     excat,
     exscat,
     exdose,
     exdostxt,
     exdosu,
     exdosfrm,
     exdosfrq,
     exdostot,
     exstdtc,
     exsttm,
     exstdy,
     exendtc,
     exendtm,
     exendy,
     exdur,
	 exacn,
	 exreason
	 ,null::text AS studyname
	 ,null::text AS drugrsp
	 ,null::text AS drugrspcd
     from (
                SELECT distinct project ::text AS studyid,
                        'TAS120_204'::text AS studyname,
                        project||substring("SiteNumber",position ('_' in "SiteNumber"))::text AS siteid,
                        null::text AS sitename,
                        null::text AS sitecountry,
                        "Subject" ::text AS usubjid,
                        --concat("instanceId","RecordPosition") ::int AS exseq, 
                        null::int as exseq,
                        /*(row_number() over (partition by [studyid],[siteid],[usubjid] order [exstdtc,exsttm]))::int AS exseq,*/
                        trim(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						("InstanceName",'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')) ::text AS visit,
                        'Futibatinib-Dose Adjusted'::text AS extrt,
                        'KRAS Gene Mutation'::text AS excat,
                        null::text AS exscat,
                        "EXOSDOSE"::numeric AS exdose,
                        null::text AS exdostxt,
                       "EXOSDOSE_Units"::text AS exdosu,
                        null::text AS exdosfrm,
                        null::text AS exdosfrq,
                        null::numeric AS exdostot,
                        --coalesce("EXOCYCSDT","EXOSTDAT") ::date AS exstdtc,
						"EXOSTDAT" ::date AS exstdtc,
                        null::time AS exsttm,
                        null::int AS exstdy,
                        --"EXOENDAT" ::date AS exendtc,
						"EXOENDAT" ::date AS exendtc,
                        null::time AS exendtm,
                        null::int AS exendy,
                        null::text AS exdur,
                        null::text AS drugrsp,
                        null::text AS drugrspcd,
						"EXOADJRE"::text AS exacn,
						"EXOMIDRE"::text AS exreason
                        from tas120_204."EXO" exo 
                        where "EXOSTDAT" is not null 
 union all
 						SELECT distinct project ::text AS studyid,
                        'TAS120_204'::text AS studyname,
                        project||substring("SiteNumber",position ('_' in "SiteNumber"))::text AS siteid,
                        null::text AS sitename,
                        null::text AS sitecountry,
                        "Subject" ::text AS usubjid,
                        --concat("instanceId","RecordPosition") ::int AS exseq, 
                        null::int as exseq,
                        /*(row_number() over (partition by [studyid],[siteid],[usubjid] order [exstdtc,exsttm]))::int AS exseq,*/
                        trim(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						("InstanceName",'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')) ::text AS visit,
                        'Futibatinib-Planned Dose'::text AS extrt,
                        'KRAS Gene Mutation'::text AS excat,
                        null::text AS exscat,
                        "EXOPDOSE"::numeric AS exdose,
                        null::text AS exdostxt,
                       "EXOPDOSE_Units"::text AS exdosu,
                        null::text AS exdosfrm,
                        null::text AS exdosfrq,
                        null::numeric AS exdostot,
                        --coalesce("EXOCYCSDT","EXOSTDAT") ::date AS exstdtc,
						"EXOCYCSDT" ::date AS exstdtc,
                        null::time AS exsttm,
                        null::int AS exstdy,
                        --"EXOENDAT" ::date AS exendtc,
						"EXOCYCEDT" ::date AS exendtc,
                        null::time AS exendtm,
                        null::int AS exendy,
                        null::text AS exdur,
                        null::text AS drugrsp,
                        null::text AS drugrspcd,
						"EXOADJRE"::text AS exacn,
						"EXOMIDRE"::text AS exreason
                        from tas120_204."EXO" exo 
                        where "EXOCYCSDT" is not null 
 union all
 					
 SELECT  distinct project ::text AS studyid,
                        'TAS120_204'::text AS studyname,
                        project||substring("SiteNumber",position ('_' in "SiteNumber"))::text AS siteid,
                        null::text AS sitename,
                        null::text AS sitecountry,
                        "Subject" ::text AS usubjid,
                        null::int as exseq,
                       -- concat("instanceId","RecordPosition") ::int AS exseq, /*(row_number() over (partition by [studyid],[siteid],[usubjid] order [exstdtc,exsttm]))::int AS exseq,*/
                        trim(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						("InstanceName",'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')) ::text AS visit,
                    case when "EXOADJDS" is not null then  concat('Binimetinib','-',"EXOADJDS",'-Dose Adjusted') else 'Binimetinib-Dose Adjusted' end ::text AS extrt,
                        'KRAS Gene Mutation'::text AS excat,
                        null::text AS exscat,
                     "EXOSDOSE2" ::numeric AS exdose,
                        null::text AS exdostxt,
                        "EXOSDOSE2_Units"::text AS exdosu,
                        null::text AS exdosfrm,
                        null::text AS exdosfrq,
                        null::numeric AS exdostot,
                       -- coalesce("EXOCYCSDT","EXOSTDAT")::date AS exstdtc,
					 "EXOSTDAT"::date AS exstdtc,
                        null::time AS exsttm,
                        null::int AS exstdy,
                        "EXOENDAT"::date AS exendtc,
                        null::time AS exendtm,
                        null::int AS exendy,
                        null::text AS exdur,
                        null::text AS drugrsp,
                        null::text AS drugrspcd,
						"EXOADJRE"::text AS exacn,
						"EXOMIDRE"::text AS exreason
                        from tas120_204."EXO2" exo2
                         where  "EXOSTDAT" is not null
    union all       
    					 SELECT  distinct project ::text AS studyid,
                        'TAS120_204'::text AS studyname,
                        project||substring("SiteNumber",position ('_' in "SiteNumber"))::text AS siteid,
                        null::text AS sitename,
                        null::text AS sitecountry,
                        "Subject" ::text AS usubjid,
                       null::int as exseq,
                       -- concat("instanceId","RecordPosition") ::int AS exseq, /*(row_number() over (partition by [studyid],[siteid],[usubjid] order [exstdtc,exsttm]))::int AS exseq,*/
                        trim(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						("InstanceName",'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')) ::text AS visit,
                      case when nullif ("EXOADJDS",'' )is not null then  concat('Binimetinib','-',"EXOADJDS",'-Planned Dose') else 'Binimetinib-Planned Dose' end::text AS extrt,
                        'KRAS Gene Mutation'::text AS excat,
                        null::text AS exscat,
                     "EXOPDOSE2" ::numeric AS exdose,
                        null::text AS exdostxt,
                        "EXOPDOSE2_Units"::text AS exdosu,
                        null::text AS exdosfrm,
                        null::text AS exdosfrq,
                        null::numeric AS exdostot,
                       -- coalesce("EXOCYCSDT","EXOSTDAT")::date AS exstdtc,
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
                        from tas120_204."EXO2" exo2
                         where  "EXOCYCSDT" is not null
 ) ex),
                         
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
join site_data sd on (ex.studyid = sd.studyid AND ex.siteid = sd.siteid);
















