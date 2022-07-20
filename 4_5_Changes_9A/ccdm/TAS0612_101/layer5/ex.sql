/*
CCDM EX mapping
Notes: Standard mapping to CCDM EX table
*/

WITH included_subjects AS (
    SELECT DISTINCT studyid, siteid, usubjid FROM subject),

    ex_data AS (             
             SELECT
                ex."project"::	text AS studyid,
                'TAS0612_101':: text as studyname,
                --ex."SiteNumber":: text AS siteid,
				concat('TAS0612_101_',split_part(ex."SiteNumber",'_',2))::text AS siteid,
                ex."Subject":: text AS usubjid,
                --row_number() over (partition by ex."studyid", ex."siteid", ex."Subject" ORDER BY ex."EXOSTDAT")::int AS exseq,
                concat("instanceId","PageRepeatNumber","RecordPosition")::int AS exseq,
              /*REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("InstanceName",'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),'Escalation',''):: text as visit,*/
			    trim(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						("InstanceName",'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: text as visit,
                'TAS0612':: text AS extrt,
                'Exposure':: text AS excat,
                NULL:: text AS exscat,
                ex."EXOSDOSE"::	NUMERIC AS exdose,
                NULL:: text AS exdostxt,
                ex."EXOSDOSE_Units":: text AS exdosu,
                NULL::					text      AS exdosfrm,
                NULL::					text      AS exdosfrq,
                NULL::					NUMERIC   AS exdostot,
                ex."EXOSTDAT"::			DATE      AS exstdtc,
                --ex."EXOSTTM"::time without time zone AS exsttm,
                Null::time without time zone AS exsttm,
                NULL::					INT 	  AS exstdy,
                ex."EXOENDAT"::			DATE      AS exendtc,
                --ex."EXOENTM"::time without time zone AS exendtm,
                 Null::time without time zone AS exendtm,
                NULL::					INT       AS exendy,
                NULL::					text      AS exdur,
                null::					text 	  as drugrsp,
                null:: 					text AS drugrspcd,
				"EXOADJRE"::text AS exacn,
				"EXOMIDRE"::text AS exreason
            FROM tas0612_101."EXO" ex   
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
      /*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
FROM ex_data ex
JOIN included_subjects s ON (ex.studyid = s.studyid AND ex.siteid = s.siteid AND ex.usubjid = s.usubjid)
join site_data sd on (ex.studyid = sd.studyid AND ex.siteid = sd.siteid);






