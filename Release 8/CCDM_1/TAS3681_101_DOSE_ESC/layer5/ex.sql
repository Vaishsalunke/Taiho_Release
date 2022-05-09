
/*
CCDM EX mapping
Notes: Standard mapping to CCDM EX table
*/

WITH included_subjects AS (
    SELECT DISTINCT studyid, siteid, usubjid FROM subject),
	
included_sites AS (
SELECT DISTINCT studyid, siteid, sitename, sitecountry,sitecountrycode, siteregion FROM site),	

    ex_data AS (
	    select studyid,
		        siteid,
				usubjid,				
				trim(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(visit
								,'<WK[0-9]D[0-9]/>\sExpansion','')
								,'<WK[0-9]D[0-9][0-9]/>\sExpansion','')
								,'<WK[0-9]DA[0-9]/>\sExpansion','')
								,'<WK[0-9]DA[0-9][0-9]/>\sExpansion','')
								,'<W[0-9]DA[0-9]/>\sExpansion','')
								,'<W[0-9]DA[0-9][0-9]/>\sExpansion','')
								,'Expansion','')
								
				    ) as visit,
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
				exseq
	--row_number() over (partition by studyid, siteid, usubjid ORDER BY exstdtc)::int AS exseq
	  from(
             -- TAS3681-101
             SELECT
               'TAS3681_101_DOSE_ESC'::				text      AS studyid,
                ex."SiteNumber"::	text      AS siteid,
                ex."Subject"::	text      AS usubjid,
                concat("instanceId","RecordPosition")::int AS exseq,
             --  REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("InstanceName",'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),'Escalation',''):: text as visit,
                trim(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					("InstanceName",'<WK[0-9]D[0-9]/>\sEscalation','')
								,'<WK[0-9]D[0-9][0-9]/>\sEscalation','')
								,'<WK[0-9]DA[0-9]/>\sEscalation','')
								,'<WK[0-9]DA[0-9][0-9]/>\sEscalation','')
								,'<W[0-9]DA[0-9]/>\sEscalation','')
								,'<W[0-9]DA[0-9][0-9]/>\sEscalation','')
								,' Escalation','')
								,'\s\([0-9][0-9]\)','')
								,'\s\([0-9]\)','')
								,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
								,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
				    ):: text as visit,


                'TAS3681'::					text      AS extrt,
                'Prostate Cancer'::					text      AS excat,
                NULL::					text      AS exscat,
                ex."EXOSDOSE"::			NUMERIC   AS exdose,
                NULL::					text      AS exdostxt,
                ex."EXOSDOSE_Units"::	text      AS exdosu,
                NULL::					text      AS exdosfrm,
                NULL::					text      AS exdosfrq,
                NULL::					NUMERIC   AS exdostot,
                ex."EXOSTDAT"::			DATE      AS exstdtc,
                ex."EXOSMDAT"::time without time zone AS exsttm,
                NULL::					INT 	  AS exstdy,
                ex."EXOENDAT"::			DATE      AS exendtc,
                ex."EXOEMDAT"::time without time zone AS exendtm,
                NULL::					INT       AS exendy,
                NULL::					text      AS exdur
           FROM tas3681_101."EXO" ex
			
			Union all
			
			SELECT
                'TAS3681_101_DOSE_ESC'::				text      AS studyid,
                ex2."SiteNumber"::	text      AS siteid,
                ex2."Subject"::	text      AS usubjid,
                concat("instanceId","RecordPosition")::int AS exseq,
              -- REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("InstanceName",'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),'Escalation',''):: text as visit,
                trim(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					(REGEXP_REPLACE
					("InstanceName",'<WK[0-9]D[0-9]/>\sEscalation','')
								,'<WK[0-9]D[0-9][0-9]/>\sEscalation','')
								,'<WK[0-9]DA[0-9]/>\sEscalation','')
								,'<WK[0-9]DA[0-9][0-9]/>\sEscalation','')
								,'<W[0-9]DA[0-9]/>\sEscalation','')
								,'<W[0-9]DA[0-9][0-9]/>\sEscalation','')
								,' Escalation','')
								,'\s\([0-9][0-9]\)','')
								,'\s\([0-9]\)','')
								,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
								,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
				    ):: text as visit,


                'TAS3681'::					text      AS extrt,
                'Prostate Cancer'::					text      AS excat,
                NULL::					text      AS exscat,
                ex2."EXOSDOSE"::			NUMERIC   AS exdose,
                NULL::					text      AS exdostxt,
                ex2."EXOSDOSE_Units"::	text      AS exdosu,
                NULL::					text      AS exdosfrm,
                NULL::					text      AS exdosfrq,
                NULL::					NUMERIC   AS exdostot,
                ex2."EXOSTDAT"::			DATE      AS exstdtc,
                ex2."EXOSTDAT"::time without time zone AS exsttm,
                NULL::					INT 	  AS exstdy,
                ex2."EXOENDAT"::			DATE      AS exendtc,
                --REGEXP_REPLACE(upper(nullif(ex2."EXOENDAT",'')),'[A-Z]',' ')::time without time zone AS exendtm,
				ex2."EXOENDAT"::time without time zone AS exendtm,
                NULL::					INT       AS exendy,
                NULL::					text      AS exdur
           FROM tas3681_101."EXO2" ex2
           )a         
     
     )

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
      /*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
FROM ex_data ex
JOIN included_subjects s ON (ex.studyid = s.studyid AND ex.siteid = s.siteid AND ex.usubjid = s.usubjid)
JOIN included_sites si ON (ex.studyid = si.studyid AND ex.siteid = si.siteid)
;


