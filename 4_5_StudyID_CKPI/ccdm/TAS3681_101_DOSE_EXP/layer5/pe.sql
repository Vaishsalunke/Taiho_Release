/*
CCDM PE mapping
Notes: Standard mapping to CCDM PE table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),
				
	maxdate AS (select "project","SiteNumber","Subject","FolderName",max("PEDAT") as pdat
				from tas3681_101."PE"
				group by "project","SiteNumber","Subject","FolderName"),

     pe_data AS (
                SELECT  'TAS3681_101_DOSE_EXP'::text AS studyid,
                        pe."SiteNumber"::text AS siteid,
                        pe."Subject"::text AS usubjid,
                       -- row_number() OVER (PARTITION BY pe."studyid", pe."siteid", pe."Subject" ORDER BY pe."serial_id")::int AS peseq,
					   concat("RecordPosition","instanceId","PageRepeatNumber")::int AS peseq,
                        'PE'::text AS petestcd,
                        'Physical Examination'::text AS petest,
                        'Physical Examination'::text AS pecat,
                        'Physical Examination'::text AS pescat,
                        null::text AS pebodsys,
                        null::text AS peorres,
                        ''::text AS peorresu,
                        null::text AS pestat,
                        null::text AS peloc,
                        --REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE("InstanceName",'<WK[0-9]D[0-9]/>\sEscalation',''),'<WK[0-9]D[0-9][0-9]/>\sEscalation',''),' Escalation ',' '),'\s\([0-9]\)',''),' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''):: text as visit,
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
					("InstanceName",'<WK[0-9]D[0-9]/>\sExpansion','')
								,'<WK[0-9]D[0-9][0-9]/>\sExpansion','')
								,'<WK[0-9]DA[0-9]/>\sExpansion','')
								,'<WK[0-9]DA[0-9][0-9]/>\sExpansion','')
								,'<W[0-9]DA[0-9]/>\sExpansion','')
								,'<W[0-9]DA[0-9][0-9]/>\sExpansion','')
								,'Expansion','')
								,'\s\([0-9][0-9]\)','')
								,'\s\([0-9]\)','')
								,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
								,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
				    )	:: text as visit,
                        pe."PEDAT"::timestamp without time zone AS pedtc,
                        null::time without time zone AS petm 
				FROM tas3681_101."PE" pe JOIN maxdate m
				ON pe."project" = m."project"
				and pe."SiteNumber" = m."SiteNumber"
				and pe."Subject" = m."Subject"
				and pe."FolderName" = m."FolderName"
				and pe."PEDAT"=m."pdat"
				where pe."PEPERF" = 'Yes'
				)

SELECT
        /*KEY (pe.studyid || '~' || pe.siteid || '~' || pe.usubjid)::text AS comprehendid, KEY*/
        pe.studyid::text AS studyid,
        pe.siteid::text AS siteid,
        pe.usubjid::text AS usubjid,
        pe.peseq::int AS peseq,
        pe.petestcd::text AS petestcd,
        pe.petest::text AS petest,
        pe.pecat::text AS pecat,
        pe.pescat::text AS pescat,
        pe.pebodsys::text AS pebodsys,
        pe.peorres::text AS peorres,
        pe.peorresu::text AS peorresu,
        pe.pestat::text AS pestat,
        pe.peloc::text AS peloc,
        pe.visit::text AS visit,
        pe.pedtc::timestamp without time zone AS pedtc,
        pe.petm::time without time zone AS petm
        /*KEY , (pe.studyid || '~' || pe.siteid || '~' || pe.usubjid || '~' || peseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM pe_data pe
JOIN included_subjects s ON (pe.studyid = s.studyid AND pe.siteid = s.siteid AND pe.usubjid = s.usubjid)
;

