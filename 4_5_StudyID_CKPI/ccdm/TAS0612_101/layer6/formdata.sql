/*
CCDM FormData mapping
Client: Regeneron
Notes: Standard mapping to CCDM FormData table
*/

WITH form_data AS (
  SELECT
    'TAS0612-101' AS studyid,
    s.siteid AS siteid,
    fd.usubjid AS usubjid,
    fd.formid AS formid,
    fd.formseq AS formseq,
    --fd.visit AS visit,
	trim(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(fd.visit,'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''))::text AS visit,
    fd.visitseq AS visitseq,
    MIN(fd.dataentrydate) AS dataentrydate,
    MIN(fd.datacollecteddate) AS datacollecteddate,
    MIN(fd.sdvdate) AS sdvdate
  FROM
    fielddata fd
    JOIN subject s ON (fd.studyid = s.studyid AND fd.usubjid = s.usubjid AND fd.siteid = s.siteid)
    JOIN fielddef fdef ON (fd.studyid = fdef.studyid AND fd.formid = fdef.formid)
GROUP BY
  fd.studyid,
  s.siteid,
  fd.usubjid,
  fd.formid,
  fd.visit,
  fd.visitseq,
  fd.formseq
)
  
SELECT
  /*KEY (studyid || '~' || siteid || '~' || usubjid)::TEXT AS comprehendid, KEY*/
  studyid::TEXT AS studyid,
  siteid::TEXT AS siteid,
  usubjid::TEXT AS usubjid,
  formid::TEXT AS formid,
  formseq::INTEGER AS formseq,
  visit::TEXT AS visit,
  visitseq::INTEGER AS visitseq,
  dataentrydate::DATE AS dataentrydate,
  datacollecteddate::DATE AS datacollecteddate,
  sdvdate::DATE AS sdvdate 
  /*KEY , (studyid || '~' || siteid || '~' || usubjid || '~' || visit || '~' || visitseq || '~' || formid || '~' || formseq )::TEXT AS objectuniquekey KEY*/
  /*KEY , NOW()::TIMESTAMP WITHOUT TIME ZONE AS comprehend_update_time KEY*/
FROM
  form_data;

