

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),
             

     ae_sev_toxgr_data AS (
                SELECT  "project"::text AS studyid,
                        concat(concat("project",'_'),split_part("SiteNumber",'_',2))::text AS siteid,
                        "Subject"::text AS usubjid,
                        "AETERM_PT"::text AS aeterm,
                        concat("RecordPosition","PageRepeatNumber")::integer AS aeseq,
                        "AESPID" ::text AS aespid,
                        concat("RecordPosition","PageRepeatNumber")::integer AS faseq,
                        'SEV'::text AS fatestcd,
                        'Severity'::text AS fatest,
                        CASE
                        WHEN("AEGR")= '0' THEN 'G0'
            WHEN("AEGR")= '1' THEN 'G1'
      WHEN("AEGR")= '2' THEN 'G2'
      WHEN("AEGR")= '3' THEN 'G3'
      WHEN("AEGR")= '4' THEN 'G4'
      WHEN("AEGR")= '5' THEN 'G5'                                
            WHEN(("AEGR")= '' OR ("AEGR") IS NULL) THEN 'Missing'
      END::text AS faorres,
      "AEGRDAT"::timestamp without time zone AS fastdtc,
                        "AEGRSTDAT"::timestamp without time zone AS faendtc
              FROM tas120_204."AE2"
                 WHERE NULLIF("AETERM_PT", '') IS NOT NULL
                )

SELECT 
        /*KEY (aegrd.studyid || '~' || aegrd.siteid || '~' || aegrd.usubjid)::text AS comprehendid, KEY*/
        aegrd.studyid::text AS studyid,
        aegrd.siteid::text AS siteid,
        aegrd.usubjid::text AS usubjid,
        aegrd.aeterm::text AS aeterm,
        aegrd.aeseq::integer AS aeseq,
        aegrd.aespid::text AS aespid,
        aegrd.faseq::integer AS faseq,
        aegrd.fatestcd::text AS fatestcd,        
        aegrd.fatest::text AS fatest,
        aegrd.faorres::text AS faorres,
        aegrd.fastdtc::date AS fastdtc,
        aegrd.faendtc::date AS faendtc
         /*KEY, (aegrd.studyid || '~' || aegrd.siteid || '~' || aegrd.usubjid || '~' || aegrd.aeseq || '~' || aegrd.faseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM ae_sev_toxgr_data aegrd
JOIN included_subjects s ON (aegrd.studyid = s.studyid AND aegrd.siteid = s.siteid AND aegrd.usubjid = s.usubjid);

