/*
CCDM EG mapping
Notes: Standard mapping to CCDM EG table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     eg_data AS (
                SELECT  null::text AS studyid,
                        null::text AS siteid,
                        null::text AS usubjid,
                        null::int AS egseq,
                        null::text AS egtestcd,
                        null::text AS egtest,
                        null::text AS egcat,
                        null::text AS egscat,
                        null::text AS egpos,
                        null::text AS egorres,
                        null::text AS egorresu,
                        null::numeric AS egstresn,
                        null::text AS egstresu,
                        null::text AS egstat,
                        null::text AS egloc,
                        null::text AS egblfl,
                        null::text AS visit,
                        null::timestamp without time zone AS egdtc,
                        null::time without time zone AS egtm )

SELECT
        /*KEY (eg.studyid::text || '~' || eg.siteid::text || '~' || eg.usubjid::text) AS comprehendid, KEY*/
        eg.studyid::text AS studyid,
        eg.siteid::text AS siteid,
        eg.usubjid::text AS usubjid,
        eg.egseq::int AS egseq,
        eg.egtestcd::text AS egtestcd,
        eg.egtest::text AS egtest,
        eg.egcat::text AS egcat,
        eg.egscat::text AS egscat,
        eg.egpos::text AS egpos,
        eg.egorres::text AS egorres,
        eg.egorresu::text AS egorresu,
        eg.egstresn::numeric AS egstresn,
        eg.egstresu::text AS egstresu,
        eg.egstat::text AS egstat,
        eg.egloc::text AS egloc,
        eg.egblfl::text AS egblfl,
        eg.visit::text AS visit,
        eg.egdtc::timestamp without time zone AS egdtc,
        eg.egtm::time without time zone AS egtm
        /*KEY , (eg.studyid || '~' || eg.siteid || '~' || eg.usubjid || '~' || eg.egseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM eg_data eg
JOIN included_subjects s ON (eg.studyid = s.studyid AND eg.siteid = s.siteid AND eg.usubjid = s.usubjid)
WHERE 1=2;
