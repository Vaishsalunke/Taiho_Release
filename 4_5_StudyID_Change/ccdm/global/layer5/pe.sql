/*
CCDM PE mapping
Notes: Standard mapping to CCDM PE table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     pe_data AS (
                SELECT  null::text AS studyid,
                        null::text AS siteid,
                        null::text AS usubjid,
                        null::int AS peseq,
                        null::text AS petestcd,
                        null::text AS petest,
                        null::text AS pecat,
                        null::text AS pescat,
                        null::text AS pebodsys,
                        null::text AS peorres,
                        null::text AS peorresu,
                        null::text AS pestat,
                        null::text AS peloc,
                        null::text AS visit,
                        null::timestamp without time zone AS pedtc,
                        null::time without time zone AS petm )

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
WHERE 1=2;
