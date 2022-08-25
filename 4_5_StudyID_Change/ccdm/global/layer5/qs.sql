/*
CCDM QS mapping
Notes: Standard mapping to CCDM QS table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     qs_data AS (
                SELECT  null::text AS studyid,
                        null::text AS siteid,
                        null::text AS usubjid,
                        null::int AS qsseq, /*(row_number() over (partition by [studyid],[siteid],[usubjid] order [qsdtc,qstm]))::int AS qsseq,*/
                        null::text AS qstestcd,
                        null::text AS qstest,
                        null::text AS qscat,
                        null::text AS qsscat,
                        null::text AS qsorres,
                        null::text AS qsorresu,
                        null::text AS qsstresc,
                        null::numeric AS qsstresn,
                        null::text AS qsstresu,
                        null::text AS qsstat,
                        null::text AS qsblfl,
                        null::text AS visit,
                        null::timestamp without time zone AS qsdtc,
                        null::time without time zone AS qstm )

SELECT
        /*KEY (qs.studyid || '~' || qs.siteid || '~' || qs.usubjid)::text AS comprehendid, KEY*/
        qs.studyid::text AS studyid,
        qs.siteid::text AS siteid,
        qs.usubjid::text AS usubjid,
        qs.qsseq::int AS qsseq,
        qs.qstestcd::text AS qstestcd,
        qs.qstest::text AS qstest,
        qs.qscat::text AS qscat,
        qs.qsscat::text AS qsscat,
        qs.qsorres::text AS qsorres,
        qs.qsorresu::text AS qsorresu,
        qs.qsstresc::text AS qsstresc,
        qs.qsstresn::numeric AS qsstresn,
        qs.qsstresu::text AS qsstresu,
        qs.qsstat::text AS qsstat,
        qs.qsblfl::text AS qsblfl,
        qs.visit::text AS visit,
        qs.qsdtc::timestamp without time zone AS qsdtc,
        qs.qstm::time without time zone AS qstm
        /*KEY , (qs.studyid || '~' || qs.siteid || '~' || qs.usubjid || '~' || qs.qsseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM qs_data qs
JOIN included_subjects s ON (qs.studyid = s.studyid AND qs.siteid = s.siteid AND qs.usubjid = s.usubjid)
WHERE 1=2;
