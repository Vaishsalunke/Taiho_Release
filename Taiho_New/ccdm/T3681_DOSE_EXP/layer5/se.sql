/*
CCDM Subject Elements mapping
Notes: Standard mapping to CCDM Subject Elements table
*/

WITH included_subjects AS (
                SELECT studyid, siteid, usubjid FROM subject ),

se AS (
                SELECT  null::text AS studyid,
                        null::text AS siteid,
                        null::text AS usubjid,
                        null::text AS seseq,
                        null::text AS etcd,
                        null::text AS element,
                        null::text AS taetord,
                        null::text AS epoch,
                        null::text AS sestdtc,
                        null::date AS sestdat,
                        null::text AS seendtc,
                        null::date AS seendat,
                        null::text AS seupdes
            )

SELECT 
        /*KEY (s.studyid || '~' || s.siteid || '~' || s.usubjid)::TEXT AS comprehendid, KEY*/
        s.studyid::text AS studyid,
        s.siteid::text AS siteid,
        s.usubjid::text AS usubjid,
        s.seseq::text AS seseq,
        s.etcd::text AS etcd,
        s.element::text AS element,
        s.taetord::text AS taetord,
        s.epoch::text AS epoch,
        s.sestdtc::text AS sestdtc,
        s.sestdat::date AS sestdat,
        s.seendtc::text AS seendtc,
        s.seendat::date AS seendat,
        s.seupdes::text AS seupdes
        /*KEY , (s.studyid || '~' || s.siteid || '~' || s.usubjid || '~' || s.seseq || '~' || s.etcd || '~' || s.sestdtc)::TEXT AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM se s
JOIN included_subjects sb ON (sb.studyid = s.studyid AND sb.siteid = s.siteid AND sb.usubjid = s.usubjid)
WHERE 1=2;
