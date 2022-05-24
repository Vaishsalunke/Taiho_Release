/*
CCDM PP mapping
Notes: Standard mapping to CCDM PP table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     pp_data AS (
                SELECT  null::text AS studyid,
                        null::text AS siteid,
                        null::text AS usubjid,
                        null::int AS ppseq,
                        null::text AS pptestcd,
                        null::text AS pptest,
                        null::text AS ppcat,
                        null::text AS pporres,
                        null::text AS pporresu,
                        null::text AS ppstresc,
                        null::int AS ppstresn,
                        null::text AS ppstresu,
                        null::text AS ppspec,
                        null::timestamp without time zone AS pprftdtc )

SELECT
        /*KEY (pp.studyid || '~' || pp.siteid || '~' || pp.usubjid)::text AS comprehendid, KEY*/
        pp.studyid::text AS studyid,
        pp.siteid::text AS siteid,
        pp.usubjid::text AS usubjid,
        pp.ppseq::int AS ppseq,
        pp.pptestcd::text AS pptestcd,
        pp.pptest::text AS pptest,
        pp.ppcat::text AS ppcat,
        pp.pporres::text AS pporres,
        pp.pporresu::text AS pporresu,
        pp.ppstresc::text AS ppstresc,
        pp.ppstresn::int AS ppstresn,
        pp.ppstresu::text AS ppstresu,
        pp.ppspec::text AS ppspec,
        pp.pprftdtc::timestamp without time zone AS pprftdtc
        /*KEY , (pp.studyid || '~' || pp.siteid || '~' || pp.usubjid || '~' || pp.ppseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM pp_data pp
JOIN included_subjects s ON (pp.studyid = s.studyid AND pp.siteid = s.siteid AND pp.usubjid = s.usubjid)
WHERE 1=2;
