/*
CCDM PC mapping
Notes: Standard mapping to CCDM PC table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     pc_data AS (
                SELECT  null::text AS studyid,
                        null::text AS siteid,
                        null::text AS usubjid,
                        null::int AS pcseq,
                        null::text AS pctestcd,
                        null::text AS pctest,
                        null::text AS pcorres,
                        null::text AS pcorresu,
                        null::text AS pcstresc,
                        null::int AS pcstresn,
                        null::text AS pcstresu,
                        null::text AS pcnam,
                        null::text AS pcspec,
                        null::int AS pclloq,
                        null::numeric AS visitnum,
                        null::timestamp without time zone AS pcdtc )

SELECT
        /*KEY (pc.studyid || '~' || pc.siteid || '~' || pc.usubjid)::text AS comprehendid, KEY*/
        pc.studyid::text AS studyid,
        pc.siteid::text AS siteid,
        pc.usubjid::text AS usubjid,
        pc.pcseq::int AS pcseq,
        pc.pctestcd::text AS pctestcd,
        pc.pctest::text AS pctest,
        pc.pcorres::text AS pcorres,
        pc.pcorresu::text AS pcorresu,
        pc.pcstresc::text AS pcstresc,
        pc.pcstresn::int AS pcstresn,
        pc.pcstresu::text AS pcstresu,
        pc.pcnam::text AS pcnam,
        pc.pcspec::text AS pcspec,
        pc.pclloq::int AS pclloq,
        pc.visitnum::numeric AS visitnum,
        pc.pcdtc::timestamp without time zone AS pcdtc
        /*KEY , (pc.studyid || '~' || pc.siteid || '~' || pc.usubjid || '~' || pcseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM pc_data pc
JOIN included_subjects s ON (pc.studyid = s.studyid AND pc.siteid = s.siteid AND pc.usubjid = s.usubjid)
WHERE 1=2;
