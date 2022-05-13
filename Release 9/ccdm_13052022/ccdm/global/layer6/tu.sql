/*
CCDM TU Table mapping
Notes: Standard mapping to CCDM TU table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),
    tu_data AS (
        SELECT  null::text AS comprehendid,
                null::text AS studyid,
                null::text AS siteid,
                null::text AS usubjid,
                null::numeric AS tuseq,
                null::text AS tugrpid,
                null::text AS turefid,
                null::text AS tuspid,
                null::text AS tulnkid,
                null::text AS tulnkgrp,
                null::text AS tutestcd,
                null::text AS tutest,
                null::text AS tuorres,
                null::text AS tustresc,
                null::text AS tunam,
                null::text AS tuloc,
                null::text AS tulat,
                null::text AS tudir,
                null::text AS tuportot,
                null::text AS tumethod,
                null::text AS tulobxfl,
                null::text AS tublfl,
                null::text AS tueval,
                null::text AS tuevalid,
                null::text AS tuacptfl,
                null::numeric AS visitnum,
                null::text AS visit,
                null::numeric AS visitdy,
                null::numeric AS taetord,
                null::text AS epoch,
                null::text AS tudtc,
                null::numeric AS tudy
                )

SELECT
    /*KEY (tu.studyid || '~' || tu.siteid || '~' || tu.usubjid)::text AS comprehendid, KEY*/  
    tu.studyid::text AS studyid,
    tu.siteid::text AS siteid,
    tu.usubjid::text AS usubjid,
    tu.tuseq::numeric AS tuseq,
    tu.tugrpid::text AS tugrpid,
    tu.turefid::text AS turefid,
    tu.tuspid::text AS tuspid,
    tu.tulnkid::text AS tulnkid,
    tu.tulnkgrp::text AS tulnkgrp,
    tu.tutestcd::text AS tutestcd,
    tu.tutest::text AS tutest,
    tu.tuorres::text AS tuorres,
    tu.tustresc::text AS tustresc,
    tu.tunam::text AS tunam,
    tu.tuloc::text AS tuloc,
    tu.tulat::text AS tulat,
    tu.tudir::text AS tudir,
    tu.tuportot::text AS tuportot,
    tu.tumethod::text AS tumethod,
    tu.tulobxfl::text AS tulobxfl,
    tu.tublfl::text AS tublfl,
    tu.tueval::text AS tueval,
    tu.tuevalid::text AS tuevalid,
    tu.tuacptfl::text AS tuacptfl,
    tu.visitnum::numeric AS visitnum,
    tu.visit::text AS visit,
    tu.visitdy::numeric AS visitdy,
    tu.taetord::numeric AS taetord,
    tu.epoch::text AS epoch,
    tu.tudtc::text AS tudtc,
    tu.tudy::numeric AS tudy
    /*KEY , (tu.studyid || '~' || tu.siteid || '~' || tu.usubjid || '~' || tu.tulnkid || '~' || tu.tuevalid )::text  AS objectuniquekey KEY*/
    /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM tu_data tu JOIN included_subjects s ON (tu.studyid = s.studyid AND tu.siteid = s.siteid AND tu.usubjid = s.usubjid)
WHERE 1=2;
