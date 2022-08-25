/*
CCDM TR Table mapping
Notes: Standard mapping to CCDM TR table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),
    tr_data AS (
        SELECT  null::text AS comprehendid,
                null::text AS studyid,
                null::text AS siteid,
                null::text AS usubjid,
                null::numeric AS trseq,
                null::text AS trgrpid,
                null::text AS trrefid,
                null::text AS trspid,
                null::text AS trlnkid,
                null::text AS trlnkgrp,
                null::text AS trtestcd,
                null::text AS trtest,
                null::text AS trorres,
                null::text AS trorresu,
                null::text AS trstresc,
                null::numeric AS trstresn,
                null::text AS trstresu,
                null::text AS trstat,
                null::text AS trreasnd,
                null::text AS trnam,
                null::text AS trmethod,
                null::text AS trlobxfl,
                null::text AS trblfl,
                null::text AS treval,
                null::text AS trevalid,
                null::text AS tracptfl,
                null::numeric AS visitnum,
                null::text AS visit,
                null::numeric AS visitdy,
                null::numeric AS taetord,
                null::text AS epoch,
                null::text AS trdtc,
                null::numeric AS trdy
                )

SELECT
    /*KEY (tr.studyid || '~' || tr.siteid || '~' || tr.usubjid)::text AS comprehendid, KEY*/  
    tr.studyid::text AS studyid,
    tr.siteid::text AS siteid,
    tr.usubjid::text AS usubjid,
    tr.trseq::numeric AS trseq,
    tr.trgrpid::text AS trgrpid,
    tr.trrefid::text AS trrefid,
    tr.trspid::text AS trspid,
    tr.trlnkid::text AS trlnkid,
    tr.trlnkgrp::text AS trlnkgrp,
    tr.trtestcd::text AS trtestcd,
    tr.trtest::text AS trtest,
    tr.trorres::text AS trorres,
    tr.trorresu::text AS trorresu,
    tr.trstresc::text AS trstresc,
    tr.trstresn::numeric AS trstresn,
    tr.trstresu::text AS trstresu,
    tr.trstat::text AS trstat,
    tr.trreasnd::text AS trreasnd,
    tr.trnam::text AS trnam,
    tr.trmethod::text AS trmethod,
    tr.trlobxfl::text AS trlobxfl,
    tr.trblfl::text AS trblfl,
    tr.treval::text AS treval,
    tr.trevalid::text AS trevalid,
    tr.tracptfl::text AS tracptfl,
    tr.visitnum::numeric AS visitnum,
    tr.visit::text AS visit,
    tr.visitdy::numeric AS visitdy,
    tr.taetord::numeric AS taetord,
    tr.epoch::text AS epoch,
    tr.trdtc::text AS trdtc,
    tr.trdy::numeric AS trdy
    /*KEY , (tr.studyid || '~' || tr.siteid || '~' || tr.usubjid || '~' || tr.trtestcd || '~' || tr.trevalid || '~' || tr.visitnum )::text  AS objectuniquekey KEY*/
    /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM tr_data tr JOIN included_subjects s ON (tr.studyid = s.studyid AND tr.siteid = s.siteid AND tr.usubjid = s.usubjid)
WHERE 1=2;
