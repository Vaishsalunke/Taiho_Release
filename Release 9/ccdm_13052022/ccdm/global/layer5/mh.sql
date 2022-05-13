/*
CCDM MH mapping
Notes: Standard mapping to CCDM MH table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     mh_data AS (
                SELECT  null::text AS studyid,
                        null::text AS siteid,
                        null::text AS usubjid,
                        null::int AS mhseq, /*(row_number() over (partition by [studyid],[siteid],[usubjid] order [mhstdtc,mhsttm]))::int AS mhseq,*/
                        null::text AS mhspid,
                        null::text AS mhterm,
                        null::text AS mhdecod,
                        null::text AS mhcat,
                        null::text AS mhscat,
                        null::text AS mhbodsys,
                        null::text AS mhstdtc_iso,
                        null::date AS mhstdtc,
                        null::time without time zone AS mhsttm,
                        null::text AS mhendtc_iso,
                        null::date AS mhendtc,
                        null::time without time zone AS mhendtm )

SELECT
        /*KEY (mh.studyid || '~' || mh.siteid || '~' || mh.usubjid)::text AS comprehendid, KEY*/
        mh.studyid::text AS studyid,
        mh.siteid::text AS siteid,
        mh.usubjid::text AS usubjid,
        mh.mhseq::int AS mhseq,
        mh.mhspid::text AS mhspid,
        mh.mhterm::text AS mhterm,
        mh.mhdecod::text AS mhdecod,
        mh.mhcat::text AS mhcat,
        mh.mhscat::text AS mhscat,
        mh.mhbodsys::text AS mhbodsys,
        mh.mhstdtc_iso::text AS mhstdtc_iso,
        mh.mhstdtc::date AS mhstdtc,
        mh.mhsttm::time without time zone AS mhsttm,
        mh.mhendtc_iso::text AS mhendtc_iso,
        mh.mhendtc::date AS mhendtc,
        mh.mhendtm::time without time zone AS mhendtm
        /*KEY , (mh.studyid || '~' || mh.siteid || '~' || mh.usubjid || '~' || mh.mhseq)::text AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM mh_data mh
JOIN included_subjects s ON (mh.studyid = s.studyid AND mh.siteid = s.siteid AND mh.usubjid = s.usubjid)
WHERE 1=2;
