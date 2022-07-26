/*
CCDM RSLT mapping
Notes: Standard mapping to CCDM RSLT table  
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     rslt_data as (
                SELECT  null::text AS studyid,
                        null::text AS siteid,
                        null::text AS usubjid,
                        null::text AS resulttype,
                        null::int AS rsltseq, /*(row_number() over (partition by [studyid],[siteid],[usubjid] order [rsltdtc,rslttm]))::int as rsltseq,*/
                        null::numeric AS visitnum, 
                        null::text AS visit,
                        null::timestamp without time zone AS rsltdtc,
                        null::time without time zone AS rslttm,
                        null::int AS rsltdy,
                        null::text AS rshttestcd,
                        null::text AS rslttest,
                        null::text AS rsltcat,
                        null::text AS rlstscat,
                        null::text AS rlstorres,
                        null::text AS rsltorresu,
                        null::text AS rsltstresc,
                        null::text AS rsltstresu,
                        null::text AS rsltmethod,
                        null::text AS tableref,
                        null::text AS objectuniquekeyref )

SELECT
        /*KEY (rslt.studyid || '~' || rslt.siteid || '~' || rslt.usubjid)::text as comprehendid, KEY*/
        rslt.studyid::text AS studyid,
        rslt.siteid::text AS siteid,
        rslt.usubjid::text AS usubjid,
        rslt.resulttype::text AS resulttype,
        rslt.rsltseq::int AS rsltseq, 
        rslt.visitnum::numeric AS visitnum, 
        rslt.visit::text AS visit,
        rslt.rsltdtc::timestamp without time zone AS rsltdtc,
        rslt.rslttm::time without time zone AS rslttm,
        rslt.rsltdy::int AS rsltdy,
        rslt.rshttestcd::text AS rshttestcd,
        rslt.rslttest::text AS rslttest,
        rslt.rsltcat::text AS rsltcat,
        rslt.rlstscat::text AS rlstscat,
        rslt.rlstorres::text AS rlstorres,
        rslt.rsltorresu::text AS rsltorresu,
        rslt.rsltstresc::text AS rsltstresc,
        rslt.rsltstresu::text AS rsltstresu,
        rslt.rsltmethod::text AS rsltmethod,
        rslt.tableref::text AS tableref,
        rslt.objectuniquekeyref::text AS objectuniquekeyref
        /*KEY ,(rslt.studyid || '~' || rslt.siteid || '~' || rslt.usubjid || '~' || rslt.resulttype || '~' || rsltseq)::text  as objectuniquekey KEY*/
        /*KEY ,now()::timestamp with time zone as comprehend_update_time KEY*/
FROM rslt_data rslt
JOIN included_subjects s ON (rslt.studyid = s.studyid AND rslt.siteid = s.siteid AND rslt.usubjid = s.usubjid)
WHERE 1=2;
