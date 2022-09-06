/*
CCDM flexfield mapping
Notes: Standard mapping to CCDM flexfield table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     flexfield_data AS (
                SELECT  null::text AS tablename,
                        null::text AS comprehendidref,
                        null::text AS objectuniquekeyref,
                        null::text AS studyid,
                        null::text AS siteid,
                        null::text AS usubjid,
                        null::text AS flexname,
                        null::text AS flexlabel,
                        null::text AS flexvalue,
                        null::timestamp without time zone AS flexvaluedtc,
                        null::time without time zone AS flexvaluetime,
                        null::int AS flexorder )

SELECT
        ff.tablename::text AS tablename,
        ff.comprehendidref::text AS comprehendidref,
        ff.objectuniquekeyref::text AS objectuniquekeyref,
        ff.studyid::text AS studyid,
        ff.siteid::text AS siteid,
        ff.usubjid::text AS usubjid,
        ff.flexname::text AS flexname,
        ff.flexlabel::text AS flexlabel,
        ff.flexvalue::text AS flexvalue,
        ff.flexvaluedtc::timestamp without time zone AS flexvaluedtc,
        ff.flexvaluetime::time without time zone AS flexvaluetime,
        ff.flexorder::int AS flexorder
        /*KEY ,now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM flexfield_data ff
JOIN included_subjects s ON (ff.studyid = s.studyid AND ff.siteid = s.siteid AND ff.usubjid = s.usubjid)
WHERE 1=2;
