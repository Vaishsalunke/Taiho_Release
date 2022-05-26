/*
CCDM DM mapping
Notes: Standard mapping to CCDM DM table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     dm_data AS (
                SELECT  null::text AS studyid,
                        null::text AS studyname,
                        null::text AS siteid,
                        null::text AS sitename,
                        null::text AS sitecountry,
                        null::text AS usubjid,
                        null::numeric AS visitnum,
                        null::text AS visit,
                        null::date AS dmdtc,
                        null::date AS brthdtc,
                        null::integer AS age,
                        null::text AS sex,
                        null::text AS race,
                        null::text AS ethnicity,
                        null::text AS armcd,
                        null::text AS arm,
                        null::text AS brthdtc_iso)

SELECT 
        /*KEY (dm.studyid || '~' || dm.siteid || '~' || dm.usubjid)::text AS comprehendid, KEY*/
        dm.studyid::text AS studyid,
        dm.studyname::text AS studyname,
        dm.siteid::text AS siteid,
        dm.sitename::text AS sitename,
        dm.sitecountry::text AS sitecountry,
        dm.usubjid::text AS usubjid,
        dm.visitnum::numeric AS visitnum,
        dm.visit::text AS visit,
        dm.dmdtc::date AS dmdtc,
        dm.brthdtc::date AS brthdtc,
        dm.age::integer AS age,
        dm.sex::text AS sex,
        dm.race::text AS race,
        dm.ethnicity::text AS ethnicity,
        dm.armcd::text AS armcd,
        dm.arm::text AS arm,
        dm.arm::text AS brthdtc_iso
        /*KEY ,(dm.studyid || '~' || dm.siteid || '~' || dm.usubjid )::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM dm_data dm
JOIN included_subjects s ON (dm.studyid = s.studyid AND dm.siteid = s.siteid AND dm.usubjid = s.usubjid)
WHERE 1=2;
