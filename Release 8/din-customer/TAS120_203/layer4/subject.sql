/*
CCDM Subject mapping
Notes: Mapping to CCDM Subject table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site ),
    
    subject_data AS (
                SELECT  distinct 'TAS120_203'::text AS studyid,
                        null::text AS studyname,
                        'TAS120_203_' || split_part("site_key",'_',2)::text AS siteid,
                        "subject_key"::text AS usubjid,
                        null::text AS screenid,
                        null::text AS randid,
                        null::text AS sitekey
                        --null::date AS exitdate,
                        --null::text AS protver
                        from TAS120_203.__subjects)

SELECT 
        /*KEY (sd.studyid || '~' || sd.siteid || '~' || sd.usubjid)::text AS comprehendid, KEY*/
        /*KEY (sd.studyid || '~' || sd.siteid)::text AS sitekey, KEY*/
        sd.studyid::text AS studyid,
        sd.studyname::text AS studyname,
        sd.siteid::text AS siteid,
        sd.usubjid::text AS usubjid,
        sd.screenid::text AS screenid,
        sd.randid::text AS randid,
        null::text AS status,
        null::date AS exitdate,
        null::text AS protver
         /*KEY, (sd.studyid || '~' || sd.siteid || '~' || sd.usubjid)::text AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM subject_data sd
JOIN included_sites si ON (si.studyid = sd.studyid AND si.siteid = sd.siteid);




