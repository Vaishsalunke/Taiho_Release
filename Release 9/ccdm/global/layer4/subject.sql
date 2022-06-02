/*
CCDM Subject mapping
Notes: Mapping to CCDM Subject table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site ),
    
    subject_data AS (
                SELECT  null::text AS studyid,
                        null::text AS studyname,
                        null::text AS siteid,
                        null::text AS usubjid,
                        null::text AS screenid,
                        null::text AS randid,
                        null::text AS status,
                        null::date AS exitdate,
                        'Original'::text AS protver,
						null::text AS armcd,
						null::text AS arm,
						'Not Available'::text AS visit_schedule_code,
						'Not Available'::text AS visit_schedule_desc)

SELECT 
        /*KEY (sd.studyid || '~' || sd.siteid || '~' || sd.usubjid)::text AS comprehendid, KEY*/
        /*KEY (sd.studyid || '~' || sd.siteid)::text AS sitekey, KEY*/
        sd.studyid::text AS studyid,
        sd.studyname::text AS studyname,
        sd.siteid::text AS siteid,
        sd.usubjid::text AS usubjid,
        sd.screenid::text AS screenid,
        sd.randid::text AS randid,
        sd.status::text AS status,
        sd.exitdate::date AS exitdate,
        sd.protver::text AS protver,
		sd.armcd::text AS armcd,
		sd.arm::text AS arm,
		sd.visit_schedule_code::text AS visit_schedule_code,
		sd.visit_schedule_desc::text AS visit_schedule_desc
        /*KEY , (sd.studyid || '~' || sd.siteid || '~' || sd.usubjid)::text AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM subject_data sd
JOIN included_sites si ON (si.studyid = sd.studyid AND si.siteid = sd.siteid)
WHERE 1=2;
