/*
CCDM Subject mapping
Client:taiho
Limits added for PR build efficiency (not applied for standard builds)
Notes: Standard mapping to CCDM Site table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site ),
    
    subject_data AS (
                   SELECT  DISTINCT 'TAS120_202'::text AS studyid,
                        "site_key"::text AS siteid,
                        "subject_key" ::text AS usubjid,
                        NULL::text AS screenid,
                        NULL::text AS randid,
                        'NA'::text AS status,
                        NULL::date AS exitdate,
						'TAS120-202 Version 2.0'::text AS protver,
						null::text AS armcd,
						nullif ("DMCOH",'')::text AS arm,
                        'Not Available'::text AS visit_schedule_code,
                        'Not Available'::text AS visit_schedule_desc
                FROM "tas120_202"."__subjects" s
				LEFT JOIN "tas120_202"."DM" e 
							ON s."subject_key" = e."Subject"
				/*LIMIT LIMIT 100 LIMIT*/)

SELECT 
        /*KEY (sd.studyid || '~' || sd.siteid || '~' || sd.usubjid)::text AS comprehendid, KEY*/
        /*KEY (sd.studyid || '~' || sd.siteid)::text AS sitekey, KEY*/
        sd.studyid::text AS studyid,
        Null::text AS studyname,
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
JOIN included_sites si ON (si.studyid = sd.studyid AND si.siteid = sd.siteid);
