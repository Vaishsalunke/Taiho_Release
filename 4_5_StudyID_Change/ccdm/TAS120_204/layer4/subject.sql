/*
CCDM Subject mapping
Notes: Mapping to CCDM Subject table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid from  site ),
    
    subject_data AS (
                SELECT  distinct 'TAS-120-204'::text AS studyid,
                        'TAS120_204'::text AS studyname,
                        'TAS120_204_' || split_part("site_key",'_',2)::text AS siteid,
                        "subject_key"::text AS usubjid,
                        null::text AS screenid,
                        null::text AS randid,
                        null::text AS sitekey,
                        --null::date AS exitdate,
                        'TAS120-204 Version 1.0'::text AS protver,
						null::text AS armcd,
						nullif ("ENRPHAS",'')::text AS arm,
                        'Not Available'::text AS visit_schedule_code,
                        'Not Available'::text AS visit_schedule_desc
                        from TAS120_204.__subjects s
						left join "tas120_204"."DM" e 
							ON s."subject_key" = e."Subject")

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
        sd.protver::text AS protver,
		sd.armcd::text AS armcd,
		sd.arm::text AS arm,
		sd.visit_schedule_code::text AS visit_schedule_code,
		sd.visit_schedule_desc::text AS visit_schedule_desc
        /*KEY , (sd.studyid || '~' || sd.siteid || '~' || sd.usubjid)::text AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM subject_data sd
JOIN included_sites si ON (si.studyid = sd.studyid AND si.siteid = sd.siteid);



