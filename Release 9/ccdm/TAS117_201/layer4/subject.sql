/*
CCDM Subject mapping
Notes: Mapping to CCDM Subject table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site ),
    
    subject_data AS (
                SELECT  DISTINCT 'TAS117_201'::text AS studyid,
                        null::text AS studyname,
                        concat('TAS117_201',substring(site_key,position('_' in site_key))) ::text AS siteid,
                        subject_key ::text AS usubjid,
                        null::text AS screenid,
                        null::text AS randid,
                        null::text AS status,
                        null::date AS exitdate,
                        'TAS117-201 Version 2.0'::text AS protver,
						null::text AS armcd,
						nullif ("ENRSTDP",'')::text AS arm,
                        'Not Available'::text AS visit_schedule_code,
                        'Not Available'::text AS visit_schedule_desc
                        from tas117_201."__subjects" s
                        left join tas117_201."ENR" e 
							on s."subject_key" = e."Subject"						)

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
JOIN included_sites si ON (si.studyid = sd.studyid AND si.siteid = sd.siteid);

