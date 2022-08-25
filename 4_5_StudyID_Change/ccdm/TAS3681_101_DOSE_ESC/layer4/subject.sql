/*
CCDM Subject mapping
Client:taiho
Limits added for PR build efficiency (not applied for standard builds)
Notes: Standard mapping to CCDM Site table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site ),
    
    subject_data AS (
                SELECT distinct 'TAS3681_101_DOSE_ESC'::text AS studyid,
								'TAS3681_101_DOSE_ESC'::text AS studyname,
						s."SiteNumber"::	text AS siteid,
						s."Subject"::	text      AS usubjid,
                        null::text AS screenid,
                        null::text AS randid,
                        null::text AS status,
                        null::date AS exitdate,
						null::text AS sitekey,
						'TAS3681-101 Version 5.0'::text AS protver,
						null::text AS armcd,
						nullif(COALESCE(	case
								 when dm."DataPageName" = 'Demography-Expansion' then 'Dose Expansion'
								 else dlt."DLTCOH"
							 end,
					         case
								 when dm2."DataPageName" = 'Demography-Expansion' then 'Dose Expansion'
								 else dlt."DLTCOH"
							 end),'')::text AS arm,
                        'Not Available'::text AS visit_schedule_code,
                        'Not Available'::text AS visit_schedule_desc
			From 	tas3681_101."IE" s
			left join	 tas3681_101."DM" dm on s."Subject" = dm."Subject"
			left join	 tas3681_101."DLT" dlt on s."Subject" = dlt."Subject"
			left join	 tas3681_101."DM2" dm2 on s."Subject" = dm2."Subject"
		where "IERANDY" != 'Expansion'
				/*LIMIT LIMIT 100 LIMIT*/)

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



