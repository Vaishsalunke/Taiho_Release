/*
CCDM Trial Visits per Arm mapping
Notes: Standard mapping to CCDM Trial Visits per Arm table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),

tva AS (
                SELECT  null::text AS studyid,
                        null::text AS armcd,
                        null::text AS arm,
                        null::text AS visitid,
                        null::text AS visitname,
                        null::NUMERIC AS visitnum,
                        null::INTEGER AS visitdy,
                        null::INTEGER AS visitwindowbefore,
                        null::INTEGER AS visitwindowafter,
                        null::text AS visit_bl
            )
                        
SELECT 
        /*KEY t.studyid::text AS comprehendid, KEY*/
        t.studyid::text AS studyid,
        t.armcd::text AS armcd,
        t.arm::text AS arm,
        t.visitid::text AS visitid,
        t.visitname::text AS visitname,
        t.visitnum::NUMERIC AS visitnum,
        t.visitdy::INTEGER AS visitdy,
        t.visitwindowbefore::INTEGER AS visitwindowbefore,
        t.visitwindowafter::INTEGER AS visitwindowafter,
        t.visit_bl::text AS visit_bl
        /*KEY , (t.studyid || '~' || t.armcd || '~' || t.visitid || '~' || t.visitnum)::TEXT AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM tva t
JOIN included_studies st ON (st.studyid = t.studyid)
WHERE 1=2;
