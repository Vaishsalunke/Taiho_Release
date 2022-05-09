/*
CCDM TV mapping
Notes: Standard mapping to CCDM TV table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),

     tv_data AS (
                SELECT  null::text AS studyid,
                        null::numeric AS visitnum,
                        null::text AS visit,
                        null::int AS visitdy,
                        null::int AS visitwindowbefore,
                        null::int AS visitwindowafter,
                        null::boolean AS isbaselinevisit)

SELECT 
        /*KEY tv.studyid::text AS comprehendid, KEY*/
        tv.studyid::text AS studyid,
        tv.visitnum::numeric AS visitnum,
        tv.visit::text AS visit,
        tv.visitdy::int AS visitdy,
        tv.visitwindowbefore::int AS visitwindowbefore,
        tv.visitwindowafter::int AS visitwindowafter,
        tv.isbaselinevisit::boolean AS isbaselinevisit
        /*KEY , (tv.studyid || '~' || tv.visit)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM tv_data tv
JOIN included_studies st ON (st.studyid = tv.studyid)
WHERE 1=2; 
