/*
CCDM VisitForm mapping
Notes: Standard mapping to CCDM VisitForm table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),

     visitform_data AS (
                SELECT  null::text AS studyid,
                        null::numeric AS visitnum,
                        null::text AS visit,
                        null::text AS formid,
                        null::boolean AS isrequired )

SELECT 
        /*KEY vf.studyid::text AS comprehendid, KEY*/
        vf.studyid::text AS studyid,
        vf.visitnum::numeric AS visitnum,
        vf.visit::text AS visit,
        vf.formid::text AS formid,
        vf.isrequired::boolean AS isrequired
        /*KEY , (vf.studyid || '~' || vf.visitnum || '~' || vf.formid)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM visitform_data vf
JOIN included_studies st ON (st.studyid = vf.studyid)
WHERE 1=2; 
