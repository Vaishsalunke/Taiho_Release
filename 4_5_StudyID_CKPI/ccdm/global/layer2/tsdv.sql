/*
CCDM Site mapping
Notes: Standard mapping to CCDM Site table
*/

WITH included_studies AS (SELECT studyid::TEXT AS studyid 
                            FROM study),

    tsdv_data AS (SELECT NULL::TEXT AS studyid,
                            NULL::TEXT AS sdvtier,
                            NULL::TEXT AS formid,
                            NULL::TEXT AS fieldid,
                            NULL::TEXT AS visit,
                            NULL::BOOLEAN AS issdv)

SELECT 
        /*KEY (t.studyid)::TEXT AS comprehendid, KEY*/
        t.studyid::TEXT AS studyid,
        t.sdvtier::TEXT AS sdvtier,
        t.formid::TEXT AS formid,
        t.fieldid::TEXT AS fieldid,
        t.visit::TEXT AS visit,
        t.issdv::BOOLEAN AS issdv
        /*KEY , (t.studyid || '~' || t.sdvtier || '~' || t.formid || '~' || t.fieldid || '~' || t.visit)::TEXT AS objectuniquekey KEY*/
        /*KEY , NOW()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time KEY*/
FROM tsdv_data t 
JOIN included_studies i ON (t.studyid = i.studyid)
WHERE 1=2; 
