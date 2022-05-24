/*
CCDM FormDef mapping
Notes: Standard mapping to CCDM FormDef table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),

     formdef_data AS (
                SELECT  null::text AS studyid,
                        null::text AS formid,
                        null::text AS formname,
                        null::boolean AS isprimaryendpoint,
                        null::boolean AS issecondaryendpoint,
                        null::boolean AS issdv,
                        null::boolean AS isrequired )

SELECT 
        /*KEY fd.studyid::text AS comprehendid, KEY*/
        fd.studyid::text AS studyid,
        fd.formid::text AS formid,
        fd.formname::text AS formname,
        fd.isprimaryendpoint::boolean AS isprimaryendpoint,
        fd.issecondaryendpoint::boolean AS issecondaryendpoint,
        fd.issdv::boolean AS issdv,
        fd.isrequired::boolean AS isrequired
        /*KEY , (fd.studyid || '~' || fd.formid)::text AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM formdef_data fd
JOIN included_studies st ON (fd.studyid = st.studyid)
WHERE 1=2;  
