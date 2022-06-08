/*
CCDM FieldDef mapping
Notes: Standard mapping to CCDM FieldDef table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),

    fielddef_data AS (
                SELECT  null::text AS studyid,
                        null::text AS formid,
                        null::text AS fieldId,
                        null::text AS fieldname,
                        null::boolean AS isprimaryendpoint,
                        null::boolean AS issecondaryendpoint,
                        null::boolean AS issdv,
                        null::boolean  AS isrequired )

SELECT         
        /*KEY fd.studyid::text AS comprehendid, KEY*/
        fd.studyid::text AS studyid,
        fd.formid::text AS formid,
        fd.fieldId::text AS fieldid,
        fd.fieldname::text AS fieldname,
        fd.isprimaryendpoint::boolean AS isprimaryendpoint,
        fd.issecondaryendpoint::boolean AS issecondaryendpoint,
        fd.issdv::boolean AS issdv,
        fd.isrequired::boolean  AS isrequired 
        /*KEY , (fd.studyid || '~' || fd.formid || '~' || fd.fieldId)::text AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM fielddef_data fd
JOIN included_studies st ON (fd.studyid = st.studyid)
WHERE 1=2;
