/*
CCDM FieldDef mapping
Notes: Standard mapping to CCDM FieldDef table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),

    fielddef_data AS (
                select distinct  "StudyOID" ::text AS studyid,
                        nullif("FormOID",'') ::text AS formid,
                        split_part(nullif("ItemOID",''),'.',2)::text AS fieldId,
                        COALESCE("SASLabel",'Missing')::text AS fieldname,
                        case when UPPER(nullif("FormOID",'')) in ('OR', 'PA', 'PR') then True else False end::boolean AS isprimaryendpoint,
                        'False'::boolean AS issecondaryendpoint,
                        case when "SourceDocument"='True' then 'True' else 'False' end::boolean AS issdv,
                        case when "Mandatory"='True' then 'True' else 'False' end::boolean  AS isrequired
                        from tas117_201."audit_ItemData" aid join tas117_201.metadata_fields mf 
                        on aid."ItemOID"=mf."OID"  and aid."FormOID"=mf."FormDefOID" )

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
JOIN included_studies st ON (fd.studyid = st.studyid);

