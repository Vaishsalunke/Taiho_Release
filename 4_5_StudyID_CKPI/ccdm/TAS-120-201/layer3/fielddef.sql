/*
CCDM FieldDef mapping
Notes: Standard mapping to CCDM FieldDef table
*/
WITH included_studies AS (
SELECT studyid FROM study ),

fielddef_data AS (
select distinct 'TAS-120-201'::text AS studyid,
nullif("FormOID",'')::text AS formid,
split_part(nullif("ItemOID",''),'.',2)::text AS fieldId,
coalesce("SASLabel",'Missing')::text AS fieldname,
case when UPPER(nullif("FormOID",''))= 'OR' or UPPER(nullif("FormOID",''))= 'CT' or UPPER(nullif("FormOID",''))= 'RT'
	then True else False end::boolean AS isprimaryendpoint,
False::boolean AS issecondaryendpoint,
case when "SourceDocument" = 'true' then true else false end::text AS issdv,
case when sv."Mandatory" = 'true' then true else false end::text AS isrequired
FROM TAS120_201."audit_ItemData" mf
left join TAS120_201.metadata_fields sv
on "FormOID" = sv."FormDefOID"
and "ItemOID" = "OID"
and "FormOID" > ''
)

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


