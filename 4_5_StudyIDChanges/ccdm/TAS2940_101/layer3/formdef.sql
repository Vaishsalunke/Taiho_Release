/*
CCDM FormDef mapping
Notes: Standard mapping to CCDM FormDef table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),

     raw_data as (select "FormDefOID",
CASE WHEN "Mandatory"::boolean = true and coalesce("SourceDocument", false)::boolean =
true THEN true ELSE false END ::boolean AS "SourceDocument","Mandatory" from
TAS2940_101."metadata_fields"),

sdv as (select "FormDefOID",count(distinct "SourceDocument") as sdv,
count(distinct "Mandatory") as mn from raw_data
group by "FormDefOID"),

sdv1 as (select distinct rd."FormDefOID",
case when sdv = 1 then "SourceDocument" else true end as sdv,
case when mn = 1 then "Mandatory" else true end as mn from raw_Data rd
left join sdv on rd."FormDefOID" = sdv."FormDefOID")

,formdef_data as (
select
'TAS2940-101'::text as studyid,
"OID"::text as formid,
"Name"::text as formname,
false ::boolean AS isprimaryendpoint,
'FALSE'::boolean as issecondaryendpoint,
case when sdv is null then false
else sdv end::boolean as issdv,
case when mn is null then false
else mn end::boolean as isrequired
from tas2940_101.metadata_forms left join
sdv1 on "OID"="FormDefOID"
)


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
JOIN included_studies st ON (fd.studyid = st.studyid);


