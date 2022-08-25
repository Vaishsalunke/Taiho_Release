/*
CCDM Studyproduct mapping
Notes: Standard mapping to CCDM Studyproduct table
*/

WITH included_studies AS (
                SELECT DISTINCT studyid FROM study ),

     studyproduct_data AS (
                SELECT  null::text AS studyid,
                        null::text AS productid,
                        null::text AS productcode,
                        null::text AS productname,
                        null::text AS genericname,
                        null::text AS productgroup,
                        null::text AS indication,
                        null::text AS molecule)

SELECT 
        /*KEY sp.studyid::text AS comprehendid, KEY*/
        sp.studyid::text AS studyid,
        sp.productid::text AS productid,
        sp.productcode::text AS productcode,
        sp.productname::text AS productname,
        sp.genericname::text AS genericname,
        sp.productgroup::text AS productgroup,
        sp.indication::text AS indication,
        sp.molecule::text AS molecule
        /*KEY , (sp.studyid || '~' || sp.productid || '~' || sp.productcode)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM studyproduct_data sp
JOIN included_studies s ON (s.studyid = sp.studyid)
WHERE 1=2; 

