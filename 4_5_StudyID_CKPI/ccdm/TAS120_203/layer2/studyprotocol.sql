/*
CCDM Studyprotocol mapping
Notes: Standard mapping to CCDM Studyprotocol table
*/

WITH included_studies AS (
                SELECT DISTINCT studyid FROM study ),

     studyprotocol_data AS (
                SELECT  'TAS-120-203'::text AS studyid,
                        'TAS120_203'::text AS protocolnumber,
                        '1.0'::text AS protocolversion,
                        '10-MAR-2020'::text AS effectivedate,
                        '2020-000945-15'::text AS eudract_num)

SELECT 
        /*KEY (sp.studyid)::text AS comprehendid, KEY*/
        sp.studyid::text AS studyid,
        sp.protocolnumber::text AS protocolnumber,
        sp.protocolversion::text AS protocolversion,
        sp.effectivedate::date AS effectivedate,
        sp.eudract_num::text AS eudract_num
        /*KEY , (sp.studyid || '~' || sp.protocolnumber || '~' || sp.protocolversion || '~' || sp.effectivedate)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM studyprotocol_data sp
JOIN included_studies s ON (s.studyid = sp.studyid);
