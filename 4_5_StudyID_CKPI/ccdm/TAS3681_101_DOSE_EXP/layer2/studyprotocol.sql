/*
CCDM Studyprotocol mapping
Notes: Standard mapping to CCDM Studyprotocol table
*/

WITH included_studies AS (
                SELECT DISTINCT studyid FROM study ),

     studyprotocol_data AS (
                SELECT  'TAS3681_101_DOSE_EXP'::text AS studyid,
                        'TAS3681_101_DOSE_EXP'::text AS protocolnumber,
                        '5.0'::text AS protocolversion,
                        '20-SEP-2018'::text AS effectivedate,
                        '2015-002745-55'::text AS eudract_num)

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
