/*
CCDM IP Accountability  mapping
Notes: Standard mapping to CCDM IP Accountability table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site ),

     ipaccountability_data AS (
                SELECT  null::text AS studyid,
                        null::text AS siteid,
                        null::text AS ipname,
                        null::text AS ipid,
                        null::numeric AS ipquantity,
                        null::date AS ipexpirationdate,
                        null::date AS ipdate,
                        null::numeric AS ipseq,
                        null::text AS ipstate,
                        null::text AS ipreASon,
                        null::text AS ipunit )

SELECT 
        /*KEY (ip.studyid || '~' || ip.siteid)::text AS comprehendid, KEY*/
        ip.studyid::text AS studyid,
        ip.siteid::text AS siteid,
        ip.ipname::text AS ipname,
        ip.ipid::text AS ipid,
        ip.ipquantity::numeric AS ipquantity,
        ip.ipexpirationdate::date AS ipexpirationdate,
        ip.ipdate::date AS ipdate,
        ip.ipseq::numeric AS ipseq,
        ip.ipstate::text AS ipstate,
        ip.ipreason::text AS ipreason,
        ip.ipunit::text AS ipunit
        /*KEY , (ip.studyid || '~' || ip.siteid || '~' || ip.ipSeq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM ipaccountability_data ip
JOIN included_sites si ON (si.studyid = ip.studyid AND si.siteid = ip.siteid)
WHERE 1=2;  

