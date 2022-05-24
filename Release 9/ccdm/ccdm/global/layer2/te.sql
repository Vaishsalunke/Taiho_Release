/*
CCDM Trial Elements mapping
Notes: Standard mapping to CCDM Trial Elements table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),

te AS (
                SELECT  null::text AS studyid,
                        null::text AS etcd,
                        null::text AS element,
                        null::text AS testrl,
                        null::text AS teenrl,
                        null::text AS tedur
            )
                        
SELECT 
        /*KEY t.studyid::text AS comprehendid, KEY*/
        t.studyid::text AS studyid,
        t.etcd::text AS etcd,
        t.element::text AS element,
        t.testrl::text AS testrl,
        t.teenrl::text AS teenrl,
        t.tedur::text AS tedur
        /*KEY , (t.studyid || '~' || t.etcd)::TEXT AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM te t
JOIN included_studies st ON (st.studyid = t.studyid)
WHERE 1=2;
