/*
CCDM Trial Arms mapping
Notes: Standard mapping to CCDM Trial Arms table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),

ta AS (
                SELECT  null::text AS studyid,
                        null::text AS armcd,
                        null::text AS arm,
                        null::NUMERIC AS taetord,
                        null::text AS etcd,
                        null::text AS element,
                        null::text AS tabranch,
                        null::text AS tatrans,
                        null::text AS epoch
            )
                        
SELECT 
        /*KEY t.studyid::text AS comprehendid, KEY*/
        t.studyid::text AS studyid,
        t.armcd::text AS armcd,
        t.arm::text AS arm,
        t.taetord::NUMERIC AS taetord,
        t.etcd::text AS etcd,
        t.element::text AS element,
        t.tabranch::text AS tabranch,
        t.tatrans::text AS tatrans,
        t.epoch::text AS epoch
        /*KEY , (t.studyid || '~' || t.armcd || '~' || t.taetord)::TEXT AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM ta t
JOIN included_studies st ON (st.studyid = t.studyid)
WHERE 1=2;
