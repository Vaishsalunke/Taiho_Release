/*
CCDM SiteMilestone mapping
Notes: Standard mapping to CCDM SiteMilestone table

Milestone Bucket Id: startup, conduct, closeout

*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site),

     sitemilestone_data AS (
                SELECT  null::text AS studyid,
                        null::text AS studyname,
                        null::text AS siteid,
                        null::int AS milestoneseq,
                        null::int AS milestoneid,
                        null::text AS milestonecat,
                        null::text AS milestonelabel_src,
                        null::text AS milestonelabel,
                        null::text AS milestonelabel_long,
                        null::text AS milestonetype,
                        null::date AS expecteddate,
                        null::boolean AS ismandatory,
                        null::text AS milestonebucketid,
                        null::text AS milestonebucketname,
                        null::boolean AS iskeymilestone,
                        null::text AS endmilestonecycletime,
                        null::boolean AS startkeymilestone
                        )

SELECT 
        /*KEY (sm.studyid || '~' || sm.siteid)::text AS comprehendid, KEY*/
        sm.studyid::text AS studyid,
        sm.studyname::text AS studyname,
        sm.siteid::text AS siteid,
        sm.milestoneseq::int AS milestoneseq,
        sm.milestoneid::int AS milestoneid,
        sm.milestonecat::text AS milestonecat,
        sm.milestonelabel_src::text AS milestonelabel_src,
        sm.milestonelabel::text AS milestonelabel,
        sm.milestonelabel_long::text AS milestonelabel_long,
        sm.milestonetype::text AS milestonetype,
        sm.expecteddate::date AS expecteddate,
        sm.ismandatory::boolean AS ismandatory,
        sm.milestonebucketid::text AS milestonebucketid,
        sm.milestonebucketname::text AS milestonebucketname,
        sm.iskeymilestone::text AS iskeymilestone,
        sm.endmilestonecycletime::text AS endmilestonecycletime,
        sm.startkeymilestone::text AS startkeymilestone
        /*KEY , (sm.studyid || '~' || sm.siteid || '~' || sm.milestonetype || '~' || sm.milestoneseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM sitemilestone_data sm
JOIN included_sites si ON (sm.studyid = si.studyid AND sm.siteid = si.siteid)
WHERE 1=2; 
