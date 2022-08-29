/*
CCDM CountryMilestone mapping
Notes: Standard mapping to CCDM CountryMilestone table
Milestone Bucket Id: startup, conduct, closeout
*/

WITH included_studies AS (
        SELECT studyid from Study),
    countrymilestone_data AS (
        SELECT  null::text AS comprehendid,
                null::text AS studyid,
                null::text AS studyname,
                null::text AS sitecountry,
                null::text AS sitecountrycode,
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
    /*KEY (cm.studyid || '~' || cm.sitecountry) ::text AS comprehendid, KEY*/ 
    cm.studyid::text AS studyid,
    cm.studyname::text AS studyname,
    cm.sitecountry::text AS sitecountry,
    cm.sitecountrycode::text AS sitecountrycode,
    cm.milestoneseq::int AS milestoneseq,
    cm.milestoneid::int AS milestoneid,
    cm.milestonecat::text AS milestonecat,
    cm.milestonelabel_src::text AS milestonelabel_src,
    cm.milestonelabel::text AS milestonelabel,
    cm.milestonelabel_long::text AS milestonelabel_long,
    cm.milestonetype::text AS milestonetype,
    cm.expecteddate::date AS expecteddate,
    cm.ismandatory::boolean AS ismandatory,
    cm.milestonebucketid::text AS milestonebucketid,
    cm.milestonebucketname::text AS milestonebucketname,
    cm.iskeymilestone::text AS iskeymilestone,
    cm.endmilestonecycletime::text AS endmilestonecycletime,
    cm.startkeymilestone::text AS startkeymilestone
    /*KEY , (cm.studyid || '~' || cm.sitecountry || '~' || cm.milestonetype || '~' || cm.milestoneseq)::text  AS objectuniquekey KEY*/
    /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM countrymilestone_data cm JOIN included_studies ON
(cm.studyid = included_studies.studyid);
