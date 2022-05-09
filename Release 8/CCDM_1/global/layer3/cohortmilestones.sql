/*
CCDM Cohortmilestones mapping
Notes: Standard mapping to CCDM Cohortmilestones table
*/

WITH included_studies AS (
                SELECT DISTINCT studyid FROM study ),

     cohortmilestones_data AS (
                SELECT  null::text AS studyid,
                        null::text AS cohortid,
                        null::text AS milestoneid,
                        null::text AS milestonecategory,
                        null::text AS milestonelabel_src,
                        null::text AS milestonelabel_short,
                        null::text AS milestonelabel_long,
                        null::text AS milestonetype,
                        null::text AS milestoneseq,
                        null::text AS milestonestartdate,
                        null::text AS milestoneenddate,
                        null::boolean AS ismandatory,
                        null::boolean AS iscriticalpath)

SELECT 
        /*KEY (cm.studyid || '~' || cm.cohortid)::text AS comprehendid, KEY*/
        cm.studyid::text AS studyid,
        cm.cohortid::text AS cohortid,
        cm.milestoneid::text AS milestoneid,
        cm.milestonecategory::text AS milestonecategory,
        cm.milestonelabel_src::text AS milestonelabel_src,
        cm.milestonelabel_short::text AS milestonelabel_short,
        cm.milestonelabel_long::text AS milestonelabel_long,
        cm.milestonetype::text AS milestonetype,
        cm.milestoneseq::text AS milestoneseq,
        cm.milestonestartdate::text AS milestonestartdate,
        cm.milestoneenddate::text AS milestoneenddate,
        cm.ismandatory::boolean AS ismandatory,
        cm.iscriticalpath::boolean AS iscriticalpath
        /*KEY , (cm.studyid || '~' || cm.cohortid || '~' || cm.milestonelabel_short || '~' || cm.milestonetype)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM cohortmilestones_data cm
JOIN included_studies s ON (s.studyid = cm.studyid)
WHERE 1=2; 

