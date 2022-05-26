/*
CCDM StudyMilestone mapping
Notes: Standard mapping to CCDM StudyMilestone table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),
milestone_sequence AS
( 
select sm.milestone_name as milestonelabel, 
case when sm.milestone_name = 'Contract Executed' then 1
 when sm.milestone_name = 'Hand-off Meeting' then 2
 when sm.milestone_name = 'Final Protocol Received' then 3
 when sm.milestone_name = 'Planned CRF/eCRF Date' then 4
 when sm.milestone_name = 'Final CRF Specifications Complete' then 5
 when sm.milestone_name = 'Database Go Live' then 6
 when sm.milestone_name = 'First Site Activated' then 7
 when sm.milestone_name = 'First Subject Screened' then 8
 when sm.milestone_name = 'First Subject Enrolled' then 9
 when sm.milestone_name = 'Last Site Activated' then 10
 when sm.milestone_name = 'First Subject Complete Expansion' then 11
 when sm.milestone_name = 'Last Subject Enrolled' then 12
 when sm.milestone_name = 'Last Subject Last Visit' then 13
 when sm.milestone_name = 'Database Lock' then 14
 when sm.milestone_name = 'Close Out Visits Complete' then 15
 when sm.milestone_name = 'Final TLFs Complete' then 16
 when sm.milestone_name = 'Final TLFs sent to Sponsor' then 17
 when sm.milestone_name = 'TMF Transfer' then 18
 when sm.milestone_name = 'Operationally Complete' then 19 end::int AS milestoneseq 
		 from tas3681_101_ctms.study_milestones sm
		),			
     studymilestone_data AS (
     select sm.studyid,
     ms.milestoneseq,
     case when sm.milestonelabel = 'First Subject Enrolled' then 'FIRST SUBJECT IN'
     when sm.milestonelabel = 'Last Subject Enrolled' then 'LAST SUBJECT IN'
     when sm.milestonelabel = 'First Site Activated' then 'FIRST SITE READY TO ENROLL' 
	when sm.milestonelabel = 'Last Site Activated' then 'ALL SITES ACTIVATED'	 
     else sm.milestonelabel end as milestonelabel,
     milestonetype,
     sm.expecteddate,
     ismandatory,
     iscriticalpath
     from
     (
                SELECT  'TAS3681_101_DOSE_ESC'::text AS studyid,
                        null::int AS milestoneseq,
                        sm."milestone_name"::text AS milestonelabel,
                        'Planned'::text AS milestonetype,
                        nullif(sm."planned_date",'')::date AS expecteddate,
                        'yes'::boolean AS ismandatory,
                        'yes'::boolean AS iscriticalpath
                        from tas3681_101_ctms.study_milestones sm                        
				UNION ALL
				SELECT  'TAS3681_101_DOSE_ESC'::text AS studyid,
                        null::int AS milestoneseq,
                        sm."milestone_name"::text AS milestonelabel,
                        'Actual'::text AS milestonetype,
                        nullif(sm."actual_date",'')::date AS expecteddate,
                        'yes'::boolean AS ismandatory,
                        'yes'::boolean AS iscriticalpath
                        from tas3681_101_ctms.study_milestones sm
                        )sm inner join milestone_sequence ms on sm.milestonelabel = ms.milestonelabel
                        where ms.milestoneseq is not null
                        )

SELECT 
        /*KEY sm.studyid::text AS comprehendid, KEY*/
        sm.studyid::text AS studyid,
		null::text AS studyname,
        sm.milestoneseq::int AS milestoneseq,
        sm.milestonelabel::text AS milestonelabel,
        sm.milestonetype::text AS milestonetype,
        sm.expecteddate::date AS expecteddate,
        sm.ismandatory::boolean AS ismandatory,
        sm.iscriticalpath::boolean AS iscriticalpath
        /*KEY , (sm.studyid || '~' || sm.milestoneType || '~' || sm.milestoneSeq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM studymilestone_data sm
JOIN included_studies st ON (sm.studyid = st.studyid);

