/*
CCDM StudyMilestone mapping
Notes: Standard mapping to CCDM StudyMilestone table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),

     seq_date as (
				select sm.milestone_name as milestonelabel, 
					   case when sm.milestone_name = 'Hand-off Meeting' then 1
							when sm.milestone_name = 'First Billable Hour' then 2
							when sm.milestone_name = 'Contract Executed' then 3
							when sm.milestone_name = 'Final Protocol Received' then 4
							when sm.milestone_name = 'Planned CRF/eCRF Date' then 5
							when sm.milestone_name = 'QuickStart Camp Complete' then 6
							when sm.milestone_name = 'Final CRF Specifications Complete' then 7
							when sm.milestone_name = 'IXRS Database Go Live' then 8
							when sm.milestone_name = 'Database Go Live' then 9
							when sm.milestone_name = 'First Site Activated' then 10
							when sm.milestone_name = 'First Subject Screened' then 11
							when sm.milestone_name = 'Last Site Activated' then 12
							when sm.milestone_name = 'First Subject Enrolled' then 13
							when sm.milestone_name = 'First Subject Complete' then 14
							when sm.milestone_name = 'Last Subject Enrolled' then 15
							when sm.milestone_name = 'QualityFinish Camp Complete' then 16
							when sm.milestone_name = 'Last Subject Last Visit' then 17
							when sm.milestone_name = 'Database Lock' then 18
							when sm.milestone_name = 'Close Out Visits Complete' then 19 
							when sm.milestone_name = 'TMF Transfer' then 20 
							when sm.milestone_name = 'Operationally Complete' then 21 
							--when sm.milestone_name = 'First Billable Hour' then 22 
							--when sm.milestone_name = 'First Subject Complete' then 23 
							--when sm.milestone_name = 'QuickStart Camp Complete' then 24 
 
					   end::int AS milestoneseq 
				from tas120_204_ctms.study_milestones sm
			),


studymilestone_data as (

SELECT 	DISTINCT
		studyid::text AS studyid,
        studyname::text AS studyname,
        sd.milestoneseq::int AS milestoneseq,
        --r.milestonelabel::text AS milestonelabel,
		case when r.milestonelabel = 'First Subject Enrolled' then 'FIRST SUBJECT IN'
			 when r.milestonelabel = 'Last Subject Enrolled' then 'LAST SUBJECT IN'
			 when r.milestonelabel = 'First Site Activated' then 'FIRST SITE READY TO ENROLL' 
			 when r.milestonelabel = 'Last Site Activated' then 'ALL SITES ACTIVATED'	 
			 else r.milestonelabel 
		end ::text AS milestonelabel,
        milestonetype::text AS milestonetype,
        expecteddate::date AS expecteddate,
        ismandatory::boolean AS ismandatory,
        iscriticalpath::boolean AS iscriticalpath

from (        


				SELECT  'TAS120_204'::text AS studyid,
                        null::text AS studyname,
                        null::int AS milestoneseq,
                        milestone_name::text AS milestonelabel,
                        'Planned'::text AS milestonetype,
                        planned_date::date AS expecteddate,
                        'yes'::boolean AS ismandatory,
                        'yes'::boolean AS iscriticalpath
				from tas120_204_ctms.study_milestones    

			union all 

				SELECT  'TAS120_204'::text AS studyid,
                        null::text AS studyname,
                        null::int AS milestoneseq,
                        milestone_name::text AS milestonelabel,
                        'Actual'::text AS milestonetype,
                        nullif(actual_date,'')::date AS expecteddate,
                        'yes'::boolean AS ismandatory,
                        'yes'::boolean AS iscriticalpath
				from tas120_204_ctms.study_milestones  
				--where actual_date<>''
				
				
		)r ,seq_date sd
		
where r.milestonelabel=sd.milestonelabel
and sd.milestoneseq is not null
		
		
		
		
								)


SELECT 
        /*KEY sm.studyid::text AS comprehendid, KEY*/
        sm.studyid::text AS studyid,
        sm.studyname::text AS studyname,
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


