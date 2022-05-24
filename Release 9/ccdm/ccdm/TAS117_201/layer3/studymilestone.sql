/*
CCDM StudyMilestone mapping
Notes: Standard mapping to CCDM StudyMilestone table
*/


WITH included_studies AS (
                SELECT studyid FROM study ),
                
    studymilestoneseq as (
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
							
							 
 
					   end::int AS milestoneseq 
				from tas117_201_ctms.study_milestones sm
   
  ),        
  
  studymilestone_data AS (
     select studyid,
     studyname,
     sm.milestoneseq,
     case when a.milestonelabel = 'First Subject Enrolled' then 'FIRST SUBJECT IN'
     	  when a.milestonelabel = 'Last Subject Enrolled' then 'LAST SUBJECT IN'
          when a.milestonelabel = 'First Site Initiated' then 'FIRST SITE READY TO ENROLL' 
	      when a.milestonelabel = 'Last Site Activated' then 'ALL SITES ACTIVATED'	 
     else a.milestonelabel end as milestonelabel,
     milestonetype,
     expecteddate,
     ismandatory,
     iscriticalpath
     from
     (
 

    
                SELECT  'TAS117_201'::text AS studyid,
                         null::text AS studyname,
                         null::int AS milestoneseq,
                         milestone_name ::text AS milestonelabel,
                        'Planned'::text AS milestonetype,
                         nullif(planned_date,'') ::date AS expecteddate,
                         'yes'::boolean AS ismandatory,
                         'yes'::boolean AS iscriticalpath 
                         from tas117_201_ctms.study_milestones sm
                         left join studymilestoneseq ms on (ms.milestonelabel = sm.milestone_name)
                         where  nullif(sm."planned_date",'')  is not null  
                        
                        union all
                        
                        SELECT  'TAS117_201'::text AS studyid,
                        null::text AS studyname,
                        null::int AS milestoneseq,
                        milestone_name ::text AS milestonelabel,
                        'Actual'::text AS milestonetype,
                        nullif(actual_date ,'') ::date AS expecteddate,
                        'yes'::boolean AS ismandatory,
                        'yes'::boolean AS iscriticalpath 
                        from tas117_201_ctms.study_milestones sm 
                        left join studymilestoneseq ms on (ms.milestonelabel = sm.milestone_name)
                        where  nullif(sm."planned_date",'')  is not null
                         
                        )a,studymilestoneseq sm
		
where a.milestonelabel=sm.milestonelabel
and sm.milestoneseq is not null
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
 




