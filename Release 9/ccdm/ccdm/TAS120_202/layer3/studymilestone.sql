/*
CCDM StudyMilestone mapping
Notes: Standard mapping to CCDM StudyMilestone table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),
                
   studymilestoneseq as (
   select  sm."type" as milestonelabel,
   row_number() over(order by convert_to_date(sm."planned_date")) as milestoneseq
   from tas120_202_ctms.milestone sm
    where  nullif(sm."planned_date",'')  is not null
    ),
	
     studymilestone_data AS (
     select studyid,
     milestoneseq,
     case when milestonelabel = 'First Subject Enrolled' then 'FIRST SUBJECT IN'
     when milestonelabel = 'Last Subject Enrolled' then 'LAST SUBJECT IN'
     when milestonelabel = 'First Site Initiated' then 'FIRST SITE READY TO ENROLL' 
	when milestonelabel = 'Last Site Activated' then 'ALL SITES ACTIVATED'	 
     else milestonelabel end as milestonelabel,
     milestonetype,
     expecteddate,
     ismandatory,
     iscriticalpath
     from
     (
                SELECT  'TAS120_202'::text AS studyid,
                        ms.milestoneseq::int AS milestoneseq,
                        sm."type"::text AS milestonelabel,
                        'Planned'::text AS milestonetype,
                        nullif(sm."planned_date",'')::date AS expecteddate,
                        'yes'::boolean AS ismandatory,
                        'yes'::boolean AS iscriticalpath
                        from tas120_202_ctms.milestone sm 
                         left join studymilestoneseq ms on (ms.milestonelabel = sm."type")
                        where  nullif(sm."planned_date",'')  is not null 
                        
                        union all
                        
                SELECT  'TAS120_202'::text AS studyid,
                        ms.milestoneseq::int AS milestoneseq,
                        sm."type"::text AS milestonelabel,
                        'Actual'::text AS milestonetype,
                        sm.actual_date::date AS expecteddate,
                        'yes'::boolean AS ismandatory,
                        'yes'::boolean AS iscriticalpath
                        from tas120_202_ctms.milestone sm 
                         left join studymilestoneseq ms on (ms.milestonelabel = sm."type")
                        where  nullif(sm."planned_date",'')  is not null 
                                          
                        )a)

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

