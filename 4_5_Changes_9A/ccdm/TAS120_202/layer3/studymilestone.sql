/*
CCDM StudyMilestone mapping
Notes: Standard mapping to CCDM StudyMilestone table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),
                
  
	
    studymilestone_data AS (
     select studyid,
     studyname,
     milestoneseq,
     milestonelabel,
     milestonetype,
     expecteddate,
     milestonebucketid,
     milestonebucketname,
     ismandatory,
     iscriticalpath
     from
     (
 

    
                SELECT  'TAS120_202'::text AS studyid,
                         null::text AS studyname,
                         tms.milestoneseq ::int AS milestoneseq,
                         tms.start_standard_label  ::text AS milestonelabel,
                        'Planned'::text AS milestonetype,
                         planned_date ::date AS expecteddate,
                         tms.milestonebucketid ::text as milestonebucketid,
                         tms.milestonebucketname ::text as milestonebucketname,
                         tms.ismandatory ::boolean AS ismandatory,
                         'yes'::boolean AS iscriticalpath 
                         from tas120_202_ctms.milestone sm
                         left join internal_config.taiho_ms_standards tms on tms.start_original_label = sm."type"  
                         where tms.milestonelevel = 'Study' and actual_date is not null
                        
                        union all
                        
                        SELECT  'TAS120_202'::text AS studyid,
                        null::text AS studyname,
                        tms.milestoneseq ::int AS milestoneseq,
                        tms.start_standard_label  ::text AS milestonelabel,
                        'Actual'::text AS milestonetype,
                        actual_date ::date AS expecteddate,
                        tms.milestonebucketid ::text as milestonebucketid,
                         tms.milestonebucketname ::text as milestonebucketname,
                        tms.ismandatory ::boolean AS ismandatory,
                        'yes'::boolean AS iscriticalpath 
                        from tas120_202_ctms.milestone sm 
                        left join internal_config.taiho_ms_standards tms on tms.start_original_label = sm."type"  
                        where tms.milestonelevel = 'Study' and actual_date is not null
                         
                        )a
		
                        )
                        

SELECT 
        /*KEY sm.studyid::text AS comprehendid, KEY*/
        sm.studyid::text AS studyid,
        null::text AS studyname,
        sm.milestoneseq::int AS milestoneseq,
        sm.milestonelabel::text AS milestonelabel,
        sm.milestonetype::text AS milestonetype,
        sm.expecteddate::date AS expecteddate,
        sm.milestonebucketid::text as milestonebucketid,
        sm.milestonebucketname::text as milestonebucketname,
        sm.ismandatory::boolean AS ismandatory,
        sm.iscriticalpath::boolean AS iscriticalpath
        /*KEY , (sm.studyid || '~' || sm.milestoneType || '~' || sm.milestoneSeq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM studymilestone_data sm
JOIN included_studies st ON (sm.studyid = st.studyid);

