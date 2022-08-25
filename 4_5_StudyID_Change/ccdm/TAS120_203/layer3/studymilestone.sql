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
 

    
                SELECT  'TAS-120-203'::text AS studyid,
                         null::text AS studyname,
                         tms.milestoneseq ::int AS milestoneseq,
                         tms.start_standard_label  ::text AS milestonelabel,
                        'Planned'::text AS milestonetype,
                         nullif(evt_dt_planned,'') ::date AS expecteddate,
                         tms.milestonebucketid ::text as milestonebucketid,
                         tms.milestonebucketname ::text as milestonebucketname,
                         tms.ismandatory ::boolean AS ismandatory,
                         'yes'::boolean AS iscriticalpath 
                         from tas120_203_ctms.study_milestones sm
                         left join internal_config.taiho_ms_standards tms on tms.start_original_label = sm.evt_desc  
                         where tms.milestonelevel = 'Study' and nullif (evt_date_actual,'') is not null and tms.vendor = 'TAIHO-PXL'
                        
                        union all
                        
                        SELECT  'TAS-120-203'::text AS studyid,
                        null::text AS studyname,
                        tms.milestoneseq ::int AS milestoneseq,
                        tms.start_standard_label  ::text AS milestonelabel,
                        'Actual'::text AS milestonetype,
                        nullif (evt_date_actual,'') ::date AS expecteddate,
                        tms.milestonebucketid ::text as milestonebucketid,
                         tms.milestonebucketname ::text as milestonebucketname,
                        tms.ismandatory ::boolean AS ismandatory,
                        'yes'::boolean AS iscriticalpath 
                        from tas120_203_ctms.study_milestones sm
                        left join internal_config.taiho_ms_standards tms on tms.start_original_label = sm.evt_desc  
                        where tms.milestonelevel = 'Study' and nullif (evt_date_actual,'') is not null and tms.vendor = 'TAIHO-PXL'
                         
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

