/*
CCDM StudyMilestone mapping
Notes: Standard mapping to CCDM StudyMilestone table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),
                
                
                
                
  studymilestoneseq as (
       select   sm.event_desc as milestonelabel, 
					   case when sm.event_desc = 'Trial Publication' then 1
					        when sm.event_desc = 'CRF / EDC Complete' then 2
					        when sm.event_desc = 'D/Collect Syst. Validated' then 3
					        when sm.event_desc = 'IRB/IEC Submission' then 4
					        when sm.event_desc = 'IRB/IEC Approval' then 5
					        when sm.event_desc = '1st Regulatory Submission' then 6
					        when sm.event_desc = '1st IRB/IEC Submission' then 7
					        when sm.event_desc = '1st Regulatory Approval' then 8
					        when sm.event_desc = '1st IRB/IEC Approval' then 9
					        when sm.event_desc = '1st SIV' then 10
					        when sm.event_desc = '1st Subject 1st Visit' then 11
					        when sm.event_desc = '1st Subject 1st Treatment' then 12
					        when sm.event_desc = 'Last Subject 1st Visit' then 13
					        when sm.event_desc = 'Last Subject Last Visit' then 14
					        when sm.event_desc = 'Trial File Closed' then 15
					        when sm.event_desc = '1st Fully Executed CTA' then 16
					        when sm.event_desc = '1st Site Ready to Enroll' then 17
					        when sm.event_desc = 'DB Soft Lock/Freeze' then 18
					        when sm.event_desc = 'Clinical Report' then 19
					        when sm.event_desc = 'Final Report Approved' then 20
					        when sm.event_desc = 'Protocol Completed' then 21
							
 
					   end::int AS milestoneseq 
				from tas2940_101_ctms.milestone_status_study sm	
   
  ),
  
   exo_FSI as (
  				SELECT  	 studyid::text AS studyid,
  							milestonelabel::text AS milestonelabel,
                        	min(FSI_Actual)::text AS FSI_Actual
                           from (	SELECT  	distinct 'TAS2940_101'::text AS studyid,
														 '1st Subject 1st Treatment'::text AS milestonelabel,
														 "EXOCYCSDT"::text AS FSI_Actual
									from tas2940_101."EXO"
								)fd 
								group by 	1,2
  ),
	
     studymilestone_data AS (
     select studyid,
           sm.milestoneseq ,
            replace(replace (replace (a.milestonelabel, 'Last Subject 1st Visit','LAST SUBJECT IN'),'1st Subject 1st Treatment','FIRST SUBJECT IN'),'Planned','')::text AS milestonelabel,
            milestonetype,
            expecteddate,
            ismandatory,
            iscriticalpath
            from
            (
            
     
                SELECT 
                        'TAS2940-101'::text AS studyid,
                        null::int AS milestoneseq,
						event_desc::text AS milestonelabel,
                        'Planned'::text AS milestonetype,
                        nullif(sm."planned_date",'')::date AS expecteddate,
                        'yes'::boolean AS ismandatory,
                        'yes'::boolean AS iscriticalpath
                        from tas2940_101_ctms.milestone_status_study sm						
                        left join studymilestoneseq ms on (ms.milestonelabel = sm.event_desc)
                        where  nullif(sm."planned_date",'')  is not null  
                                                
                        union all

                select
                        'TAS2940-101'::text AS studyid,
                         null::int AS milestoneseq,
                         event_desc::text AS milestonelabel,
						 'Actual'::text AS milestonetype,
                        case 
                           when lower(event_desc)='1st subject 1st treatment' and  nullif(actual_date,'') is null then exo_FSI.FSI_Actual
                        else nullif(actual_date,'')end ::date AS expecteddate,
                        'yes'::boolean AS ismandatory,
                        'yes'::boolean AS iscriticalpath
     				    from tas2940_101_ctms.milestone_status_study sm
					    left join exo_FSI  on (sm.event_desc=exo_FSI.milestonelabel)
                        left join studymilestoneseq ms on (ms.milestonelabel = sm.event_desc)
                        where  nullif(sm."planned_date",'')  is not null 
                         
                        )a,studymilestoneseq sm
                        where a.milestonelabel=sm.milestonelabel
                        and sm.milestoneseq is not null
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










