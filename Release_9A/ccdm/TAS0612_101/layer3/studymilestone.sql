/*
CCDM StudyMilestone mapping
Notes: Standard mapping to CCDM StudyMilestone table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),
                
  studymilestoneseq as (
   select  sm.event_desc as milestonelabel,
   row_number() over(order by convert_to_date(sm."planned_date")) as milestoneseq
   from tas0612_101_ctms.milestone_status_study sm
    where  nullif(sm."planned_date",'') is not null
   
  ),
  
  exo_FSI as (
  				SELECT  	distinct	'TAS0612_101'::text AS studyid,
  							'1st Subject 1st Treatment'::text AS milestonelabel,
                        	min("EXOSTDAT")::text AS FSI_Actual
                from 		tas0612_101."EXO"
				group by 	1,2
  ),
	
     studymilestone_data AS (
                SELECT  'TAS0612-101'::text AS studyid,
                        ms.milestoneseq::int AS milestoneseq,
                         replace(replace (replace (event_desc, 'Last Subject 1st Visit','LAST SUBJECT IN'),'1st Subject 1st Treatment','FIRST SUBJECT IN'),'Planned','')::text AS milestonelabel,
                        'Planned'::text AS milestonetype,
                        nullif(sm."planned_date",'')::date AS expecteddate,
                        'yes'::boolean AS ismandatory,
                        'yes'::boolean AS iscriticalpath
                        from tas0612_101_ctms.milestone_status_study sm
                        left join studymilestoneseq ms on (ms.milestonelabel = sm.event_desc)
                        where  nullif(sm."planned_date",'')  is not null  
                                                
                        union all

                          SELECT  'TAS0612-101'::text AS studyid,
                        ms.milestoneseq::int AS milestoneseq,
                         replace(replace (replace (event_desc, 'Last Subject 1st Visit','LAST SUBJECT IN'),'1st Subject 1st Treatment','FIRST SUBJECT IN'),'Planned','')::text AS milestonelabel,
                        'Actual'::text AS milestonetype,
                        case when lower(event_desc)='1st subject 1st treatment' and  nullif(actual_date,'') is null then exo_FSI.FSI_Actual
                        else nullif(actual_date,'')end ::date AS expecteddate,
                        'yes'::boolean AS ismandatory,
                        'yes'::boolean AS iscriticalpath
     				from tas0612_101_ctms.milestone_status_study sm
     				left join exo_FSI  on (sm.event_desc=exo_FSI.milestonelabel)
                        left join studymilestoneseq ms on (ms.milestonelabel = sm.event_desc)
                        where  nullif(sm."planned_date",'')  is not null 
									
                         
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
