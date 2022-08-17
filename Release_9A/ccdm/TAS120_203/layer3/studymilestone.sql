/*
CCDM StudyMilestone mapping
Notes: Standard mapping to CCDM StudyMilestone table
*/

WITH included_studies AS (
                SELECT studyid FROM study),
                
                
     studymilestone_data AS (
     		select * from(
		     			SELECT  'TAS-120-203'::text AS studyid,
								 null::text AS studyname,
								 case
							     when evt_desc = '1st Site Selected' then 1
							     when evt_desc = '1st Qual Visit/Call Compl' then 2
							     when evt_desc = 'Last Regulatory Approval' then 3
							     when evt_desc = '1st Screened Subject' then 4
							     when evt_desc = '1st Subject 1st Visit' then 5
							     when evt_desc = 'Last Regulatory Submission Complete' then 6
							     when evt_desc = 'Last Subject 1st Treatment' then 7
							     when evt_desc = 'Last Subject Compl. Study' then 8
							     when evt_desc = 'Database Lock Date' then 9
							     end::int AS milestoneseq,
		                        "evt_desc"::text AS milestonelabel,
		                        'Actual'::text AS milestonetype,
		                        nullif(evt_date_actual,'')::date AS expecteddate,
		                        'Yes'::boolean AS ismandatory,
		                        'Yes'::boolean AS iscriticalpath
		                        from tas120_203_ctms.study_milestones
		                        
		                        union all
		                SELECT  'TAS-120-203'::text AS studyid,
		                        null::text AS studyname,
		                         case
							     when evt_desc = '1st Site Selected' then 1
							     when evt_desc = '1st Qual Visit/Call Compl' then 2
							     when evt_desc = 'Last Regulatory Approval' then 3
							     when evt_desc = '1st Screened Subject' then 4
							     when evt_desc = '1st Subject 1st Visit' then 5
							     when evt_desc = 'Last Regulatory Submission Complete' then 6
							     when evt_desc = 'Last Subject 1st Treatment' then 7
							     when evt_desc = 'Last Subject Compl. Study' then 8
							     when evt_desc = 'Database Lock Date' then 9
							     end::int AS milestoneseq,
		                        "evt_desc"::text AS milestonelabel,
		                        'Planned'::text AS milestonetype,
		                        nullif(evt_dt_planned,'')::date AS expecteddate,
		                        'Yes'::boolean AS ismandatory,
		                        'Yes'::boolean AS iscriticalpath
		                        from tas120_203_ctms.study_milestones
		                        where evt_dt_planned != '')sm
		                        where milestoneseq is not null
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


