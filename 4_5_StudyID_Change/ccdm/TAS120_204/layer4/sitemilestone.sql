/*
CCDM SiteMilestone mapping
Notes: Standard mapping to CCDM SiteMilestone table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site),
                
     sitemilestone_data AS (   
	 select studyid,
	 studyname,
	 siteid,
	 --row_number() over (partition by studyid,siteid order by expecteddate,milestonelabel) as milestoneseq,
	 milestoneseq,
	 milestonelabel,
	 milestonetype,
	 expecteddate,
	 ismandatory,
	 milestonebucketid,
	 milestonebucketname,
	 iskeymilestone,
	 startkeymilestone,
	 milestoneid,
	 milestonelabel_src,
	 milestonelabel_long,
	 milestonecat,
	 endmilestonecycletime
	 from
	 (
	 SELECT  		distinct	'TAS-120-204'::text AS studyid,
						'TAS120_204'::text AS studyname,
                        concat('TAS120_204_',split_part ("site_number",'_',2))::text AS siteid,
                        tms.milestoneseq::int AS milestoneseq,
                        replace (tms.start_standard_label,'FIRST SUBJECT FIRST TREATMENT','First Subject In')::text AS milestonelabel,
                        'Planned'::text AS milestonetype,
                        nullif(sm."planned_date",'')::date AS expecteddate,
                        'yes'::boolean AS ismandatory,
						tms.milestonebucketid:: text as milestonebucketid,
						tms.milestonebucketname:: text as milestonebucketname,
						tms.ismandatory::boolean as iskeymilestone,
						tms.ismandatory::text as startkeymilestone,
						tms.milestoneid:: text as milestoneid,
						tms.start_original_label:: text as milestonelabel_src,
						tms.start_standard_label:: text as milestonelabel_long,
						null::text AS milestonecat,
						tms.endmilestonecycletime_standard:: text as endmilestonecycletime
					from tas120_204_ctms.site_milestones sm
					left join internal_config.taiho_ms_standards tms on upper(start_original_label) = upper(milestone_name)
					where tms.milestonelevel = 'Site'
	UNION 
	 select distinct	'TAS-120-204'::text AS studyid,
						'TAS120_204'::text AS studyname,
                        concat('TAS120_204_',split_part ("site_number",'_',2))::text AS siteid,
                        tms.milestoneseq::int AS milestoneseq,
                        replace (tms.start_standard_label,'FIRST SUBJECT FIRST TREATMENT','First Subject In')::text AS milestonelabel,
                        'Actual'::text AS milestonetype,
                        nullif(sm."actual_date",'')::date AS expecteddate,
                        'yes'::boolean AS ismandatory,
						tms.milestonebucketid:: text as milestonebucketid,
						tms.milestonebucketname:: text as milestonebucketname,
						tms.ismandatory::boolean as iskeymilestone,
						tms.ismandatory::text as startkeymilestone,
						tms.milestoneid:: text as milestoneid,
						tms.start_original_label:: text as milestonelabel_src,
						tms.start_standard_label:: text as milestonelabel_long,
						null::text AS milestonecat,
						tms.endmilestonecycletime_standard:: text as endmilestonecycletime
					from tas120_204_ctms.site_milestones sm
					left join internal_config.taiho_ms_standards tms on upper(start_original_label) = upper(milestone_name)
					where tms.milestonelevel = 'Site'
	union 
	Select sm.*,tms.endmilestonecycletime_standard:: text as endmilestonecycletime 
	from(
			select studyid,
				   studyname,
				   siteid,
				   milestoneseq,
				   milestonelabel,
				   milestonetype,
				   min(expecteddate) as expecteddate,
				   ismandatory,
				   milestonebucketid,
				   milestonebucketname,
				   iskeymilestone,
				   startkeymilestone,
				   milestoneid,
				   milestonelabel_src,
				   milestonelabel_long,
				   milestonecat
	   
			from (	   
					select distinct	'TAS-120-204'::text AS studyid,
									'TAS120_204'::text AS studyname,
									concat('TAS120_204_',split_part (ex."SiteNumber" ,'_',2))::text AS siteid,
									999::int AS milestoneseq,
									'FIRST SUBJECT IN'::text AS milestonelabel,
									'Actual'::text AS milestonetype,
									case when lower("EXOADJYN")= 'yes' then "EXOSTDAT"
										else "EXOCYCSDT"
									end::date AS expecteddate,
                       '			yes'::boolean AS ismandatory,
									'Startup':: text as milestonebucketid,
									'Study Startup':: text as milestonebucketname,
									true::boolean as iskeymilestone,
									'yes'::text as startkeymilestone,
									'MS999':: text as milestoneid,
									'FIRST SUBJECT IN':: text as milestonelabel_src,
									'FIRST SUBJECT IN':: text as milestonelabel_long,
									null::text AS milestonecat
					from tas120_204."EXO" ex						
	
				)e 
	
					group by 1,2,3,4,5,6,8,9,10,11,12,13,14,15,16					
					)sm
					left join (Select replace (tms.start_standard_label,'FIRST SUBJECT FIRST TREATMENT','FIRST SUBJECT IN') as start_standard_label1, tms.endmilestonecycletime_standard 
					from internal_config.taiho_ms_standards tms where start_standard_label='FIRST SUBJECT FIRST TREATMENT' and tms.milestonelevel = 'Site')tms  
					on upper(sm.milestonelabel) = upper(tms.start_standard_label1)
union 

Select sm.*,tms.endmilestonecycletime_standard:: text as endmilestonecycletime from(
					select distinct	'TAS-120-204'::text AS studyid,
						'TAS120_204'::text AS studyname,
                        concat('TAS120_204_',split_part (ex."SiteNumber" ,'_',2))::text AS siteid,
                        999::int AS milestoneseq,
                        'FIRST SUBJECT IN'::text AS milestonelabel,
                        'Planned'::text AS milestonetype,
                         null::date AS expecteddate,
                        'yes'::boolean AS ismandatory,
						'Startup':: text as milestonebucketid,
						'Study Startup':: text as milestonebucketname,
						true::boolean as iskeymilestone,
						'yes'::text as startkeymilestone,
						'MS999':: text as milestoneid,
						'FIRST SUBJECT IN':: text as milestonelabel_src,
						'FIRST SUBJECT IN':: text as milestonelabel_long,
						null::text AS milestonecat
						
					from tas120_204."EXO" ex
					group by 1,2,3,4,5,6,8,9,10,11,12,13,14,15,16					
					)sm
					left join (Select replace (tms.start_standard_label,'FIRST SUBJECT FIRST TREATMENT','FIRST SUBJECT IN') as start_standard_label1, tms.endmilestonecycletime_standard 
					from internal_config.taiho_ms_standards tms where start_standard_label='FIRST SUBJECT FIRST TREATMENT' and tms.milestonelevel = 'Site')tms  
					on upper(sm.milestonelabel) = upper(tms.start_standard_label1)
					
union	


					select distinct	'TAS-120-204'::text AS studyid,
						'TAS120_204'::text AS studyname,
                        concat('TAS120_204_',split_part (s."site_number",'_',2))::text AS siteid,
                        998::int AS milestoneseq,
                        'Site Activated'::text AS milestonelabel,
                        'Planned'::text AS milestonetype,
                         nullif(site_activated_date,'')::date AS expecteddate,
                        'yes'::boolean AS ismandatory,
						'Startup':: text as milestonebucketid,
						'Study Startup':: text as milestonebucketname,
						true::boolean as iskeymilestone,
						'yes'::text as startkeymilestone,
						'MS998':: text as milestoneid,
						'Site Activated':: text as milestonelabel_src,
						'Site Activated':: text as milestonelabel_long,
						null::text AS milestonecat,
						null::text as endmilestonecycletime
				 from  tas120_204_ctms.sites s
				 
union	

	select distinct	'TAS-120-204'::text AS studyid,
						'TAS120_204'::text AS studyname,
                        concat('TAS120_204_',split_part (s."site_number",'_',2))::text AS siteid,
                        998::int AS milestoneseq,
                        'Site Activated'::text AS milestonelabel,
                        'Actual'::text AS milestonetype,
                         nullif(site_activated_date,'')::date AS expecteddate,
                        'yes'::boolean AS ismandatory,
						'Startup':: text as milestonebucketid,
						'Study Startup':: text as milestonebucketname,
						true::boolean as iskeymilestone,
						'yes'::text as startkeymilestone,
						'MS998':: text as milestoneid,
						'Site Activated':: text as milestonelabel_src,
						'Site Activated':: text as milestonelabel_long,
						null::text AS milestonecat,
						null::text as endmilestonecycletime
				 from tas120_204_ctms.sites s
				 
union	


					select distinct	'TAS-120-204'::text AS studyid,
						'TAS120_204'::text AS studyname,
                        concat('TAS120_204_',split_part (s."site_number",'_',2))::text AS siteid,
                        997::int AS milestoneseq,
                        'Site Selected'::text AS milestonelabel,
                        'Planned'::text AS milestonetype,
                         nullif(site_selected_date,'')::date AS expecteddate,
                        'yes'::boolean AS ismandatory,
						'Startup':: text as milestonebucketid,
						'Study Startup':: text as milestonebucketname,
						true::boolean as iskeymilestone,
						'yes'::text as startkeymilestone,
						'MS997':: text as milestoneid,
						'Site Selected':: text as milestonelabel_src,
						'Site Selected':: text as milestonelabel_long,
						null::text AS milestonecat,
						null::text as endmilestonecycletime
				 from tas120_204_ctms.sites s
				 
union	

	select distinct	'TAS-120-204'::text AS studyid,
						'TAS120_204'::text AS studyname,
                        concat('TAS120_204_',split_part (s."site_number",'_',2))::text AS siteid,
                        997::int AS milestoneseq,
                        'Site Selected'::text AS milestonelabel,
                        'Actual'::text AS milestonetype,
                         nullif(site_selected_date,'')::date AS expecteddate,
                        'yes'::boolean AS ismandatory,
						'Startup':: text as milestonebucketid,
						'Study Startup':: text as milestonebucketname,
						true::boolean as iskeymilestone,
						'yes'::text as startkeymilestone,
						'MS997':: text as milestoneid,
						'Site Selected':: text as milestonelabel_src,
						'Site Selected':: text as milestonelabel_long,
						null::text AS milestonecat,
						null::text as endmilestonecycletime
				 from tas120_204_ctms.sites s 
					)sm
					)
					
SELECT 
        /*KEY (sm.studyid || '~' || sm.siteid)::text AS comprehendid, KEY*/
        sm.studyid::text AS studyid,
		sm.studyname::text AS studyname,
        sm.siteid::text AS siteid,
        sm.milestoneseq::int AS milestoneseq,
		sm.milestoneid:: text as milestoneid,
		sm.milestonecat::text AS milestonecat,
		sm.milestonelabel_src:: text as milestonelabel_src,
        sm.milestonelabel::text AS milestonelabel,
		sm.milestonelabel_long:: text as milestonelabel_long,
        sm.milestonetype::text AS milestonetype,
        sm.expecteddate::date AS expecteddate,
        sm.ismandatory::boolean AS ismandatory,
        sm.milestonebucketid::text as milestonebucketid,
		sm.milestonebucketname:: text as milestonebucketname,
		sm.iskeymilestone::boolean as iskeymilestone,
		sm.endmilestonecycletime:: text as endmilestonecycletime,
		sm.startkeymilestone::text as startkeymilestone
        /*KEY , (sm.studyid || '~' || sm.siteid || '~' || sm.milestonetype || '~' || sm.milestoneseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM sitemilestone_data sm
JOIN included_sites si ON (sm.studyid = si.studyid AND sm.siteid = si.siteid);








