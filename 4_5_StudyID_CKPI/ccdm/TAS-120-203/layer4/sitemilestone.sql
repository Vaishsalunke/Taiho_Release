/*
CCDM SiteMilestone mapping
Notes: Standard mapping to CCDM SiteMilestone table
Milestone Bucket Id: startup, conduct, closeout
*/


WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site),
                
             milestonelabel as (
select 'TAS120_203'as studyid,column_name ,case 
                         when column_name='actual_activation' then 'Site Activation'
                           when column_name='actual_product' then 'Product'
                           when column_name='actual_reg_approval' then 'Regulatory Approval'
                           when column_name='actual_closeout' then 'Closeout'
                           when column_name='site_cta' then 'CTA'
                           when column_name='actual_ready_siv' then 'Ready SIV'
                           when column_name='actual_draft_cta' then 'Draft CTA'
                           when column_name='actual_ready_qual' then 'Ready Qualified'
                           when column_name='actual_initiation' then 'Initiation'
                           when column_name='actual_srp' then 'SRP'
                           when column_name='actual_draft_srp' then 'Draft SRP'
                           when column_name='actual_irb_sub' then 'IRB Submission'
                           when column_name='actual_prequal' then 'Pre-Qualification'
                           when column_name='actual_reg_sub' then 'Regulatory Submission'
                           when column_name='actual_irb_approval' then 'IRB Approval'
                           when column_name='actual_selected' then 'Selected'
                           when column_name='actual_qual' then 'Qualification'
                           when column_name='site_cda' then 'CDA'
                           when column_name='actual_all_contracts' then 'All Contracts'
                           when column_name='actual_licence' then 'License'
                            end
                           ::text AS milestonelabel from information_schema."columns" 
                           where lower(table_name)='site_milestones'
and lower(table_schema)='tas120_203_ctms' 
and column_name not in ('__dq_flag','__generation_time','__hash','id_','comprehend_update_time','closeout_status',
'site_number','site_id','study_number','irb_type')
),              

    sitemilestone_data AS (
       
	 select studyid,
	 studyname,
	 siteid,
	-- row_number() over (partition by studyid,siteid order by expecteddate,milestonelabel) as milestoneseq,
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
                SELECT distinct 'TAS-120-203'::text AS studyid,
                        'TAS120_203'::text AS studyname,
                        concat('TAS120_203_',sm."site_number")::text AS siteid,
                        tms.milestoneseq::int AS milestoneseq,
                        tms.milestoneid::text AS milestoneid,
                        null::text AS milestonecat,
                        tms.start_standard_label::text AS milestonelabel_src,
                        ml.milestonelabel::text AS milestonelabel,
                        tms.start_standard_label::text AS milestonelabel_long,
                        'Actual'::text AS milestonetype,
                         case 
                           when column_name='actual_activation' then nullif("actual_activation",'')
                           when column_name='actual_product' then nullif("actual_product",'')
                           when column_name='actual_reg_approval' then nullif("actual_reg_approval",'')
                           when column_name='actual_closeout' then nullif("actual_closeout",'')
                           when column_name='site_cta' then nullif("site_cta",'')
                           when column_name='actual_ready_siv' then nullif("actual_ready_siv",'')
                           when column_name='actual_draft_cta' then nullif("actual_draft_cta",'')
                           when column_name='actual_ready_qual' then nullif("actual_ready_qual",'')
                           when column_name='actual_initiation' then nullif("actual_initiation",'')
                           when column_name='actual_srp' then nullif("actual_srp",'')
                           when column_name='actual_draft_srp' then nullif("actual_draft_srp",'')
                           when column_name='actual_irb_sub' then nullif("actual_irb_sub",'')
                           when column_name='actual_prequal' then nullif("actual_prequal",'')
                           when column_name='actual_reg_sub' then nullif("actual_reg_sub",'')
                           when column_name='actual_irb_approval' then nullif("actual_irb_approval",'')
                           when column_name='actual_selected' then nullif("actual_selected",'')
                           when column_name='actual_qual' then nullif("actual_qual",'')
                           when column_name='site_cda' then nullif("site_cda",'')
                           when column_name='actual_all_contracts' then nullif("actual_all_contracts",'')
                           when column_name='actual_licence' then nullif("actual_licence",'')
                           end::date AS expecteddate,
                        'Yes'::boolean AS ismandatory,
                        tms.milestonebucketid::text AS milestonebucketid,
                        tms.milestonebucketname::text AS milestonebucketname,
                        tms.ismandatory::boolean AS iskeymilestone,
                        tms.endmilestonecycletime_original::text AS endmilestonecycletime,
                        tms.ismandatory::boolean AS startkeymilestone
                        from tas120_203_ctms.site_milestones sm
                        join milestonelabel ml ON(ml.studyid='TAS120_203')
                        join internal_config.taiho_ms_standards tms
                        on upper(tms.start_original_label) = upper(ml.milestonelabel)
                        where tms.milestonelevel = 'Site'
                        )a 
                        
)
SELECT 
        /*KEY (sm.studyid || '~' || sm.siteid)::text AS comprehendid, KEY*/
        sm.studyid::text AS studyid,
        sm.studyname::text AS studyname,
        sm.siteid::text AS siteid,
        sm.milestoneseq::int AS milestoneseq,
        sm.milestoneid::text AS milestoneid,
        sm.milestonecat::text AS milestonecat,
        sm.milestonelabel_src::text AS milestonelabel_src,
        sm.milestonelabel::text AS milestonelabel,
        sm.milestonelabel_long::text AS milestonelabel_long,
        sm.milestonetype::text AS milestonetype,
        sm.expecteddate::date AS expecteddate,
        sm.ismandatory::boolean AS ismandatory,
        sm.milestonebucketid::text AS milestonebucketid,
        sm.milestonebucketname::text AS milestonebucketname,
        sm.iskeymilestone::text AS iskeymilestone,
        sm.endmilestonecycletime::text AS endmilestonecycletime,
        sm.startkeymilestone::text AS startkeymilestone
        /*KEY , (sm.studyid || '~' || sm.siteid || '~' || sm.milestonetype || '~' || sm.milestoneseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM sitemilestone_data sm
JOIN included_sites si ON (sm.studyid = si.studyid AND sm.siteid = si.siteid);













