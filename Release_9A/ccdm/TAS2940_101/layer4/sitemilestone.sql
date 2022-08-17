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
	 milestoneseq,
	 replace( replace(milestonelabel,'SITE ACTIVATED','Site Activated'),'PRE-STUDY VISIT','Site Selected') as milestonelabel,
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
	 endmilestonecycletime
	 
	 from
	 (
      
                select distinct 
                'TAS2940-101'::text AS studyid,
                        'TAS2940_101'::text AS studyname,
                        concat('TAS2940_101_',"site_number")::text AS siteid,
                        tms.milestoneseq ::int AS milestoneseq,
                        tms.milestoneid ::text AS milestoneid,                       
                        tms.start_original_label ::text AS milestonelabel_src,
                        replace (tms.start_standard_label,'FIRST SUBJECT FIRST TREATMENT','FIRST SUBJECT IN') ::text AS milestonelabel,
                        tms.start_standard_label ::text AS milestonelabel_long,
                        'Planned'::text AS milestonetype,
                        nullif(planned_date,'') ::date AS expecteddate,
                        'yes'::boolean AS ismandatory,
                        tms.milestonebucketid ::text AS milestonebucketid,
                        tms.milestonebucketname ::text AS milestonebucketname,
                        tms.ismandatory ::boolean AS iskeymilestone,
                        tms.endmilestonecycletime_standard ::text AS endmilestonecycletime,
                        tms.ismandatory ::boolean AS startkeymilestone
                        from tas2940_101_ctms.milestone_status_site mss 
                        left join 
                        internal_config.taiho_ms_standards tms
                        on upper(mss.event_desc)=upper(tms.start_original_label)
                        where tms.milestonelevel = 'Site'
                        
                        union all 
                        SELECT distinct 
                        'TAS2940-101'::text AS studyid,
                        'TAS2940_101'::text AS studyname,
                        concat('TAS2940_101_',"site_number")::text AS siteid,
                        tms.milestoneseq ::int AS milestoneseq,
                        tms.milestoneid ::text AS milestoneid,                      
                        tms.start_original_label ::text AS milestonelabel_src,
                        replace (tms.start_standard_label,'FIRST SUBJECT FIRST TREATMENT','FIRST SUBJECT IN') ::text AS milestonelabel,
                        tms.start_standard_label ::text AS milestonelabel_long,
                        'Actual'::text AS milestonetype,
                        case when event_desc='1st Subject 1st Treatment' then gf.Exosdat else nullif(expected_date,'') ::date end AS expecteddate,
                        'yes'::boolean AS ismandatory,
                        tms.milestonebucketid ::text AS milestonebucketid,
                        tms.milestonebucketname ::text AS milestonebucketname,
                        tms.ismandatory ::boolean AS iskeymilestone,
                        tms.endmilestonecycletime_standard ::text AS endmilestonecycletime,
                        tms.ismandatory ::boolean AS startkeymilestone
                        from tas2940_101_ctms.milestone_status_site mss 
                        left join 
                        internal_config.taiho_ms_standards tms
                        on upper(mss.event_desc)=upper(tms.start_original_label)
                        left join(  select min(Exosdat) as Exosdat,"SiteNumber" 
									from(
											select 
											"EXOCYCSDT"::date as Exosdat,"SiteNumber" from tas2940_101."EXO" 
										)r group by "SiteNumber") gf
						on concat('TAS2940_101_',mss.site_number) = concat('TAS2940_101_',split_part(gf."SiteNumber",'_',2))
						where tms.milestonelevel = 'Site'
                        )sm
                        )

SELECT 
        /*KEY (sm.studyid || '~' || sm.siteid)::text AS comprehendid, KEY*/
        sm.studyid::text AS studyid,
        sm.studyname::text AS studyname,
        sm.siteid::text AS siteid,
        sm.milestoneseq::int AS milestoneseq,
        sm.milestoneid::text AS milestoneid,        
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


