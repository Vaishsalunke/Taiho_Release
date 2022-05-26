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
	 milestonecat,
	 endmilestonecycletime
	 from
	 (
	 SELECT  		distinct	'TAS0612_101'::text AS studyid,
						'TAS0612_101'::text AS studyname,
                        concat('TAS0612_101_',sm."site_number")::text AS siteid,
                        tms.milestoneseq::int AS milestoneseq,
                        replace (tms.start_standard_label,'FIRST SUBJECT FIRST TREATMENT','FIRST SUBJECT IN')::text AS milestonelabel,
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
					from tas0612_101_ctms.milestone_status_site sm
					left join internal_config.taiho_ms_standards tms on upper(start_original_label) = upper(event_desc)
					where tms.milestonelevel = 'Site'
	UNION ALL
	 select distinct	'TAS0612_101'::text AS studyid,
						'TAS0612_101'::text AS studyname,
                        concat('TAS0612_101_',sm."site_number")::text AS siteid,
                        tms.milestoneseq::int AS milestoneseq,
                        replace (tms.start_standard_label,'FIRST SUBJECT FIRST TREATMENT','FIRST SUBJECT IN')::text AS milestonelabel,
                        'Actual'::text AS milestonetype,
                        --nullif(sm."actual_date",'')::date AS expecteddate,
                        case when event_desc='1st Subject 1st Treatment' then x.Exosdat else nullif(expected_date,'')::date end::date AS expecteddate,
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
					from tas0612_101_ctms.milestone_status_site sm
					left join internal_config.taiho_ms_standards tms on upper(start_original_label) = upper(event_desc)
					left join(  select min("EXOSTDAT")::date as Exosdat,split_part("SiteNumber",'_',2) as site from tas0612_101."EXO"
 					group by "SiteNumber")x
 					on sm.site_number = x.site
					where tms.milestonelevel = 'Site'
	
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



