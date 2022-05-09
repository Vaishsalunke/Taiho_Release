/*
CCDM CountryMilestone mapping
Notes: Standard mapping to CCDM CountryMilestone table
Milestone Bucket Id: startup, conduct, closeout
*/

WITH included_studies AS (
        SELECT studyid from Study),
        
 
countrymilestone_data AS (
        select 	studyid,
			studyname,
			sitecountry,
			sitecountrycode,
			--row_number() over (partition by studyid,sitecountry order by  expecteddate ) as milestoneseq,
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
			endmilestonecycletime,
			milestonecat
			from (
 SELECT		distinct
                'TAS117_201'::text AS studyid,
                'TAS117_201'::text AS studyname,
                src.country_name::text AS sitecountry,
                src.alpha_3_code::text AS sitecountrycode,
				tms.milestoneseq::int AS milestoneseq,				
                tms.start_standard_label::text AS milestonelabel,
                'Planned'::text AS milestonetype,
                nullif(min(planned_date),'')::date AS expecteddate,
                'Yes'::boolean AS ismandatory,
                tms.milestonebucketid::text AS milestonebucketid,
                tms.milestonebucketname::text AS milestonebucketname,
                tms.ismandatory::boolean AS iskeymilestone,
                tms.ismandatory::text AS startkeymilestone,
                tms.milestoneid::text AS milestoneid,
                tms.start_original_label::text AS milestonelabel_src,
                tms.start_standard_label::text AS milestonelabel_long,
				tms.endmilestonecycletime_standard:: text as endmilestonecycletime,
                null::text AS milestonecat
               from tas117_201_ctms.country_milestones 
   left join internal_config.site_region_config src on 	upper(alpha_2_code) = upper(country_code)
   left join internal_config.taiho_ms_standards tms on upper(start_original_label) = upper(milestone_name)
   where nullif(actual_date,'') is not null and tms.milestonelevel ='Country'
   group by 1,2,3,4,5,6,7,9,10,11,12,13,14,15,16,17,18
 
   
   union 
   
    SELECT		distinct
                'TAS117_201'::text AS studyid,
                'TAS117_201'::text AS studyname,
                src.country_name::text AS sitecountry,
                src.alpha_3_code::text AS sitecountrycode,
				tms.milestoneseq::int AS milestoneseq,				
                tms.start_standard_label::text AS milestonelabel,
                'Actual'::text AS milestonetype,
                nullif(min(actual_date),'')::date AS expecteddate,
               'Yes'::boolean AS ismandatory,
                tms.milestonebucketid::text AS milestonebucketid,
                tms.milestonebucketname::text AS milestonebucketname,
                tms.ismandatory::boolean AS iskeymilestone,
                tms.ismandatory::text AS startkeymilestone,
                tms.milestoneid::text AS milestoneid,
                tms.start_original_label::text AS milestonelabel_src,
                tms.start_standard_label::text AS milestonelabel_long,
				tms.endmilestonecycletime_standard:: text as endmilestonecycletime,
                null::text AS milestonecat
               from tas117_201_ctms.country_milestones 
   left join internal_config.site_region_config src on 	upper(alpha_2_code) = upper(country_code)
   left join internal_config.taiho_ms_standards tms on upper(start_original_label) = upper(milestone_name)
   where nullif(actual_date,'') is not null and tms.milestonelevel ='Country'
    group by 1,2,3,4,5,6,7,9,10,11,12,13,14,15,16,17,18)a
)
SELECT
    /*KEY (cm.studyid || '~' || cm.sitecountry) ::text AS comprehendid, KEY*/
    cm.studyid::text AS studyid,
    cm.studyname::text AS studyname,
    cm.sitecountry::text AS sitecountry,
    cm.sitecountrycode::text AS sitecountrycode,
    cm.milestoneseq::int AS milestoneseq,
    cm.milestoneid::text AS milestoneid,
    cm.milestonecat::text AS milestonecat,
    cm.milestonelabel_src::text AS milestonelabel_src,
    cm.milestonelabel::text AS milestonelabel,
    cm.milestonelabel_long::text AS milestonelabel_long,
    cm.milestonetype::text AS milestonetype,
    cm.expecteddate::date AS expecteddate,
    cm.ismandatory::boolean AS ismandatory,
    cm.milestonebucketid::text AS milestonebucketid,
    cm.milestonebucketname::text AS milestonebucketname,
    cm.iskeymilestone::boolean AS iskeymilestone,
    cm.startkeymilestone::text AS startkeymilestone,
    cm.endmilestonecycletime:: text as endmilestonecycletime
     /*KEY , (cm.studyid || '~' || cm.sitecountry || '~' || cm.milestonetype || '~' || cm.milestoneseq)::text  AS objectuniquekey KEY*/
    /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM countrymilestone_data cm JOIN included_studies ON
(cm.studyid = included_studies.studyid);











