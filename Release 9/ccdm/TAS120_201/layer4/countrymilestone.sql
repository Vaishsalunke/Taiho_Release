/*
CCDM CountryMilestone mapping
Notes: Standard mapping to CCDM CountryMilestone table
Milestone Bucket Id: startup, conduct, closeout
*/

WITH included_studies AS (
        SELECT studyid from Study),
        
 site_region_config AS
        (
          SELECT UPPER(alpha_3_code)::text AS alpha_3_code,
                    UPPER(country_name)::text AS country_name,
                    siteregion::text AS siteregion
          FROM internal_config.site_region_config
        ),
        
taiho_ms_standards AS (
                       select * 
                       from internal_config.taiho_ms_standards 
                       where milestonelevel='Country'
                     ),
    countrymilestone_data AS (
    select  studyid,
            studyname,
            sitecountry,
            sitecountrycode,
            --row_number() over (partition by studyid,sitecountry order by  expecteddate, milestonelabel) as milestoneseq,
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
			
            from (
        SELECT  Distinct
                'TAS120_201'::text AS studyid,
                'TAS120_201'::text AS studyname,
                src.country_name::text AS sitecountry,
                "country"::text AS sitecountrycode,
                --null:: int AS milestoneseq,
                tms.milestoneseq::int AS milestoneseq,
                tms.start_standard_label::text AS milestonelabel,
                'Planned'::text AS milestonetype,
                 nullif(planned_date,'')::date AS expecteddate,
                'Yes'::boolean AS ismandatory,
                 tms.milestonebucketid::text AS milestonebucketid,
                  tms.milestonebucketname::text AS milestonebucketname,
                  tms.ismandatory::boolean AS iskeymilestone,
                tms.ismandatory::boolean AS startkeymilestone,
                tms.milestoneid::text AS milestoneid,
                null::text AS milestonecat,
                tms.start_original_label::text AS milestonelabel_src,
                tms.start_standard_label::text AS milestonelabel_long,
                tms.endmilestonecycletime_standard::text as endmilestonecycletime  
 from TAS120_201_ctms.milestone_status_country
   left join site_region_config src on upper(alpha_3_code) = upper(country)
   left join taiho_ms_standards tms on trim(upper(event_desc)) = trim(upper(start_original_label))
   UNION
   SELECT  Distinct
                'TAS120_201'::text AS studyid,
                'TAS120_201'::text AS studyname,
                src.country_name::text AS sitecountry,
                "country"::text AS sitecountrycode,
                --null:: int AS milestoneseq,
                tms.milestoneseq::int AS milestoneseq,
                tms.start_standard_label::text AS milestonelabel,
                'Actual'::text AS milestonetype,
                 nullif(actual_date,'')::date AS expecteddate,
                'Yes'::boolean AS ismandatory,
                 tms.milestonebucketid::text AS milestonebucketid,
                  tms.milestonebucketname::text AS milestonebucketname,
                  tms.ismandatory::boolean AS iskeymilestone,
                tms.ismandatory::boolean AS startkeymilestone,
                tms.milestoneid::text AS milestoneid,
                null::text AS milestonecat,
                tms.start_original_label::text AS milestonelabel_src,
                tms.start_standard_label::text AS milestonelabel_long,
                tms.endmilestonecycletime_standard::text as endmilestonecycletime  
 from TAS120_201_ctms.milestone_status_country
   left join site_region_config src on upper(alpha_3_code) = upper(country)
   left join taiho_ms_standards tms on trim(upper(event_desc)) = trim(upper(start_original_label))
   )a 
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
    cm.iskeymilestone::text AS iskeymilestone,
    cm.endmilestonecycletime::text AS endmilestonecycletime,
    cm.startkeymilestone::text AS startkeymilestone
    /*KEY , (cm.studyid || '~' || cm.sitecountry || '~' || cm.milestonetype || '~' || cm.milestoneseq)::text  AS objectuniquekey KEY*/
    /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM countrymilestone_data cm JOIN included_studies ON
(cm.studyid = included_studies.studyid);


