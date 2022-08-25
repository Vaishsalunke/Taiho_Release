/*
CCDM CountryMilestone mapping
Notes: Standard mapping to CCDM CountryMilestone table
Milestone Bucket Id: startup, conduct, closeout
*/

with included_studies AS (
        SELECT studyid from Study),
        
 
        

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
            endmilestonecycletime
			
            from (
        SELECT  Distinct
                'TAS-120-203'::text AS studyid,
                'TAS120_203'::text AS studyname,
                src.country_name::text AS sitecountry,
                "country"::text AS sitecountrycode,
                tms.milestoneseq::int AS milestoneseq,
                tms.start_standard_label::text AS milestonelabel,  
                'Planned'::text AS milestonetype,
                 nullif(evt_dt_planned,'')::date AS expecteddate,
                'Yes'::boolean AS ismandatory,
                 tms.milestonebucketid::text AS milestonebucketid,
                  tms.milestonebucketname::text AS milestonebucketname,
              case 
                  when 
                  milestonebucketid='startup' then 'True'
                  else 'false'
                  end ::boolean AS iskeymilestone,
                tms.ismandatory::boolean AS startkeymilestone,
                tms.milestoneid::text AS milestoneid,
                tms.start_original_label::text AS milestonelabel_src,  
                tms.start_standard_label::text AS milestonelabel_long, 
                tms.endmilestonecycletime_standard::text as endmilestonecycletime  
 from tas120_203_ctms.country_milestones
   left join internal_config.site_region_config src on upper(alpha_3_code) = upper(country)
   left join internal_config.taiho_ms_standards tms on trim(upper(evt_desc)) = trim(upper(start_original_label))
   where tms.milestonelevel ='Country'
   UNION
   SELECT  Distinct
                'TAS-120-203'::text AS studyid,
                'TAS120_203'::text AS studyname,
                src.country_name::text AS sitecountry,
                "country"::text AS sitecountrycode,
                tms.milestoneseq::int AS milestoneseq,
                tms.start_standard_label::text AS milestonelabel,
                'Actual'::text AS milestonetype,
                 nullif(evt_date_actual,'')::date AS expecteddate,
                'Yes'::boolean AS ismandatory,
                 tms.milestonebucketid::text AS milestonebucketid,
                  tms.milestonebucketname::text AS milestonebucketname,
                 case 
                  when 
                  milestonebucketid='startup' then 'True'
                  else 'false'
                  end ::boolean AS iskeymilestone,
                tms.ismandatory::boolean AS startkeymilestone,
                tms.milestoneid::text AS milestoneid,
                tms.start_original_label::text AS milestonelabel_src,
                tms.start_standard_label::text AS milestonelabel_long,
                tms.endmilestonecycletime_standard::text as endmilestonecycletime  
 from tas120_203_ctms.country_milestones
   left join internal_config.site_region_config src on upper(alpha_3_code) = upper(country)
   left join internal_config.taiho_ms_standards tms on trim(upper(evt_desc)) = trim(upper(start_original_label))
   where tms.milestonelevel ='Country'
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









