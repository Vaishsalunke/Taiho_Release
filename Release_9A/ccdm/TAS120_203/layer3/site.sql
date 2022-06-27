/*
CCDM Site mapping
Notes: Standard mapping to CCDM Site table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),
                
sitecountrycode_data AS (
                SELECT studyid, countryname_iso, countrycode3_iso FROM studycountry ),

    site_data AS (
                select distinct b.*, 
                sr.siteregion::text AS siteregion
                --,sr.country_name::text AS sitecountry 
                from (
                select a.*, 
                cc.countrycode3_iso::text AS sitecountrycode from (
                SELECT  distinct 'TAS120_203'::text AS studyid,
                        null::text AS studyname,
                        'TAS120_203_' || split_part("name",'_',1)::text AS siteid,
                        --split_part("name",'_',2)::text AS sitename,
						case when tss.sitename_std is null then split_part(s."name",'_',2) else tss.sitename_std end::text AS sitename,
                        'PXL'::text AS croid,
                        'PXL'::text AS sitecro,
                         --cn.country::text AS sitecountry1,
						 case when tss.sitecountry = 'United States of America' then 'United States' else tss.sitecountry end ::text AS sitecountry,
                        TRUE::text as statusapplicable,
                        sm.actual_selected::date AS sitecreationdate,
                        nullif(sm.actual_activation,'')::date AS siteactivationdate,
                        nullif(sm.actual_closeout,'')::date AS sitedeactivationdate,
                        null::text AS siteinvestigatorname,
                        null::text AS sitecraname,
                        null::text AS siteaddress1,
                        null::text AS siteaddress2,
                        null::text AS sitecity,
                        null::text AS sitestate,
                        null::text AS sitepostal,
                        sm.closeout_status::text AS sitestatus,
                        case when sm.closeout_status = 'Recruiting' then sm.actual_activation
                             when sm.closeout_status = 'Start Up' then sm.actual_srp
                             when sm.closeout_status = 'Cancelled' then sm.actual_qual
                        end::date AS sitestatusdate 
                        from tas120_203.__sites s
                        left join tas120_203_ctms.site_milestones sm on 
                        split_part("name",'_',1) = sm.site_number
						left join tas120_203_ctms.center cn on 
						split_part("name",'_',1) = cn.site_number
						left join internal_config.taiho_sitename_standards tss
						on
                         'TAS120_203_' || split_part(s."name",'_',1) = tss.siteid
                        --and  ms."center_name" = tss.sitename
                        where tss.studyid = 'TAS120_203'
                        )a 
                		left join sitecountrycode_data cc
                		on a.studyid = cc.studyid
                		AND LOWER(a.sitecountry)=LOWER(cc.countryname_iso)
                		)b 
                		left join internal_config.site_region_config sr
				        on b.sitecountrycode = sr.alpha_3_code
                		)
                		                		
                		
SELECT 
        /*KEY (s.studyid || '~' || s.siteid)::text AS comprehendid, KEY*/
        s.studyid::text AS studyid,
        s.studyname::text AS studyname,
        s.siteid::text AS siteid,
        s.sitename::text AS sitename,
        s.croid::text AS croid,
        s.sitecro::text AS sitecro,
        case when s.sitecountry='United States' then 'United States of America'
        else s.sitecountry
        end::text AS sitecountry,
        --s.sitecountry::text AS sitecountry,
        s.sitecountrycode::text AS sitecountrycode,
        s.siteregion::text AS siteregion,
        s.sitecreationdate::date AS sitecreationdate,
        s.siteactivationdate::date AS siteactivationdate,
        s.sitedeactivationdate::date AS sitedeactivationdate,
        s.siteinvestigatorname::text AS siteinvestigatorname,
        s.sitecraname::text AS sitecraname,
        s.siteaddress1::text AS siteaddress1,
        s.siteaddress2::text AS siteaddress2,
        s.sitecity::text AS sitecity,
        s.sitestate::text AS sitestate,
        s.sitepostal::text AS sitepostal,
        s.sitestatus::text AS sitestatus,
        s.sitestatusdate::date AS sitestatusdate,
		s.statusapplicable::BOOLEAN as statusapplicable
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM site_data s 
JOIN included_studies st ON (s.studyid = st.studyid);




