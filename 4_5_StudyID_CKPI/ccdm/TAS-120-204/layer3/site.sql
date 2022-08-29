/*
CCDM Site mapping
Notes: Standard mapping to CCDM Site table
*/
WITH included_studies AS (
                SELECT studyid FROM study ),
                
sitecountrycode_data AS (
                SELECT studyid, countryname_iso, countrycode3_iso FROM studycountry),

    site_data AS (
                 select distinct  b.*, sr.siteregion::text AS siteregion from (
                select a.*, 
                cc.countrycode3_iso::text AS sitecountrycode from (
                 SELECT  distinct 'TAS-120-204'::text AS studyid,
                        'TAS120_204'::text AS studyname,
                        'TAS120_204_' || split_part("name",'_',1)::text AS siteid,
                        --split_part("name",'_',2)::text AS sitename,
                        case when tss.sitename_std is null then split_part(s."name",'_',2) else tss.sitename_std end::text AS sitename,
                        'Syneos'::text AS croid,
                        'Syneos'::text AS sitecro,
                        --'United States'::text AS sitecountry, 
                        case when tss.sitecountry = 'United States of America' then 'United States' else tss.sitecountry end ::text AS sitecountry,                    
                        TRUE::text as statusapplicable,
                        sm.site_selected_date::date AS sitecreationdate,
                        sm.site_activated_date::date AS siteactivationdate,
                        coalesce (nullif(sm.site_closed_date,''), nullif(sm.site_terminated_date,''))::date AS sitedeactivationdate,
                        null::text AS siteinvestigatorname,
                        null::text AS sitecraname,
                        null::text AS siteaddress1,
                        null::text AS siteaddress2,
                        null::text AS sitecity,
                        null::text AS sitestate,
                        null::text AS sitepostal,
                        sm.site_status::text AS sitestatus,
						case when lower(site_status)='activated' then sm.site_activated_date
                        	 when lower(site_status)='selected' then sm.site_selected_date
                        end::date AS sitestatusdate
                        from tas120_204.__sites s
                        left join tas120_204_ctms.sites sm
						on split_part("name",'_',1) = split_part(sm."site_number",'_',2)
						left join internal_config.taiho_sitename_standards tss
						on
                         'TAS120_204_' || split_part(s."name",'_',1) = tss.siteid
                        --and  ms."center_name" = tss.sitename
                        --where tss.studyid = 'TAS120_204'
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
        s.statusapplicable::boolean as statusapplicable
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM site_data s 
JOIN included_studies st ON (s.studyid = st.studyid);



