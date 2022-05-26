/*
CCDM Site mapping
Notes: Standard mapping to CCDM Site table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),
               
  sitecountrycode_data AS (
                SELECT studyid, countryname_iso, countrycode3_iso FROM studycountry),  
               
 mss_sitestatusdate as ( select site_number, site_status, max(actual_date) as sitestatusdate
                         from tas2940_101_ctms.milestone_status_site
                         group by 1,2  ),
 
creationdate as (select site_number, case when ms.event_desc = 'Pre-Study Visit' then ms.actual_date
                        end ::date AS creationdate from tas2940_101_ctms.milestone_status_site ms where event_desc ='Pre-Study Visit'order by site_number ASC),
activationdate as( select site_number, case when ms.event_desc = 'Site Ready to Enroll' then nullif(ms.actual_date,'')
                        end::date AS activationdate from tas2940_101_ctms.milestone_status_site ms where event_desc ='Site Ready to Enroll' order by site_number ASC),
deactivationdate as( select site_number, case when ms.event_desc = 'Site Closed' then nullif(ms.actual_date,'')
                        end::date AS deactivationdate from tas2940_101_ctms.milestone_status_site ms where event_desc ='Site Closed'order by site_number ASC),
    site_data AS (
                select distinct b.*, sr.siteregion::text AS siteregion from (
                select a.*,
                cc.countrycode3_iso::text AS sitecountrycode from (  
                   
                SELECT  distinct 'TAS2940_101'::text AS studyid,
                        'TAS2940_101'::text AS studyname,
                        'TAS2940_101_' || split_part("name",'_',1)::text AS siteid,
                        split_part("name",'_',2)::text AS sitename,
                        'UBC'::text AS croid,
                        'UBC'::text AS sitecro,
                        case when "name" like '%201_Gustave Roussy_201%' then 'France'
                        else 'United States' end::text AS sitecountry,
                        cd.creationdate::date AS sitecreationdate,
                      	ad.activationdate::date AS siteactivationdate,
                        dd.deactivationdate::date AS sitedeactivationdate,
                        TRUE::text as statusapplicable,                        
                        null::text AS siteinvestigatorname,
                        null::text AS sitecraname,
                        null::text AS siteaddress1,
                        null::text AS siteaddress2,
                        null::text AS sitecity,
                        null::text AS sitestate,
                        null::text AS sitepostal,
                         mss.site_status::text AS sitestatus
                        ,mss.sitestatusdate::date AS sitestatusdate  
                        from TAS2940_101.__sites s
                        left join tas2940_101_ctms.milestone_status_site ms
                        on split_part(s."name",'_',1) = ms.site_number
                        left join mss_sitestatusdate mss
                         on split_part(s."name",'_',1) = mss.site_number
                        left join creationdate cd
                        	on split_part(s."name",'_',1)=cd.site_number
                        left join activationdate ad
                        	on split_part(s."name",'_',1)=ad.site_number
                        left join deactivationdate dd
                        	on split_part(s."name",'_',1)=dd.site_number
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




