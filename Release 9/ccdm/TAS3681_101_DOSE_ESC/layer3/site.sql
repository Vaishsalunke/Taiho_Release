/*
CCDM Site mapping
Client:taiho
Limits added for PR build efficiency (not applied for standard builds)
Notes: Standard mapping to CCDM Site table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),

    site_data AS (
                SELECT distinct 'TAS3681_101_DOSE_ESC'::text AS studyid,
								null::text AS studyname,
                        "site_number"::text AS siteid,
                       case when tss.sitename_std is null then s1."facility_name"
                       else tss.sitename_std end::text AS sitename,
                        'Syneos'::text AS croid,
                        'Syneos'::text AS sitecro,
                        "principal_investigator"::text AS siteinvestigatorname,
                        null::text AS sitecraname,
                        case 
                        When trim(country_code) = 'KR' then 'South Korea'
                        When trim(country_code) = 'JP' then 'Japan'
                        When trim(country_code) = 'DE' then 'Germany'
                        When trim(country_code) = 'UK' then 'United Kingdom'
                        When trim(country_code) = 'ES' then 'Spain'
                        When trim(country_code) = 'TR' then 'Turkey'
                        When trim(country_code) = 'BE' then 'Belgium'
                        When trim(country_code) = 'SG' then 'Singapore'
                        When trim(country_code) = 'US' then 'United States'
                        When trim(country_code) = 'NL' then 'Netherlands'
                        When trim(country_code) = 'HK' then 'Hong Kong'
                        When trim(country_code) = 'SE' then 'Sweden'
                        When trim(country_code) = 'CA' then 'Canada'
                        When trim(country_code) = 'FR' then 'France'
                        When trim(country_code) = 'PT' then 'Portugal'
                        When trim(country_code) = 'IT' then 'Italy'
                        When trim(country_code) = 'GB' then 'United Kingdom'
                        else 'United States'
                        end::text AS sitecountry,
                        case
                            when length(trim(SUBSTRING( site_number,POSITION('_' in site_number)+1)))=3
                                 THEN CASE when substring(substring(site_number,POSITION('_' in site_number)+1),1,1)='1' then 'North America'
                                           when substring(substring(site_number,POSITION('_' in site_number)+1),1,1)='2' then 'Europe'
                                           when substring(substring(site_number,POSITION('_' in site_number)+1),1,1)='3' then 'Europe'
                                           when substring(substring(site_number,POSITION('_' in site_number)+1),1,1)='6' then 'Europe'
                                         else 'North America'
                                      end
                        else 'North America' end ::text AS siteregion,
                        nullif(site_selected_date,'')::date AS sitecreationdate,
                        --null::date AS siteplannedactivationdate,
                        case when site_status = 'Activated' then
                        nullif(site_activated_date,'')
                        end::date AS siteactivationdate,
                        nullif(site_closed_date,'')::date AS sitedeactivationdate,
                        address_line1::text AS siteaddress1,
                        address_line2::text AS siteaddress2,
                        city::text AS sitecity,
                        null::text AS sitestate,
                        postal_code::text AS sitepostal,
                        replace(replace(site_status,'Activated','Initiating'),'Dropped','Cancelled')::text AS sitestatus,
                        case when site_status = 'Activated' then nullif(site_activated_date,'')
                             when site_status = 'Selected' then nullif(site_selected_date,'')
                             when site_status in ('Recommended', 'Back-up') then coalesce (nullif(site_activated_date,''),nullif(site_selected_date,''))
                        end::date AS sitestatusdate,
                        true::boolean AS statusapplicable
            From tas3681_101_ctms.sites s1 left join internal_config.taiho_sitename_standards tss
                        on 
                         s1."site_number" = tss.siteid
                        and  s1."facility_name" = tss.sitename
                        where tss.studyid = 'TAS3681_101' and s1."site_number" in (Select distinct "SiteNumber" From    tas3681_101."IE"
        where "IERANDY" != 'Expansion')
            and length(country_code)<=2 and country_code <> ''--siteid TAS3681101_105 excluded due to invalid data in source
                /*LIMIT LIMIT 100 LIMIT*/),

 sitecountrycode_data AS (
                SELECT studyid, countryname_iso, countrycode3_iso FROM studycountry)

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
        cc.countrycode3_iso::text AS sitecountrycode,
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
        s.statusapplicable::boolean AS statusapplicable 
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM site_data s 
JOIN included_studies st ON (s.studyid = st.studyid)
LEFT JOIN sitecountrycode_data cc ON (s.studyid = cc.studyid AND LOWER(s.sitecountry)=LOWER(cc.countryname_iso));


