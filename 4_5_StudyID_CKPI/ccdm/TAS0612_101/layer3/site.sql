/*
CCDM Site mapping
Client:taiho
Limits added for PR build efficiency (not applied for standard builds)
Notes: Standard mapping to CCDM Site table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),
    
    sitecountrycode_data AS (
                SELECT studyid, countryname_iso, countrycode3_iso FROM studycountry),
     latest_site as (
    SELECT distinct 'TAS0612-101'::text AS studyid,
           'TAS0612_101_'||"site_number"::text AS siteid,
                        case when tss.sitename_std is null then ms."center_name"
                        else tss.sitename_std end::text AS sitename,
                        'UBC'::text AS croid,
                        'UBC'::text AS sitecro,
                        --"investigator_name"::text AS siteinvestigatorname, different siteinvestigator for same site
                        null::text AS siteinvestigatorname,
                        null::text AS sitecraname,
                        case
                            when trim("country") = 'USA' then 'United States'
                            when trim("country") = 'FRA' then 'France'
                            when trim("country") = 'BEL' then 'Belgium'
                            when trim("country") = 'ITA' then 'Italy'
                            when trim("country") = 'GBR' then 'United Kingdom'
                            when trim("country") = 'NLD' then 'Netherland'
                            when trim("country") = 'ESP' then 'Spain'
                        else 'United States'
                        end::text AS sitecountry,
                        case
                            when trim("country") = 'USA' then 'North America'
                            else 'Europe'
                        end ::text AS siteregion,
                         nullif(sc."siv_plannned_date",'')::date AS sitecreationdate,
                        --nullif(sc."siv_plannned_date",'')::date AS siteplannedactivationdate,
                        sc."siv_date"::date AS siteactivationdate,
                        case when lower(sc.closeout_status) = 'site closeout' then sc.cov_visit_end_date
                        else null end::date AS sitedeactivationdate,
                        null::text AS siteaddress1,
                        null::text AS siteaddress2,
                        null::text AS sitecity,
                        null::text AS sitestate,
                        null::text AS sitepostal,
                        sc."siv_report_date"::date AS sitestatusdate,
                        true::boolean AS statusapplicable,
                        max(ms."id_") as id1
                        From TAS0612_101_ctms."milestone_status_site" ms
                      left join TAS0612_101_ctms."site_closeout" sc on
                        ms."site_number" = sc."site_id"
                        left join internal_config.taiho_sitename_standards tss
                        on
                         'TAS0612_101_'||"site_number" = tss.siteid
                        and  ms."center_name" = tss.sitename
                        where --tss.studyid = 'TAS0612_101' and
							length(ms."site_number") = 3
                        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
    ),
    site_data AS (
        select distinct replace(ls.studyid,'TAS0612_101','TAS0612-101') as studyid,
           ls.siteid,
                       ls.sitename,
                        ls.croid,
                        ls.sitecro,
                        ls.siteinvestigatorname,
                        ls.sitecraname,
                       ls.sitecountry,
                        ls.siteregion,
                        ls.sitecreationdate,
                         ls.siteactivationdate,
                        ls.sitedeactivationdate,
                        ls.siteaddress1,
                        ls.siteaddress2,
                        ls.sitecity,
                        ls.sitestate,
                        ls.sitepostal,
                        ls.sitestatusdate,
                        ls.statusapplicable,
                        ls.id1,
        replace(ms."site_status",'Site Ready to Enroll','Initiating')::text AS sitestatus
        from latest_site ls
        left join TAS0612_101_ctms."milestone_status_site" ms
        on ls.siteid = concat('TAS0612_101_',ms."site_number")
        and ls.id1 = ms."id_"
        )
            
SELECT
        /*KEY (s.studyid || '~' || s.siteid)::text AS comprehendid, KEY*/
        s.studyid::text AS studyid,
        null::text AS studyname,
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

