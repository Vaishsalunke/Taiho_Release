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

   site_data AS (

                SELECT distinct 'TAS120_202'::text AS studyid,
                        'TAS120_202_'||"site_#"::text AS siteid,
                        case when tss.sitename_std is null then psc."account_name"
						else tss.sitename_std end::text AS sitename,
                        'IQVIA'::text AS croid,
                        'IQVIA'::text AS sitecro,
						"pi_name"::text AS siteinvestigatorname,
						null::text AS sitecraname,
                        case 
							when length("site_#") = 3
								 THEN CASE 
										   when left("site_#",1)='1' then 'United States'
										   when "site_#" between '200' and '249' then 'United Kingdom'
										   when "site_#" between '250' and '299' then 'Sweden'
										   when "site_#" between '300' and '349' then 'Spain'
										   when "site_#" between '350' and '399' then 'Netherlands'
										   when "site_#" between '400' and '449' then 'Italy'
										   when "site_#" between '450' and '499' then 'Germany'
										   when "site_#" between '500' and '549' then 'France'
										   when "site_#" between '550' and '599' then 'Belgium'
										   when "site_#" between '600' and '649' then 'Turkey'
										   when "site_#" between '650' and '699' then 'Singapore'
										   when "site_#" between '700' and '749' then 'South Korea'
										   when "site_#" between '750' and '799' then 'Hong Kong'
										   when "site_#" between '800' and '849' then 'Japan'
										   when "site_#" between '900' and '949' then 'Portugal'
										   else 'United States'
										 end
						else 'United States' end::text AS sitecountry,
						
                        case 
							when length("site_#") = 3
								 THEN CASE 
										   when left("site_#",1)='1' then 'North America'			 
										   when "site_#" between '200' and '249' then 'Europe'
										   when "site_#" between '250' and '299' then 'Europe'
										   when "site_#" between '300' and '349' then 'Europe'
										   when "site_#" between '350' and '399' then 'Europe'
										   when "site_#" between '400' and '449' then 'Europe'
										   when "site_#" between '450' and '499' then 'Europe'
										   when "site_#" between '500' and '549' then 'Europe'
										   when "site_#" between '550' and '599' then 'Europe'
										   when "site_#" between '600' and '649' then 'Middle East, North Africa, and Greater Arabia'
										   when "site_#" between '650' and '699' then 'Asia'
										   when "site_#" between '700' and '749' then 'Asia'
										   when "site_#" between '750' and '799' then 'Asia'
										   when "site_#" between '800' and '849' then 'Asia'
										   when "site_#" between '900' and '949' then 'Europe'
								     else 'North America'
									 end
						else 'North America' end::text AS siteregion,
                        "created_date/time_stamp_for_the_project_site"::date AS sitecreationdate,
						--null::date AS siteplannedactivationdate,
                        "updated_date/_site_status_change"::date AS siteactivationdate,
                        case when lower(site_status) = 'closed' then "updated_date/_site_status_change"
                        else null end::date AS sitedeactivationdate,
                        "address"::text AS siteaddress1,
                        "address_line_2"::text AS siteaddress2,
                        "city"::text AS sitecity,
                        "state/province"::text AS sitestate,
                        "postal_code"::text AS sitepostal,
                        case when "site_status" in ('Enrollment Open','Initiated','Ready for Initiation') then 'Initiating'
				when "site_status" = 'Dropped' then 'Cancelled' else INITCAP("site_status") end::text AS sitestatus,
                        "updated_date/_site_status_change"::date AS sitestatusdate,
                        true::boolean AS statusapplicable    
				     FROM tas120_202_ctms."project_site_contact" psc left join internal_config.taiho_sitename_standards tss
						on 
						 'TAS120_202_'||"site_#" = tss.siteid
						and  psc."account_name" = tss.sitename
						where tss.studyid = 'TAS120_202'
				/*LIMIT LIMIT 100 LIMIT*/)

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

