/*
CCDM Studycountry mapping
Notes: Standard mapping to CCDM Studycountry table
*/

WITH included_studies AS (
                SELECT DISTINCT studyid FROM study ),
				
	 site_country as (Select country,case
											when trim("country") = 'USA' then 'United States'
											when trim("country") = 'FRA' then 'France'
											when trim("country") = 'BEL' then 'Belgium'
											when trim("country") = 'ITA' then 'Italy'
											when trim("country") = 'GBR' then 'United Kingdom'
											when trim("country") = 'NLD' then 'Netherland'
											when trim("country") = 'ESP' then 'Spain'
											else 'United States'
									 end:: text as country_name 
					  from TAS0612_101_ctms."milestone_status_site" ),
	 
     studycountry_data AS (
                  SELECT distinct 'TAS0612-101'::text AS studyid,				
						coalesce(case when sc.country_name = 'United States' then 'United States of America' else s.country_name end,'')::text AS country_src,
                        ''::text AS countrystatus_src,
                        ''::text AS countrystatus,
                        alpha_3_code::text AS countrycode,
                        alpha_3_code::text AS countrycode3_iso,
                        alpha_2_code::text AS countrycode2_iso,
                        s.country_name::text AS countryname_iso,
                        null::text AS countryactivationdate,
                        null::text AS countrydeactivationdate,
                        null::text AS countrystatusdate,
                        'UBC'::text AS croid
from internal_config.site_region_config s
left join site_country sc
on trim(lower(s.country_name)) = trim(lower(sc.country_name))
)

SELECT 
        /*KEY (sc.studyid || '~' || sc.countryname_iso)::text AS comprehendid, KEY*/
        sc.studyid::text AS studyid,
        sc.country_src::text AS country_src,
        sc.countrystatus_src::text AS countrystatus_src,
        sc.countrystatus::text AS countrystatus,
        sc.countrycode::text AS countrycode,
        sc.countrycode3_iso::text AS countrycode3_iso,
        sc.countrycode2_iso::text AS countrycode2_iso,
        sc.countryname_iso::text AS countryname_iso,
        sc.countryactivationdate::text AS countryactivationdate,
        sc.countrydeactivationdate::text AS countrydeactivationdate,
        sc.countrystatusdate::text AS countrystatusdate,
        sc.croid::text AS croid
        /*KEY , (sc.studyid || '~' || sc.countrycode || '~' || sc.countrycode3_iso || '~' || sc.countrycode2_iso || '~' || sc.countryname_iso)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM studycountry_data sc
JOIN included_studies s ON (s.studyid = sc.studyid);




