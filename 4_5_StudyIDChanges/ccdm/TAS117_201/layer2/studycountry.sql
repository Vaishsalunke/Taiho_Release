/*
CCDM Studycountry mapping
Notes: Standard mapping to CCDM Studycountry table
*/

WITH included_studies AS (
                SELECT DISTINCT studyid FROM study ),

     studycountry_data AS (
                SELECT distinct 'TAS117-201'::text AS studyid,				 
						 case 
							when length(trim(SUBSTRING( "name",1, POSITION('_' in "name")-1)))=3
								THEN CASE when left(SUBSTRING( "name",1, POSITION('_' in "name")-1),1)='0' then 'US'
										  when SUBSTRING( "name",1, POSITION('_' in "name")-1) between '100' and '149' then 'United Kingdom'
										  when SUBSTRING( "name",1, POSITION('_' in "name")-1) between '150' and '199' then 'Spain'
										  when SUBSTRING( "name",1, POSITION('_' in "name")-1) between '200' and '249' then 'Portugal'
										  when SUBSTRING( "name",1, POSITION('_' in "name")-1) between '250' and '299' then 'France'
										  when SUBSTRING( "name",1, POSITION('_' in "name")-1) between '300' and '349' then 'Italy'
										  when SUBSTRING( "name",1, POSITION('_' in "name")-1) between '350' and '399' then 'Canada'
									 end
						else 'United States of America' end::text AS country_src,
                        ''::text AS countrystatus_src,
                        ''::text AS countrystatus,
                        alpha_3_code::text AS countrycode,
                        alpha_3_code::text AS countrycode3_iso,
                        alpha_2_code::text AS countrycode2_iso,
                        s.country_name::text AS countryname_iso,
                        null::text AS countryactivationdate,
                        null::text AS countrydeactivationdate,
                        null::text AS countrystatusdate,
                        'Syneos'::text AS croid
from internal_config.site_region_config s
left join tas117_201."__sites" c
on (case 
							when length(trim(SUBSTRING( "name",1, POSITION('_' in "name")-1)))=3
								THEN CASE when left(SUBSTRING( "name",1, POSITION('_' in "name")-1),1)='0' then 'US'
										  when SUBSTRING( "name",1, POSITION('_' in "name")-1) between '100' and '149' then 'United Kingdom'
										  when SUBSTRING( "name",1, POSITION('_' in "name")-1) between '150' and '199' then 'Spain'
										  when SUBSTRING( "name",1, POSITION('_' in "name")-1) between '200' and '249' then 'Portugal'
										  when SUBSTRING( "name",1, POSITION('_' in "name")-1) between '250' and '299' then 'France'
										  when SUBSTRING( "name",1, POSITION('_' in "name")-1) between '300' and '349' then 'Italy'
										  when SUBSTRING( "name",1, POSITION('_' in "name")-1) between '350' and '399' then 'Canada'
									 end
						else 'United States' end) = s.country_name)

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
