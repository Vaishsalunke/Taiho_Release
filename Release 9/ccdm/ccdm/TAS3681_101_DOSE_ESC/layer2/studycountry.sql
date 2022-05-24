/*
CCDM Studycountry mapping
Notes: Standard mapping to CCDM Studycountry table
*/

WITH included_studies AS (
                SELECT DISTINCT studyid FROM study ),
				

     studycountry_data AS (
                 SELECT distinct 'TAS3681_101_DOSE_ESC'::text AS studyid,				 
						coalesce(case when c.country_name = 'United States' then 'United States of America' else c.country_name end,'')::text AS country_src,
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
left join tas3681_101_ctms.sites c
on c.country_name = s.country_name
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

