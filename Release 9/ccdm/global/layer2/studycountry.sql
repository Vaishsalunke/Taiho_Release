/*
CCDM Studycountry mapping
Notes: Standard mapping to CCDM Studycountry table
*/

WITH included_studies AS (
                SELECT DISTINCT studyid FROM study ),

     studycountry_data AS (
                SELECT  null::text AS studyid,
                        null::text AS country_src,
                        null::text AS countrystatus_src,
                        null::text AS countrystatus,
                        null::text AS countrycode,
                        null::text AS countrycode3_iso,
                        null::text AS countrycode2_iso,
                        null::text AS countryname_iso,
                        null::text AS countryactivationdate,
                        null::text AS countrydeactivationdate,
                        null::text AS countrystatusdate,
                        null::text AS croid)

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
JOIN included_studies s ON (s.studyid = sc.studyid)
WHERE 1=2; 

