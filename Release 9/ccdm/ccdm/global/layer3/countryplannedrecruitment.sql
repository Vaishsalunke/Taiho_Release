/*
CCDM countryplannedrecruitment mapping
Notes: Standard mapping to CCDM countryplannedrecruitment table
*/

WITH included_studies AS (
                SELECT distinct studyid FROM study ),

     countryplannedrecruitment_data AS (
                SELECT  null::text AS studyid,
                        null::text AS studyname,
                        null::text AS sitecountry,
                        null::text AS sitecountrycode,
                        null::text AS category,
                        null::text AS frequency,
                        null::date AS enddate,
                        null::text AS type,
                        null::NUMERIC AS recruitmentcount )

SELECT
        /*KEY (cpr.studyid || '~' || cpr.sitecountrycode)::text AS comprehendid, KEY*/
        cpr.studyid::text AS studyid,
        cpr.studyname::text AS studyname,
        cpr.sitecountry::text AS sitecountry,
        cpr.sitecountrycode::text AS sitecountrycode,
        cpr.category::text AS category,
        cpr.frequency::text AS frequency,
        cpr.enddate::date AS enddate,
        cpr.type::text AS type,
        cpr.recruitmentcount::NUMERIC AS recruitmentcount
        /*KEY ,(cpr.studyid || '~' || cpr.sitecountrycode || '~' || cpr.category || '~' || cpr.frequency || '~' || cpr.enddate || '~' || cpr.type)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM countryplannedrecruitment_data cpr
JOIN included_studies st ON (st.studyid = cpr.studyid)
WHERE 1=2; 
