/*
CCDM Summary of Expenses on a Site Table mapping
Notes: Standard mapping to CCDM Summary of Expenses on a Site Table
*/

WITH included_sites AS (
                 SELECT DISTINCT studyid, siteid FROM site ),

     siteexpendituresummary AS (
            SELECT  null::text AS studyid,
                    null::text AS studyname,
                    null::text AS sitecountry,
                    null::text AS sitecountrycode,
                    null::text AS siteid,
                    null::integer AS subjects,
                    null::text AS event_category_id,
                    null::text AS event_category_name,
                    null::date AS cost_incurred_date,
                    null::integer AS cost_incurred_month,
                    null::text AS cost_incurred_quarter,
                    null::integer AS cost_incurred_year,
                    null::text AS costcurrency,
                    null::numeric AS currencycatplannedexp, 
                    null::numeric AS currencycatactualexp, 
                    null::numeric AS currencycatforecastedexp, 
                    null::numeric AS usdcatplannedexp, 
                    null::numeric AS usdcatactualexp, 
                    null::numeric AS usdcatforecastedexp, 
                    null::text AS join_helper )


SELECT 
        /*KEY (se.studyid || '~' || se.siteid)::TEXT AS comprehendid, KEY*/
        se.studyid::text AS studyid,
        se.studyname::text AS studyname,
        se.sitecountry::text AS sitecountry,
        se.sitecountrycode::text AS sitecountrycode,
        se.siteid::text AS siteid,
        se.subjects::integer AS subjects,
        se.event_category_id::text AS event_category_id,
        se.event_category_name::text AS event_category_name,
        se.cost_incurred_date::date AS cost_incurred_date,
        extract (month from se.cost_incurred_date)::integer AS cost_incurred_month,
        case when cost_incurred_month <= 3 then 'Jan-Mar'
        when cost_incurred_month >3 and cost_incurred_month <=6 then 'Apr-Jun'
        when cost_incurred_month >6 and cost_incurred_month <=9 then 'July-Sep'
        when cost_incurred_month >9 and cost_incurred_month <=12 then 'Oct-Dec'
        end as cost_incurred_quarter,
        extract (year from se.cost_incurred_date)::text AS cost_incurred_year,
        se.costcurrency::text AS costcurrency,
        se.currencycatplannedexp::numeric AS currencycatplannedexp,
        se.currencycatactualexp::numeric AS currencycatactualexp,
        se.currencycatforecastedexp::numeric AS currencycatforecastedexp,
        se.usdcatplannedexp::numeric AS usdcatplannedexp,
        se.usdcatactualexp::numeric AS usdcatactualexp,
        se.usdcatforecastedexp::numeric AS usdcatforecastedexp,
        (se.studyid || '~' || se.siteid || '~' || se.sitecountry || '~' || se.event_category_id || '~' || se.cost_incurred_date || '~' || se.costcurrency)::text AS join_helper
        /*KEY , (se.studyid || '~' || se.siteid || '~' || se.event_category_id || '~' || random())::TEXT AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM siteexpendituresummary se
JOIN included_sites si ON (si.studyid = se.studyid AND si.siteid = se.siteid)
WHERE 1=2;
