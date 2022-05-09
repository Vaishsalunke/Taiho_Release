/*
CCDM Expense Details per Site Table mapping
Notes: Standard mapping to CCDM Expense Details per Site Table
*/

WITH included_sites AS (
                 SELECT DISTINCT studyid, siteid FROM site ),

     siteexpendituredata AS (
            SELECT  null::text AS studyid,
                    null::text AS studyname,
                    null::text AS sitecountry,
                    null::text AS sitecountrycode,
                    null::text AS siteid,
                    null::text AS usubjid,
                    null::text AS event_category_id,
                    null::text AS event_category_name,
                    null::text AS event_subcategory_id,
                    null::text AS event_subcategory_name,
                    null::date AS cost_incurred_date,
                    null::integer AS cost_incurred_month,
                    null::integer AS cost_incurred_year,
                    null::text AS costcurrency,
                    null::numeric AS currencybudgetamnt,
                    null::numeric AS currencyactualamnt,
                    null::numeric AS currencyforecastedamnt,
                    null::numeric AS usdbudgetamnt,
                    null::numeric AS usdactualamnt,
                    null::numeric AS usdforecastedamnt,
                    null::text AS payeename,
                    null::text AS payeestatus,
                    null::date AS paymentdate,
                    null::integer AS paymentmonth,
                    null::integer AS paymentyear,
                    null::numeric AS currencypaidamnt,
                    null::numeric AS usdpaidamnt,
                    null::text AS join_helper )
SELECT 
        /*KEY (sd.studyid || '~' || sd.siteid)::TEXT AS comprehendid, KEY*/
        sd.studyid::text AS studyid,
        sd.studyname::text AS studyname,
        sd.sitecountry::text AS sitecountry,
        sd.sitecountrycode::text AS sitecountrycode,
        sd.siteid::text AS siteid,
        sd.usubjid::text AS usubjid,
        sd.event_category_id::text AS event_category_id,
        sd.event_category_name::text AS event_category_name,
        sd.event_subcategory_id::text AS event_subcategory_id,
        sd.event_subcategory_name::text AS event_subcategory_name,
        sd.cost_incurred_date::date AS cost_incurred_date,
        extract (month from sd.cost_incurred_date)::integer AS cost_incurred_month,
        extract (year from sd.cost_incurred_date)::integer AS cost_incurred_year,
        sd.costcurrency::text AS costcurrency,
        sd.currencybudgetamnt::numeric AS currencybudgetamnt,
        sd.currencyactualamnt::numeric AS currencyactualamnt,
        sd.currencyforecastedamnt::numeric AS currencyforecastedamnt,
        sd.usdbudgetamnt::numeric AS usdbudgetamnt,
        sd.usdactualamnt::numeric AS usdactualamnt,
        sd.usdforecastedamnt::numeric AS usdforecastedamnt,
        sd.payeename::text AS payeename,
        sd.payeestatus::text AS payeestatus,
        sd.paymentdate::date AS paymentdate,
        extract (month from sd.paymentdate)::integer AS paymentmonth,
        extract (year from sd.paymentdate)::integer AS paymentyear,
        sd.currencypaidamnt::numeric AS currencypaidamnt,
        sd.usdpaidamnt::numeric AS usdpaidamnt,
        (sd.studyid || '~' || sd.siteid || '~' || sd.sitecountry || '~' || sd.event_category_id || '~' || sd.cost_incurred_date || '~' || sd.costcurrency)::text AS join_helper
        /*KEY , (sd.studyid || '~' || sd.siteid || '~' || sd.event_subcategory_id || '~' || random())::TEXT AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM siteexpendituredata sd
JOIN included_sites si ON (si.studyid = sd.studyid AND si.siteid = sd.siteid)
WHERE 1=2;
