/*
CCDM SitePlannedStatistic mapping
Notes: Standard mapping to CCDM SitePlannedStatistic table
*/

/*
CCDM SitePlannedStatistic mapping
Notes: Standard mapping to CCDM SitePlannedStatistic table
*/
WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site),

     siteplannedstatistic_data AS  (
										SELECT  'TAS120_204'::text AS studyid,
												concat('TAS120_204_',split_part(site_number,'_',2))::text AS siteid,
												'Site Activation'::text AS statcat,
												'Count'::text AS statsubcat,
												count(*)::NUMERIC AS statval,
												'Count'::text AS statunit 
							
										from tas120_204_ctms.site_visits 
										where visit_status in ('Projected','Complete','Scheduled')
										group by 1,2,3,4,6
                					)

SELECT 
        /*KEY (sps.studyid || '~' || sps.siteid)::text AS comprehendid, KEY*/
        sps.studyid::text AS studyid,
        sps.siteid::text AS siteid,
        sps.statcat::text AS statcat,
        sps.statsubcat::text AS statsubcat,
        sps.statval::NUMERIC AS statval,
        sps.statunit::text AS statunit
        /*KEY , (sps.studyid || '~' || sps.siteid || '~' || sps.statCat)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM siteplannedstatistic_data sps
JOIN included_sites si ON (sps.studyid = si.studyid AND sps.siteid = si.siteid);






