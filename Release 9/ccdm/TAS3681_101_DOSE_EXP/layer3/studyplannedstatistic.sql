/*
CCDM StudyPlannedStatistic mapping
Notes: Standard mapping to CCDM StudyPlannedStatistic table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),

     studyplannedstatistic_data AS (
                SELECT  'TAS3681_101_DOSE_EXP'::text AS studyid,
                        'SITE_ACTIVATION'::text AS statcat,
                        'Count'::text AS statsubcat,
                        count(visit_status)::int AS statval,
                        'Count'::text AS statunit
					from
						tas3681_101_ctms.site_visits
						where visit_status in ('Completed','Projected','Scheduled')
						)

SELECT 
        /*KEY sps.studyid::text AS comprehendid, KEY*/
        sps.studyid::text AS studyid,
        sps.statcat::text AS statcat,
        sps.statsubcat::text AS statsubcat,
        sps.statval::int AS statval,
        sps.statunit::text AS statunit
        /*KEY , (sps.studyid || '~' || sps.statCat)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM studyplannedstatistic_data sps
JOIN included_studies st ON (st.studyid = sps.studyid);

