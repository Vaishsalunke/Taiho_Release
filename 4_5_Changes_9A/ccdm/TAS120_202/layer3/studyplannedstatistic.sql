/*
CCDM StudyPlannedStatistic mapping
Notes: Standard mapping to CCDM StudyPlannedStatistic table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),

     studyplannedstatistic_data AS (
                 SELECT  replace("protocol_#",'-','_')::text AS studyid,
                        'SITE_ACTIVATION'::text AS statcat,
                        'Count'::text AS statsubcat,
                        count(visit_status)::int AS statval,
                        'Count'::text AS statunit
                   from tas120_202_ctms.monvisit_tracker
						where visit_status = 'Planned'
						group by "protocol_#"
						)

SELECT 
        /*KEY sps.studyid::text AS comprehendid, KEY*/
        sps.studyid::text AS studyid,
        sps.statcat::text AS statcat,
        sps.statsubcat::text AS statsubcat,
        sps.statval::NUMERIC AS statval,
        sps.statunit::text AS statunit
        /*KEY , (sps.studyid || '~' || sps.statCat)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM studyplannedstatistic_data sps
JOIN included_studies st ON (st.studyid = sps.studyid);

