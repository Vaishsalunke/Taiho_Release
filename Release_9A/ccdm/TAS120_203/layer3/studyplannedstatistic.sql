/*
CCDM StudyPlannedStatistic mapping
Notes: Standard mapping to CCDM StudyPlannedStatistic table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),

     studyplannedstatistic_data AS (
                SELECT  'TAS-120-203'::text AS studyid,
                        'SITE_ACTIVATION'::text AS statcat,
                        'COUNT'::text AS statsubcat,
                        count("actual_activation")::NUMERIC AS statval,
                        'COUNT'::text AS statunit
						from tas120_203_ctms.site_milestones 
						where closeout_status = 'Recruiting')

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



