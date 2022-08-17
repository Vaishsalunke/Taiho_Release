/*
CDM StudyCro mapping
Notes: Standard mapping to CDM StudyCro table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),

    studycro_data AS (
                SELECT  'TAS-120-202'::text AS studyid,
						'TAS120_202'::text AS studyname,
                        'IQVIA'::text AS croid,
                        'IQVIA'::text AS croname,
                        null::text AS crodescription )

SELECT /*KEY sc.studyid::text AS comprehendid, KEY*/
        (sc.studyid || '~' || sc.croid)::text AS crokey,
        sc.studyid::text AS studyid,
        sc.studyname::text AS studyname,
        sc.croid::text AS croid,
        sc.croname::text AS croname,
        sc.crodescription::text AS crodescription
        /*KEY , (sc.studyid || '~' || sc.croid)::text::text AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/		
FROM studycro_data sc
JOIN included_studies st ON (sc.studyid = st.studyid);
