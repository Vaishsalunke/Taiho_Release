/*
CCDM Study mapping
Notes: Standard mapping to CCDM Study table
*/

WITH study_data AS (
                    SELECT  'TAS2940_101'::text AS studyid,
                            'TAS2940_101'::text AS studyname,
                            'A Phase 1 Study of TAS2940 in Patients with Locally
							Advanced or Metastatic Solid Tumors with EGFR
								and/or HER2 Aberrations'::text AS studydescription,
                            'Active'::text AS studystatus,
                            'Phase 1'::text AS studyphase,
                            'Taiho Oncology'::text AS studysponsor,
                            'Oncology'::text AS therapeuticarea,
                            'TAS2940'::text AS program,
                            'Locally Advanced or Metastatic Solid Tumor Cancer'::text AS medicalindication,
                            '2021-09-16'::date AS studystartdate,
                            '2025-06-30'::date AS studycompletiondate,
                            null::date AS studystatusdate)
                            --null::boolean AS isarchived,
                            --null::text AS sub_ta)

SELECT 
        /*KEY s.studyid::text AS comprehendid, KEY*/
        s.studyid::text AS studyid,
        s.studyname::text AS studyname,
        s.studydescription::text AS studydescription,
        s.studystatus::text AS studystatus,
        s.studyphase::text AS studyphase,
        s.studysponsor::text AS studysponsor,
        s.therapeuticarea::text AS therapeuticarea,
        s.program::text AS program,
        s.medicalindication::text AS medicalindication,
        s.studystartdate::date AS studystartdate,
        s.studycompletiondate::date AS studycompletiondate,
        s.studystatusdate::date AS studystatusdate,
        null::boolean AS isarchived,
        null::text AS sub_ta		
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM study_data s;

