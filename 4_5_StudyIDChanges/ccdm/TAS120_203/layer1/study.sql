/*
CCDM Study mapping
Notes: Standard mapping to CCDM Study table
*/

WITH study_data AS (
                    SELECT  'TAS-120-203'::text AS studyid,
                            'TAS120_203'::text AS studyname,
                            'A Phase 2 Study Evaluating Futibatinib (TAS-120) Plus
							Pembrolizumab in the Treatment of Advanced or Metastatic
							Urothelial Carcinoma'::text AS studydescription,
                            'Active'::text AS studystatus,
                            'Phase 2'::text AS studyphase,
                            'Taiho Oncology'::text AS studysponsor,
                            'Oncology'::text AS therapeuticarea,
                            'TAS120'::text AS program,
                            'Carcinoma and Urinogenital Cancer'::text AS medicalindication,
                            '2021-01-21'::date AS studystartdate,
                            '2022-12-30'::date AS studycompletiondate,
                            null::date AS studystatusdate,
                            'ORR by RECIST 1.1'::text as primaryendpoint)
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
        null::text AS sub_ta,
        s.primaryendpoint::text as primaryendpoint
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM study_data s;

