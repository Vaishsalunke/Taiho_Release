/*
CCDM Study mapping
Notes: Standard mapping to CCDM Study table
*/


WITH study_data AS (
                     SELECT  'TAS0612_101'::text AS studyid,
                            'TAS0612_101'::text AS studyname,
                            'A Phase 1 Study of TAS0612 in Patients with Locally Advanced or Metastatic Solid Tumors'::text AS studydescription,
                            'Active'::text AS studystatus,
                            'Phase 1'::text AS studyphASe,
                            'Taiho Oncology'::text AS studysponsor,
                            'Oncology'::text AS therapeuticarea,
                            'TAS0612'::text AS program,
                            'Solid Tumor'::text AS medicalindication,
                            '29-June-2020'::date AS studystartdate,
                            null::date AS studycompletiondate,
                            null::date AS studystatusdate,
                            null::boolean AS isarchived )

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
        s.isarchived::boolean AS isarchived,
        null::text AS sub_ta		
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM study_data s;

