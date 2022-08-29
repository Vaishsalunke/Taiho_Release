/*
CCDM Study mapping
Notes: Standard mapping to CCDM Study table
*/

WITH study_data AS (
                    SELECT  'TAS117-201'::text AS studyid,
                            'TAS117_201'::text AS studyname,
                            'A Phase 2 Study of TAS-117 in Patients with Advanced Solid Tumors
Harboring Germline PTEN Inactivating Mutations'::text AS studydescription,
                            'Active'::text AS studystatus,
                            'Phase 2'::text AS studyphASe,
                            'Taiho Oncology'::text AS studysponsor,
                            'Oncology'::text AS therapeuticarea,
                            'TAS117'::text AS program,
                            'Advanced or Metastatic Solid Tumors With Germline PTEN Inactivating Mutations'::text AS medicalindication,
                            '2021-03-31'::date AS studystartdate,
                            '2024-12-30'::date AS studycompletiondate,
                            null::date AS studystatusdate,
                            null::boolean AS isarchived,
                            null::text AS sub_ta,
                            'Part A - DLTs, Part B - ORR per RECIST 1.1, based on ICR'::text as primaryendpoint)

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
        s.sub_ta::text AS sub_ta,
        s.primaryendpoint::text as primaryendpoint
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM study_data s;





