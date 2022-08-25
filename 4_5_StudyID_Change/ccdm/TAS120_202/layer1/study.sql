/*
CCDM Study mapping
Notes: Standard mapping to CCDM Study table
*/


WITH study_data AS (
                     SELECT  'TAS-120-202'::text AS studyid,
                            'TAS120_202'::text AS studyname,
                            'A PHASE 2 STUDY OF FUTIBATINIB IN PATIENTS WITH SPECIFIC FGFR ABERRATIONS'::text AS studydescription,
                            'Active'::text AS studystatus,
                            'Phase 2'::text AS studyphASe,
                            'Taiho Oncology'::text AS studysponsor,
                            'Oncology'::text AS therapeuticarea,
                            'TAS120'::text AS program,
                            'Futibatinib in Patients With Specific FGFR Aberrations'::text AS medicalindication,
                            '30-June-2020'::date AS studystartdate,
                            null::date AS studycompletiondate,
                            null::date AS studystatusdate,
                            null::boolean AS isarchived,
                            'ORR by RECIST 1.1'::text as primaryendpoint )

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
        null::text AS sub_ta,
        s.primaryendpoint::text as primaryendpoint
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM study_data s;

