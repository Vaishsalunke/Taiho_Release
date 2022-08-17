/*
CCDM Study mapping
Notes: Standard mapping to CCDM Study table
*/

WITH study_data AS (
                    SELECT  'TAS-120-204'::text AS studyid,
                            'TAS120_204'::text AS studyname,
                            'A Phase 1b/2 open-label, nonrandomized study of FGFR inhibitor
						futibatinib in combination with MEK-inhibitor binimetinib in patients
							with advanced KRAS mutant cancer'::text AS studydescription,
                            'Active'::text AS studystatus,
                            'Phase 1b/2'::text AS studyphase,
                            'Taiho Oncology'::text AS studysponsor,
                            'Oncology'::text AS therapeuticarea,
                            'TAS120'::text AS program,
                            'KRAS Gene Mutation'::text AS medicalindication,
                            '2021-09-20'::date AS studystartdate,
                            '2024-12-30'::date AS studycompletiondate,
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

