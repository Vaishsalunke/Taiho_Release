/*
CCDM Study mapping
Notes: Standard mapping to CCDM Study table
*/

WITH study_data AS (
                    SELECT  null::text AS studyid,
                            null::text AS studyname,
                            null::text AS studydescription,
                            null::text AS studystatus,
                            null::text AS studyphASe,
                            null::text AS studysponsor,
                            null::text AS therapeuticarea,
                            null::text AS program,
                            null::text AS medicalindication,
                            null::date AS studystartdate,
                            null::date AS studycompletiondate,
                            null::date AS studystatusdate,
                            null::boolean AS isarchived,
                            null::text AS sub_ta)

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
        s.sub_ta::text AS sub_ta		
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM study_data s
WHERE 1=2;
