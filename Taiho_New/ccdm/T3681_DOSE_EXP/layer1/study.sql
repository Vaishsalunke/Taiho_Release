/*
CCDM Study mapping
Notes: Standard mapping to CCDM Study table
*/


WITH study_data AS (
                    SELECT  'T3681_DOSE_EXP'::text AS studyid,
                            'TAS3681_101_DOSE_EXP'::text AS studyname,
                            'A Phase 1, Open-Label, Non-Randomized, Safety, Tolerability, and
Pharmacokinetic Study of TAS3681 in Patients with Metastatic Castration-Resistant Prostate Cancer'::text AS studydescription,
                            'Active'::text AS studystatus,
                            'Phase 1'::text AS studyphASe,
                            'Taiho Oncology'::text AS studysponsor,
                            'Oncology'::text AS therapeuticarea,
                            'TAS3681'::text AS program,
                            'Metastatic Castration-Resistant Prostate Cancer'::text AS medicalindication,
                            '01-Mar-2016'::date AS studystartdate,
                            '30-Sep-2020'::date AS studycompletiondate,
                            null::date AS studystatusdate,
                            null::boolean AS isarchived,
                            'To further evaluate the safety and preliminary efficacy of TAS3681'::text as primaryendpoint)

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

