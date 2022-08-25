/*
CCDM Study mapping
Notes: Standard mapping to CCDM Study table
*/

WITH study_data AS (
							SELECT  'TAS-120-201'::text AS studyid,
                            'TAS120_201'::text AS studyname,
                            'A Phase 2 Study of TAS-120 in Metastatic Breast Cancers Harboring Fibroblast Growth Factor Receptor (FGFR) Amplifications'::text AS studydescription,
                            'Active'::text AS studystatus,
                            'Phase 2'::text AS studyphASe,
                            'Taiho Oncology'::text AS studysponsor,
                            'Oncology'::text AS therapeuticarea,
                            'TAS120'::text AS program,
                            'Metastatic Breast Cancers Harboring Fibroblast Growth Factor Receptor (FGFR) Amplifications'::text AS medicalindication,
                            '15-Dec-2019'::date AS studystartdate,
                            null::date AS studycompletiondate,
                            null::date AS studystatusdate,
                            null::boolean AS isarchived,
							null::text AS sub_ta,
                            'Cohort 1 and 2 - ORR, Cohort 3 - CBR, Cohort 4 - 6-month PFS'::text as primaryendpoint
					)

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


