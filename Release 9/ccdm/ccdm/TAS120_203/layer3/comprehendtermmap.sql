/*
CCDM Comprehendtermmap mapping
Notes: Standard mapping to CDM comprehendtermmap table
*/

WITH included_studies AS ( 
              SELECT studyid FROM study )

SELECT 'sitemonitoringvisit'::text AS tablename,
       'visitname'::text AS columnname,
       s.studyid::text AS studyid, 
       'SITE CLOSE-OUT VISIT'::text AS comprehendterm,
       'Close Out Visit'::text AS originalterm
FROM included_studies s
UNION ALL
SELECT 'sitemonitoringvisitschedule'::text AS tablename,
       'visitname'::text AS columnname,
       s.studyid::text AS studyid, 
       'SITE CLOSE-OUT VISIT'::text AS comprehendterm,
       'Close Out Visit'::text AS originalterm
FROM included_studies s
UNION ALL
SELECT 'studymilestone'::text AS tablename,
       'milestonelabel'::text AS columnname,
       s.studyid,
       'STUDY START'::text AS comprehendterm,
       'Study Start'::text AS originalterm
FROM included_studies s
UNION ALL
SELECT 'studymilestone'::text AS tablename,
       'milestonelabel'::text AS columnname,
       s.studyid,
       'FIRST SITE READY TO ENROLL'::text AS comprehendterm,
       'First Site Initiated'::text AS originalterm
FROM included_studies s
UNION ALL
SELECT 'studymilestone'::text AS tablename,
       'milestonelabel'::text AS columnname,
       s.studyid,
       'FIRST SUBJECT IN'::text AS comprehendterm,
       'First Subject Treat/Obs'::text AS originalterm
FROM included_studies s
UNION ALL
SELECT 'studymilestone'::text AS tablename,
       'milestonelabel'::text AS columnname,
       s.studyid,
       'LAST SUBJECT IN'::text AS comprehendterm,
       'Last Subject in Treat/Obs'::text AS originalterm
FROM included_studies s
UNION ALL
SELECT 'studymilestone'::text AS tablename,
       'milestonelabel'::text AS columnname,
       s.studyid,
       'ALL SITES ACTIVATED'::text AS comprehendterm,
       'All Sites Activated'::text AS originalterm
FROM included_studies s
UNION ALL
SELECT 'studymilestone'::text AS tablename,
       'milestonelabel'::text AS columnname,
       s.studyid,
       'LAST SUBJECT LAST VISIT'::text AS comprehendterm,
       'Last Subject Last Visit'::text AS originalterm
FROM included_studies s
UNION ALL
SELECT 'studymilestone'::text AS tablename,
       'milestonelabel'::text AS columnname,
       s.studyid,
       '50% SITES ACTIVATED'::text AS comprehendterm,
       '50% Sites Activated'::text AS originalterm
FROM included_studies s
UNION ALL
SELECT 'studymilestone'::text AS tablename,
       'milestonelabel'::text AS columnname,
       s.studyid,
       '50% SUBJECTS ENROLLED'::text AS comprehendterm,
       '50% Subjects Enrolled'::text AS originalterm
FROM included_studies s
UNION ALL
SELECT 'studymilestone'::text AS tablename,
       'milestonelabel'::text AS columnname,
       s.studyid,
       'DATABASE LOCK'::text AS comprehendterm,
       'Database Lock'::text AS originalterm
FROM included_studies s
UNION ALL
SELECT 'sitemilestone'::text AS tablename,
       'milestonelabel'::text AS columnname,
       s.studyid,
       'SITE ACTIVATED'::text AS comprehendterm,
       'Site Activated'::text AS originalterm
FROM included_studies s
UNION ALL
SELECT 'countrymilestone'::text AS tablename,
       'milestonelabel'::text AS columnname,
       s.studyid,
       'LAST SUBJECT IN'::text AS comprehendterm,
       'Last Subject In'::text AS originalterm
FROM included_studies s
UNION ALL
SELECT 'countrymilestone'::text AS tablename,
       'milestonelabel'::text AS columnname,
       s.studyid,
       'FIRST REGULATORY SUBMISSION'::text AS comprehendterm,
       'First Regulatory Submission'::text AS originalterm
FROM included_studies s
UNION ALL
SELECT 'countrymilestone'::text AS tablename,
       'milestonelabel'::text AS columnname,
       s.studyid,
       'LAST SUBJECT LAST VISIT'::text AS comprehendterm,
       'Last Subject Last Visit'::text AS originalterm
FROM included_studies s
UNION ALL
SELECT 'countrymilestone'::text AS tablename,
       'milestonelabel'::text AS columnname,
       s.studyid,
       'FIRST SUBJECT RANDOMIZED'::text AS comprehendterm,
       'First Subject Randomized'::text AS originalterm
FROM included_studies s
UNION ALL
SELECT 'countrymilestone'::text AS tablename,
       'milestonelabel'::text AS columnname,
       s.studyid,
       '50% SITES ACTIVATED'::text AS comprehendterm,
       '50% Sites Activated'::text AS originalterm
FROM included_studies s
UNION ALL
SELECT 'countrymilestone'::text AS tablename,
       'milestonelabel'::text AS columnname,
       s.studyid,
       'FIRST SITE INITIATED'::text AS comprehendterm,
       'First Site Initiated'::text AS originalterm
FROM included_studies s
UNION ALL
SELECT 'countrymilestone'::text AS tablename,
       'milestonelabel'::text AS columnname,
       s.studyid,
       'FIRST REGULATORY APPROVAL'::text AS comprehendterm,
       'First Regulatory Approval'::text AS originalterm
FROM included_studies s
UNION ALL
SELECT 'countrymilestone'::text AS tablename,
       'milestonelabel'::text AS columnname,
       s.studyid,
       'ALL SITES ACTIVATED'::text AS comprehendterm,
       'All Sites Activated'::text AS originalterm
FROM included_studies s
UNION ALL
SELECT 'countrymilestone'::text AS tablename,
       'milestonelabel'::text AS columnname,
       s.studyid,
       'FIRST SUBJECT IN'::text AS comprehendterm,
       'First Subject In'::text AS originalterm
FROM included_studies s
UNION ALL
SELECT 'countrymilestone'::text AS tablename,
       'milestonelabel'::text AS columnname,
       s.studyid,
       'LAST SUBJECT RANDOMIZED'::text AS comprehendterm,
       'Last Subject Randomized'::text AS originalterm
FROM included_studies s
UNION ALL
SELECT 'countrymilestone'::text AS tablename,
       'milestonelabel'::text AS columnname,
       s.studyid,
       'FIRST SUPPLIES SENT'::text AS comprehendterm,
       'First Supplies Sent'::text AS originalterm
FROM included_studies s
UNION ALL
SELECT 'countrymilestone'::text AS tablename,
       'milestonelabel'::text AS columnname,
       s.studyid,
       'LAST DOSE TO LAST SUBJECT'::text AS comprehendterm,
       'Last Dose to Last Subject'::text AS originalterm
FROM included_studies s
UNION ALL
SELECT 'countrymilestone'::text AS tablename,
       'milestonelabel'::text AS columnname,
       s.studyid,
       'FIRST SITE READY TO ENROLL'::text AS comprehendterm,
       'First Site Ready to Enroll'::text AS originalterm
FROM included_studies s
UNION ALL
SELECT 'countrymilestone'::text AS tablename,
       'milestonelabel'::text AS columnname,
       s.studyid,
       '50% SUBJECTS ENROLLED'::text AS comprehendterm,
       '50% Subjects Enrolled'::text AS originalterm
FROM included_studies s
UNION ALL
SELECT 'countrymilestone'::text AS tablename,
       'milestonelabel'::text AS columnname,
       s.studyid,
       'ALL SUBJECTS ENROLLED'::text AS comprehendterm,
       'All Subjects Enrolled'::text AS originalterm
FROM included_studies s;

