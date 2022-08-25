/*
CCDM SiteIssue mapping
Notes: Standard mapping to CCDM SiteIssue table
*/


WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site ),

     siteissue_data AS (
                SELECT  'TAS-120-203'::text AS studyid,
                        concat('TAS120_203_',site_number)::text AS siteid,
                        site_issue_id ::int AS issueid,
                        issue_category ::text AS issuetype,
                        issue_description ::text AS issuetext,
                        issue_status_openclosed ::text AS issuestatus,
                        nullif(date_issue_occurred,'') ::date AS issueopeneddate,
                        null::date AS issueresponsedate,
                        nullif(closed_date,'') ::date AS issuecloseddate,
                        null::text AS maskingtype
                 from TAS120_203_ctms.site_issues       
                )

SELECT 
        /*KEY (si.studyid || '~' || si.siteid)::text AS comprehendid, KEY*/
        si.studyid::text AS studyid,
        si.siteid::text AS siteid,
        si.issueid::int AS issueid,
        si.issuetype::text AS issuetype,
        si.issuetext::text AS issuetext,
        si.issuestatus::text AS issuestatus,
        si.issueopeneddate::date AS issueopeneddate,
        si.issueresponsedate::date AS issueresponsedate,
        si.issuecloseddate::date AS issuecloseddate,
        si.maskingtype::text AS maskingtype
        /*KEY, (si.studyid || '~' || si.siteid || '~' || si.issueid)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM siteissue_data si
JOIN included_sites s ON (s.studyid = si.studyid AND s.siteid = si.siteid);




