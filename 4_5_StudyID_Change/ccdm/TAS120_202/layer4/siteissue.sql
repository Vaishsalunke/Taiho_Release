/*
CCDM SiteIssue mapping
Notes: Standard mapping to CCDM SiteIssue table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site ),

     siteissue_data AS (
                SELECT  'TAS-120-202'::text AS studyid,
                        concat('TAS120_202_',"site_#")::text AS siteid,
                        "id_"::int AS issueid,
                       severity ::text AS issuetype,
                        category::text AS issuetext,
                        status::text AS issuestatus,
                        open_date::date AS issueopeneddate,
                        null::date AS issueresponsedate,
                        due_date::date AS issuecloseddate 
from tas120_202_ctms.ai_report)

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
		null::text AS maskingtype
        /*KEY , (si.studyid || '~' || si.siteid || '~' || si.issueid)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM siteissue_data si
JOIN included_sites s ON (s.studyid = si.studyid AND s.siteid = si.siteid); 

