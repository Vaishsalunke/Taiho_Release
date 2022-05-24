/*
CCDM SiteIssue mapping
Notes: Standard mapping to CCDM SiteIssue table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site ),
     siteissue_data AS (
                SELECT  'TAS3681_101_DOSE_ESC'::text AS studyid,
                        a."site_number"::text AS siteid,
                        a."id_"::numeric AS issueid,
                        a."issue_type"::text AS issuetype,
                        a."ai_category"::text AS issuetext,
                        a."ai_status"::text AS issuestatus,
                        nullif(a."ai_created_date",'')::date AS issueopeneddate,
                        nullif(a."ai_due_date",'')::date AS issueresponsedate,
                        nullif(a."ai_closed_date",'')::date AS issuecloseddate
						from
						tas3681_101_ctms."action_items" a)
					

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

