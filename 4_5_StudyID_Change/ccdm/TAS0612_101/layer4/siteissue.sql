/*
CCDM SiteIssue mapping
Notes: Standard mapping to CCDM SiteIssue table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site ),

     siteissue_data AS (
                SELECT  'TAS0612-101'::text AS studyid,
                       concat( concat(replace(protocol,'-','_'),'_'),sitenumber)::text AS siteid,
                        id_::int AS issueid,
                       issue_category_code ::text AS issuetype,
                        issue_category_desc::text AS issuetext,
                        case when resolved = '1' then 'Closed'
                        	when resolved = '0' then 'Open' end ::text AS issuestatus,
                        nullif(datediscovered,'')::date AS issueopeneddate,
                        null::date AS issueresponsedate,
                        nullif(dateresolved,'')::date AS issuecloseddate 
from tas0612_101_ctms.issues_actions)

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
