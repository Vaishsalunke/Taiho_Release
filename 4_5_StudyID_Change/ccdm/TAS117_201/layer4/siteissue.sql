/*
CCDM SiteIssue mapping
Notes: Standard mapping to CCDM SiteIssue table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site ),

     siteissue_data AS (
                SELECT  'TAS117-201' ::text AS studyid,
                        concat('TAS117_201_',split_part(site_number,'_',2)) ::text AS siteid,
                        id_ ::int AS issueid,
                        issue_type ::text AS issuetype,
                        ai_category ::text AS issuetext,
                        ai_status ::text AS issuestatus,
                        nullif(ai_created_date,'') ::date AS issueopeneddate,
                        nullif(ai_due_date,'') ::date AS issueresponsedate,
                        nullif(ai_closed_date,'') ::date AS issuecloseddate,
                        null::text AS maskingtype
                from tas117_201_ctms.action_items ai      
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
        /*KEY , (si.studyid || '~' || si.siteid || '~' || si.issueid)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM siteissue_data si
JOIN included_sites s ON (s.studyid = si.studyid AND s.siteid = si.siteid);



