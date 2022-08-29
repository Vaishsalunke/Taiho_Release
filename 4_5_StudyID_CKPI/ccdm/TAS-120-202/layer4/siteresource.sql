/*
 CCDM SiteResource mapping
 Notes: Standard mapping to CCDM SiteResource table
*/

WITH included_sites AS (
                SELECT DISTINCT studyid, siteid FROM site),

     siteresource_data AS (
                select studyid,
                        siteid,
                        resourcetype,
                        resourcename,
                        row_number() over (partition by studyid, siteid order by resourcestdtc)::integer AS resourceseq,
                        resourcestdtc,
                        resourceenddtc,
                        resourceemail
                        from (
                SELECT  'TAS-120-202'::text AS studyid,
                        'TAS120_202_' || "site_#"::text AS siteid,
                        'Primary Investigator'::text AS resourcetype,
                        "pi_name"::text AS resourcename,
                        null::integer AS resourceseq,
                        null::date AS resourcestdtc,
                        null::date AS resourceenddtc,
                        "pi_email"::text AS resourceemail
                   from tas120_202_ctms.project_site_contact)site_sub)
                   
SELECT 
        /*KEY (sr.studyid || '~' || sr.siteid)::text AS comprehendid, KEY*/
        sr.studyid::text AS studyid,
        sr.siteid::text AS siteid,
        sr.resourcetype::text AS resourcetype,
        sr.resourcename::text AS resourcename,
        sr.resourceseq::integer AS resourceseq,
        sr.resourcestdtc::date AS resourcestdtc,
        sr.resourceenddtc::date AS resourceenddtc,
        sr.resourceemail::text AS resourceemail
         /*KEY, (sr.studyid || '~' || sr.siteid || '~' || sr.resourceseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM siteresource_data sr
JOIN included_sites si ON (sr.studyid = si.studyid AND sr.siteid = si.siteid);

