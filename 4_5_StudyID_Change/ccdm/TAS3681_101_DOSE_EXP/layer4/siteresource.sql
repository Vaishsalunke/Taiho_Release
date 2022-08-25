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
                SELECT --protocol_id::text AS studyid,
                        'TAS3681_101_DOSE_EXP'::text AS studyid,
                        site_number::text AS siteid,
                        ''::text AS resourcetype,
                        facility_name::text AS resourcename,
                        null::integer AS resourceseq,
                        nullif(site_activated_date, '')::date AS resourcestdtc,
                        nullif(site_closed_date, '')::date AS resourceenddtc,
                        "email"::text AS resourceemail
                   from tas3681_101_ctms.sites)site_sub)

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
        /*KEY , (sr.studyid || '~' || sr.siteid || '~' || sr.resourceseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM siteresource_data sr
JOIN included_sites si ON (sr.studyid = si.studyid AND sr.siteid = si.siteid);
