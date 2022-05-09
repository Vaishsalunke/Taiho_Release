/*
CCDM CM mapping
Notes: Standard mapping to CCDM CM table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     cm_data AS (
                SELECT  project::text AS studyid,
                        'TAS120_203'::text AS studyname,
                        project||substring("SiteNumber",position ('_' in "SiteNumber"))::text AS siteid,
                        null::text AS sitename,
                        null::text AS sitecountry,
                        null::text AS sitecountrycode,
                        null::text AS siteregion,
                        "Subject"::text AS usubjid,
                        concat("RecordPosition","PageRepeatNumber") ::integer AS cmseq,
                        null::text AS cmspid,
                        "CMTRT"::text AS cmtrt,
                        "CMTRT_TN"::text AS cmmodify,
                        "CMTRT_PT"::text AS cmdecod,
                        "CMTRT_ATC"::text AS cmcat,
                        'Concomitant Medications'::text AS cmscat,
                        "CMINDC"::text AS cmindc,
                        null::numeric AS cmdose,
                        null::text AS cmdostxt,
                        null::text AS cmdosu,
                        null::text AS cmdosfrm,
                        null::text AS cmdosfrq,
                        null::numeric AS cmdostot,
                        --case when "CMROUTE"='Other' then "CMROUTEOTH" else "CMROUTE" end::text AS cmroute,
						CASE WHEN "CMROUTE"='Other' then  "CMROUTE" || "CMROUTEOTH"   ELSE  "CMROUTE"::text end::text AS cmroute,
                        null::text AS cmongo,
                        null::text AS cmstdtc_iso,
                        "CMSTDAT"::timestamp without time zone AS cmstdtc,
                        null::text AS cmendtc_iso,
                        "CMENDAT"::timestamp without time zone AS cmendtc,
                        null::time without time zone AS cmsttm,
                        null::time without time zone AS cmentm 
                        from tas120_203."CM" ),
                        
     site_data as (select distinct studyid,siteid,sitename,sitecountry,sitecountrycode,siteregion from site)
	 

SELECT 
        /*KEY (cm.studyid || '~' || cm.siteid || '~' || cm.usubjid)::text AS comprehendid, KEY*/
        cm.studyid::text AS studyid,
        cm.studyname::text AS studyname,
        cm.siteid::text AS siteid,
        sd.sitename::text AS sitename,
        sd.sitecountry::text AS sitecountry,
        sd.sitecountrycode::text AS sitecountrycode,
        sd.siteregion::text AS siteregion,
        cm.usubjid::text AS usubjid,
        cm.cmseq::integer AS cmseq,
        cm.cmspid::text AS cmspid,
        cm.cmtrt::text AS cmtrt,
        cm.cmmodify::text AS cmmodify,
        cm.cmdecod::text AS cmdecod,
        cm.cmcat::text AS cmcat,
        cm.cmscat::text AS cmscat,
        cm.cmindc::text AS cmindc,
        cm.cmdose::numeric AS cmdose,
        cm.cmdostxt::text AS cmdostxt,
        cm.cmdosu::text AS cmdosu,
        cm.cmdosfrm::text AS cmdosfrm,
        cm.cmdosfrq::text AS cmdosfrq,
        cm.cmdostot::numeric AS cmdostot,
        cm.cmroute::text AS cmroute,
        cm.cmongo::text AS cmongo,
        cm.cmstdtc_iso::text AS cmstdtc_iso,
        cm.cmstdtc::timestamp without time zone AS cmstdtc,
        cm.cmendtc_iso::text AS cmendtc_iso,
        cm.cmendtc::timestamp without time zone AS cmendtc,
        cm.cmsttm::time without time zone AS cmsttm,
        cm.cmentm::time without time zone AS cmentm
        /*KEY , (cm.studyid || '~' || cm.siteid || '~' || cm.usubjid || '~' || cm.cmseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM cm_data cm
JOIN included_subjects s ON (cm.studyid = s.studyid AND cm.siteid = s.siteid AND cm.usubjid = s.usubjid)
join site_data sd on (cm.studyid = sd.studyid AND cm.siteid = sd.siteid);

