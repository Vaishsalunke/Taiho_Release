/*
CCDM CM mapping
Notes: Standard mapping to CCDM CM table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     cm_data AS (
                SELECT  null::text AS studyid,
                        null::text AS studyname,
                        null::text AS siteid,
                        null::text AS sitename,
                        null::text AS sitecountry,
                        null::text AS sitecountrycode,
                        null::text AS siteregion,
                        null::text AS usubjid,
                        null::integer AS cmseq,
                        null::text AS cmspid,
                        null::text AS cmtrt,
                        null::text AS cmmodify,
                        null::text AS cmdecod,
                        null::text AS cmcat,
                        null::text AS cmscat,
                        null::text AS cmindc,
                        null::numeric AS cmdose,
                        null::text AS cmdostxt,
                        null::text AS cmdosu,
                        null::text AS cmdosfrm,
                        null::text AS cmdosfrq,
                        null::numeric AS cmdostot,
                        null::text AS cmroute,
                        null::text AS cmongo,
                        null::text AS cmstdtc_iso,
                        null::timestamp without time zone AS cmstdtc,
                        null::text AS cmendtc_iso,
                        null::timestamp without time zone AS cmendtc,
                        null::time without time zone AS cmsttm,
                        null::time without time zone AS cmentm )

SELECT 
        /*KEY (cm.studyid || '~' || cm.siteid || '~' || cm.usubjid)::text AS comprehendid, KEY*/
        cm.studyid::text AS studyid,
        cm.studyname::text AS studyname,
        cm.siteid::text AS siteid,
        cm.sitename::text AS sitename,
        cm.sitecountry::text AS sitecountry,
        cm.sitecountrycode::text AS sitecountrycode,
        cm.siteregion::text AS siteregion,
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
WHERE 1=2;
