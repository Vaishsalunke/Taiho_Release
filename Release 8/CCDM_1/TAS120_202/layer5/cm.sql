/*
CCDM CM mapping
Notes: Standard mapping to CCDM CM table
*/

WITH included_subjects AS (
    SELECT DISTINCT studyid, siteid, usubjid FROM subject),
	included_site AS (
	SELECT DISTINCT studyid, siteid, sitename, sitecountry, sitecountrycode, siteregion FROM site),	

     cm_data AS (

                SELECT  "project"::text AS studyid,
                        "SiteNumber"::text AS siteid,
                        "Subject"::text AS usubjid,
                        concat("RecordPosition","PageRepeatNumber")::integer AS cmseq,
                        "CMTRT"::text AS cmtrt,
                        "CMTRT_TN"::text AS cmmodify,
                        "CMTRT_PT"::text AS cmdecod,
                        "CMTRT_ATC"::text AS cmcat,
                        'Concomitant Medications'::text AS cmscat,
                        "CMINDC"::text AS cmindc,
                        null::numeric AS cmdose,
						null::text AS cmdosu,
						null::text AS cmdosfrm,
						null::text AS cmdosfrq,
                        null::numeric AS cmdostot,
                        "CMROUTE"::text AS cmroute,
                        case when cmstdtc='' or cmstdtc like '%0000%' then null
							else to_date(cmstdtc,'DD Mon YYYY') 
						end ::timestamp without time zone AS cmstdtc,
						case when cmendtc='' or cmendtc like '%0000%' then null
							else to_date(cmendtc,'DD Mon YYYY') 
						end ::timestamp without time zone AS cmendtc,
                        null::time without time zone AS cmsttm,
                        null::time without time zone AS cmentm
                FROM 
( select *,concat(replace (replace (substring (upper("CMSTDAT_RAW"),1,2),'UN','01'),'UK','01'),replace(substring(upper("CMSTDAT_RAW"),3),'UNK','Jan')) AS cmstdtc,
	     concat(replace(replace(substring(upper("CMENDAT_RAW"),1,2),'UN','01'),'UK','01'),replace(substring(upper("CMENDAT_RAW"),3),'UNK','Jan')) AS cmendtc
from "tas120_202"."CM"	
)cm 
)

SELECT 
        /*KEY (cm.studyid || '~' || cm.siteid || '~' || cm.usubjid)::text AS comprehendid, KEY*/
        cm.studyid::text AS studyid,
        'TAS120_202'::text AS studyname,
        cm.siteid::text AS siteid,
        si.sitename::text AS sitename,
        si.sitecountry::text AS sitecountry,
        si.sitecountrycode::text AS sitecountrycode,
        si.siteregion::text AS siteregion,
        cm.usubjid::text AS usubjid,
        cm.cmseq::integer AS cmseq,
        null::text AS cmspid,
        cm.cmtrt::text AS cmtrt,
        cm.cmmodify::text AS cmmodify,
        cm.cmdecod::text AS cmdecod,
        cm.cmcat::text AS cmcat,
        cm.cmscat::text AS cmscat,
        cm.cmindc::text AS cmindc,
        cm.cmdose::numeric AS cmdose,
        null::text AS cmdostxt,
        cm.cmdosu::text AS cmdosu,
        cm.cmdosfrm::text AS cmdosfrm,
        cm.cmdosfrq::text AS cmdosfrq,
        cm.cmdostot::numeric AS cmdostot,
        cm.cmroute::text AS cmroute,
        null::text AS cmongo,
        null::text AS cmstdtc_iso,
        cm.cmstdtc::timestamp without time zone AS cmstdtc,
        null::text AS cmendtc_iso,
        cm.cmendtc::timestamp without time zone AS cmendtc,
        cm.cmsttm::time without time zone AS cmsttm,
        cm.cmentm::time without time zone AS cmentm
        /*KEY , (cm.studyid || '~' || cm.siteid || '~' || cm.usubjid || '~' || cm.cmseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM cm_data cm
JOIN included_subjects s ON (cm.studyid = s.studyid AND cm.siteid = s.siteid AND cm.usubjid = s.usubjid)
JOIN included_site si ON (cm.studyid = si.studyid AND cm.siteid = si.siteid);

