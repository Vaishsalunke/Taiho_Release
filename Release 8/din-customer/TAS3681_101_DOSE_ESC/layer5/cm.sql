/*
CCDM CM mapping
Notes: Standard mapping to CCDM CM table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),
				
included_sites AS (
SELECT DISTINCT studyid, siteid, sitename, sitecountry,sitecountrycode, siteregion FROM site),

     cm_data AS (
                 -- TAS3681-101
                SELECT  'TAS3681_101_DOSE_ESC'::text AS studyid,
					  'TAS3681_101_DOSE_ESC'::text AS studyname,
                        "SiteNumber"::text AS siteid,
                        "Subject"::text AS usubjid,
                        concat("RecordPosition","InstanceRepeatNumber","PageRepeatNumber")::integer AS cmseq,
                        "CMTRT"::text AS cmtrt,
                        "CMTRT_TN"::text AS cmmodify,
                        "CMTRT_PT"::text AS cmdecod,
                        "CMTRT_ATC"::text AS cmcat,
                        'Concomitant Medications'::text AS cmscat,
                        "CMINDC"::text AS cmindc,
                        Null::numeric AS cmdose,
                        Null::text AS cmdosu,
                        Null::text AS cmdosfrm,
                        Null::text AS cmdosfrq,
                        Null::numeric AS cmdostot,
                        "CMROUTE"::text AS cmroute,
                        case when cmstdtc='' or cmstdtc like '%0000%' then null
							else to_date(cmstdtc,'DD Mon YYYY') 
						end ::timestamp without time zone AS cmstdtc,
						case when cmendtc='' or cmendtc like '%0000%' then null
							else to_date(cmendtc,'DD Mon YYYY') 
						end ::timestamp without time zone AS cmendtc,
                        null::time without time zone AS cmsttm,
                        null::time without time zone AS cmentm,
						"CMTRT_ATC"::text As CMATCTXT1,
						"CMTRT_ATC2"::text As CMATCTXT2,
						"CMTRT_ATC3"::text As CMATCTXT3
                FROM 
( select *,concat(replace(replace(substring(upper(case when length("CMSTDAT_RAW")<10 then null else "CMSTDAT_RAW" end),1,2),'UN','01'),'UK','01'),replace(substring(upper(case when length("CMSTDAT_RAW")<10 then null else "CMSTDAT_RAW" end),3),'UNK','Jan')) AS cmstdtc,
	     concat(replace(replace(substring(upper("CMENDAT_RAW"),1,2),'UN','01'),'UK','01'),replace(substring(upper("CMENDAT_RAW"),3),'UNK','Jan')) AS cmendtc
from tas3681_101."CM"	
)cm 
     )

SELECT 
        /*KEY (cm.studyid || '~' || cm.siteid || '~' || cm.usubjid)::text AS comprehendid, KEY*/
        cm.studyid::text AS studyid,
		cm.studyname::text As studyname,
        cm.siteid::text AS siteid,
		si.sitename::text AS sitename,
		si.sitecountry::text AS sitecountry,
		si.sitecountrycode::text AS sitecountrycode,
		si.siteregion::text AS siteregion,
        cm.usubjid::text AS usubjid,
        cm.cmseq::integer AS cmseq,
		null::text as cmspid,
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
        cm.cmendtc::timestamp without time zone AS cmendtc, --client requested change
        cm.cmsttm::time without time zone AS cmsttm,
        cm.cmentm::time without time zone AS cmentm,
		cm.CMATCTXT1::text As CMATCTXT1,
		cm.CMATCTXT2::text As CMATCTXT2,
		cm.CMATCTXT3::text As CMATCTXT3
        /*KEY , (cm.studyid || '~' || cm.siteid || '~' || cm.usubjid || '~' || cm.cmseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
FROM cm_data cm
JOIN included_subjects s ON (cm.studyid = s.studyid AND cm.siteid = s.siteid AND cm.usubjid = s.usubjid)
JOIN included_sites si ON (cm.studyid = si.studyid AND cm.siteid = si.siteid);

