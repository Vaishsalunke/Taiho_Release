/*
CCDM CM mapping
Notes: Standard mapping to CCDM CM table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),
	 included_site AS (
	SELECT DISTINCT studyid, siteid, sitename, sitecountry, sitecountrycode, siteregion FROM site),	

     cm_data AS (
                 -- TAS0621-101
                 select studyid,
                 siteid,
                 usubjid,
                -- row_number() over (partition by studyid, siteid, usubjid order by cmstdtc) as cmseq,
				cmseq,
                 cmtrt,
                 cmmodify,
                 cmdecod,
                 cmcat,
                 cmscat,
                 cmindc,
                 cmdose,
                 cmdosu,
                 cmdosfrm,
                 cmdosfrq,
                 cmdostot,
                 cmroute,
                 cmstdtc,
                 cmendtc,
                 cmsttm,
                 cmentm
                 from (
                SELECT  "project"::text AS studyid,
                        --"SiteNumber"::text AS siteid,
						concat('TAS0612_101_',split_part("SiteNumber",'_',2))::text AS siteid,
                        --substring(trim("Subject"),0,8)::text AS usubjid,
                        "Subject"::text AS usubjid,
                        concat("RecordPosition","PageRepeatNumber")::integer AS cmseq,
                        coalesce(nullif("CMTRT",''),'Missing')::text AS cmtrt,
                        "CMTRT_TN"::text AS cmmodify,
                        "CMTRT_PT"::text AS cmdecod,
                        "CMTRT_ATC"::text AS cmcat,
                        'Concomitant Medications'::text AS cmscat,
                        coalesce(nullif("CMINDC",''),'Missing')::text AS cmindc,
                        Null::numeric AS cmdose,
                        Null::text AS cmdosu,
                        Null::text AS cmdosfrm,
                        Null::text AS cmdosfrq,
                        Null::numeric AS cmdostot,
                        "CMROUTE"::text AS cmroute,
                        case when cmstdtc='' then null
							else to_date(cmstdtc,'DD Mon YYYY') 
						end ::timestamp without time zone AS cmstdtc,
						case when cmendtc='' then null
							else to_date(cmendtc,'DD Mon YYYY') 
						end ::timestamp without time zone AS cmendtc,
                        null::time without time zone AS cmsttm,
                        null::time without time zone AS cmentm
                FROM 

( select *,case when length("CMSTDAT_RAW")<>11 then null
else concat(replace (replace (substring (upper("CMSTDAT_RAW"),1,2),'UN','01'),'UK','01'),replace(substring(upper("CMSTDAT_RAW"),3),'UNK','Jan'))end AS cmstdtc,
case when length("CMENDAT_RAW")<>11 then null
else concat(replace(replace(substring(upper("CMENDAT_RAW"),1,2),'UN','01'),'UK','01'),replace(substring(upper("CMENDAT_RAW"),3),'UNK','Jan'))end AS cmendtc
from tas0612_101."CM"	
)cm 

)cm
     )

SELECT 
        /*KEY (cm.studyid || '~' || cm.siteid || '~' || cm.usubjid)::text AS comprehendid, KEY*/
        cm.studyid::text AS studyid,
        null::text AS studyname,
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

