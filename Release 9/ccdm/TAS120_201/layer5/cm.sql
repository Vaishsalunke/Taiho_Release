/*
CCDM CM mapping
Notes: Standard mapping to CCDM CM table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

          cm_data AS (
                SELECT 
				"project"	::text	AS	studyid	,
				"SiteNumber"	::text	AS	siteid	,
				"Subject"	::text	AS	usubjid	,
				concat("RecordPosition","InstanceRepeatNumber","PageRepeatNumber")::integer AS cmseq,
				"CMTRT"::text	AS	cmtrt	,
				"CMTRT_TN"::text	AS	cmmodify	,
				"CMTRT_PT"::text	AS	cmdecod	,
				"CMTRT_ATC"::text	AS	cmcat	,
				'Concomitant Medications'::text	AS	cmscat	,
				"CMINDC"::text	AS	cmindc	,
				Null::numeric 	AS	cmdose	,
				NULL::text	AS	cmdosu	,
				NULL::text	AS	cmdosfrm	,
				NULL::text	AS	cmdosfrq	,
				NULL::text 	AS	cmdostot	,
				CASE WHEN "CMROUTE"='Other' then  "CMROUTE" || "CMROUTEOTH"   ELSE  "CMROUTE"::text END	AS	cmroute	,
				cmstdtc::timestamp without time zone AS cmstdtc,
				cmendtc::timestamp without time zone AS cmendtc,
				NULL::time without time zone	AS	cmsttm	,
				NULL::time without time zone	AS	cmentm
				,null::text AS studyname
,null::text AS cmspid
,null::text AS cmdostxt
,null::text AS cmongo
,null::text AS cmstdtc_iso
,null::text AS cmendtc_iso
,"CMTRT_ATC"::text As CMATCTXT1,
                "CMTRT_ATC2"::text As CMATCTXT2,
                "CMTRT_ATC3"::text As CMATCTXT3
				FROM  
( select *,case when ((length(trim("CMSTDAT_RAW"))= 11 or length(trim("CMSTDAT_RAW"))= 10) and "CMSTDAT_RAW" not like '%0000%') and "CMSTDAT_RAW" !=''
then to_date(concat(replace (replace (substring (upper("CMSTDAT_RAW"),1,2),'UN','01'),'UK','01'),replace(substring(upper("CMSTDAT_RAW"),3),'UNK','Jan')),'DD Mon YYYY')
else null end as cmstdtc,
case when ((length(trim("CMENDAT_RAW"))= 11 or length(trim("CMENDAT_RAW"))= 10) and "CMENDAT_RAW" not like '%0000%' ) and "CMENDAT_RAW" !=''
then to_date(concat(replace(replace(substring(upper("CMENDAT_RAW"),1,2),'UN','01'),'UK','01'),replace(substring(upper("CMENDAT_RAW"),3),'UNK','Jan')),'DD Mon YYYY') 
else null end AS cmendtc
from "tas120_201"."CM"	
)cm 

 )
,included_sites AS (
SELECT DISTINCT studyid, siteid, sitename, sitecountry,sitecountrycode, siteregion FROM site)

SELECT 
        /*KEY (cm.studyid || '~' || cm.siteid || '~' || cm.usubjid)::text AS comprehendid, KEY*/
        cm.studyid::text AS studyid,
        cm.studyname::text AS studyname,
        cm.siteid::text AS siteid,
        si.sitename::text AS sitename,
        si.sitecountry::text AS sitecountry,
        si.sitecountrycode::text AS sitecountrycode,
        si.siteregion::text AS siteregion,
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
        cm.cmentm::time without time zone AS cmentm,
		CMATCTXT1::text As CMATCTXT1,
		CMATCTXT2::text As CMATCTXT2,
		CMATCTXT3::text As CMATCTXT3
        /*KEY , (cm.studyid || '~' || cm.siteid || '~' || cm.usubjid || '~' || cm.cmseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM cm_data cm
JOIN included_subjects s ON (cm.studyid = s.studyid AND cm.siteid = s.siteid AND cm.usubjid = s.usubjid)
JOIN included_sites si ON (cm.studyid = si.studyid AND cm.siteid = si.siteid);


