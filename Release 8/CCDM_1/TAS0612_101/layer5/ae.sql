/*
CCDM AE mapping
Notes: Standard mapping to CCDM AE table
*/

WITH included_subjects AS (
    SELECT DISTINCT studyid, siteid, usubjid FROM subject),
included_site AS (
	SELECT DISTINCT studyid, siteid, sitename, sitecountry, sitecountrycode, siteregion FROM site),	

    ae_data AS (
SELECT "project"::text AS studyid,
                       concat(concat("project",'_'),split_part("SiteNumber",'_',2))::text AS siteid,
                       ae."Subject"::text AS usubjid,
                       nullif(ae."AETERM_PT",'')::text AS aeterm,
                       nullif(ae."AETERM",'')::text AS aeverbatim,
                       coalesce(nullif(ae."AETERM_SOC",''),'Not Available')::text AS aebodsys,
                       ae."AESTDAT"::timestamp without time zone AS aestdtc,
                       ae."AEENDAT"::timestamp without time zone AS aeendtc,
					   case when(ae."AEGR")= '0' then 'G0'
							when(ae."AEGR")= '1' then 'G1'
							when(ae."AEGR")= '2' then 'G2'
							when(ae."AEGR")= '3' then 'G3'
							when(ae."AEGR")= '4' then 'G4'
							when(ae."AEGR")= '5' then 'G5'															   
		                    when((ae."AEGR")= '' or (ae."AEGR") is null) then 'Missing'
						end::text AS aesev,
					   case when lower(ae."AESER")='yes' then 'Serious' 
                       when lower(ae."AESER")='no' then 'Non-Serious' 
                       else 'Unknown' end::text AS aeser ,
					   CASE WHEN lower("AEREL") in ('possibly related','definitely related','probably related','related') THEN 'Yes' 
					   WHEN lower("AEREL") in ('unrelated','not reasonably possible','not related') THEN 'No'  
					   else 'Unknown' END::text AS aerelnst,

					   ae."AESTDAT"::time without time zone AS aesttm,
					   ae."AEENDAT"::time without time zone AS aeentm,
                       ae."AETERM_LLT" ::text AS aellt,
					   nullif(ae."AETERM_LLT_CD"::text,''):: int AS aelltcd,
					   nullif(ae."AETERM_PT_CD"::text,''):: int AS aeptcd,
					   ae."AETERM_HLT" ::text AS aehlt,
					   nullif(ae."AETERM_HLT_CD"::text,''):: int AS aehltcd,
					   ae."AETERM_HLGT" ::text AS aehlgt,
					   nullif(ae."AETERM_HLGT_CD"::text,''):: int AS aehlgtcd,
					   nullif(ae."AETERM_SOC_CD"::text,''):: int AS aebdsycd,
					   nullif(ae."AETERM_SOC",'') ::text AS aesoc,
					   nullif(ae."AETERM_SOC_CD"::text,'') :: int AS aesoccd,
					  -- ROW_NUMBER () OVER (PARTITION BY "project", ae."SiteNumber", ae."Subject" ORDER BY ae."AESTDAT") as aeseq,
                      concat("RecordPosition","PageRepeatNumber")::int as aeseq,
					  coalesce(ae."AEACTSN")::text  AS aeacn,
					  "AEOUT"::Text as aeout,
					  "AEGR":: Text as aetoxgr,
					  "MinCreated":: date as aerptdt,
					  nullif("AETERM_PT",''):: Text as preferredterm,
					  null::boolean as aesi
		    FROM TAS0612_101."AE2" ae 
  )

SELECT 
        /*KEY (ae.studyid || '~' || ae.siteid || '~' || ae.usubjid)::text AS comprehendid, KEY*/
        ae.studyid::text AS studyid,
        null::text AS studyname,
        ae.siteid::text AS siteid,
        si.sitename::text AS sitename,
        si.sitecountry::text AS sitecountry,
        si.sitecountrycode::text AS sitecountrycode,
        si.siteregion::text AS siteregion,
        ae.usubjid::text AS usubjid,
        ae.aeterm::text AS aeterm,
        ae.aeverbatim::text AS aeverbatim,
        ae.aebodsys::text AS aebodsys,
        ae.aestdtc::timestamp without time zone AS aestdtc,
        ae.aeendtc::timestamp without time zone AS aeendtc,
        ae.aesev::text AS aesev,
        ae.aeser::text AS aeser,
        ae.aerelnst::text AS aerelnst,
        ae.aeseq::int AS aeseq,
        ae.aesttm::time without time zone AS aesttm,
        ae.aeentm::time without time zone AS aeentm,
        ae.aellt::text AS aellt,
        ae.aelltcd::int AS aelltcd,
        ae.aeptcd::int AS aeptcd,
        ae.aehlt::text AS aehlt,
        ae.aehltcd::int AS aehltcd,
        ae.aehlgt::text AS aehlgt,
        ae.aehlgtcd::int AS aehlgtcd,
        ae.aebdsycd::int AS aebdsycd,
        ae.aesoc::text AS aesoc,
        ae.aesoccd::int AS aesoccd,
        ae.aeacn::text AS aeacn,
        ae.aeout::text AS aeout,
        null::text AS aetox,
        ae.aetoxgr::text AS aetoxgr,
        null::text AS aeongo,
        null::text AS aecomm,
        null::text AS aespid,
        null::text AS aestdtc_iso,
        null::text AS aeendtc_iso,
		ae.aerptdt::timestamp without time zone AS aerptdt,
        ae.preferredterm::text AS preferredterm,
        ae.aesi::boolean AS aesi,
        null::text AS aeepreli
        /*KEY , (ae.studyid || '~' || ae.siteid || '~' || ae.usubjid || '~' || ae.aeseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM ae_data ae
JOIN included_subjects s ON (ae.studyid = s.studyid AND ae.siteid = s.siteid AND ae.usubjid = s.usubjid)
JOIN included_site si ON (ae.studyid = si.studyid AND ae.siteid = si.siteid);
