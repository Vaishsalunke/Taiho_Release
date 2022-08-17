/*
CCDM AE mapping
Notes: Standard mapping to CCDM AE table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     ae_data AS (
                SELECT  'TAS117-201'::text AS studyid,
                        null::text AS studyname,
                        concat(project,substring("SiteNumber",position('_' in "SiteNumber"))) ::text AS siteid,
                        null::text AS sitename,
                        null::text AS sitecountry,
                        null::text AS sitecountrycode,
                        null::text AS siteregion,
                        "Subject" ::text AS usubjid,
                        nullif("AETERM_PT",'') ::text AS aeterm,
                        nullif("AETERM",'') ::text AS aeverbatim,
                        coalesce(nullif("AETERM_SOC",''),'Not Available') ::text AS aebodsys,
                        "AESTDAT" ::timestamp without time zone AS aestdtc,
                        "AEENDAT" ::timestamp without time zone AS aeendtc,
                        case when "AEGRAD"='1 - Mild' then 'G1'
                        			  when "AEGRAD"='2 - Moderate' then 'G2'
                        			  when "AEGRAD"='3 - Severe' then 'G3'
                        			  when "AEGRAD"='4 - Life-threatening' then 'G4'
                        			  when "AEGRAD"='5 - Death' then 'G5'
                        			  when ("AEGRAD"='' or "AEGRAD" is null) then 'Missing'
                        end::text AS aesev,
                        case when "AESER" ='Yes' then 'Serious'
                             when "AESER" ='No' then 'Non-Serious'
                             when ("AESER"='' or "AESER" is null) then 'Unknown'
                        end::text AS aeser,
                        case when lower("AEREL") in ('related','possibly related','definitely related','probably related') then 'Yes'
                        	 when lower("AEREL") in ('not related','unrelated') then 'No'
                        	 else 'Missing'
                        end::text AS aerelnst,
                        concat("RecordPosition","InstanceRepeatNumber","PageRepeatNumber")::int AS aeseq ,
                        "AESTDAT" ::time without time zone AS aesttm,
                        "AEENDAT" ::time without time zone AS aeentm,
                        "AETERM_LLT" ::text AS aellt,
                        nullif("AETERM_LLT_CD",'') ::int AS aelltcd,
                        nullif("AETERM_PT_CD",'') ::int AS aeptcd,
                        "AETERM_HLT" ::text AS aehlt,
                        nullif("AETERM_HLT_CD",'') ::int AS aehltcd,
                        "AETERM_HLGT" ::text AS aehlgt,
                        nullif("AETERM_HLGT_CD",'') ::int AS aehlgtcd,
                        nullif("AETERM_SOC_CD",'') ::int AS aebdsycd,
                        nullif("AETERM_SOC",'') ::text AS aesoc,
                        nullif("AETERM_SOC_CD",'') ::int AS aesoccd,
                        case when "ACN1" = 1 then 'None'
                        	 when "ACN2" = 1 then 'Dose Reduced'
                        	 when "ACN3" = 1 then 'Interrupted'
                        	 when "ACN4" = 1 then 'Discontinued'
                        else 'None' end::text AS aeacn,
                        nullif("AEOUT",'') ::text AS aeout,
                        null::text AS aetox,
                        "AEGRAD" ::text AS aetoxgr,
                        null::text AS aeongo,
                        null::text AS aecomm,
                        null::text AS aespid,
                        null::text AS aestdtc_iso,
                        null::text AS aeendtc_iso,
                        nullif("AETERM_PT",'') ::text AS preferredterm,
                        "MinCreated" ::timestamp without time zone AS aerptdt,
                        null::boolean AS aesi,
                        null::text AS aeepreli
                      from tas117_201."AE" a ),
	
     site_data as (select distinct studyid,siteid,sitename,sitecountry,sitecountrycode,siteregion from site)	

SELECT 
        /*KEY (ae.studyid || '~' || ae.siteid || '~' || ae.usubjid)::text AS comprehendid, KEY*/
        ae.studyid::text AS studyid,
        ae.studyname::text AS studyname,
        ae.siteid::text AS siteid,
        sd.sitename::text AS sitename,
        sd.sitecountry::text AS sitecountry,
        sd.sitecountrycode::text AS sitecountrycode,
        sd.siteregion::text AS siteregion,
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
        ae.aetox::text AS aetox,
        ae.aetoxgr::text AS aetoxgr,
        ae.aeongo::text AS aeongo,
        ae.aecomm::text AS aecomm,
        ae.aespid::text AS aespid,
        ae.aestdtc_iso::text AS aestdtc_iso,
        ae.aeendtc_iso::text AS aeendtc_iso,
        ae.preferredterm::text AS preferredterm,
        ae.aerptdt::timestamp without time zone AS aerptdt,
        ae.aesi::boolean AS aesi,
        ae.aeepreli::text AS aeepreli
        /*KEY , (ae.studyid || '~' || ae.siteid || '~' || ae.usubjid || '~' || ae.aeseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM ae_data ae
JOIN included_subjects s ON (ae.studyid = s.studyid AND ae.siteid = s.siteid AND ae.usubjid = s.usubjid)
join site_data sd on (ae.studyid = sd.studyid AND ae.siteid = sd.siteid);



