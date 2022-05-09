WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),
             
 included_sites AS (
SELECT DISTINCT studyid, siteid, sitename, sitecountry,sitecountrycode, siteregion FROM site),  

     ae_sev_toxgr_data AS (
                SELECT  "project"::text AS studyid,
                        concat(concat("project",'_'),split_part("SiteNumber",'_',2))::text AS siteid,
                        "Subject"::text AS usubjid,
                        "AETERM_PT"::text AS aeterm,
                        concat("RecordPosition","PageRepeatNumber")::integer AS aeseq,
                        "AESPID" ::text AS aespid,
                        concat("RecordPosition","PageRepeatNumber")::integer AS faseq,
                        'SEV'::text AS fatestcd,
                        'Severity'::text AS fatest,
                        case when "AEGRAD"='1 - Mild' then 'G1'
                        			  when "AEGRAD"='2 - Moderate' then 'G2'
                        			  when "AEGRAD"='3 - Severe' then 'G3'
                        			  when "AEGRAD"='4 - Life-threatening' then 'G4'
                        			  when "AEGRAD"='5 - Death' then 'G5'
                        			  when ("AEGRAD"='' or "AEGRAD" is null) then 'Missing'
                        end::text AS faorres,
      "AEGSDAT" ::timestamp without time zone AS fastdtc,
                        "AEGEDAT" ::timestamp without time zone AS faendtc
              FROM tas117_201."AE"
                 WHERE NULLIF("AETERM_PT", '') IS NOT null
                )

SELECT 
        /*KEY (aegrd.studyid || '~' || aegrd.siteid || '~' || aegrd.usubjid)::text AS comprehendid, KEY*/
        aegrd.studyid::text AS studyid,
        aegrd.siteid::text AS siteid,
        aegrd.usubjid::text AS usubjid,
        aegrd.aeterm::text AS aeterm,
        aegrd.aeseq::integer AS aeseq,
        aegrd.aespid::text AS aespid,
        aegrd.faseq::integer AS faseq,
        aegrd.fatestcd::text AS fatestcd,        
        aegrd.fatest::text AS fatest,
        aegrd.faorres::text AS faorres,
        aegrd.fastdtc::date AS fastdtc,
        aegrd.faendtc::date AS faendtc
        /*KEY , (aegrd.studyid || '~' || aegrd.siteid || '~' || aegrd.usubjid || '~' || aegrd.aeseq || '~' || aegrd.faseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM ae_sev_toxgr_data aegrd
JOIN included_subjects s ON (aegrd.studyid = s.studyid AND aegrd.siteid = s.siteid AND aegrd.usubjid = s.usubjid)
JOIN included_sites si ON (aegrd.studyid = si.studyid AND aegrd.siteid = si.siteid);

