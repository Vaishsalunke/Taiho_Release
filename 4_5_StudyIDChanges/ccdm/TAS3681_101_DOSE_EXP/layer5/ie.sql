/*
CCDM IE mapping
Notes: Standard mapping to CCDM IE table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     ie_data AS (
				-- TAS3681-101
				select studyid,
				siteid,
				usubjid,
				visitnum,
				visit,
				iedtc,
				row_number() OVER (PARTITION BY studyid,siteid,usubjid ORDER BY iedtc)::int AS ieseq,
                ietestcd,
                ietest,
                iecat,
                iescat
                from
                (
               SELECT  'TAS3681_101_DOSE_EXP'::text AS studyid,
                        "SiteNumber"::text AS siteid,
                        "Subject"::text AS usubjid,
                        "FolderSeq"::numeric AS visitnum,
                       -- "FolderName"::text AS visit,
                        "InstanceName"::text AS visit,
                        max(COALESCE("MinCreated", "RecordDate"))::date AS iedtc,
                        null::int as ieseq,
						max("IETESTCD")::text AS ietestcd,
                        max(case 
                            when "IEYN"='No' 
                                    then "IETESTCD"
                    	end)::text AS ietest,
                        nullif("IECAT",'')::text AS iecat,
                        null::text AS iescat
                from tas3681_101."IE"	ie	
				group by 1,2,3,4,5,7,10,11
				)a
				)

SELECT 
        /*KEY (ie.studyid || '~' || ie.siteid || '~' || ie.usubjid)::text AS comprehendid, KEY*/
        ie.studyid::text AS studyid,
        ie.siteid::text AS siteid,
        ie.usubjid::text AS usubjid,
        ie.visitnum::numeric AS visitnum,
        ie.visit::text AS visit,
        ie.iedtc::date AS iedtc,
        ie.ieseq::integer AS ieseq,
        ie.ietestcd::text AS ietestcd,
        ie.ietest::text AS ietest,
        ie.iecat::text AS iecat,
        ie.iescat::text AS iescat
        /*KEY , (ie.studyid || '~' || ie.siteid || '~' || ie.usubjid || '~' || ie.ieseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM ie_data ie 
JOIN included_subjects s ON (ie.studyid = s.studyid AND ie.siteid = s.siteid AND ie.usubjid = s.usubjid)
 ;


