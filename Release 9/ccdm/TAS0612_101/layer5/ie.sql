/*
CCDM IE mapping
Notes: Standard mapping to CCDM IE table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     ie_data AS (
				SELECT  "project"::text AS studyid,
                        concat(concat("project",'_'),split_part("SiteNumber",'_',2))::text AS siteid,
                        "Subject"::text AS usubjid,
                        "FolderSeq"::numeric AS visitnum,
                        trim(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						("InstanceName",'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]',''))::text AS visit,
                        max(COALESCE("MinCreated", "RecordDate"))::date AS iedtc,
                        row_number() OVER (PARTITION BY "project","SiteNumber","Subject" ORDER BY "serial_id")::int AS ieseq,
						max("IETESTCD")::text AS ietestcd,
                        max(case 
                        		when "IEYN"='No' 
                        				then "IETESTCD"
                        	end)::text AS ietest,
                        nullif("IECAT",'')::text AS iecat,
                        null::text AS iescat
                from tas0612_101."IE"	ie	
				group by "project","SiteNumber","Subject","FolderSeq","InstanceName","serial_id",iecat,iescat
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
JOIN included_subjects s ON (ie.studyid = s.studyid AND ie.siteid = s.siteid AND ie.usubjid = s.usubjid);




