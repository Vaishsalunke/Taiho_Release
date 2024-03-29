/*
CCDM IE mapping
Notes: Standard mapping to CCDM IE table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     ie_data AS(
		
                SELECT distinct 'TAS-120-203'::text AS studyid,
                        'TAS120_203_' || split_part("SiteNumber",'_',2)::text AS siteid,
                        "Subject"::text AS usubjid,
                        row_number() OVER (PARTITION BY ie.studyid, ie.siteid, ie."Subject" ORDER BY ie."serial_id")::integer AS ieseq,
                        "FolderSeq"::numeric AS visitnum,
                        trim(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			("InstanceName",'\s\([0-9][0-9]\)','')
						   ,'\s\([0-9]\)','')
						   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
						   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: text as visit,
                        COALESCE("MinCreated","RecordDate")::date AS iedtc,
                        nullif("IETESTCD",'')::text AS ietestcd,
                        nullif("IETESTCD",'')::text AS ietest,
                        null::text AS iecat,
                        null::text AS iescat 
                        from TAS120_203."IC" ie)

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
        /*KEY, (ie.studyid || '~' || ie.siteid || '~' || ie.usubjid || '~' || ie.ieseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM ie_data ie 
JOIN included_subjects s ON (ie.studyid = s.studyid AND ie.siteid = s.siteid AND ie.usubjid = s.usubjid);



