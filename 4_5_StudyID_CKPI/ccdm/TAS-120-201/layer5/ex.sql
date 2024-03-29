/*
CCDM EX mapping
Notes: Standard mapping to CCDM EX table
*/
WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     ex_data AS (
SELECT
     replace (studyid,'TAS120_201','TAS-120-201') as studyid,
     siteid,
     usubjid,
     row_number() over (partition by ex."studyid", ex."siteid", ex."usubjid" ORDER BY ex."exstdtc")::int AS exseq,
	--- exseq,
     visit,
     extrt,
     excat,
     exscat,
     exdose,
     exdostxt,
     exdosu,
     exdosfrm,
     exdosfrq,
     exdostot,
     exstdtc,
     exsttm,
     exstdy,
     exendtc,
     exendtm,
     exendy,
     exdur,
	 exacn,
	 exreason
	 ,null::text AS studyname
	 ,null::text AS drugrsp
	 ,null::text AS drugrspcd
     from
     
 (
        SELECT 
        "SiteNumber"::text	AS	siteid,
        "Subject"::text	AS	usubjid,
        NULL::INT  AS exseq,
        "project"::text	AS	studyid,
        trim(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			("InstanceName",'\s\([0-9][0-9]\)','')
						   ,'\s\([0-9]\)','')
						   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
						   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: text as visit,
		'TAS120-201-Dose Adjusted'::text	AS	extrt,
        'Metastatic Breast Cancer'::text	AS	excat,
        NULL::text	AS	exscat,
        "EXOSDOSE"::numeric	AS	exdose,
        --case when "EXOADJYN" = 'Yes' then "EXOSDOSE" else "EXOPDOSE" end::numeric	AS	exdose,
        NULL::text	AS	exdostxt,
        "EXOSDOSE_Units"::text	AS	exdosu,
        ---case when "EXOADJYN" = 'Yes' then "EXOSDOSE_Units" else "EXOPDOSE_Units" end::text	AS	exdosu,
        NULL::text	AS	exdosfrm,
        NULL::text	AS	exdosfrq,
        NULL::text	AS	exdostot,
        --coalesce("EXOCYCSDT","EXOSTDAT")::text	AS	exstdtc,
		"EXOSTDAT"::date AS exstdtc,
        NULL::time without time zone	AS	exsttm,
        NULL::text	AS	exstdy,
        "EXOENDAT"::date	AS	exendtc,
        NULL::time without time zone	AS	exendtm,
        NULL::text	AS	exendy,
        NULL::text	AS	exdur,
		"EXOADJRE"::text AS exacn,
		"EXOMIDRE"::text AS exreason
       from tas120_201."EXO"
       where "EXOSTDAT" is not null
        
        UNION all
        			
         SELECT 
        "SiteNumber"::text	AS	siteid,
        "Subject"::text	AS	usubjid,
        NULL::INT  AS exseq,
        "project"::text	AS	studyid,
        trim(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			("InstanceName",'\s\([0-9][0-9]\)','')
						   ,'\s\([0-9]\)','')
						   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
						   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: text as visit,
		'TAS120-201-Planned Dose'::text	AS	extrt,
        'Metastatic Breast Cancer'::text	AS	excat,
        NULL::text	AS	exscat,
     	"EXOPDOSE"::numeric	AS	exdose,
        --case when "EXOADJYN" = 'Yes' then "EXOSDOSE" else "EXOPDOSE" end::numeric	AS	exdose,
        NULL::text	AS	exdostxt,
        "EXOPDOSE_Units"::text	AS	exdosu,
        ---case when "EXOADJYN" = 'Yes' then "EXOSDOSE_Units" else "EXOPDOSE_Units" end::text	AS	exdosu,
        NULL::text	AS	exdosfrm,
        NULL::text	AS	exdosfrq,
        NULL::text	AS	exdostot,
        --coalesce("EXOCYCSDT","EXOSTDAT")::text	AS	exstdtc,
		"EXOCYCSDT"::date AS exstdtc,
        NULL::time without time zone	AS	exsttm,
        NULL::text	AS	exstdy,
       "EXOCYCEDT"::date	AS	exendtc,
        NULL::time without time zone	AS	exendtm,
        NULL::text	AS	exendy,
        NULL::text	AS	exdur,
		"EXOADJRE"::text AS exacn,
		"EXOMIDRE"::text AS exreason
       from tas120_201."EXO"
       where "EXOCYCSDT" is not null
        
        union all 
        
        SELECT 
        "SiteNumber"::text	AS	siteid,
        "Subject"::text	AS	usubjid,
       	NULL::INT  AS exseq,
        "project"::text	AS	studyid,
        trim(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			("InstanceName",'\s\([0-9][0-9]\)','')
						   ,'\s\([0-9]\)','')
						   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
						   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: text as visit,
        'Fulvestrant-Dose Adjusted'::text	AS	extrt,
        'Metastatic Breast Cancer'::text	AS	excat,
        NULL::text	AS	exscat,
        "EXISDOSE"::numeric	AS	exdose,
        NULL::text	AS	exdostxt,
        "EXISDOSE_Units"::text	AS	exdosu,
        NULL::text	AS	exdosfrm,
        NULL::text	AS	exdosfrq,
        NULL::text	AS	exdostot,
        "EXISTDAT"::date	AS	exstdtc,
        NULL::time without time zone	AS	exsttm,
        NULL::text	AS	exstdy,
        null::date AS exendtc,
        NULL::time without time zone	AS	exendtm,
        NULL::text	AS	exendy,
        NULL::text	AS	exdur,
		Null::text AS exacn,
		Null::text AS exreason
        from tas120_201."EXI"
        where  "EXISTDAT" is not null 
        
union all 
        	
			        SELECT 
        "SiteNumber"::text	AS	siteid,
        "Subject"::text	AS	usubjid,
       	NULL::INT  AS exseq,
        "project"::text	AS	studyid,
        trim(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			(REGEXP_REPLACE
			("InstanceName",'\s\([0-9][0-9]\)','')
						   ,'\s\([0-9]\)','')
						   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
						   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')):: text as visit,
        'Fulvestrant-Planned Dose'::text	AS	extrt,
        'Metastatic Breast Cancer'::text	AS	excat,
        NULL::text	AS	exscat,
       "EXIPDOSE"::numeric	AS	exdose,
        NULL::text	AS	exdostxt,
        "EXIPDOSE_Units"::text	AS	exdosu,
        NULL::text	AS	exdosfrm,
        NULL::text	AS	exdosfrq,
        NULL::text	AS	exdostot,
        "EXISTDAT"::date	AS	exstdtc,
        NULL::time without time zone	AS	exsttm,
        NULL::text	AS	exstdy,
        null::date AS exendtc,
        NULL::time without time zone	AS	exendtm,
        NULL::text	AS	exendy,
        NULL::text	AS	exdur,
		Null::text AS exacn,
		Null::text AS exreason
        from tas120_201."EXI"
        where  "EXISTDAT" is not null 
        
) ex )
		,included_sites AS (
SELECT DISTINCT studyid, siteid, sitename, sitecountry,sitecountrycode, siteregion FROM site)

SELECT
        /*KEY (ex.studyid || '~' || ex.siteid || '~' || ex.usubjid)::text AS comprehendid, KEY*/
        ex.studyid::text AS studyid,
        ex.studyname::text AS studyname,
        ex.siteid::text AS siteid,
        si.sitename::text AS sitename,
        si.sitecountry::text AS sitecountry,
        ex.usubjid::text AS usubjid,
        ex.exseq::int AS exseq, 
        ex.visit::text AS visit,
        ex.extrt::text AS extrt,
        ex.excat::text AS excat,
        ex.exscat::text AS exscat,
        ex.exdose::numeric AS exdose,
        ex.exdostxt::text AS exdostxt,
        ex.exdosu::text AS exdosu,
        ex.exdosfrm::text AS exdosfrm,
        ex.exdosfrq::text AS exdosfrq,
        ex.exdostot::numeric AS exdostot,
        ex.exstdtc::date AS exstdtc,
        ex.exsttm::time AS exsttm,
        ex.exstdy::int AS exstdy,
        ex.exendtc::date AS exendtc,
        ex.exendtm::time AS exendtm,
        ex.exendy::int AS exendy,
        ex.exdur::text AS exdur,
        ex.drugrsp::text AS drugrsp,
        ex.drugrspcd::text AS drugrspcd,
		ex.exacn::text AS exacn,
		ex.exreason::text AS exreason
        /*KEY , (ex.studyid || '~' || ex.siteid || '~' || ex.usubjid || '~' || ex.exseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM ex_data ex
JOIN included_subjects s ON (ex.studyid = s.studyid AND ex.siteid = s.siteid AND ex.usubjid = s.usubjid)
JOIN included_sites si ON (ex.studyid = si.studyid AND ex.siteid = si.siteid);



