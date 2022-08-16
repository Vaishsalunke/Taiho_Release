/*
CCDM DM mapping
Notes: Standard mapping to CCDM DM table
*/


with included_subjects as ( select 	distinct studyid, siteid, usubjid from subject ),
	
	included_site AS (
	SELECT DISTINCT studyid, siteid, sitename, sitecountry, sitecountrycode, siteregion FROM site),
	
	ex_data as(
				select  	"project","SiteNumber","Subject",max("EXOENDAT") as max_exendtc
				FROM 		tas0612_101."EXO" 
				group by	1,2,3
			  ),
	

dm_dm2 as(select	dm."project"::text as studyid,
					concat('TAS0612_101_',split_part(dm."SiteNumber",'_',2))::text as siteid,
					dm."Subject"::text as usubjid,
					dm."FolderSeq"::numeric as visitnum,
					trim(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(dm."InstanceName",'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')) :: text as visit,
					COALESCE(dm."MinCreated", dm."RecordDate"):: date as dmdtc,
					max_exendtc:: date as brthdtc,
					dm."DMAGE"::integer as age,
					dm."DMSEX"::text as sex,
					coalesce(dm."DMRACE", dm."DMOTH")::text as race,
					dm."DMETHNIC"::text as ethnicity,
					tr."TAPHASE" :: text as armcd,
					tr."TAPHASE" :: text as arm
		 from		tas0612_101."DM" dm
		 left join ex_data e3 on dm."project" = e3.project and dm."SiteNumber"= e3."SiteNumber"and dm."Subject" =e3."Subject"
		 left join tas0612_101."TREAT" tr on dm."project" = tr.project and dm."SiteNumber" = tr."SiteNumber" and dm."Subject" = tr."Subject" 
		 ) 
SELECT 
        /*KEY (dm.studyid || '~' || dm.siteid || '~' || dm.usubjid)::text AS comprehendid, KEY*/
        dm.studyid::text AS studyid,
        null::text AS studyname,
        dm.siteid::text AS siteid,
        si.sitename::text AS sitename,
        si.sitecountry::text AS sitecountry,
        dm.usubjid::text AS usubjid,
        dm.visitnum::numeric AS visitnum,
        dm.visit::text AS visit,
        dm.dmdtc::date AS dmdtc,
        dm.brthdtc::date AS brthdtc,
        dm.age::integer AS age,
        dm.sex::text AS sex,
        dm.race::text AS race,
        dm.ethnicity::text AS ethnicity,
        dm.armcd::text AS armcd,
        dm.arm::text AS arm,
        dm.arm::text AS brthdtc_iso
        /*KEY ,(dm.studyid || '~' || dm.siteid || '~' || dm.usubjid )::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM dm_dm2 dm
JOIN included_subjects s ON (dm.studyid = s.studyid AND dm.siteid = s.siteid AND dm.usubjid = s.usubjid)
JOIN included_site si ON (dm.studyid = si.studyid AND dm.siteid = si.siteid);








