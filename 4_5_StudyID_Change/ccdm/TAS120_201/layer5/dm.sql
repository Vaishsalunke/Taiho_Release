/*
CCDM DM mapping
Notes: Standard mapping to CCDM DM table
*/

with included_subjects as ( select 	distinct studyid, siteid, usubjid from subject ),

ex_data AS(select "project", "SiteNumber", "Subject",max("EXOCYCEDT") as max_exendtc
    from tas120_201."EXO" group by 1,2,3),

dm_data as(select 	distinct	'TAS-120-201'::text as studyid,
					dm."SiteNumber"::text as siteid,
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
					coalesce(DS."DSDAT"  ,max_exendtc):: date as brthdtc,
					nullif(dm."DMAGE",'')::integer as age,
					dm."DMSEX"::text as sex,
					coalesce(dm."DMRACE", dm."DMOTH")::text as race,
					dm."DMETHNIC"::text as ethnicity,
					null:: text as armcd,
					e."COHORT":: text as arm
					,null::text AS studyname
					--,null::text AS sitename
					--,null::text AS sitecountry
					,null::text AS brthdtc_iso
		 from		tas120_201."DM" dm
		 left join 	tas120_201."ENR" e  on dm.project=e.project  and dm."SiteNumber" =e."SiteNumber"  and dm."Subject" =e."Subject"
		 left join 	tas120_201."DS" DS on  dm.project=DS.project  and dm."SiteNumber" =DS."SiteNumber"  and dm."Subject" =DS."Subject"
		 left join 	ex_data e2  on dm.project=e2.project  and dm."SiteNumber" =e2."SiteNumber"  and dm."Subject" =e2."Subject"
		 	 )
		,included_sites AS (
                SELECT DISTINCT studyid, siteid, sitename, sitecountry,sitecountrycode, siteregion FROM site )
SELECT 
        /*KEY (dm.studyid || '~' || dm.siteid || '~' || dm.usubjid)::text AS comprehendid, KEY*/
        dm.studyid::text AS studyid,
        dm.studyname::text AS studyname,
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
FROM dm_data dm
JOIN included_subjects s ON (dm.studyid = s.studyid AND dm.siteid = s.siteid AND dm.usubjid = s.usubjid)
JOIN included_sites si ON(dm.siteid=si.siteid AND dm.studyid=si.studyid);





