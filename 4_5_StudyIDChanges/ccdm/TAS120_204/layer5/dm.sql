/*
CCDM DM mapping
Notes: Standard mapping to CCDM DM table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),
               
     exo_data AS(	select		project,"SiteNumber","Subject",max(exendtc) as max_exendtc 
     			from		(	select		project,"SiteNumber","Subject", "EXOCYCEDT" as exendtc 
     						from 		tas120_204."EXO"
       						union all
       						select 		project,"SiteNumber","Subject","EXOCYCEDT" as exendtc 
       						from 		tas120_204."EXO2"       
       					)a 
       			group by	1,2,3
       		),                  

     dm_data AS (
                SELECT  distinct 'TAS-120-204' ::text AS studyid,
                        'TAS120_204'::text AS studyname,
                        'TAS120_204_' || split_part(dm."SiteNumber",'_',2)::text AS siteid,
                        null::text AS sitename,
                        null::text AS sitecountry,
                        dm."Subject" ::text AS usubjid,
                        dm."FolderSeq" ::numeric AS visitnum,
                        trim(REGEXP_REPLACE
(REGEXP_REPLACE
(REGEXP_REPLACE
(REGEXP_REPLACE
(dm."InstanceName",'\s\([0-9][0-9]\)','')
  ,'\s\([0-9]\)','')
  ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
  ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')) ::text AS visit,
                        coalesce (dm."MinCreated" ,dm."RecordDate") ::date AS dmdtc,
                        coalesce(eot."EOTLDDAT",exo.max_exendtc)::date AS brthdtc,
                        "DMAGE" ::integer AS age,
                        "DMSEX" ::text AS sex,
                        coalesce("DMRACE" ,"DMOTH") ::text AS race,
                        "DMETHNIC" ::text AS ethnicity,
                        null::text AS armcd,
                        nullif(dm."ENRPHAS",'')::text AS arm,
                        null::text AS brthdtc_iso
                      from tas120_204."DM" dm
                      left join tas120_204."EOT" eot on 'TAS-120-204' = eot.project and dm."SiteNumber"=eot."SiteNumber" and dm."Subject"=eot."Subject"
                      left join exo_data exo on 'TAS-120-204' = exo.project and dm."SiteNumber"=exo."SiteNumber" and dm."Subject"=exo."Subject"      
                     
                        ),
site_data as (select distinct studyid,siteid,sitename,sitecountry,sitecountrycode,siteregion from site)

SELECT
        /*KEY (dm.studyid || '~' || dm.siteid || '~' || dm.usubjid)::text AS comprehendid, KEY*/
        dm.studyid::text AS studyid,
        dm.studyname::text AS studyname,
        dm.siteid::text AS siteid,
        sd.sitename::text AS sitename,
        sd.sitecountry::text AS sitecountry,
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
        dm.brthdtc_iso::text AS brthdtc_iso
         /*KEY,(dm.studyid || '~' || dm.siteid || '~' || dm.usubjid )::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM dm_data dm
JOIN included_subjects s ON (dm.studyid = s.studyid AND dm.siteid = s.siteid AND dm.usubjid = s.usubjid)
join site_data sd on (dm.studyid = sd.studyid AND dm.siteid = sd.siteid);