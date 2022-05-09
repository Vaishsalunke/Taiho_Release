/*
CCDM DM mapping
Notes: Standard mapping to CCDM DM table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     dm_data AS (
                select distinct  dm.project ::text AS studyid,
                        'TAS120_203'::text AS studyname,
                        dm.project||substring(dm."SiteNumber",position ('_' in dm."SiteNumber"))::text AS siteid,
                        null::text AS sitename,
                        null::text AS sitecountry,
                        dm."Subject" ::text AS usubjid,
                        dm."FolderSeq" ::numeric AS visitnum,
                        dm."FolderName" ::text AS visit,
                        coalesce (dm."MinCreated" ,dm."RecordDate") ::date AS dmdtc,
                        null::date AS brthdtc,
                        "DMAGE" ::integer AS age,
                        "DMSEX" ::text AS sex,
                        coalesce("DMRACE" ,"DMOTH") ::text AS race,
                        "DMETHNIC" ::text AS ethnicity,
                        null::text AS armcd,
                        nullif(enr."ENRCOHO",'')::text AS arm,
                        null::text AS brthdtc_iso
                        from tas120_203."DM" dm
                       	     left join tas120_203."ENR" enr
                       	     on dm.project =enr.project
								and dm."SiteNumber" =enr."SiteNumber"
								and dm."Subject" =enr."Subject" ),
                        
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
        dm.arm::text AS brthdtc_iso
        /*KEY ,(dm.studyid || '~' || dm.siteid || '~' || dm.usubjid )::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM dm_data dm
JOIN included_subjects s ON (dm.studyid = s.studyid AND dm.siteid = s.siteid AND dm.usubjid = s.usubjid)
join site_data sd on (dm.studyid = sd.studyid AND dm.siteid = sd.siteid);

