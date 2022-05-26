/*
CCDM DM mapping
Notes: Standard mapping to CCDM DM table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     dm_data AS (
                SELECT  distinct dm.project ::text AS studyid,
                        'TAS2940_101'::text AS studyname,
                        'TAS2940_101_' || split_part(dm."SiteNumber",'_',2)::text AS siteid,
                        null::text AS sitename,
                        null::text AS sitecountry,
                        dm."Subject" ::text AS usubjid,
                        dm."FolderSeq" ::numeric AS visitnum,
                        --dm."FolderName" ::text AS visit,
                        trim(dm."InstanceName") ::text AS visit,
                        coalesce (dm."MinCreated" ,dm."RecordDate") ::date AS dmdtc,
                        null::date AS brthdtc,
                        "DMAGE" ::integer AS age,
                        "DMSEX" ::text AS sex,
                        coalesce(nullif("DMRACE",''),nullif("DMOTH",'')) ::text AS race,
                        nullif("DMETHNIC",'') ::text AS ethnicity,
                        null::text AS armcd,
                        nullif(concat("ENRPHAS","ENRCOHO"),'')::text AS arm,
                        null::text AS brthdtc_iso
                     from TAS2940_101."DM" dm
                       	     --left join  tas120_204."ENR" enr
                       	     
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
join site_data sd on (dm.studyid = sd.studyid AND dm.siteid = sd.siteid)
;




