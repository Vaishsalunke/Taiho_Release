/*
CCDM DM mapping
Notes: Standard mapping to CCDM DM table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),
     
     ex_data as (select "project","SiteNumber","Subject",max("DAENDAT") as exendtc
     from tas117_201."DA" 
     group by 1,2,3),

     dm_data AS (
                SELECT  'TAS117-201' ::text AS studyid,
                        null::text AS studyname,
                        concat(d.project,substring(d."SiteNumber",position('_' in d."SiteNumber"))) ::text AS siteid,
                        null::text AS sitename,
                        null::text AS sitecountry,
                        d."Subject" ::text AS usubjid,
                        d."FolderSeq" ::numeric AS visitnum,
                        trim(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(REGEXP_REPLACE
						(d."InstanceName",'\s\([0-9][0-9]\)','')
									   ,'\s\([0-9]\)','')
									   ,' [0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')
									   ,' [0-9][0-9]\s[A-Z][a-z][a-z]\s[0-9][0-9][0-9][0-9]','')) ::text AS visit,
                        coalesce (d."MinCreated",d."RecordDate")::date AS dmdtc,
                        coalesce(e2."EOTLDAT",e3.exendtc)::date AS brthdtc,
                        "DMAGE" ::integer AS age,
                        "DMSEX" ::text AS sex,
                        coalesce ("DMRACE","DMOTH")::text AS race,
                        "DMETHNIC" ::text AS ethnicity,
                        null::text AS armcd,
                        nullif ("ENRSTDP",'')::text AS arm,
                        null::text AS brthdtc_iso
                        from tas117_201."DM" d 
                        left join tas117_201."ENR" e 
                        on d.project = e.project and d."SiteNumber"= e."SiteNumber" and d."Subject" =e."Subject"
                        left join tas117_201."EOT" e2 
                        on d.project = e2.project and d."SiteNumber"= e2."SiteNumber" and d."Subject" =e2."Subject"
                        left join ex_data e3 
                        on d.project = e3.project and d."SiteNumber"= e3."SiteNumber"and d."Subject" =e3."Subject"
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
        dm.arm::text AS brthdtc_iso
        /*KEY ,(dm.studyid || '~' || dm.siteid || '~' || dm.usubjid )::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM dm_data dm
JOIN included_subjects s ON (dm.studyid = s.studyid AND dm.siteid = s.siteid AND dm.usubjid = s.usubjid)
join site_data sd on (dm.studyid = sd.studyid AND dm.siteid = sd.siteid);











