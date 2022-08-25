/*
CCDM DM mapping
Notes: Standard mapping to CCDM DM table
*/
with included_subjects as ( select 	distinct studyid, siteid, usubjid from subject ),

	
included_sites AS (
SELECT DISTINCT studyid, siteid, sitename, sitecountry,sitecountrycode, siteregion FROM site),	

ex_data as (select		'TAS3681_101_DOSE_EXP' as "project","SiteNumber","Subject",max("exendtc") as max_exendtc
				 from	(
							select	"project","SiteNumber","Subject",e1."EXOENDAT" as exendtc from tas3681_101."EXO" e1
							union all
							select	"project","SiteNumber","Subject",e2."EXOENDAT" as exendtc from tas3681_101."EXO2" e2
							
				 )ex
				group by	1,2,3
				),

dm_dm2 as( select distinct 	a.studyid,
					a.siteid,
					a.usubjid,
					a.visitnum,
					a.visit,
					a.dmdtc,
					max_exendtc:: date as brthdtc,
					a.age,
					a.sex,
					a.race,
					a.ethnicity,
					a.armcd,
					a.arm
					from
					(
					  select Distinct 'TAS3681_101_DOSE_EXP'::text as studyid,
							 dm."SiteNumber"::text as siteid,
							 dm."Subject"::text as usubjid,
							 dm."FolderSeq"::numeric as visitnum,
							 --dm."FolderName" :: text as visit,
							 dm."InstanceName" :: text as visit,
							 COALESCE(dm."MinCreated", dm."RecordDate"):: date as dmdtc,
							 null:: date as brthdtc,
							 dm."DMAGE"::integer as age,
							 dm."DMSEX"::text as sex,
							 coalesce(dm."DMRACE", dm."DMOTH")::text as race,
							 dm."DMETHNIC"::text as ethnicity,
							 null:: text as armcd,
							 case
								 when dm."DataPageName" = 'Demography-Expansion' then 'Dose Expansion'||' '||ie."IEEXPGRP"
								 else dlt."DLTCOH"||' '||ie."IEEXPGRP"::text
							 end:: text as arm
					  from	 tas3681_101."DM" dm
					  left join tas3681_101."DLT" dlt 
					  on	   (dm."project" = dlt.project and 
								dm."SiteNumber" = dlt."SiteNumber" and 
								dm."Subject" = dlt."Subject")
					  left join tas3681_101."IE" ie 
					  on	   (dm."project" = ie.project and 
								dm."SiteNumber" = ie."SiteNumber" and 
								dm."Subject" = ie."Subject")		
					  
					  union all
					  
					  select distinct 'TAS3681_101_DOSE_EXP'::text as studyid,
							 dm2."SiteNumber"::text as siteid,
							 dm2."Subject"::text as usubjid,
							 dm2."FolderSeq"::numeric as visitnum,
							 dm2."InstanceName" :: text as visit,
							 COALESCE(dm2."MinCreated", dm2."RecordDate"):: date as dmdtc,
							null:: date as brthdtc,
							 dm2."DMAGE"::integer as age,
							 dm2."DMSEX"::text as sex,
							 coalesce(dm2."DMRACE", dm2."DMOTH")::text as race,
							 dm2."DMETHNIC"::text as ethnicity,
							 null:: text as armcd,
							 case
								 when dm2."DataPageName" = 'Demography-Expansion' then 'Dose Expansion'||' '||ie."IEEXPGRP"
								 else dlt."DLTCOH"||' '||ie."IEEXPGRP"::text
							 end:: text as arm
					  from	 tas3681_101."DM2" dm2
					  left join tas3681_101."DLT" dlt 
					  on	   (dm2."project" = dlt.project and 
								dm2."SiteNumber" = dlt."SiteNumber" and 
								dm2."Subject" = dlt."Subject")
					  left join tas3681_101."IE" ie 
					  on	   (dm2."project" = ie.project and 
								dm2."SiteNumber" = ie."SiteNumber" and 
								dm2."Subject" = ie."Subject")
						where (ie."Subject",ie."SiteNumber",ie.serial_id) in (select "Subject","SiteNumber",max(serial_id) 
						from tas3681_101."IE" ie 
						group by 1,2)		
					  )a
					  left join ex_data e3 
                        on a.studyid = e3.project and a.siteid= e3."SiteNumber"and a.usubjid =e3."Subject"
	)
select
	/*KEY (dm.studyid || '~' || dm.siteid || '~' || dm.usubjid)::text AS comprehendid, KEY*/
	dm.studyid::text as studyid,
	null::text AS studyname,
	dm.siteid::text as siteid,
	si.sitename::text AS sitename,
	si.sitecountry::text AS sitecountry,
	dm.usubjid::text as usubjid,
	dm.visitnum::numeric as visitnum,
	dm.visit::text as visit,
	dm.dmdtc::date as dmdtc,
	dm.brthdtc::date as brthdtc,
	dm.age::integer as age,
	dm.sex::text as sex,
	dm.race::text as race,
	dm.ethnicity::text as ethnicity,
	dm.armcd::text as armcd,
	dm.arm::text as arm,
	dm.arm::text AS brthdtc_iso
	/*KEY , (dm.studyid || '~' || dm.siteid || '~' || dm.usubjid)::text  AS objectuniquekey KEY*/
	/*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
from
	dm_dm2 dm join included_subjects s on (dm.studyid = s.studyid and dm.siteid = s.siteid and dm.usubjid = s.usubjid)
	JOIN included_sites si ON (dm.studyid = si.studyid AND dm.siteid = si.siteid);
	
