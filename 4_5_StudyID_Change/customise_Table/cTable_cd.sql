/*Mapping script for Cancer Diagnosis - Listing
Table name : "ctable_listing"."cTable_cd"
Project name : Taiho*/

CREATE SCHEMA IF NOT EXISTS "ctable_listing";

drop table if exists "ctable_listing"."cTable_cd";

create table "ctable_listing"."cTable_cd" as
with cd as	(
			
			--Study TAS0612_101.CD Data set 
			select	distinct 'TAS0612-101' as project,
					concat('TAS0612_101_',split_part("SiteNumber",'_',2)) as sitenumber,
					split_part("Site",'_',2) as site,
					"Subject" as subject,
					"DataPageName" as datapagename,
					"InstanceName" as instancename,
					"CDTYPE" as CDTYPE ,
					"CDTYOTH" as CDTYOTH,
					"CDINIDAT" as CDINIDAT,
					"CDITDTN" as CDITDTN,
					"CDTUMLOC" as CDTUMLOC,
					"CDSTAGE" as CDSTAGE,
					"CDSTOTH" as CDSTOTH,
					"CDIND" as CDIND,
					"CDMLOC" as CDMLOC,
					"CDMLOTH" as CDMLOTH,
					"CDMTDTN" as CDMTDTN,
					"CDMLOCD" as CDMLOCD,
					"CDMETDAT" as CDMETDAT
			from 	tas0612_101."CD"
			
			union 
			
			--Study TAS120_201.CD Data set
			select	distinct 'TAS-120-201' as project,
					concat('TAS120_201_',split_part("SiteNumber",'_',2)) as SiteNumber,
					split_part("Site",'_',2) as Site,
					"Subject" as Subject,
					"DataPageName" as DataPageName,
					"InstanceName" as InstanceName,
					"CDTYPE" as CDTYPE,
					null::text as CDTYOTH ,--
					"CDINIDAT" as CDINIDAT,
					"CDITDTN" as CDITDTN ,
					null::text as CDTUMLOC,
					null::text as CDSTAGE,
					null::text as CDSTOTH,
					null::text as CDIND,
					null::text as CDMLOC,
					null::text as CDMLOTH,
					null::numeric as CDMTDTN,
					null::text as CDMLOCD,
					null::timestamp as CDMETDAT
			from 	tas120_201."CD"
			
			union
			
			--Study TAS120_202.CD Data set 
			select	distinct 'TAS-120-202' as project,
					concat('TAS120_202_',split_part("SiteNumber",'_',2)) as SiteNumber,
					split_part("Site",'_',2) as Site,
					"Subject" as Subject,
					"DataPageName" as DataPageName,
					"InstanceName" as InstanceName,
					"CDTYPE" as CDTYPE,
					"CDTYOTH" as CDTYOTH ,
					"CDINIDAT" as CDINIDAT,
					"CDITDTN" as CDITDTN,
					null::text as CDTUMLOC,
					null::text as CDSTAGE,
					null::text as CDSTOTH,
					"CDIND" as CDIND,
					null::text as CDMLOC,
					null::text as CDMLOTH,
					"CDMTDTN" as CDMTDTN ,
					null::text as CDMLOCD,
					"CDMETDAT" as CDMETDAT
			from 	tas120_202."CD"
			
			union
			
			select	distinct s."studyid" as Project,
					"SiteNumber" as SiteNumber,
					split_part("Site",'-',2) as Site,
					"Subject" as Subject,
					"DataPageName" as DataPageName,
					"InstanceName" as InstanceName,
					"CDTYPE" as CDTYPE ,
					null::text as CDTYOTH ,
					"CDINIDAT" as CDINIDAT,
					null::numeric as CDITDTN ,
					null::text as CDTUMLOC,
					null::text as CDSTAGE,
					null::text as CDSTOTH,
					null::text as CDIND,
					null::text as CDMLOC,
					null::text as CDMLOTH,
					null::numeric as CDMTDTN,
					null::text as CDMLOCD,
					"CDMETDAT" as CDMETDAT
			from 	tas3681_101."CD" c
			inner join cqs.subject s on left(s.studyid, 11) = c.project and s.siteid = c."SiteNumber" 
and s.usubjid = c."Subject" 


			union 
			--Study TAS120_203.CD Data set
			select	distinct 'TAS-120-203' as project,
					concat('TAS120_203_',split_part("SiteNumber",'_',2)) as SiteNumber,
					split_part("Site",'_',2) as Site,
					"Subject" as Subject,
					"DataPageName" as DataPageName,
					"InstanceName" as InstanceName,
					"CDTUMLOC" as CDTYPE,
					"CDSUBOTH" as CDTYOTH ,
					"CDINIDAT" as CDINIDAT,
					"CDITDTN" as CDITDTN,
					case when "CDTUMLOC" ='Bladder' then (case when "CDLOCSUB" ='Other' then "CDSUBOTH" 
					else "CDLOCSUB" end)
					else "CDTUMLOC" end ::text as CDTUMLOC,
					null::text as CDSTAGE,
					null::text as CDSTOTH,
					"CDIND" as CDIND,
					null::text as CDMLOC,
					null::text as CDMLOTH,
					"CDMTDTN" as CDMTDTN ,
					null::text as CDMLOCD,
					"CDMETDAT" as CDMETDAT
					from 	tas120_203."CD"
					
			union 
			--Study TAS120_204.CD Data set
			select	distinct 'TAS-120-204' as project,
					concat('TAS120_204_',split_part("SiteNumber",'_',2)) as SiteNumber,
					split_part("Site",'_',2) as Site,
					"Subject" as Subject,
					"DataPageName" as DataPageName,
					"InstanceName" as InstanceName,
					"CDTYPE" as CDTYPE,
					"CDTYOTH" as CDTYOTH ,
					"CDINIDAT" as CDINIDAT,
					null::numeric as CDITDTN,
					"CDTUMLOC"::text as CDTUMLOC,
					"CDSTAGE" ::text as CDSTAGE,
					null::text as CDSTOTH,
					"CDIND" as CDIND,
					null::text as CDMLOC,
					null::text as CDMLOTH,
					null::numeric as CDMTDTN ,
					null::text as CDMLOCD,
					"CDMETDAT" as CDMETDAT
					from 	tas120_204."CD"
			
				union
				--Study TAS117_201.CD Data set
				select	distinct 'TAS117-201' as project,
					concat('TAS117_201_',split_part("SiteNumber",'_',2)) as SiteNumber,
					split_part("Site",'_',2) as Site,
					"Subject" as Subject,
					"DataPageName" as DataPageName,
					"InstanceName" as InstanceName,
					"CDPTDG" as CDTYPE,
					"CDPTDGOT" as CDTYOTH ,
					"CDINIDTC" as CDINIDAT,
					null::numeric as CDITDTN,
					null::text as CDTUMLOC,
					null::text as CDSTAGE,
					null ::text as CDSTOTH,
					"CDTUMGRD" as CDIND,
					null::text as CDMLOC,
					null::text as CDMLOTH,
					null::numeric as CDMTDTN ,
					null::text as CDMLOCD,
					null::timestamp as CDMETDAT
					from 	TAS117_201."CD"
			
			union 
			--Study TAS2940_101.CD Data set
			select	distinct 'TAS2940-101' as project,
					concat('TAS2940_101_',split_part("SiteNumber",'_',2)) as SiteNumber,
					split_part("Site",'_',2) as Site,
					"Subject" as Subject,
					"DataPageName" as DataPageName,
					"InstanceName" as InstanceName,
					"CDTYPE" as CDTYPE,
					"CDTYOTH" as CDTYOTH ,
					"CDINIDAT" as CDINIDAT,
					null::numeric as CDITDTN,
					null::text as CDTUMLOC,
					"CDSTAGE"::text as CDSTAGE,
					null::text as CDSTOTH,
					"CDIND" as CDIND,
					null::text as CDMLOC,
					null::text as CDMLOTH,
					null::numeric as CDMTDTN ,
					null::text as CDMLOCD,
					null::timestamp as CDMETDAT
					from 	tas2940_101."CD"
) 

select	cd.project,
		cd.sitenumber,
		cd.site,
		cd.subject,
		cd.datapagename,
		cd.InstanceName,
		cd.CDTYPE ,
		cd.CDTYOTH,
		cd.CDINIDAT,
		cd.CDITDTN,
		cd.CDTUMLOC,
		cd.CDSTAGE,
		cd.CDSTOTH,
		cd.CDIND,
		cd.CDMLOC,
		cd.CDMLOTH,
		cd.CDMTDTN,
		cd.CDMLOCD,
		cd.CDMETDAT
from 	cd;


	
--ALTER TABLE "ctable_listing"."cTable_cd" OWNER TO "taiho-dev-app-clinical-master-write";

--ALTER TABLE "ctable_listing"."cTable_cd" OWNER TO "taiho-stage-app-clinical-master-write";

--ALTER TABLE "ctable_listing"."cTable_cd" OWNER TO "taiho-app-clinical-master-write";


