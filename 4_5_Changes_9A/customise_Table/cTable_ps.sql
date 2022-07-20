/*Mapping script for Prior Surgery - Listing
Table name : "ctable_listing"."cTable_ps"
Project name : Taiho*/

CREATE SCHEMA IF NOT EXISTS "ctable_listing";

drop table if exists "ctable_listing"."cTable_ps";

create table "ctable_listing"."cTable_ps" as
with ps as	(select distinct
"project" "Project",
"SiteNumber",
"Site",
"Subject" "Subject",
"RecordId",
"InstanceName" as FolderName,
null:: text as "SGSURG",
null:: text as  "SGOTH",
"SGRSTYPE",
"SGDAT"
from tas0612_101."SG"

union all
select distinct
"project" "Project",
"SiteNumber",
"Site",
"Subject" "Subject",
"RecordId",
"InstanceName" as FolderName,
"SGTERM" "SGSURG",
"SGTEROTH" "SGOTH",
"SGRSTYPE",
"SGDAT"
from 
tas120_201."SG"
union all
select distinct
"project" "Project",
"SiteNumber",
"Site",
"Subject" "Subject",
"RecordId",
"InstanceName" as FolderName,
"SGSURG",
"SGOTH",
"SGRSTYPE",
"SGDAT"
from 
tas120_202."SG"
union all
select distinct
s."studyid" "Project",
sg."SiteNumber",
sg."Site",
sg."Subject" "Subject",
sg."RecordId",
sg."InstanceName" as FolderName,
sg."SGTERM" "SGSURG",
sg."SGTEROTH" "SGOTH",
sg."SGRSTYPE",
sg."SGDAT"
from 
 tas3681_101."SG" sg
inner join cqs.subject s on left(s.studyid, 11) = sg.project and s.siteid = sg."SiteNumber" 
and s.usubjid = sg."Subject" 
union all 
select distinct
"project" "Project",
concat("project",substring("SiteNumber",position('_' in "SiteNumber"))) as "SiteNumber",
"Site",
"Subject" "Subject",
"RecordId",
"InstanceName" as FolderName,
"SGMETH" as "SGSURG",
null as "SGOTH",
"SGRSTYPE",
"SGDAT"
from 
tas120_203."SG"
union all 
select distinct
"project" "Project",
concat("project",substring("SiteNumber",position('_' in "SiteNumber"))) as "SiteNumber",
"Site",
"Subject" "Subject",
"RecordId",
"InstanceName" as FolderName,
"SGMETH" as "SGSURG",
"SGMETHO" as "SGOTH",
"SGRSTYPE",
"SGDAT"
from 
tas120_204."SG"
union all 
select distinct
"project" "Project",
concat("project",substring("SiteNumber",position('_' in "SiteNumber"))) as "SiteNumber",
"Site",
"Subject" "Subject",
"RecordId",
"InstanceName" as FolderName,
 "SGMETH" as "SGSURG",
"SGMETHO" as "SGOTH",
"SGRSTYPE",
"SGDAT"
from 
tas2940_101."SG"
union all 
select distinct
"project" "Project",
concat("project",substring("SiteNumber",position('_' in "SiteNumber"))) as "SiteNumber",
"Site",
"Subject" "Subject",
"RecordId",
"InstanceName" as FolderName,
"PSURG" as SGSURG ,
"PSPERF" as "SGOTH",
"PSRSLT",
"PSDAT"
from 
tas117_201."PS"


) 

select	ps."Project",
		ps."SiteNumber",
		ps."Site",
		ps."Subject",
		ps."RecordId",
		ps.FolderName,
		ps."SGSURG",
		ps."SGOTH",
		ps."SGRSTYPE",
		ps."SGDAT",
		(ps."Project"||'~'||ps."SiteNumber"||'~'||ps."Site"||'~'||ps."Subject"||'~'||ps.FolderName||'~'||ps."RecordId") as objectuniquekey
from 	ps;

	
--ALTER TABLE "ctable_listing"."cTable_ps" OWNER TO "taiho-dev-app-clinical-master-write";

--ALTER TABLE "ctable_listing"."cTable_ps" OWNER TO "taiho-stage-app-clinical-master-write";	

--ALTER TABLE "ctable_listing"."cTable_ps" OWNER TO "taiho-app-clinical-master-write";


