/*Mapping script for Prior Radiotherapy - Listing
Table name : "ctable_listing"."cTable_pr"
Project name : Taiho*/

CREATE SCHEMA IF NOT EXISTS "ctable_listing";

drop table if exists "ctable_listing"."cTable_pr";

create table "ctable_listing"."cTable_pr" as
with pr as	(select distinct
"project" "Project","SiteNumber",
"Site",
"Subject",
"RecordId",
"FolderName",
"RTINTENT" ,
"RTLOC" ,
"RTLOCOTH" ,
"RTSTDAT"::date ,
"RTENDAT"::date
from
tas0612_101."RT"
union all
select distinct
"project" "Project","SiteNumber",
"Site",
"Subject",
"RecordId",
"FolderName",
"RTINTENT" ,
"RTLOC" ,
"RTLOCOTH" ,
"RTSTDAT"::date ,
"RTENDAT"::date 
from
tas120_201."RT"
union all
select distinct
"project" "Project","SiteNumber",
"Site",
"Subject",
"RecordId",
"FolderName",
"RTINTENT" ,
"RTLOC" ,
"RTLOCOTH" ,
"RTSTDAT"::date ,
"RTENDAT"::date 
from
tas120_202."RT"
union all
select distinct
s."studyid" "Project",
"SiteNumber",
"Site",
"Subject",
"RecordId",
"FolderName",
"RTINTENT" ,
"RTLOC" ,
"RTLOCOTH" ,
"RTSTDAT"::date ,
"RTENDAT"::date 
from
tas3681_101."RT" rt
inner join cqs.subject s on left(s.studyid, 11) = rt.project and s.siteid = rt."SiteNumber" 
and s.usubjid = rt."Subject" 
UNION all 
select distinct
"project" "Project",
concat("project",substring("SiteNumber",position('_' in "SiteNumber"))) "SiteNumber",
"Site",
"Subject",
"RecordId",
"FolderName",
"RTINTENT" ,
"RTLOC" ,
"RTLOCOTH" ,
"RTSTDAT"::date ,
"RTENDAT"::date 
from
tas120_203."RT"
UNION all 
select distinct
"project" "Project",
concat("project",substring("SiteNumber",position('_' in "SiteNumber"))) "SiteNumber",
"Site",
"Subject",
"RecordId",
"FolderName",
"RTINTENT" ,
"RTLOC" ,
"RTLOCOTH" ,
"RTSTDAT"::date ,
"RTENDAT"::date 
from
tas120_204."RT"
union all 
select distinct
"project" "Project",
concat("project",substring("SiteNumber",position('_' in "SiteNumber"))) "SiteNumber",
"Site",
"Subject",
"RecordId",
"FolderName",
"RTINTENT" ,
"RTLOC" ,
"RTLOCOTH" ,
"RTSTDAT"::date ,
"RTENDAT"::date 
from
tas2940_101."RT"

union all
select distinct
"project" "Project",
concat("project",substring("SiteNumber",position('_' in "SiteNumber"))) "SiteNumber",
"Site",
"Subject",
"RecordId",
"FolderName",
"PRINTEN" ,
"PRLOC" ,
"PRDSITLO" ,
"PRSTDTC"::date ,
"PRENDTC"::date  
from tas117_201."PR" p
) 

select	pr."Project",
		pr."SiteNumber",
		pr."Site",
		pr."Subject",
		pr."RecordId",
		pr."FolderName",
		pr."RTINTENT" ,
		pr."RTLOC",
		pr."RTLOCOTH",
		pr."RTSTDAT",
		pr."RTENDAT",
		(pr."Project"||'~'||pr."SiteNumber"||'~'||pr."Site"||'~'||pr."Subject"||'~'||pr."FolderName"||'~'||pr."RecordId") as objectuniquekey

from 	pr;

	
--ALTER TABLE "ctable_listing"."cTable_pr" OWNER TO "taiho-dev-app-clinical-master-write";

--ALTER TABLE "ctable_listing"."cTable_pr" OWNER TO "taiho-stage-app-clinical-master-write";

--ALTER TABLE "ctable_listing"."cTable_pr" OWNER TO "taiho-app-clinical-master-write";





