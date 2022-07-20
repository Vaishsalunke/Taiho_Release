/*Mapping script for Prior Systemic Drug Therapies - Listing
Table name : "ctable_listing"."cTable_psdt"
Project name : Taiho*/

CREATE SCHEMA IF NOT EXISTS "ctable_listing";

drop table if exists "ctable_listing"."cTable_psdt";

create table "ctable_listing"."cTable_psdt" as
with psdt as	(select distinct
"project" "Project",
"siteid",
"Site",
"Subject" "Subject",
"RecordId",
"InstanceName",	
null "CTGRPID",
"CTINTENT" "CTTYPE", 
"CTTERM" ,	
"CTSTDAT",
"CTENDAT",
"CTREASDC" ,	
"CTDCROTH" ,
null::date "CTRADDAT",	
null "CTRESP"
from
tas0612_101."CT"
union all
select distinct
"project" "Project",
"siteid",
"Site",
"Subject" "Subject",
"RecordId",
"InstanceName",	
"CTGRPID" ,
"CTINTENT" "CTTYPE",
"CTTERM" ,	
"CTSTDAT",
"CTENDAT",
"CTREASDC" ,	
"CTDCROTH" ,	
"CTREASDAT" "CTRADDAT",	
"CTBSTRSP" "CTRESP" 
from
tas120_201."CT"
union all
select distinct
"project" "Project",
"siteid",
"Site",
"Subject" "Subject",
"RecordId",
"InstanceName",	
"CTGRPID" ,
"CTINTENT" "CTTYPE",
"CTTERM" ,	
"CTSTDAT",
"CTENDAT",
"CTREASDC",	
"CTDCROTH" ,	
"CTRADDAT",	
"CTRESP" 
from
tas120_202."CT"
union all
select distinct
s."studyid" "Project",	
ct."siteid",
ct."Site",
ct."Subject",
ct."RecordId",
ct."InstanceName",	
ct."CTGRPID" ,
ct."CTINTENT" "CTTYPE",
ct."CTTERM" ,	
ct."CTSTDAT",
ct."CTENDAT",
ct."CTREASDC" ,	
ct."CTDCROTH",	
ct."CTIMGDAT" "CTRADDAT",	
null "CTRESP"
from
tas3681_101."CT" ct
inner join cqs.subject s on left(s.studyid, 11) = ct.project and s.siteid = ct."SiteNumber" 
and s.usubjid = ct."Subject" 
union all 
select distinct
"project" "Project",
"siteid",
"Site",
"Subject" "Subject",
"RecordId",
"InstanceName",	
"CTGRPID" ,
"CTINTENT" "CTTYPE",
"CTTERM" ,	
"CTSTDAT",
"CTENDAT",
"CTREASDC" ,	
"CTDCROTH" ,	
null::date "CTRADDAT",	
null "CTRESP" 
from
tas120_203."CT"
union all 
select distinct
"project" "Project",
"siteid",
"Site",
"Subject" "Subject",
"RecordId",
"InstanceName",	
"CTGRPID" ,
"CTINTENT" "CTTYPE",
"CTTERM" ,	
"CTSTDAT",
"CTENDAT",
"CTREASDC" ,	
"CTDCROTH" ,	
null::date "CTRADDAT",	
null "CTRESP" 
from
tas120_204."CT"
union all 
select distinct
"project" "Project",
"siteid",
"Site",
"Subject" "Subject",
"RecordId",
"InstanceName",	
"CTGRPID" ,
"CTINTENT" "CTTYPE",
"CTTERM" ,	
"CTSTDAT",
"CTENDAT",
"CTREASDC" ,	
"CTDCROTH" ,	
null::date "CTRADDAT",	
null "CTRESP" 
from
tas2940_101."CT"
union all 
select distinct
"project" "Project",
"siteid",
"Site",
"Subject" "Subject",
"RecordId",
"InstanceName",	
"PATREG" as "CTGRPID" ,
"PATYPE" as "CTTYPE",
"PADRUG" as "CTTERM" ,	
"PASTDTC" as "CTSTDAT",
"PAENDTC" as "CTENDAT",
"PADISC" as "CTREASDC",	
"PATHERSP" as "CTDCROTH" ,	
null::date "CTRADDAT",	
null "CTRESP" 
from tas117_201."PA" p 

) 

select distinct	psdt."Project",
		psdt."siteid",
		psdt."Site",
		psdt."Subject",
		psdt."RecordId",
		psdt."InstanceName" as FolderName,
		psdt."CTGRPID" ,
		psdt."CTTYPE",
		psdt."CTTERM",
		psdt."CTSTDAT",
		psdt."CTENDAT",
		psdt."CTREASDC",
		psdt."CTDCROTH"
		,psdt."CTRADDAT",
		psdt."CTRESP"
		,(psdt."Project"||'~'||psdt."siteid"||'~'||psdt."Site"||'~'||psdt."Subject"||'~'||psdt."InstanceName"||'~'||psdt."RecordId") as objectuniquekey
from 	psdt;

	
--ALTER TABLE "ctable_listing"."cTable_psdt" OWNER TO "taiho-dev-app-clinical-master-write";

--ALTER TABLE "ctable_listing"."cTable_psdt" OWNER TO "taiho-stage-app-clinical-master-write";	

--ALTER TABLE "ctable_listing"."cTable_psdt" OWNER TO "taiho-app-clinical-master-write";



