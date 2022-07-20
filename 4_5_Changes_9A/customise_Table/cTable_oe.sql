/*Mapping script for Ophthalmological Examination - Listing
Table name : "ctable_listing"."cTable_oe"
Project name : Taiho*/


CREATE SCHEMA IF NOT EXISTS "ctable_listing";

drop table if exists "ctable_listing"."cTable_oe";

create table "ctable_listing"."cTable_oe" as
with oe_data as (
select
"project" "Project",
"SiteNumber",
"Site",
"Subject" "Subject",
"RecordId",
"OEPERF" ,
"InstanceName" ,
"OEDAT"::date  ,
"OEABN" ,
"Examination",
"SLBL_DESC",
"RSLBR_DESC"




from(
select
"project",
"SiteNumber",
"Site",
"Subject",
"RecordId",
"OEPERF" ,
"InstanceName" ,
"OEDAT"::date  ,
"OEABN",
"OEL_DESC",
"RSLBL_DESC",
"FUND_LEDESC",
"OER_DESC",
"FUND_REDESC",
SLBL_DESC_1 :: text as "SLBL_DESC",
RSLBR_DESC_1:: text as "RSLBR_DESC",
Examination :: text as "Examination"
from tas120_201."OPE"
cross  join lateral (values  
							('External Ocular Examination', "OEL_DESC","OER_DESC"),
					 		 ('Routine Slit Lamp Biomicroscopy',"RSLBL_DESC","RSLBR_DESC"),
					 		 ('Direct/Indirect Fundoscopy',"FUND_LEDESC","FUND_REDESC")
					 )t (Examination,SLBL_DESC_1,RSLBR_DESC_1)) ope_201

union all

select 	
op."project" "Project",
op."SiteNumber",
op."Site",
op."Subject",
op."RecordId",
v."OPHTPERF" as "OEPERF",
op."InstanceName" ,
"OPEDAT"::date "OEDAT" ,
null as "OEABN",
"Examination",
"SLBL_DESC",
"RSLBR_DESC"



from
(
select  distinct "project",
"SiteNumber",
"Site",
"Subject",
"RecordId",
"InstanceName" ,
"OPEDAT" ,
"OPELTABN",
"OPELTABN1",
"OPEFUNLSPEC1",
"OPERTABN",
"OPERTABN1",
"OPEFUNSPEC",
SLBL_DESC_1 :: text as "SLBL_DESC",
RSLBR_DESC_1:: text as "RSLBR_DESC",
Examination :: text as "Examination"
from tas120_202."OPE"
cross  join lateral (values  
							('External Ocular Examination', "OPELTABN","OPERTABN"),
					 		 ('Routine Slit Lamp Biomicroscopy',"OPELTABN1","OPERTABN1"),
					 		 ('Direct/Indirect Fundoscopy',"OPEFUNLSPEC1","OPEFUNSPEC")
					 )t (Examination,SLBL_DESC_1,RSLBR_DESC_1)) op
left join
tas120_202."VISIT" v on
op."project" = v."project" and
op."SiteNumber" = v."SiteNumber" and
op."Subject" = v."Subject" and
op."InstanceName" = v."InstanceName"

union all

select
"project" "Project",
concat("project",substring("SiteNumber",position('_' in "SiteNumber"))) as "SiteNumber",
"Site",
"Subject",
"RecordId",
"OPEPYN" as "OEPERF",
"InstanceName" ,
"OPEDAT"::date "OEDAT" ,
"OPEYN" as "OEABN",
"Examination",
"SLBL_DESC",
"RSLBR_DESC"


from
(
select distinct
"project",
"SiteNumber",
"Site",
"Subject",
"RecordId",
"OPEPYN" ,
"InstanceName" ,
"OPEDAT",
"OPEYN",
"OPELEDS",
"OPELEABN",
"OPELEFSDES",
"OPEREDS",
"OPEREABN",
"OPEREFSDES",
SLBL_DESC_1 :: text as "SLBL_DESC",
RSLBR_DESC_1:: text as "RSLBR_DESC",
Examination :: text as "Examination"
from tas120_203."OPE"
cross  join lateral (values  
							('External Ocular Examination',"OPELEDS","OPEREDS"),
					 		 ('Routine Slit Lamp Biomicroscopy',"OPELEABN","OPEREABN"),
					 		 ('Direct/Indirect Fundoscopy',"OPELEFSDES","OPEREFSDES")
					 )t (Examination,SLBL_DESC_1,RSLBR_DESC_1)
)ope_120_203

union all

select
op."project" "Project",
concat(op."project",substring(op."SiteNumber",position('_' in op."SiteNumber"))) as "SiteNumber",
op."Site",
op."Subject",
op."RecordId",
v."OPEPERF" as  "OEPERF",
op."InstanceName" ,
"OEDAT"::date "OEDAT" ,
"OEABN" as "OEABN",
"Examination",
"SLBL_DESC",
"RSLBR_DESC"


from
(
select DISTINCT
"project",
"SiteNumber",
"Site",
"Subject",
"RecordId",
"InstanceName" ,
"OEDAT"::date  ,
"OEABN",
"OEL_DESC",
"RSLBL_DESC",
"FUND_LEDESC",
"OER_DESC",
--"RSLBR_DESC",
"FUND_REDESC",
SLBL_DESC_1 :: text as "SLBL_DESC",
RSLBR_DESC_1:: text as "RSLBR_DESC",
Examination :: text as "Examination"
from tas120_204."OPE"
cross  join lateral (values  
							('External Ocular Examination',"OEL_DESC","OER_DESC"),
					 		 ('Routine Slit Lamp Biomicroscopy',"RSLBL_DESC","RSLBR_DESC"),
					 		 ('Direct/Indirect Fundoscopy',"FUND_LEDESC","FUND_REDESC")
					 )t (Examination,SLBL_DESC_1,RSLBR_DESC_1)
) op
left join
tas120_204."VISIT" v on
op."project" = v."project" and
op."SiteNumber" = v."SiteNumber" and
op."Subject" = v."Subject" and
op."InstanceName" = v."InstanceName"


union all

select
op."project" "Project",
concat(op."project",substring(op."SiteNumber",position('_' in op."SiteNumber"))) as "SiteNumber",
op."Site",
op."Subject",
op."RecordId",
v."OPEPERF" as "OEPERF",
op."InstanceName" ,
"OEDAT"::date "OEDAT",
null as "OEABN",
op."OETEST" as "Examination",
op."OECOMML" as "SLBL_DESC",
op."OECOMMR" as "RSLBR_DESC"

from tas2940_101."OPE" op
left join
tas2940_101."VISIT" v on
op."project" = v."project" and
op."SiteNumber" = v."SiteNumber" and
op."Subject" = v."Subject" and
op."InstanceName" = v."InstanceName"
)

select oe."Project",
oe."SiteNumber",
oe."Site",
oe."Subject",
oe."RecordId",
oe."OEPERF",
oe."InstanceName",
oe."OEDAT" ,
oe."OEABN",
oe."SLBL_DESC",
oe."RSLBR_DESC",
oe."Examination",
(oe."Project"||'~'||oe."SiteNumber"||'~'||oe."Site"||'~'||oe."Subject"||'~'||oe."InstanceName"||'~'||oe."RecordId") as objectuniquekey
from oe_data oe;







--ALTER TABLE "ctable_listing"."cTable_oe" OWNER TO "taiho-dev-app-clinical-master-write";

--ALTER TABLE "ctable_listing"."cTable_oe" OWNER TO "taiho-stage-app-clinical-master-write";

--ALTER TABLE "ctable_listing"."cTable_oe" OWNER TO "taiho-app-clinical-master-write";


