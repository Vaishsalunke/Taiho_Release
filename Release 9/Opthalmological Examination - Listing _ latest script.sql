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
"FolderName" ,
"OEDAT"::date  ,
"OEABN" ,
"Examination",

case when "Examination" = 'External Ocular Examination' then  "OEL_DESC"
when "Examination" = 'Routine Slit Lamp Biomicroscopy' then "RSLBL_DESC"
when "Examination" =  'Direct/Indirect Fundoscopy' then "FUND_LEDESC"
end as "SLBL_DESC",
case when "Examination" = 'External Ocular Examination' then "OER_DESC"
when "Examination" = 'Routine Slit Lamp Biomicroscopy' then "RSLBR_DESC"
when "Examination" = 'Direct/Indirect Fundoscopy' then  "FUND_REDESC"
end as "RSLBR_DESC"
from(
select
"project",
"SiteNumber",
"Site",
"Subject",
"RecordId",
"OEPERF" ,
"FolderName" ,
"OEDAT"::date  ,
"OEABN",
"OEL_DESC",
"RSLBL_DESC",
"FUND_LEDESC",
"OER_DESC",
"RSLBR_DESC",
"FUND_REDESC",
case when  "OEL_DESC" <>'' then  'External Ocular Examination'
when "RSLBL_DESC" <>'' then  'Routine Slit Lamp Biomicroscopy'
when  "FUND_LEDESC" <>'' then 'Direct/Indirect Fundoscopy'
when  "OER_DESC"  <>''  then 'External Ocular Examination'
when "RSLBR_DESC"  <>'' then 'Routine Slit Lamp Biomicroscopy'
when  "FUND_REDESC" <>'' then 'Direct/Indirect Fundoscopy'
end as "Examination"
from
tas120_201."OPE") ope_201

union all

select
op."project" "Project",
op."SiteNumber",
op."Site",
op."Subject",
op."RecordId",
v."OPHTPERF" as "OEPERF",
op."FolderName" ,
"OPEDAT"::date "OEDAT" ,
null as "OEABN",
"Examination",
case when "Examination" = 'External Ocular Examination' then "OPELTABN"
when "Examination" = 'Routine Slit Lamp Biomicroscopy' then "OPELTABN1"
when "Examination" = 'Direct/Indirect Fundoscopy' then "OPEFUNLSPEC1"
end as "SLBL_DESC",

case when "Examination" = 'External Ocular Examination' then "OPERTABN"
when "Examination" = 'Routine Slit Lamp Biomicroscopy' then "OPERTABN1"
when "Examination" = 'Direct/Indirect Fundoscopy' then "OPEFUNSPEC"
end as "RSLBR_DESC"
from
(
select "project",
"SiteNumber",
"Site",
"Subject",
"RecordId",
"FolderName" ,
"InstanceName",
"OPEDAT" ,
"OPELTABN",
"OPELTABN1",
"OPEFUNLSPEC1",
"OPERTABN",
"OPERTABN1",
"OPEFUNSPEC",
case when "OPELTABN" <> '' then 'External Ocular Examination'
when "OPELTABN1" <> '' then 'Routine Slit Lamp Biomicroscopy'
when "OPEFUNLSPEC1" <> '' then 'Direct/Indirect Fundoscopy'
when "OPERTABN" <> '' then 'External Ocular Examination'
when "OPERTABN1" <> '' then 'Routine Slit Lamp Biomicroscopy'
when "OPEFUNSPEC" <>'' then 'Direct/Indirect Fundoscopy'
end as "Examination"
from tas120_202."OPE"
) op
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
"FolderName" ,
"OPEDAT"::date "OEDAT" ,
"OPEYN" as "OEABN",
"Examination",
case when "Examination" = 'External Ocular Examination' then "OPELEDS"
when  "Examination" = 'Routine Slit Lamp Biomicroscopy' then "OPELEABN"
when "Examination" = 'Direct/Indirect Fundoscopy' then "OPELEFSDES"
end as "SLBL_DESC",

case when "Examination" = 'External Ocular Examination' then "OPEREDS"
when "Examination" = 'Routine Slit Lamp Biomicroscopy' then "OPEREABN"
when "Examination" = 'Direct/Indirect Fundoscopy' then "OPEREFSDES"
end as "RSLBR_DESC"

from
(
select
"project",
"SiteNumber",
"Site",
"Subject",
"RecordId",
"OPEPYN" ,
"FolderName" ,
"OPEDAT",
"OPEYN",
"OPELEDS",
"OPELEABN",
"OPELEFSDES",
"OPEREDS",
"OPEREABN",
"OPEREFSDES",
case when "OPELEDS" <>'' then 'External Ocular Examination'
when "OPELEABN" <>'' then 'Routine Slit Lamp Biomicroscopy'
when "OPELEFSDES" <>'' then 'Direct/Indirect Fundoscopy'
when "OPEREDS" <>'' then 'External Ocular Examination'
when "OPEREABN"<>'' then 'Routine Slit Lamp Biomicroscopy'
when "OPEREFSDES" <>'' then 'Direct/Indirect Fundoscopy'
end as "Examination"
from tas120_203."OPE") o

union all

select
op."project" "Project",
concat(op."project",substring(op."SiteNumber",position('_' in op."SiteNumber"))) as "SiteNumber",
op."Site",
op."Subject",
op."RecordId",
v."OPEPERF" as  "OEPERF",
op."FolderName" ,
"OEDAT"::date "OEDAT" ,
"OEABN" as "OEABN",
"Examination",
case when "Examination" = 'External Ocular Examination' then "OEL_DESC"
when "Examination" = 'Routine Slit Lamp Biomicroscopy' then "RSLBL_DESC"
when "Examination" = 'Direct/Indirect Fundoscopy' then "FUND_LEDESC"
end as "SLBL_DESC",

case when "Examination" = 'External Ocular Examination' then "OER_DESC"
when  "Examination" = 'Routine Slit Lamp Biomicroscopy' then "RSLBR_DESC"
when  "Examination" = 'Direct/Indirect Fundoscopy' then "FUND_REDESC"
end as "RSLBR_DESC"
from
(
select "project",
"SiteNumber",
"Site",
"Subject",
"RecordId",
"FolderName" ,
"InstanceName",
"OEDAT"::date  ,
"OEABN",
"OEL_DESC",
"RSLBL_DESC",
"FUND_LEDESC",
"OER_DESC",
"RSLBR_DESC",
"FUND_REDESC",

case when "OEL_DESC" <>'' then 'External Ocular Examination'
when "RSLBL_DESC"<>'' then 'Routine Slit Lamp Biomicroscopy'
when "FUND_LEDESC"<>'' then 'Direct/Indirect Fundoscopy'
when "OER_DESC" <>'' then  'External Ocular Examination'
when "RSLBR_DESC" <>'' then 'Routine Slit Lamp Biomicroscopy'
when "FUND_REDESC" <>''then 'Direct/Indirect Fundoscopy'
end as "Examination"
from tas120_204."OPE"
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
op."FolderName" ,
"OEDAT"::date "OEDAT" ,
null as "OEABN",
null as "Examination",
null as "SLBL_DESC",
null as "RSLBR_DESC"
from tas2940_101."OPE" op 
left join 
tas2940_101."VISIT" v on
op."project" = v."project" and
op."SiteNumber" = v."SiteNumber" and
op."Subject" = v."Subject" and
op."InstanceName" = v."InstanceName"
) 

select	oe."Project",
		oe."SiteNumber",
		oe."Site",
		oe."Subject",
		oe."RecordId",
		oe."OEPERF",
		oe."FolderName",
		oe."OEDAT" ,
		oe."OEABN",
		oe."Examination",
		oe."SLBL_DESC",
		oe."RSLBR_DESC",
		(oe."Project"||'~'||oe."SiteNumber"||'~'||oe."Site"||'~'||oe."Subject"||'~'||oe."FolderName"||'~'||oe."RecordId") as objectuniquekey
from 	oe_data oe;



	
--ALTER TABLE "ctable_listing"."cTable_oe" OWNER TO "taiho-dev-app-clinical-master-write";

--ALTER TABLE "ctable_listing"."cTable_oe" OWNER TO "taiho-stage-app-clinical-master-write";

--ALTER TABLE "ctable_listing"."cTable_oe" OWNER TO "taiho-app-clinical-master-write";	


