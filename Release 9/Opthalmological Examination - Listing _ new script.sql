/*Mapping script for Ophthalmological Examination - Listing
Table name : "ctable_listing"."cTable_oe"
Project name : Taiho*/


CREATE SCHEMA IF NOT EXISTS "ctable_listing";

drop table if exists "ctable_listing"."cTable_oe";

create table "ctable_listing"."cTable_oe" as

with oe as	(

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
trim(both ',' from trim(concat(OEL_DESC_1,RSLBL_DESC_1, FUND_LEDESC_1))) as "SLBL_DESC", --"AFLE",
trim(both ',' from trim(concat(OER_DESC_1,RSLBR_DESC_1, FUND_REDESC_1))) as "RSLBR_DESC"	 --"AFRE"
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
case when "OER_DESC"<>'' and trim(regexp_replace("OER_DESC", E'[\\n\\r]+', ' ', 'g' ))<>'' then 'External Ocular Examination -'|| "OER_DESC" || ', ' end as OER_DESC_1 ,
case when "RSLBR_DESC" <>'' and trim(regexp_replace("RSLBR_DESC", E'[\\n\\r]+', ' ', 'g' ))<>'' then 'Routine Slit Lamp bio -'||"RSLBR_DESC" ||', '  end as RSLBR_DESC_1,
case when "FUND_REDESC"<>'' and trim(regexp_replace("FUND_REDESC", E'[\\n\\r]+', ' ', 'g' ))<>'' then 'Direct/Indirect Fundoscopy -' ||"FUND_REDESC" end as FUND_REDESC_1,
case when "OEL_DESC"<>'' and trim(regexp_replace("OEL_DESC", E'[\\n\\r]+', ' ', 'g' ))<>'' then 'External Ocular Examination -'|| "OEL_DESC" || ', ' end as OEL_DESC_1 ,
case when "RSLBL_DESC" <>'' and trim(regexp_replace("RSLBL_DESC", E'[\\n\\r]+', ' ', 'g' ))<>'' then 'Routine Slit Lamp bio -'||"RSLBL_DESC" ||', '  end as RSLBL_DESC_1,
case when "FUND_LEDESC"<>'' and trim(regexp_replace("FUND_LEDESC", E'[\\n\\r]+', ' ', 'g' ))<>'' then 'Direct/Indirect Fundoscopy -' ||"FUND_LEDESC" end as FUND_LEDESC_1
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
trim(both ',' from trim(concat(OPELTABN_1,OPELTABN1_1, OPEFUNLSPEC1_1))) as "SLBL_DESC", --"AFLE",
trim(both ',' from trim(concat(OPERTABN_1,OPERTABN1_1, OPEFUNSPEC_1))) as 	"RSLBR_DESC"	 --"AFRE"
--coalesce("OPELTABN", "OPELTABN1") as "RSLBR_DESC",
--coalesce("OPERTABN", "OPERTABN1") as "SLBL_DESC"
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
case when "OPELTABN" <>'' and trim(regexp_replace("OPELTABN", E'[\\n\\r]+', ' ', 'g' ))<>'' 
			then 'External Ocular Examination -'|| "OPELTABN" || ', ' end as OPELTABN_1 ,
case when "OPELTABN1" <>'' and trim(regexp_replace("OPELTABN1", E'[\\n\\r]+', ' ', 'g' ))<>'' 
			then 'Routine Slit Lamp bio -'||"OPELTABN1" ||', '  end as OPELTABN1_1,
case when "OPEFUNLSPEC1"<>'' and trim(regexp_replace("OPEFUNLSPEC1", E'[\\n\\r]+', ' ', 'g' ))<>'' 
			then 'Direct/Indirect Fundoscopy -' ||"OPEFUNLSPEC1" end as OPEFUNLSPEC1_1,
case when "OPERTABN"<>'' and trim(regexp_replace("OPERTABN", E'[\\n\\r]+', ' ', 'g' ))<>'' 
			then 'External Ocular Examination -'|| "OPERTABN" || ', ' end as OPERTABN_1 ,
case when "OPERTABN1" <>'' and trim(regexp_replace("OPERTABN1", E'[\\n\\r]+', ' ', 'g' ))<>'' 
			then 'Routine Slit Lamp bio -'||"OPERTABN1" ||', '  end as OPERTABN1_1,
case when "OPEFUNSPEC"<>'' and trim(regexp_replace("OPEFUNSPEC", E'[\\n\\r]+', ' ', 'g' ))<>'' 
			then 'Direct/Indirect Fundoscopy -' ||"OPEFUNSPEC" end as OPEFUNSPEC_1
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
trim(both ',' from trim(concat(OPELEDS_1,OPELEABN_1, OPELEFSDES_1))) as "SLBL_DESC", --"AFLE",
trim(both ',' from trim(concat(OPEREDS_1,OPEREABN_1, OPEREFSDES_1))) as "RSLBR_DESC"	 --"AFRE"
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
case when "OPEREDS"<>'' and trim(regexp_replace("OPEREDS", E'[\\n\\r]+', ' ', 'g' ))<>'' 
			then 'External Ocular Examination -'|| "OPEREDS" || ', ' end as OPEREDS_1 ,
case when "OPEREABN" <>'' and trim(regexp_replace("OPEREABN", E'[\\n\\r]+', ' ', 'g' ))<>'' 
			then 'Routine Slit Lamp bio -'||"OPEREABN" ||', '  end as OPEREABN_1,
case when "OPEREFSDES"<>'' and trim(regexp_replace("OPEREFSDES", E'[\\n\\r]+', ' ', 'g' ))<>'' 
			then 'Direct/Indirect Fundoscopy -' ||"OPEREFSDES" end as OPEREFSDES_1,
case when "OPELEDS"<>'' and trim(regexp_replace("OPELEDS", E'[\\n\\r]+', ' ', 'g' ))<>'' 
			then 'External Ocular Examination -'|| "OPELEDS" || ', ' end as OPELEDS_1 ,
case when "OPELEABN" <>'' and trim(regexp_replace("OPELEABN", E'[\\n\\r]+', ' ', 'g' ))<>'' 
			then 'Routine Slit Lamp bio -'||"OPELEABN" ||', '  end as OPELEABN_1,
case when "OPELEFSDES"<>'' and trim(regexp_replace("OPELEFSDES", E'[\\n\\r]+', ' ', 'g' ))<>'' 
			then 'Direct/Indirect Fundoscopy -' ||"OPELEFSDES" end as OPELEFSDES_1
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
trim(both ',' from trim(concat(OEL_DESC_1,RSLBL_DESC_1, FUND_LEDESC_1))) as "SLBL_DESC", --"AFLE",
trim(both ',' from trim(concat(OER_DESC_1,RSLBR_DESC_1, FUND_REDESC_1))) as "RSLBR_DESC"	 --"AFRE"
--case when "OEABN" = 'Yes' then "OEL_DESC" when "OEABN" = 'No' then "RSLBL_DESC" else "OEL_DESC" end as  "RSLBR_DESC",
--case when "OEABN" = 'Yes' then "OER_DESC" when "OEABN" = 'No' then "RSLBR_DESC" else "OER_DESC" end as "SLBL_DESC" 
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
case when "OER_DESC"<>'' and trim(regexp_replace("OER_DESC", E'[\\n\\r]+', ' ', 'g' ))<>'' 
			then 'External Ocular Examination -'|| "OER_DESC" || ', ' end as OER_DESC_1 ,
case when "RSLBR_DESC" <>'' and trim(regexp_replace("RSLBR_DESC", E'[\\n\\r]+', ' ', 'g' ))<>'' 
			then 'Routine Slit Lamp bio -'||"RSLBR_DESC" ||', '  end as RSLBR_DESC_1,
case when "FUND_REDESC"<>'' and trim(regexp_replace("FUND_REDESC", E'[\\n\\r]+', ' ', 'g' ))<>'' 
			then 'Direct/Indirect Fundoscopy -' ||"FUND_REDESC" end as FUND_REDESC_1,
case when "OEL_DESC"<>'' and trim(regexp_replace("OEL_DESC", E'[\\n\\r]+', ' ', 'g' ))<>'' 
			then 'External Ocular Examination -'|| "OEL_DESC" || ', ' end as OEL_DESC_1 ,
case when "RSLBL_DESC" <>'' and trim(regexp_replace("RSLBL_DESC", E'[\\n\\r]+', ' ', 'g' ))<>'' 
			then 'Routine Slit Lamp bio -'||"RSLBL_DESC" ||', '  end as RSLBL_DESC_1,
case when "FUND_LEDESC"<>'' and trim(regexp_replace("FUND_LEDESC", E'[\\n\\r]+', ' ', 'g' ))<>'' 
			then 'Direct/Indirect Fundoscopy -' ||"FUND_LEDESC" end as FUND_LEDESC_1
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
null as "RSLBR_DESC",
null as "SLBL_DESC" 
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
		oe."RSLBR_DESC",
		oe."SLBL_DESC",
		(oe."Project"||'~'||oe."SiteNumber"||'~'||oe."Site"||'~'||oe."Subject"||'~'||oe."FolderName"||'~'||oe."RecordId") as objectuniquekey
from 	oe;

	
--ALTER TABLE "ctable_listing"."cTable_oe" OWNER TO "taiho-dev-app-clinical-master-write";

--ALTER TABLE "ctable_listing"."cTable_oe" OWNER TO "taiho-stage-app-clinical-master-write";

--ALTER TABLE "ctable_listing"."cTable_oe" OWNER TO "taiho-app-clinical-master-write";	


